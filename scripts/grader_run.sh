#!/usr/bin/env bash
# Our copy of the grader's run, from README.md "How we run your submission".
#   fresh provider, make seed, 3x make worker in their own process groups,
#   kill -9 one group at 60 s, SIGTERM the rest at 120 s (+5 s SIGKILL),
#   then verify.py.
set -euo pipefail
cd "$(dirname "$0")/.."

export PROVIDER_URL="${PROVIDER_URL:-http://127.0.0.1:${PROVIDER_HOST_PORT:-8080}}"
export DATABASE_URL="${DATABASE_URL:-postgresql://dispatch:dispatch@127.0.0.1:${POSTGRES_HOST_PORT:-5432}/dispatch}"
KILL_AT="${KILL_AT:-60}"
END_AT="${END_AT:-120}"
LOGS=logs; mkdir -p "$LOGS"; rm -f "$LOGS"/worker-*.log

echo "== fresh provider + empty tables"
docker compose restart provider >/dev/null
until curl -sf "$PROVIDER_URL/healthz" >/dev/null; do sleep 0.5; done
uv run python -c "
from dispatcher import db; from dispatcher.config import Config
c = db.connect(Config.from_env().database_url)
c.execute('DROP TABLE IF EXISTS actions, receipts, provider_state')"
make -s seed

# Start each worker in its own process group (setsid is not on macOS). The
# launcher calls setsid then execs make, so $! is the group leader's pid.
PIDS=()
for i in 1 2 3; do
  WORKER_ID="w$i" python3 -c 'import os,sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' \
    make -s worker >"$LOGS/worker-$i.log" 2>&1 &
  PIDS+=($!)
done
sleep 1; echo "== process groups:"; ps -o pid,pgid,command -p "${PIDS[*]// /,}" || true
START=$(date +%s)
echo "== started workers pgids=${PIDS[*]} at t=0"

sleep "$KILL_AT"
echo "== t=$(( $(date +%s) - START )) kill -9 process group of worker 1 (pgid ${PIDS[0]})"
kill -9 -- "-${PIDS[0]}" 2>/dev/null || true

# Wait until END_AT, or until the survivors have exited on their own.
while (( $(date +%s) - START < END_AT )); do
  alive=0
  for p in "${PIDS[@]:1}"; do kill -0 "$p" 2>/dev/null && alive=1; done
  (( alive == 0 )) && { echo "== t=$(( $(date +%s) - START )) all survivors exited"; break; }
  sleep 1
done
echo "== t=$(( $(date +%s) - START )) SIGTERM survivors"
for p in "${PIDS[@]:1}"; do kill -TERM -- "-$p" 2>/dev/null || true; done
sleep 5
for p in "${PIDS[@]:1}"; do kill -KILL -- "-$p" 2>/dev/null || true; done
for p in "${PIDS[@]}"; do while kill -0 "$p" 2>/dev/null; do sleep 0.2; done; done

echo; echo "== verify.py"
uv run python verify.py --provider "$PROVIDER_URL"
echo; echo "== our table"
uv run python -c "
from dispatcher import db; from dispatcher.config import Config
c = db.connect(Config.from_env().database_url)
print(c.execute('SELECT status, count(*) FROM actions GROUP BY status ORDER BY status').fetchall())
print(c.execute('SELECT count(*) AS receipts_seen, (SELECT sent_cursor FROM provider_state) AS cursor FROM receipts').fetchone())
print('failed:', c.execute(\"SELECT id, failure_reason FROM actions WHERE status='FAILED'\").fetchall())"
