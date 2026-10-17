# Code structure and how to run it

Companion to `DECISIONS.md` (the why) and `TAKE_HOME.md` (the brief). This
file is the what and the how: where each piece lives, how data flows through
it, and the exact commands to run and verify it.

## Layout

```
.
├── Makefile                 the grader's two targets: seed, worker
├── pyproject.toml           deps: psycopg[binary] 3, httpx. Python 3.12 (.python-version)
├── uv.lock
├── dispatcher/              the submission
│   ├── config.py            every setting, read from the environment
│   ├── db.py                schema + one function per SQL statement (the guarded transitions)
│   ├── provider.py          HTTP client: turns every wire outcome into one of five observations
│   ├── reconcile.py         the /sent scan that resolves "did it send or not"
│   ├── worker.py            the state machine: claim, process, heartbeat, signals
│   └── seed.py              `make seed`: create schema, load seed/actions.json
├── scripts/grader_run.sh    local reproduction of the grading run
├── DECISIONS.md             the one-pager the brief asks for
├── notes/                   working record: provider facts, design decisions, run log, schema draft
└── (kit)                    README.md, TAKE_HOME.md, docker-compose.yml, provider/, seed/, verify.py
```

## Module responsibilities

| Module | Knows about | Does not know about |
|---|---|---|
| `config.py` | environment variables, defaults, the numbers justified in DECISIONS.md | anything else |
| `db.py` | Postgres, the three tables, every transition's WHERE clause | HTTP, the provider |
| `provider.py` | HTTP, timeouts, how to classify a response or a socket error | the database, retries, state |
| `reconcile.py` | paging `/sent`, the receipts table, the scan lease | sending, leases on actions |
| `worker.py` | the order things happen in, the heartbeat thread, signals | SQL text, HTTP details |
| `seed.py` | the seed file format | everything else |

Read `worker.py` first; it calls into the others in the order the state
machine needs them.

## Data: three tables

| Table | Rows | Purpose |
|---|---|---|
| `actions` | 500, one per seeded action | the state machine. `status`, lease (`leased_by`, `leased_until`), `send_issued_at`, `attempts`, `receipt_id`, `failure_reason` |
| `receipts` | one per receipt ever read from `/sent` | the provider's truth table, persisted so a shared cursor loses nothing |
| `provider_state` | exactly one | shared `pause_until` (rate limit), `sent_cursor`, and the scan lease |

`db.py` creates all three with `CREATE TABLE IF NOT EXISTS`; `make seed` runs it.

## Flow of one action

```
run()        claim_one()  ──►  process(action)
                                 │
                                 ├─ send_issued_at set?  ──►  reconcile.resolve()  ──►  found: SENT / not found: attempts+1
                                 │
                                 ├─ attempts ≥ 8?  ──►  FAILED
                                 │
                                 ├─ wait_for_provider()   (pacing sleep, then shared pause check)
                                 ├─ mark_send_issued()    (commits BEFORE the request)
                                 └─ provider.send()
                                        200  ──►  SENT
                                        400  ──►  FAILED
                                        429  ──►  set shared pause, clear flag, loop
                                        500  ──►  clear flag, attempts+1, loop
                                        timeout / reset  ──►  keep flag, scan on next loop
heartbeat thread: every 3 s, extend leased_until WHERE leased_by = me
```

## Run it

Prerequisites: Docker with Compose, `uv` (https://docs.astral.sh/uv/). uv
fetches Python 3.12 if the machine lacks it.

```bash
docker compose up -d --build --wait          # Postgres 16 + the mock provider
uv sync                                      # creates .venv with the two deps
make seed                                    # schema + 500 actions (idempotent)
make worker                                  # one foreground worker; run three in three terminals
```

Both targets read `PROVIDER_URL` and `DATABASE_URL` from the environment and
default to the `127.0.0.1` values in `README.md`.

If host port 5432 is already taken (a local Postgres, for example), publish the
container elsewhere and point the workers at it:

```bash
echo POSTGRES_HOST_PORT=55432 > .env
docker compose up -d --wait
export DATABASE_URL=postgresql://dispatch:dispatch@127.0.0.1:55432/dispatch
```

## Verify it

### The full grading run, reproduced

```bash
./scripts/grader_run.sh            # add POSTGRES_HOST_PORT=55432 in front if you remapped
```

The script restarts the provider (empty `/sent`), drops and reseeds the
tables, starts three workers each in its own process group, sends `kill -9`
to one group at 60 s, SIGTERM to the rest at 120 s (or as soon as they exit on
their own), then runs `verify.py` and prints our table's status counts. Worker
logs land in `logs/worker-{1,2,3}.log`.

What a clean run looks like:

```
present in /sent          500
duplicated action_ids     0
wrong recipient           0
violations (worst window) ≤ 2
violations (>100 ms late) 0
Nothing out of bounds.
[{'status': 'SENT', 'count': 500}]
```

To see the kill being recovered: take the last `claimed <id>` line in
`logs/worker-1.log` and grep that id across all three logs. You will see
another worker claim it about 15 s later and mark it SENT.

### By hand

```bash
docker compose restart provider              # /sent must hold only one run
make seed
make worker &  make worker &  make worker &  # or three terminals
# wait for "all actions terminal, exiting" from each
uv run python verify.py
```

### Inspect the database

```bash
docker compose exec postgres psql -U dispatch -d dispatch -c \
  "SELECT status, count(*) FROM actions GROUP BY status"
docker compose exec postgres psql -U dispatch -d dispatch -c \
  "SELECT id, attempts, failure_reason FROM actions WHERE status <> 'SENT'"
docker compose exec postgres psql -U dispatch -d dispatch -c \
  "SELECT pause_until, sent_cursor, scan_leased_by FROM provider_state"
```

## Knobs

All optional, read by `config.py`:

| Variable | Default | Meaning |
|---|---|---|
| `LEASE_SECONDS` | 15 | how long a claim lasts without a heartbeat |
| `HEARTBEAT_SECONDS` | 3 | how often a live worker extends its lease |
| `CLIENT_TIMEOUT` | 2 | connect + read timeout on every provider call |
| `MAX_ATTEMPTS` | 8 | 500s and not-found-in-/sent retries before FAILED (429 excluded) |
| `MIN_SEND_INTERVAL` | 0.25 | seconds between provider calls, per worker |
| `WORKER_ID` | host:pid | name in logs and in `leased_by` |

The provider's own knobs (`PROVIDER_SEED`, fault rates, rate limit) are in
`README.md`; set them before `docker compose up` to try a different fault
sequence. The grader uses the defaults.

## Logs

One line per event, `HH:MM:SS.mmm [worker_id] message`, UTC, flushed on every
line so a `kill -9` loses nothing. Grep targets: `claimed`, `SENT`, `via /sent
scan`, `429 pause`, `unknown outcome`, `lease lost`, `FAILED`, `inherited`.
