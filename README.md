# Take-home kit

The brief is in `TAKE_HOME.md`. This file covers how to run what we give you.

```
provider/            mock provider (Python stdlib, no dependencies)
docker-compose.yml   PostgreSQL 16 + the provider
seed/actions.json    the 500 actions
verify.py            reports what reached the provider (Python stdlib)
```

Part 2 arrives separately, after you hand in part 1, as a `review/`
directory with its own README.

## Running it

```
docker compose up -d --build --wait
```

| Service | From your machine | From another container on the compose network |
|---|---|---|
| PostgreSQL | `postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch` | `postgresql://dispatch:dispatch@postgres:5432/dispatch` |
| Provider | `http://127.0.0.1:8080` | `http://provider:8080` |

If those host ports are taken, set `POSTGRES_HOST_PORT` or `PROVIDER_HOST_PORT`
before `docker compose up`.

Use `127.0.0.1` rather than `localhost`. On some Docker Desktop setups,
connections to `::1` on published ports hang instead of being refused, and
clients that try IPv6 first then wait seconds on every request.

The provider keeps everything in memory. To start over from an empty `/sent`,
restart it: `docker compose restart provider`.

## How we run your submission

This is the contract the check relies on:

1. Fresh `docker compose up` using this `docker-compose.yml` with the default
   fault profile.
2. `make seed` once. Your Makefile, your schema.
3. `make worker` three times, as three separate processes, each with these
   variables set:
   - `PROVIDER_URL`, e.g. `http://provider:8080`
   - `DATABASE_URL`, e.g. `postgresql://dispatch:dispatch@postgres:5432/dispatch`

   Read both from the environment. You may default to the `127.0.0.1` values
   above when they are unset.
4. At 60 s we `kill -9` one worker's whole process group (`make` and
   everything it started). At 120 s, or as soon as all workers have exited,
   the remaining workers get SIGTERM and then SIGKILL 5 s later.
5. `python verify.py` against the provider.

`make worker` must stay in the foreground until the worker exits. It must not
fork into the background, start a container, or call `setsid`. A worker may
exit on its own once it has nothing left to do.

The run happens on Linux. On Windows, use WSL for `make` and process signals.

Dependencies: we run `pip install -r requirements.txt` if you have one, or
`uv sync` if you have a `pyproject.toml`. With uv, have your Makefile call
`uv run ...`.

## The provider

```
POST /send       {action_id, recipient, body}  ->  200 {receipt_id}
GET  /sent?since=<cursor>&limit=<n>            ->  200 {receipts: [...], next}
GET  /stats                                    ->  200 {...}
GET  /healthz                                  ->  200 (for the compose healthcheck; no faults, not rate limited)
```

- `/sent`: start with `since=0`. Pass `next` back as `since`. An empty
  `receipts` list means you're at the end for now. `limit` must be
  between 1 and 100 and defaults to 100.
- The rate limit and its `Retry-After` windows are per provider, not per
  connection, and they apply to `/send` and `/sent` alike.
- `/stats` is for you and for `verify.py`. It is not rate limited and never
  faults. Besides counters, it reports `violations` (total),
  `max_violations_per_window`, and `windows`: the list of `Retry-After`
  windows the provider issued. Each window has its reason, its violation count,
  and `violation_offsets_ms`, how long after the window opened each violating
  request arrived. `retry_after_remaining` is non-zero while a window is open.

### Knobs

All of these are environment variables on the `provider` service. Set them
in the shell before `docker compose up`, or in an `.env` file next to the
compose file.

| Variable | Default | |
|---|---|---|
| `PROVIDER_SEED` | `1` | seeds the fault sequence |
| `PROVIDER_FAULTS` | `on` | `off` turns every fault off (rate limiting stays on) |
| `FAULT_RATE_429`, `_500`, `_HANG`, `_DROP` | see compose | probability per request |
| `FAULTS_ON_SENT` | `on` | whether `GET /sent` gets 429/500 faults too |
| `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST` | see compose | token bucket |
| `PROVIDER_LOG` | `off` | `on` logs every request and the fault drawn for it |

The same seed draws the same sequence of faults **in request-arrival order**.
With several workers, the order requests arrive in varies from run to run,
and so does which action gets which fault. Don't expect a run to replay
exactly.

We check with the defaults. Change them as much as you like while you
develop.

## verify.py

```
python verify.py                 # PROVIDER_URL defaults to http://127.0.0.1:8080
python verify.py --json
```

It prints what reached the provider and a list of warnings for anything out
of bounds: duplicate sends, missing actions, a wrong recipient, more than 3
requests inside one `Retry-After` window, or more than 3 requests over the
run that arrived over 100 ms into a window. (A request already on the wire
when the window opens lands within milliseconds. One that arrives later came
from a worker that could have known.)

Warnings are not a pass/fail verdict. We read them alongside your
`DECISIONS.md`. If your run produces one, explain it there.

Exit codes: 0 means the report was produced, whatever it says; 2 means the
provider was unreachable. It only reads the provider. Run it once the workers
have stopped, and against a provider that has seen only one run. Restart the
provider between runs.

## The submission (dispatcher)

- `dispatcher/`: `config.py` (env), `db.py` (schema + every guarded transition), `provider.py` (HTTP → five observations), `reconcile.py` (the `/sent` scan), `worker.py` (the state machine), `seed.py`.
- `make seed` creates the schema and loads the 500 actions (idempotent). `make worker` runs one foreground worker via `uv run`.
- `scripts/grader_run.sh` reproduces the grading run locally. If host port 5432 is taken, set `POSTGRES_HOST_PORT` (and `DATABASE_URL` accordingly).
- `DECISIONS.md` is the one-pager; `notes/` holds the longer design record and provider behaviour reference.
