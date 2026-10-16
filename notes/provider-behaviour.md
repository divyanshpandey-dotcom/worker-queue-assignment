# Mock provider: observed behaviour

Reference notes for the dispatcher. Everything here is read from
`provider/server.py`, `verify.py` and `docker-compose.yml` in this kit.
Line numbers refer to `provider/server.py` unless stated.

## 1. Endpoints

| Method | Path | Rate limited | Faults | Purpose |
|---|---|---|---|---|
| POST | `/send` | yes | 429, 500, hang, drop, drop_before | send one email, returns `{receipt_id, action_id}` |
| GET | `/sent?since=<int>&limit=<1..100>` | yes | 429, 500 only | page through every recorded send, in send order |
| GET | `/stats` | no | none | counters, windows, violations (used by verify.py) |
| GET | `/healthz` | no | none | compose healthcheck |

`/send` body must be JSON with `action_id`, `recipient`, `body`, all non-empty
strings, otherwise 400 (lines 268-275). 400 is the only non-retryable response.

## 2. How a request is admitted (lines 145-185)

One function, one lock, in this order:

1. **Open window check** (line 151). If a Retry-After window is open, the request
   is counted as a violation and gets 429 with the remaining seconds. No token
   is consumed, no fault is drawn, nothing is recorded.
2. **Token bucket** (line 159). Refill at `RATE_LIMIT_RPS`, cap at
   `RATE_LIMIT_BURST`. If fewer than one token, open a new window
   (reason `rate_limit`) and return 429.
3. **Take a token, draw a fault** (line 167). One seeded RNG, in arrival order.
4. **Act on the draw**:
   - `429` (line 168): open a window of `randint(RETRY_AFTER_MIN, RETRY_AFTER_MAX)`
     seconds, reason `fault`. Not recorded.
   - `500`, `drop_before` (line 172): return. Not recorded.
   - `ok`, `hang`, `drop` (line 184): **receipt appended to `self.receipts`**.
     The send has happened. Only the response delivery differs.

Key consequence: for hang and drop, the receipt is in `/sent` before the socket
misbehaves. There is no gap between "sent" and "visible".

## 3. How the response is delivered (lines 325-338)

```
429         -> immediate JSON 429 with Retry-After header
500         -> immediate JSON 500
drop_before -> immediate connection reset (RST)
ok          -> sleep latency, then 200 JSON
hang        -> sleep latency, then hold socket up to HANG_MAX_SECONDS (300 s), then RST
drop        -> sleep latency, then RST
```

Latency is uniform 20-120 ms (lines 95-96). RST is forced via `SO_LINGER` with
zero timeout (line 375), so the client sees `ConnectionResetError` or
`RemoteDisconnected`, never a clean close.

## 4. What the client sees, grouped

| Client observation | Possible provider draw | Was it sent? | Safe action |
|---|---|---|---|
| 200 + receipt | ok | yes | mark SENT |
| 429 + Retry-After | window, bucket empty, or fault | no | pause everyone until window closes, then retry |
| 500 | fault | no | retry (paced) |
| 400 | bad payload | no | FAILED, do not retry |
| read timeout | hang | **yes** | scan `/sent`, then decide |
| connection reset | drop or drop_before | **yes** (drop) / no (drop_before) | scan `/sent`, then decide |

Only the last two rows are ambiguous. The scan does not need to know which draw
it was.

## 5. `/sent` mechanics (lines 217-220, 303-322)

- `since` is a **list offset**, not a time or an ID. `since=0` is the first
  receipt ever recorded.
- `next = since + len(page)`. Pass it back as `since`. An empty `receipts` list
  means you have read everything recorded so far.
- `limit` must be 1..100, default 100. Out of range is 400.
- The list only grows (in-memory, append only). A stored cursor is always valid.
- No lookup by `action_id` or `recipient`. Finding one action means paging
  forward from your cursor.
- `/sent` draws only `429` and `500` faults (line 190, `send=False` branch). It
  never hangs or drops.
- `/sent` consumes the same token bucket and opens the same windows as `/send`.

## 6. Rate limiting and windows

Defaults from `docker-compose.yml`:

| Setting | Default |
|---|---|
| `RATE_LIMIT_RPS` | 12 |
| `RATE_LIMIT_BURST` | 12 |
| `RETRY_AFTER_MIN` / `MAX` | 1 / 2 s |
| `FAULT_RATE_429` | 0.02 |
| `FAULT_RATE_500` | 0.04 |
| `FAULT_RATE_HANG` | 0.03 |
| `FAULT_RATE_DROP` | 0.03 |
| `FAULT_RATE_DROP_BEFORE` | 0 (off) |
| `FAULTS_ON_SENT` | on |
| `HANG_MAX_SECONDS` | 300 |

- Windows are **global**: one per provider, shared by all workers and both
  endpoints.
- A window is opened by: bucket empty, or an injected 429 fault.
- Every request arriving inside an open window is a violation and gets another
  429 (with remaining seconds, minimum 1).
- Violations are recorded with their offset in ms from window open
  (`violation_offsets_ms`), which is what verify.py uses to tell "already in
  flight" from "sent after the 429 was visible".

## 7. What verify.py measures (`verify.py` lines 36-38, 95-120)

| Check | Threshold | Source |
|---|---|---|
| duplicate `action_id` in `/sent` | any | receipts |
| seeded action missing from `/sent` | any | receipts vs seed |
| receipt recipient != seeded recipient | any | receipts vs seed |
| requests inside one window | > 3 | `stats.max_violations_per_window` |
| requests > 100 ms into a window, whole run | > 3 | `violation_offsets_ms` |
| `action_id` in `/sent` not in seed | any | receipts |

Why 3: three workers, each with at most one request in flight when a window
opens. Those land within milliseconds. Anything later means a worker sent after
the 429 was already observable, i.e. backoff was not shared.

verify.py reads `/stats` first, waits out any open window, then pages `/sent`
from `since=""`. Its own requests cannot count against the run.

## 8. Seed facts (`seed/actions.json`)

- 500 actions, 500 unique `id` values (UUID strings).
- 330 unique recipients. Several actions share a recipient. **Dedup key is
  `action_id`, not recipient.**
- Bodies contain non-ASCII (emoji, Slovene). Store as UTF-8 text.

## 9. Grader run shape (`README.md`, `TAKE_HOME.md`)

- `docker compose up`, then `make seed` once, then `make worker` x3 in
  foreground, each in its own process group.
- t = 60 s: `kill -9` one worker's whole process group. No signal handler runs.
- t = 120 s: SIGTERM to the rest, SIGKILL 5 s later.
- Then `python verify.py`.
- A worker may exit on its own when nothing is left to do.

## 10. Design constraints that fall out of the above

1. **Hang needs a client timeout.** Without one, a single hang parks a worker
   for 300 s, longer than the run.
2. **Library-level retries must be off.** A client that silently retries a
   reset POST produces a duplicate send that never appears in your own logs.
3. **One in-flight provider request per worker.** Otherwise more than 3
   requests can be in flight when a window opens.
4. **Shared pause across workers.** On any 429, all workers stop until the
   window closes. Per-process backoff fails the late-violation check.
5. **Pace under 12 rps total**, including `/sent` pages, to avoid opening
   rate-limit windows at all.
6. **Lease length L**: `client_timeout + reconcile_time < L << 60 s`. Small
   claim batches keep the dead worker's blast radius small.
7. **Inherited or timed-out work goes through `/sent` first.** Absence after
   reaching the end of `/sent` is proof of not-sent, because the provider
   records synchronously before responding.
8. **Attempt counter lives in the DB row**, so an inheriting worker knows the
   history. Bound retries; 400 goes straight to FAILED.
9. **Keep-alive sockets may be stale after a drop.** Either close per request or
   accept the occasional spurious reset as one more ambiguous outcome.

## 11. What this mock does that a real provider might not

- Records the receipt synchronously, before responding. A real provider may
  enqueue and record later, making "absent from /sent" meaningless.
- Promises 500 means nothing was sent. Real 500s are often ambiguous.
- Keeps everything in memory; `/sent` cursor is a list offset. Real cursors
  are opaque and may not be stable.

The API ask that removes the whole ambiguity: idempotent `/send` keyed on
`action_id`, returning the original receipt on a repeat. Lookup by `action_id`
is the weaker fallback ask.
