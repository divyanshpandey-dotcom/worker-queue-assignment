# Dispatcher design: decisions locked so far

Companion notes. Every number here is a design choice or a provider default,
not a measurement. Re-check against a real run before quoting any of them.

## State machine

States: PENDING, SENDING, SENT, FAILED. `status` column on `actions`.

| From | Event | Guard | To |
|---|---|---|---|
| PENDING / expired SENDING | worker claims | status in (PENDING, SENDING), lease NULL or expired, `FOR UPDATE SKIP LOCKED`, 1 row | SENDING (leased_by=me) |
| SENDING | 200 | leased_by=me, status SENDING | SENT + receipt_id |
| SENDING | 500 | leased_by=me, attempts < 8 | SENDING, attempts+1, flag cleared |
| SENDING | 500, attempts = 8 | leased_by=me | FAILED (exhausted) |
| SENDING | 429 | sets shared pause_until = max(old, now+Retry-After) | SENDING, no attempt consumed, flag cleared |
| SENDING | 400 | leased_by=me | FAILED (bad request) |
| SENDING | timeout / reset, scan finds action | leased_by=me | SENT + receipt from /sent |
| SENDING | timeout / reset, scan to end finds nothing | attempts < 8 | SENDING, attempts+1, resend |
| SENDING (expired) | another worker claims | lease expired | SENDING, new owner; scan first iff send_issued_at not NULL |

Heartbeat is not a transition: `leased_until = now()+15s WHERE leased_by=me`.

## The ambiguous outcome

Client groups outcomes by what it sees, not by what the provider drew:
- 200: sent.  429/500/400: not sent.  timeout/reset: unknown.
- Unknown -> `send_issued_at` is set -> scan.
- Scan = receipts table first (hit is final), else page /sent from shared
  cursor until empty page, persisting every receipt + cursor per page in one
  transaction, singleton row locked for the scan.
- Absence after end-of-list proves not-sent **because this mock appends the
  receipt before responding/hanging/resetting**. Real providers may not.

## Numbers and why

| Setting | Value | Reason |
|---|---|---|
| lease | 15 s | floor: 1 timeout (2) + 1 scan (~3) = 5. ceiling: kill at 60, verify at 120, worst 5-bad-draw chain after inherit ~10 s -> done by ~85 |
| client timeout | 2 s | normal latency 20-120 ms, so 2 s is ~16x the max; a hang otherwise holds 300 s. Was 5 s; run 2 showed hang timeouts as the second biggest time sink |
| heartbeat | 3 s | 5 beats per lease, survives 2 misses |
| claim batch | 1 row | blast radius on kill = 1 action; no lease pressure from queued rows |
| claim order | expired leases first (`NULLS LAST`) | stranded rows don't wait behind the PENDING pool; keeps the budget trace true |
| attempts | 8 (500 + not-found only) | 0.12^8 per action is negligible; 429 is not the action's fault |
| pause | shared row, read before every provider call, incl. /sent | per-process backoff fails the >3-late check |
| in-flight | 1 request per worker | verify allows 3 in-flight when a window opens = 1 per worker |
| pacing | 0.25 s per worker, measured from request start, incl. /sent | 3 workers = 12 nominal/s, real rate is well under because pauses and timeouts eat time. Runs 2-4: every window was an injected fault, none from an empty bucket. Pause is checked AFTER the pacing sleep, right before the request, or a 429 seen elsewhere during the sleep is missed (run 2: one 268 ms late request) |

## Signals

- kill -9: no handler runs. Lease expiry is the only recovery. Heartbeat
  thread dies with the process, so the lease stops being extended.
- SIGTERM at 120 s: stop claiming, exit. Nothing held needs finishing.

## Client discipline

- Library-level retries OFF. One request per attempt, owned by our loop.
- Explicit connect + read timeouts.
- Expect `ConnectionResetError` / `RemoteDisconnected` after drops; treat as
  unknown. Stale keep-alive after a reset is one more unknown.

## API ask (DECISIONS.md)

Idempotent `/send` keyed on action_id, returning the original receipt on a
repeat. Fallback: lookup by action_id. Reason: removes the ambiguous state at
the source instead of giving us a tool to manage it.

## Deliberately not built

- Dead-letter requeue of FAILED rows.
- Time-based delivery deadline (production would use one, business-driven).
- Batch claiming (would matter at 50k/h to cut DB round trips).

## Worst failure found while building (run 1, 2026-10-16)

All three workers froze at the same instant with no kill involved.
`pg_blocking_pids` showed w2 and w3 blocked on `UPDATE provider_state`
(set_pause) behind w1, which was looping on `pause_remaining` inside an open
transaction.

Cause: the first scan implementation held `SELECT ... FOR UPDATE` on the
singleton row in a transaction for the whole scan, including the HTTP calls.
Postgres `now()` is frozen at transaction start. A 429 on `/sent` inside that
transaction wrote `pause_until = frozen_now + 2`, and every later check
computed `pause_until - frozen_now = 2.0` forever. The lock was never
released; everyone else queued behind it.

Fix: no transaction ever spans a provider call. The scan is guarded by a lease
on the singleton row (`scan_leased_by`, `scan_leased_until`), re-extended on
every page and expiring on its own if the scanner dies. Receipts and cursor
still move together in one short transaction guarded by that lease. Pause
arithmetic uses `clock_timestamp()`.

Second lesson from the same run: the harness recorded the wrong pid, so the
kill -9 hit nothing. Always print process groups and check the kill landed.

## Run log (local, scripts/grader_run.sh, kill -9 at 60 s)

| Run | Change | present | dup | worst window | late | note |
|---|---|---|---|---|---|---|
| 1 | first version | - | - | - | - | all workers froze: frozen now() in scan transaction. Harness kill hit nothing |
| 2 | scan lease, clock_timestamp | 489/500 | 0 | 2 | 1 | correct but too slow: timeout 5 s, pacing 0.33 s, pause check before pacing sleep |
| 3 | timeout 2 s, pacing 0.25 s, pause check last | 500/500 | 0 | 2 | 0 | survivors finished all work at 86 s and exited on their own |
| 4 | same code, PROVIDER_SEED=7 | 500/500 | 0 | 1 | 0 | 25 hangs, 18 drops; survivors done at 91 s; stranded row recovered 15.7 s after the kill |
