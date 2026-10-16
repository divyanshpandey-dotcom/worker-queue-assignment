# DECISIONS

**Run:** `docker compose up -d --wait`, `make seed`, `make worker` ×3 (`uv`, Python 3.12). `scripts/grader_run.sh` reproduces the grader (kill -9 at 60 s, verify at 120 s). Last local run: 500/500 in `/sent`, 0 duplicates, worst window 2, 0 late; survivors drained everything by 86 s and exited.

## State machine

`actions.status` ∈ {PENDING, SENDING, SENT, FAILED}. Every transition is one `UPDATE … WHERE id=? AND leased_by=me AND status='SENDING'`; rowcount 0 means the lease was lost and the worker drops the row.

| From | Event | Guard | To |
|---|---|---|---|
| PENDING / expired SENDING | claim | `FOR UPDATE SKIP LOCKED`, lease NULL or expired (expired first), 1 row | SENDING, lease 15 s |
| SENDING | 200 | mine | SENT + receipt_id |
| SENDING | 500 | mine, attempts < 8 | SENDING, attempts+1 |
| SENDING | 429 | sets shared `pause_until = max(old, now+Retry-After)` | SENDING, no attempt spent |
| SENDING | 400, or attempts = 8 | mine | FAILED + reason |
| SENDING | timeout/reset, scan finds it | mine | SENT, receipt from `/sent` |
| SENDING | timeout/reset, scan hits end | mine, attempts < 8 | SENDING, attempts+1, resend |

A heartbeat thread extends the lease every 3 s (guarded by `leased_by=me`); kill -9 kills it too, so the row is claimable within 15 s. `send_issued_at` is committed *before* every `POST /send` and cleared on 429/500; an inheriting worker scans `/sent` iff it is set.

## Timeout after a possible send

Outcomes are grouped by what the client sees: 200 sent; 429/500/400 provably not sent; timeout or reset **unknown** (hang, drop, drop_before are indistinguishable). Unknown → our `receipts` table (a hit is final), else page `/sent` from a shared cursor until an empty page, persisting receipts and cursor together per page under a scan lease. Found → SENT; end of list → provably not sent, resend. No DB transaction ever spans a provider call. **Guaranteed:** exactly-once, because this provider appends the receipt *before* responding, hanging or resetting, so absence after end-of-list is proof. **Not guaranteed:** with a provider that records asynchronously, "absent now" proves nothing and there is no safe move without idempotency on their side. Lease 15 s sits between the floor (one 2 s timeout + one scan ≈ 5 s) and the ceiling (60 s left after the kill; worst 5-bad-draw chain after inheriting ≈ 10 s).

## Ask the provider

Idempotent `POST /send` keyed on `action_id`, returning the original receipt on a repeat. It removes the ambiguous state at the source; a lookup by `action_id` would only make managing it cheaper and still races asynchronous recording.

## What verify.py cannot catch

It sees the provider, not us: a SENT row holding the wrong receipt, a FAILED row that deserved retries, a worker that silently stopped claiming while others finished. I would check `receipts` vs `actions.receipt_id` agreement, FAILED reasons, per-worker claim counts, and that no lease is still live at exit.

## 50,000 actions/hour

≈14 rps, above the 12 rps bucket: the provider limit breaks first, then the single `provider_state` row (read before every call, written on every 429). Changes: shared token bucket, in-memory `pause_until` with a short TTL, small claim batches, sharded cursors, a dedicated reconciler off the send path, and a delivery deadline instead of a fixed attempt cap.

## Deliberately not built

Dead-letter requeue of FAILED rows, deadlines, batch claiming, metrics, tests beyond the grader reproduction and the per-transition SQL checks run against the real DB during development.

## Warnings seen along the way

Run 1 froze all workers: the scan held a `FOR UPDATE` transaction across HTTP calls, and Postgres freezes `now()` per transaction, so a 429 inside it made the pause check return 2.0 s forever while everyone queued on the row lock. Fixed with the scan lease and `clock_timestamp()`. Run 2 had one request 268 ms late: the pause was read before the pacing sleep; it is now read immediately before the request. Details in `notes/design-decisions.md`.
