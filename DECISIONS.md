# DECISIONS

**Context.** Three `make worker` processes drain 500 actions from Postgres into a provider that returns 429s, 500s, hangs and connection resets, and does not deduplicate. Run: `docker compose up -d --wait` · `make seed` · `make worker` ×3 (`uv`, Python 3.12). `scripts/grader_run.sh` reproduces the grading run (kill -9 one worker at 60 s, verify at 120 s). Two local runs on different fault seeds: **500/500 in `/sent`, 0 duplicates, worst window ≤ 2, 0 late requests**; the survivors drained everything by 86–91 s and exited on their own. No workflow framework: the state machine is the `status` column plus the SQL in `dispatcher/db.py`.

## 1. State machine

**What.** Four states on `actions.status`: `PENDING → SENDING → SENT | FAILED`. SENT carries the `receipt_id`, FAILED a reason.

**How.** Each transition is a single `UPDATE … WHERE id=? AND leased_by=me AND status='SENDING'`. The WHERE clause is the guard; a rowcount of 0 means another worker owns the row now, and the caller drops it without acting.

| From | Event | Guard | To |
|---|---|---|---|
| PENDING, or SENDING with expired lease | claim | `FOR UPDATE SKIP LOCKED`, lease NULL or `< now()`, expired first, 1 row | SENDING, lease 15 s |
| SENDING | 200 | mine | SENT + `receipt_id` |
| SENDING | 500 | mine, attempts < 8 | SENDING, attempts+1 |
| SENDING | 429 | writes shared `pause_until = max(old, now+Retry-After)` | SENDING, no attempt spent |
| SENDING | 400, or attempts = 8 | mine | FAILED + reason |
| SENDING | timeout/reset → scan finds action | mine | SENT, receipt from `/sent` |
| SENDING | timeout/reset → scan reaches end | mine, attempts < 8 | SENDING, attempts+1, resend |

**Why.** Four states, not five: "unsure whether it was sent" is not a separate state but a flag inside SENDING (`send_issued_at`, §2), because the next worker's behaviour differs only in whether it scans first. Guards live in SQL rather than Python so that three processes cannot both believe they own a row; Postgres is the only arbiter.

## 2. Leases and the killed worker

**What.** Every claimed row carries `leased_by` and `leased_until` (15 s). A heartbeat thread extends the lease every 3 s, guarded by `leased_by = me`. `send_issued_at` is committed *before* every `POST /send` and cleared on 429/500.

**How.** kill -9 kills the heartbeat too, so the lease simply expires; the claim query treats an expired SENDING row like a PENDING one, expired rows first. The inheritor reads `send_issued_at`: set means the previous owner may have reached the provider, so scan `/sent` first (§3); NULL means it died before the request, so send directly. Observed in both runs: the stranded row was re-claimed 15.5–15.7 s after the kill and SENT within 300 ms.

**Why.** *15 s* sits between a floor and a ceiling: the floor is the longest legitimate hold (one 2 s timeout + one scan ≈ 5 s), the ceiling is the 60 s left after the kill minus a worst-case chain of five consecutive bad draws after inheriting (≈ 10 s). *Heartbeat over a long fixed lease* because it keeps the lease short without racing slow-but-alive workers; it was weighed against a fixed 30 s lease, which would have eaten half the recovery budget. *The lease clock never pauses during rate limiting*: pausing it would make a dead worker indistinguishable from a waiting one. *Flag-then-send, not send-then-flag*: a crash between the two then costs one unnecessary scan instead of a duplicate email. *One row per claim*: the kill strands exactly one action and no queued rows age toward expiry; batching was rejected for this run and deferred to §7. *Expired rows first* so a stranded row never waits behind the PENDING backlog.

## 3. Timeout after a possible send

**What.** The client classifies by what it *sees*, not what the provider drew: 200 = sent; 429/500/400 = provably not sent; **timeout or connection reset = unknown**. Hang, drop and drop_before are indistinguishable on the wire, and all three land in "unknown".

**How.** For an unknown: look in our `receipts` table (a hit is final). On a miss, page `GET /sent` from a shared cursor until an **empty page**, inserting every receipt and advancing the cursor in one short transaction per page. Found → SENT with that receipt; end of list → provably not sent → resend. One scanner at a time, via a lease on the cursor row (`scan_leased_by/until`), re-extended per page. No database transaction is ever open while waiting on the provider. `/sent` pages go through the same pause and pacing as sends, and get the same 429/500 handling.

**Why.** Three options for an unknown: retry blindly (≈ 6 % of sends would duplicate, the trap the brief points at); mark FAILED (loses deliverable mail); or consult `/sent`. Only the third is correct, and it is cheap: the whole run is five pages, shared through the cursor. The *receipts table* exists because a shared cursor is only safe if nothing read past it is ever discarded: otherwise worker A pages past X's receipt looking for Y, worker B later starts from the cursor, never sees X, and resends it. A *lease* instead of a row lock because the row-lock version deadlocked (§9).

**Guaranteed:** exactly-once, because this provider appends the receipt *before* it responds, hangs or resets, so "absent after end of list" is proof of not-sent. **Not guaranteed:** against a provider that records asynchronously, "absent now" proves nothing and no client-side strategy is safe without idempotency on their side. **Assumed:** HTTP library retries are disabled (one wire request per attempt; a silent retry on reset would duplicate invisibly) and one in-flight request per worker, which is what makes "≤ 3 inside a window" achievable.

## 4. Rate limit

**What.** A single `provider_state` row holds `pause_until`. On any 429, the worker that saw it writes `pause_until = GREATEST(old, now + Retry-After)`. Every worker reads it immediately before every provider call, after its own pacing sleep of 0.25 s per request.

**How.** Pause check last, right before the socket opens; a second 429 can only lengthen the pause, never shorten it. `/sent` pages count too.

**Why.** Per-process backoff was rejected: when worker A gets a 429, workers B and C have no way to know, and each request they send after that is a late violation. `verify.py` allows three in-flight requests per window, exactly one per worker, so backoff has to be shared. The pause is read *after* the pacing sleep because in run 2 it was read before, and a 429 that arrived during the sleep produced one request 268 ms late. Pacing at 0.25 s per worker gives 12 nominal rps across three workers, equal to the bucket, and well under it in practice because pauses and timeouts eat time; every window in our runs was an injected fault, none from an empty bucket. A full shared token bucket was weighed and deferred (§7).

## 5. What I would ask the provider to add

**Idempotent `POST /send` keyed on `action_id`**, returning the original receipt on a repeat. It removes the ambiguous state at the source: a retry becomes safe by definition. The alternative, a lookup by `action_id`, only makes managing the ambiguity cheaper and still races a provider that records asynchronously.

## 6. What verify.py cannot catch

It sees the provider, not our table. It cannot see a SENT row holding the wrong receipt, a FAILED row that deserved more retries, a worker that silently stopped claiming while the others finished, or two actions with identical bodies to one recipient. I would check `receipts` against `actions.receipt_id`, FAILED reasons, per-worker claim counts, and that no lease is still live when the workers exit.

## 7. What breaks first at 50,000 actions/hour

≈ 14 rps against a 12 rps bucket: **the provider's rate limit breaks first**. Next, the single `provider_state` row, read before every call and written on every 429, becomes a hot row; then the scan sitting in the send path adds latency to every ambiguous outcome. Changes, in order: a shared token bucket across workers instead of pause-only; cache `pause_until` in memory with a short TTL; claim small batches; shard the cursor; move reconciliation to a dedicated worker; replace the attempt cap with a delivery deadline.

## 8. Deliberately not built

**Dead-letter requeue:** FAILED rows keep their reason but nothing re-queues them; every fault here is recoverable and FAILED never fired. **Delivery deadlines:** a fixed cap of 8 attempts (429s excluded) is easier to reason about for a 2-minute run; production would use a business deadline. **Batch claiming:** see §2 and §7. **Metrics:** stdout logs are enough to read a 2-minute run, not to operate a service. **Test suite:** instead, every SQL transition was exercised against the real Postgres (guards fire and refuse correctly) and the grading run was reproduced four times; unit tests for outcome classification and the inherit-then-scan path would be next.

## 9. Warnings seen along the way (none in the final runs)

Run 1 froze all three workers: the scan held `FOR UPDATE` in a transaction that spanned HTTP calls; Postgres freezes `now()` for a transaction's lifetime, so a 429 inside it made the pause check return 2.0 s forever while everyone queued on the row lock. Fixed with the scan lease and `clock_timestamp()`. Run 2 finished 489/500 with one 268 ms-late request: pause read before the pacing sleep (now after), timeout 5 → 2 s, pacing 0.33 → 0.25 s. Full record: `notes/design-decisions.md`.
