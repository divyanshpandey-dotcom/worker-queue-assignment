-- Draft schema for review. Not yet wired to anything.

-- ---------------------------------------------------------------------------
-- 1. actions: one row per seeded action. The status column IS the state machine.
-- ---------------------------------------------------------------------------
CREATE TABLE actions (
    id              TEXT PRIMARY KEY,            -- action_id from seed, dedup key
    recipient       TEXT NOT NULL,
    body            TEXT NOT NULL,

    status          TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'FAILED')),

    -- lease: who holds the row and until when. NULL when nobody does.
    leased_by       TEXT,
    leased_until    TIMESTAMPTZ,

    -- set (and committed) immediately before POST /send; cleared on 500/429.
    -- non-NULL on an inherited row means: scan /sent before sending.
    send_issued_at  TIMESTAMPTZ,

    -- counts 500s and "not found in /sent" retries. 429 does not count.
    attempts        INT NOT NULL DEFAULT 0,

    -- terminal data
    receipt_id      TEXT,                        -- set on SENT
    failure_reason  TEXT,                        -- set on FAILED

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim query only ever looks at non-terminal rows. Keep the index small.
CREATE INDEX actions_claimable_idx
    ON actions (leased_until)
    WHERE status IN ('PENDING', 'SENDING');

-- ---------------------------------------------------------------------------
-- 2. receipts: every receipt any worker has ever read from /sent.
--    Nothing read past the shared cursor may be lost, so this is append-only
--    and keyed on receipt_id (insert ... on conflict do nothing).
-- ---------------------------------------------------------------------------
CREATE TABLE receipts (
    receipt_id      TEXT PRIMARY KEY,
    action_id       TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL,
    seen_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- lookup path: "has this action ever appeared in /sent?"
CREATE INDEX receipts_action_idx ON receipts (action_id);

-- ---------------------------------------------------------------------------
-- 3. provider_state: exactly one row. Shared across all workers.
-- ---------------------------------------------------------------------------
CREATE TABLE provider_state (
    singleton       BOOL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    pause_until     TIMESTAMPTZ,                 -- NULL or past = no pause
    sent_cursor     BIGINT NOT NULL DEFAULT 0,   -- last 'next' from /sent
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO provider_state DEFAULT VALUES;

-- ---------------------------------------------------------------------------
-- The transitions, as the SQL that guards them.
-- ---------------------------------------------------------------------------

-- CLAIM (PENDING -> SENDING, or expired SENDING -> SENDING with new owner).
-- One row. SKIP LOCKED keeps three workers from blocking each other.
-- Returns 0 rows when nothing is claimable.
--
--   WITH candidate AS (
--     SELECT id FROM actions
--      WHERE status IN ('PENDING', 'SENDING')
--        AND (leased_until IS NULL OR leased_until < now())
--      ORDER BY leased_until NULLS LAST, created_at   -- expired leases first
--      LIMIT 1
--      FOR UPDATE SKIP LOCKED
--   )
--   UPDATE actions a
--      SET status = 'SENDING', leased_by = $worker, leased_until = now() + $lease,
--          updated_at = now()
--     FROM candidate WHERE a.id = candidate.id
--   RETURNING a.*;

-- HEARTBEAT (no state change). Guard: still mine.
--   UPDATE actions SET leased_until = now() + $lease
--    WHERE id = $id AND leased_by = $worker AND status = 'SENDING';

-- MARK SEND ISSUED (no state change). Must commit before POST /send.
--   UPDATE actions SET send_issued_at = now()
--    WHERE id = $id AND leased_by = $worker AND status = 'SENDING';

-- 200 -> SENT. Guard: still mine, still SENDING. Check rowcount == 1.
--   UPDATE actions SET status = 'SENT', receipt_id = $rid,
--          leased_by = NULL, leased_until = NULL, send_issued_at = NULL
--    WHERE id = $id AND leased_by = $worker AND status = 'SENDING';

-- 500 / not-in-/sent -> stay SENDING, attempts + 1, flag cleared.
--   UPDATE actions SET attempts = attempts + 1, send_issued_at = NULL
--    WHERE id = $id AND leased_by = $worker AND status = 'SENDING'
--   RETURNING attempts;        -- caller compares to MAX_ATTEMPTS

-- attempts exhausted / 400 -> FAILED.
--   UPDATE actions SET status = 'FAILED', failure_reason = $reason,
--          leased_by = NULL, leased_until = NULL, send_issued_at = NULL
--    WHERE id = $id AND leased_by = $worker AND status = 'SENDING';

-- 429 -> shared pause. max() so a second 429 never shortens the pause.
--   UPDATE provider_state
--      SET pause_until = GREATEST(COALESCE(pause_until, 'epoch'), now() + $retry_after);
-- (and clear send_issued_at on the action row as for 500)

-- SCAN one page. Lock the singleton row for the duration of the scan so only
-- one worker pages at a time; receipts and cursor move in one transaction.
--   BEGIN;
--   SELECT sent_cursor FROM provider_state FOR UPDATE;
--   -- GET /sent?since=<cursor>  (outside the DB, inside the lock)
--   INSERT INTO receipts (receipt_id, action_id, recipient, sent_at)
--        VALUES (...) ON CONFLICT (receipt_id) DO NOTHING;
--   UPDATE provider_state SET sent_cursor = $next;
--   COMMIT;
