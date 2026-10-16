"""Every SQL statement the dispatcher runs, one function each.

This module is the state machine's enforcement layer. Each transition is a
single UPDATE whose WHERE clause is the guard, and the caller checks the row
count to learn whether the transition fired. Nothing here talks HTTP.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import psycopg
from psycopg.rows import dict_row

SCHEMA = """
CREATE TABLE IF NOT EXISTS actions (
    id              TEXT PRIMARY KEY,
    recipient       TEXT NOT NULL,
    body            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'FAILED')),
    leased_by       TEXT,
    leased_until    TIMESTAMPTZ,
    send_issued_at  TIMESTAMPTZ,
    attempts        INT NOT NULL DEFAULT 0,
    receipt_id      TEXT,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS actions_claimable_idx
    ON actions (leased_until) WHERE status IN ('PENDING', 'SENDING');

CREATE TABLE IF NOT EXISTS receipts (
    receipt_id      TEXT PRIMARY KEY,
    action_id       TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL,
    seen_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS receipts_action_idx ON receipts (action_id);

CREATE TABLE IF NOT EXISTS provider_state (
    singleton       BOOL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    pause_until     TIMESTAMPTZ,
    sent_cursor     BIGINT NOT NULL DEFAULT 0,
    scan_leased_by  TEXT,
    scan_leased_until TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO provider_state DEFAULT VALUES ON CONFLICT DO NOTHING;
"""


@dataclass
class Action:
    id: str
    recipient: str
    body: str
    status: str
    attempts: int
    send_issued_at: datetime | None


def connect(database_url: str) -> psycopg.Connection:
    # autocommit: each statement below is its own transaction unless a function
    # opens an explicit one. That keeps every guard atomic on its own.
    return psycopg.connect(database_url, autocommit=True, row_factory=dict_row)


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA)


# -- seeding ---------------------------------------------------------------

def insert_actions(conn: psycopg.Connection, rows: Iterable[tuple[str, str, str]]) -> int:
    """Insert-or-ignore so `make seed` is idempotent. Returns rows inserted."""
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO actions (id, recipient, body) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
            list(rows),
        )
        return cur.rowcount


# -- claim / lease ---------------------------------------------------------

def claim_one(conn: psycopg.Connection, worker_id: str, lease_seconds: float) -> Action | None:
    """PENDING -> SENDING, or an expired SENDING row to a new owner.

    One row. Expired leases first so a dead worker's row is picked up as soon as
    its lease lapses instead of waiting behind the PENDING pool. SKIP LOCKED
    means three workers never wait on each other.
    """
    row = conn.execute(
        """
        WITH candidate AS (
            SELECT id FROM actions
             WHERE status IN ('PENDING', 'SENDING')
               AND (leased_until IS NULL OR leased_until < now())
             ORDER BY leased_until NULLS LAST, created_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED
        )
        UPDATE actions a
           SET status = 'SENDING',
               leased_by = %(w)s,
               leased_until = now() + make_interval(secs => %(lease)s),
               updated_at = now()
          FROM candidate
         WHERE a.id = candidate.id
        RETURNING a.id, a.recipient, a.body, a.status, a.attempts, a.send_issued_at
        """,
        {"w": worker_id, "lease": lease_seconds},
    ).fetchone()
    return Action(**row) if row else None


def heartbeat(conn: psycopg.Connection, worker_id: str, action_id: str, lease_seconds: float) -> bool:
    """Extend my lease. Not a state change. False means I no longer own the row."""
    cur = conn.execute(
        """
        UPDATE actions
           SET leased_until = now() + make_interval(secs => %(lease)s), updated_at = now()
         WHERE id = %(id)s AND leased_by = %(w)s AND status = 'SENDING'
        """,
        {"id": action_id, "w": worker_id, "lease": lease_seconds},
    )
    return cur.rowcount == 1


def release(conn: psycopg.Connection, worker_id: str, action_id: str) -> None:
    """Give a SENDING row back without changing attempts (graceful shutdown)."""
    conn.execute(
        """
        UPDATE actions
           SET leased_by = NULL, leased_until = NULL, updated_at = now()
         WHERE id = %(id)s AND leased_by = %(w)s AND status = 'SENDING'
        """,
        {"id": action_id, "w": worker_id},
    )


# -- the risky moment ------------------------------------------------------

def mark_send_issued(conn: psycopg.Connection, worker_id: str, action_id: str) -> bool:
    """Committed BEFORE POST /send. A non-NULL flag on an inherited row means
    'the previous owner may have sent this; scan /sent before you do'."""
    cur = conn.execute(
        """
        UPDATE actions SET send_issued_at = now(), updated_at = now()
         WHERE id = %(id)s AND leased_by = %(w)s AND status = 'SENDING'
        """,
        {"id": action_id, "w": worker_id},
    )
    return cur.rowcount == 1


# -- transitions out of SENDING -------------------------------------------

_OWNED = "WHERE id = %(id)s AND leased_by = %(w)s AND status = 'SENDING'"


def mark_sent(conn: psycopg.Connection, worker_id: str, action_id: str, receipt_id: str) -> bool:
    cur = conn.execute(
        f"""
        UPDATE actions
           SET status = 'SENT', receipt_id = %(rid)s,
               leased_by = NULL, leased_until = NULL, send_issued_at = NULL, updated_at = now()
        {_OWNED}
        """,
        {"id": action_id, "w": worker_id, "rid": receipt_id},
    )
    return cur.rowcount == 1


def mark_failed(conn: psycopg.Connection, worker_id: str, action_id: str, reason: str) -> bool:
    cur = conn.execute(
        f"""
        UPDATE actions
           SET status = 'FAILED', failure_reason = %(reason)s,
               leased_by = NULL, leased_until = NULL, send_issued_at = NULL, updated_at = now()
        {_OWNED}
        """,
        {"id": action_id, "w": worker_id, "reason": reason},
    )
    return cur.rowcount == 1


def record_not_sent(conn: psycopg.Connection, worker_id: str, action_id: str, count_attempt: bool) -> int | None:
    """Provider proved nothing was sent (500, 429) or a scan reached the end
    without finding the action. Clear the flag; bump attempts unless it was a
    429, which says nothing about this action. Returns the new attempt count,
    or None if the lease was lost."""
    row = conn.execute(
        f"""
        UPDATE actions
           SET send_issued_at = NULL,
               attempts = attempts + %(inc)s,
               updated_at = now()
        {_OWNED}
        RETURNING attempts
        """,
        {"id": action_id, "w": worker_id, "inc": 1 if count_attempt else 0},
    ).fetchone()
    return row["attempts"] if row else None


# -- shared provider state -------------------------------------------------

def set_pause(conn: psycopg.Connection, retry_after_seconds: float) -> None:
    """Any worker that sees a 429 pauses everyone. GREATEST so a second 429
    never shortens an existing pause."""
    conn.execute(
        """
        UPDATE provider_state
           SET pause_until = GREATEST(COALESCE(pause_until, 'epoch'::timestamptz),
                                      clock_timestamp() + make_interval(secs => %(s)s)),
               updated_at = now()
        """,
        {"s": retry_after_seconds},
    )


def pause_remaining(conn: psycopg.Connection) -> float:
    """Seconds every worker must still wait before touching the provider."""
    row = conn.execute(
        "SELECT GREATEST(0, EXTRACT(EPOCH FROM (pause_until - clock_timestamp()))) AS s FROM provider_state"
    ).fetchone()
    return float(row["s"] or 0.0)


# -- receipts / reconciliation --------------------------------------------

def receipt_for(conn: psycopg.Connection, action_id: str) -> str | None:
    """A hit here is final: the action has been seen in /sent."""
    row = conn.execute(
        "SELECT receipt_id FROM receipts WHERE action_id = %s ORDER BY sent_at LIMIT 1", (action_id,)
    ).fetchone()
    return row["receipt_id"] if row else None


class ScanLeaseLost(Exception):
    """Another worker took over the scan because we were too slow."""


def acquire_scan_lease(conn: psycopg.Connection, worker_id: str, lease_seconds: float) -> int | None:
    """Exactly one worker pages /sent at a time, enforced by a lease on the
    singleton row rather than a row lock, so no transaction ever spans a
    provider call and a dead scanner releases the scan by expiry.
    Returns the cursor to page from, or None if someone else holds the scan."""
    row = conn.execute(
        """
        UPDATE provider_state
           SET scan_leased_by = %(w)s,
               scan_leased_until = clock_timestamp() + make_interval(secs => %(lease)s),
               updated_at = now()
         WHERE scan_leased_until IS NULL OR scan_leased_until < clock_timestamp()
        RETURNING sent_cursor
        """,
        {"w": worker_id, "lease": lease_seconds},
    ).fetchone()
    return int(row["sent_cursor"]) if row else None


def store_page(
    conn: psycopg.Connection,
    worker_id: str,
    receipts: list[dict],
    next_cursor: int,
    lease_seconds: float,
) -> list[str]:
    """Persist a page and advance the cursor in one short transaction, guarded
    by the scan lease so a stale scanner cannot move the cursor. Nothing read
    past the shared cursor is ever lost. Returns the action_ids seen."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO receipts (receipt_id, action_id, recipient, sent_at)
                VALUES (%s, %s, %s, %s) ON CONFLICT (receipt_id) DO NOTHING
                """,
                [(r["receipt_id"], r["action_id"], r["recipient"], r["sent_at"]) for r in receipts],
            )
        cur = conn.execute(
            """
            UPDATE provider_state
               SET sent_cursor = %(next)s,
                   scan_leased_until = clock_timestamp() + make_interval(secs => %(lease)s),
                   updated_at = now()
             WHERE scan_leased_by = %(w)s AND scan_leased_until >= clock_timestamp()
            """,
            {"next": next_cursor, "w": worker_id, "lease": lease_seconds},
        )
        if cur.rowcount != 1:
            raise ScanLeaseLost()  # rolls back the inserts too
    return [r["action_id"] for r in receipts]


def release_scan_lease(conn: psycopg.Connection, worker_id: str) -> None:
    conn.execute(
        """
        UPDATE provider_state SET scan_leased_by = NULL, scan_leased_until = NULL, updated_at = now()
         WHERE scan_leased_by = %(w)s
        """,
        {"w": worker_id},
    )
