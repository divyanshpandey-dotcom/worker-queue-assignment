"""Resolve an ambiguous send by reading the provider's truth table, /sent.

Rule: the receipts table first, and a hit is final. On a miss, page /sent from
the shared cursor until an empty page, persisting every receipt as we go.
Only an empty page proves "not sent", and only because this provider appends
the receipt before it responds, hangs or resets.

One scanner at a time, via a lease on the singleton row. No database
transaction is ever open while we wait on the provider: Postgres freezes
now() for a transaction's lifetime, and a dead scanner must not pin a lock.
"""

from __future__ import annotations

import time
from typing import Callable

import psycopg

from . import db
from .provider import Kind, Page, ProviderClient

MAX_PAGE_FAILURES = 30  # 500s / resets on /sent before giving up this scan


def resolve(
    conn: psycopg.Connection,
    provider: ProviderClient,
    worker_id: str,
    action_id: str,
    scan_lease_seconds: float,
    before_call: Callable[[], None],
    log: Callable[[str], None],
) -> str | None:
    """Return the receipt_id if the action has reached /sent, else None once
    the end of the list has been reached. Raises RuntimeError if the provider
    would not let us finish the scan."""
    while True:
        hit = db.receipt_for(conn, action_id)
        if hit:
            return hit
        cursor = db.acquire_scan_lease(conn, worker_id, scan_lease_seconds)
        if cursor is None:
            # Someone else is paging and persists everything they read, so
            # our receipt may land in the table without us doing anything.
            time.sleep(0.3)
            continue
        try:
            return _scan(conn, provider, worker_id, action_id, cursor, scan_lease_seconds, before_call, log)
        except db.ScanLeaseLost:
            log("scan lease lost mid-scan, re-acquiring")
            continue
        finally:
            db.release_scan_lease(conn, worker_id)


def _scan(conn, provider, worker_id, action_id, cursor, lease, before_call, log) -> str | None:
    failures = 0
    while True:
        before_call()  # shared pause + pacing, same as a send
        result = provider.sent_page(cursor)
        if isinstance(result, Page):
            seen = db.store_page(conn, worker_id, result.receipts, result.next, lease)
            log(f"scan since={cursor} got={len(seen)} next={result.next}")
            cursor = result.next
            if action_id in seen:
                return db.receipt_for(conn, action_id)
            if not result.receipts:
                return None  # end of list: provably not sent
            continue
        if result.kind is Kind.PAUSED:
            db.set_pause(conn, result.retry_after)
            continue
        failures += 1
        if failures >= MAX_PAGE_FAILURES:
            raise RuntimeError(f"/sent unavailable after {failures} failures ({result.detail})")
        time.sleep(0.2)
