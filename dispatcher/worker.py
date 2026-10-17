"""`make worker`: the state machine.

One row at a time. Every provider call goes through wait_for_provider(), which
honours the shared pause and per-worker pacing. A heartbeat thread keeps the
lease alive; if it ever fails to, the main loop drops the row.
"""

import signal
import sys
import threading
import time
from datetime import datetime, timezone

from . import db, reconcile
from .config import Config
from .provider import Kind, ProviderClient


class Worker:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.conn = db.connect(cfg.database_url)
        self.provider = ProviderClient(cfg.provider_url, cfg.client_timeout)
        self.stop = threading.Event()
        self.lease_lost = threading.Event()
        self.current: str | None = None  # action id the heartbeat should extend
        self.current_lock = threading.Lock()
        self.last_call = 0.0

    # -- logging ---------------------------------------------------------

    def log(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        print(f"{ts} [{self.cfg.worker_id}] {msg}", flush=True)

    # -- heartbeat thread --------------------------------------------------

    def heartbeat_loop(self) -> None:
        # Own connection: psycopg connections are not safe to share across threads.
        hb = db.connect(self.cfg.database_url)
        while not self.stop.wait(self.cfg.heartbeat_seconds):
            with self.current_lock:
                aid = self.current
            if aid is None:
                continue
            try:
                ok = db.heartbeat(hb, self.cfg.worker_id, aid, self.cfg.lease_seconds)
            except Exception as e:  # cannot prove ownership -> treat as lost
                self.log(f"heartbeat error on {aid}: {e}")
                ok = False
            if not ok:
                self.lease_lost.set()

    def set_current(self, aid: str | None) -> None:
        with self.current_lock:
            self.current = aid
        self.lease_lost.clear()

    # -- provider discipline -----------------------------------------------

    def wait_for_provider(self) -> None:
        """Block until our own pacing allows and the shared pause has passed.

        Pacing first, pause check last: the pause must be read as close to the
        request as possible, or a 429 another worker saw during our pacing
        sleep is missed and we land late inside its window."""
        gap = self.cfg.min_send_interval - (time.monotonic() - self.last_call)
        if gap > 0:
            time.sleep(gap)
        while True:
            remaining = db.pause_remaining(self.conn)
            if remaining <= 0:
                break
            time.sleep(min(remaining, 0.5))
        self.last_call = time.monotonic()

    # -- one action ---------------------------------------------------------

    def process(self, action: db.Action) -> None:
        cfg, w, aid = self.cfg, self.cfg.worker_id, action.id
        self.set_current(aid)
        # An inherited row with the flag set means the previous owner may have
        # sent. A row we just timed out on means the same thing.
        needs_scan = action.send_issued_at is not None
        attempts = action.attempts
        try:
            while True:
                if self.stop.is_set():
                    db.release(self.conn, w, aid)
                    self.log(f"{aid} released on shutdown")
                    return
                if self.lease_lost.is_set():
                    self.log(f"{aid} lease lost, dropping")
                    return

                if needs_scan:
                    rid = reconcile.resolve(
                        self.conn, self.provider, w, aid, cfg.lease_seconds, self.wait_for_provider, self.log
                    )
                    if rid:
                        if db.mark_sent(self.conn, w, aid, rid):
                            self.log(f"{aid} SENT via /sent scan receipt={rid}")
                        return
                    # Provably not sent: this attempt is spent.
                    n = db.record_not_sent(self.conn, w, aid, count_attempt=True)
                    if n is None:
                        return
                    attempts = n
                    needs_scan = False
                    self.log(f"{aid} not in /sent, attempts={attempts}")

                if attempts >= cfg.max_attempts:
                    db.mark_failed(self.conn, w, aid, f"exhausted after {attempts} attempts")
                    self.log(f"{aid} FAILED exhausted")
                    return

                self.wait_for_provider()
                if not db.mark_send_issued(self.conn, w, aid):
                    return  # lease lost between claim and send
                out = self.provider.send(aid, action.recipient, action.body)

                if out.kind is Kind.SENT:
                    if db.mark_sent(self.conn, w, aid, out.receipt_id):
                        self.log(f"{aid} SENT receipt={out.receipt_id}")
                    return
                if out.kind is Kind.REJECTED:
                    db.mark_failed(self.conn, w, aid, f"400 {out.detail}")
                    self.log(f"{aid} FAILED 400")
                    return
                if out.kind is Kind.PAUSED:
                    db.set_pause(self.conn, out.retry_after)
                    if db.record_not_sent(self.conn, w, aid, count_attempt=False) is None:
                        return
                    self.log(f"{aid} 429 pause {out.retry_after}s")
                    continue
                if out.kind is Kind.NOT_SENT:
                    n = db.record_not_sent(self.conn, w, aid, count_attempt=True)
                    if n is None:
                        return
                    attempts = n
                    self.log(f"{aid} 500, attempts={attempts}")
                    continue
                # UNKNOWN: flag stays set, scan before anything else.
                self.log(f"{aid} unknown outcome ({out.detail}), will scan /sent")
                needs_scan = True
        finally:
            self.set_current(None)

    # -- main loop ----------------------------------------------------------

    def work_remaining(self) -> int:
        row = self.conn.execute(
            "SELECT count(*) AS n FROM actions WHERE status IN ('PENDING', 'SENDING')"
        ).fetchone()
        return int(row["n"])

    def run(self) -> int:
        threading.Thread(target=self.heartbeat_loop, daemon=True, name="heartbeat").start()
        self.log(f"worker up lease={self.cfg.lease_seconds}s timeout={self.cfg.client_timeout}s")
        idle_since: float | None = None
        try:
            while not self.stop.is_set():
                action = db.claim_one(self.conn, self.cfg.worker_id, self.cfg.lease_seconds)
                if action is None:
                    # Nothing claimable right now. Exit only when nothing is left
                    # anywhere, including rows another worker still holds: if that
                    # worker dies, someone has to be around to inherit.
                    if self.work_remaining() == 0:
                        self.log("all actions terminal, exiting")
                        return 0
                    time.sleep(0.5)
                    continue
                inherited = action.send_issued_at is not None or action.attempts > 0
                self.log(f"claimed {action.id} attempts={action.attempts}{' inherited' if inherited else ''}")
                self.process(action)
        finally:
            self.stop.set()
            self.provider.close()
            self.conn.close()
        self.log("stopped")
        return 0


def main() -> int:
    cfg = Config.from_env()
    worker = Worker(cfg)

    def on_signal(signum, _frame):
        worker.log(f"signal {signal.Signals(signum).name}, stopping after current step")
        worker.stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    return worker.run()


if __name__ == "__main__":
    sys.exit(main())
