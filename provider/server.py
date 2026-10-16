"""Mock outreach provider for the backend take-home.

Endpoints
---------
POST /send                 {action_id, recipient, body} -> 200 {receipt_id, action_id}
GET  /sent?since=<cursor>  -> 200 {receipts: [...], next}   (send order, paginated)
GET  /stats                -> 200 request counts, Retry-After windows and violations
GET  /healthz              -> 200 {"ok": true}

The provider does not deduplicate on action_id: every accepted POST /send is a
new send with a new receipt_id.

Faults (POST /send unless noted)
--------------------------------
- 429 with Retry-After (also on GET /sent). Either the token bucket is empty or a
  fault was injected. Either way, the provider opens a *global* Retry-After window.
  Any request that arrives while a window is open counts as a violation and gets
  another 429.
- 500. Nothing was sent (also on GET /sent).
- hang. The send is recorded and no response ever comes back. The connection is
  held open until the client gives up.
- drop. The send is recorded and the connection is reset without a response.
- drop_before. The connection is reset without recording anything. Off by default.

Faults are drawn from one RNG seeded with PROVIDER_SEED, in request arrival
order. The same seed with the same arrival order gives the same fault sequence.
The provider keeps all state in memory, so restart the container to reset it.

Configuration is read from the environment; see Config.from_env.
"""

from __future__ import annotations

import json
import math
import os
import random
import select
import socket
import struct
import sys
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    seed: int
    faults: bool
    rate_429: float
    rate_500: float
    rate_hang: float
    rate_drop: float
    rate_drop_before: float
    faults_on_sent: bool
    rps: float
    burst: int
    retry_after_min: int
    retry_after_max: int
    latency_min_ms: int
    latency_max_ms: int
    hang_max_s: float
    page_size: int
    log_requests: bool

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            host=_env("PROVIDER_HOST", "0.0.0.0"),
            port=int(_env("PROVIDER_PORT", "8080")),
            seed=int(_env("PROVIDER_SEED", "1")),
            faults=_env("PROVIDER_FAULTS", "on").lower() in ("1", "on", "true", "yes"),
            rate_429=float(_env("FAULT_RATE_429", "0.02")),
            rate_500=float(_env("FAULT_RATE_500", "0.04")),
            rate_hang=float(_env("FAULT_RATE_HANG", "0.03")),
            rate_drop=float(_env("FAULT_RATE_DROP", "0.03")),
            rate_drop_before=float(_env("FAULT_RATE_DROP_BEFORE", "0")),
            faults_on_sent=_env("FAULTS_ON_SENT", "on").lower() in ("1", "on", "true", "yes"),
            rps=float(_env("RATE_LIMIT_RPS", "12")),
            burst=int(_env("RATE_LIMIT_BURST", "12")),
            retry_after_min=int(_env("RETRY_AFTER_MIN", "1")),
            retry_after_max=int(_env("RETRY_AFTER_MAX", "2")),
            latency_min_ms=int(_env("LATENCY_MIN_MS", "20")),
            latency_max_ms=int(_env("LATENCY_MAX_MS", "120")),
            hang_max_s=float(_env("HANG_MAX_SECONDS", "300")),
            page_size=int(_env("SENT_PAGE_SIZE", "100")),
            log_requests=_env("PROVIDER_LOG", "off").lower() in ("1", "on", "true", "yes"),
        )


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Window:
    opened_at: str
    opened_mono: float
    seconds: int
    reason: str
    violations: int = 0
    # Arrival time of each violating request, in ms after the window opened.
    violation_offsets_ms: list[int] = field(default_factory=list)

    @property
    def until(self) -> float:
        return self.opened_mono + self.seconds


@dataclass(frozen=True)
class Decision:
    kind: str  # ok | 429 | 500 | hang | drop | drop_before
    retry_after: int = 0
    receipt: dict | None = None


class Provider:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.lock = threading.Lock()
        self.fault_rng = random.Random(cfg.seed)
        self.latency_rng = random.Random(cfg.seed + 1)
        self.receipts: list[dict] = []
        self.windows: list[Window] = []
        self.tokens = float(cfg.burst)
        self.refilled_at = time.monotonic()
        self.requests: Counter[str] = Counter()
        self.responses: Counter[str] = Counter()
        self.started_mono = time.monotonic()

    # All admission decisions happen under one lock so that arrival order,
    # window bookkeeping, the fault sequence and send order agree.
    def admit(self, endpoint: str, payload: dict | None = None) -> Decision:
        cfg = self.cfg
        with self.lock:
            now = time.monotonic()
            self.requests[endpoint] += 1

            window = self._open_window(now)
            if window is not None:
                window.violations += 1
                window.violation_offsets_ms.append(round((now - window.opened_mono) * 1000))
                return Decision("429", retry_after=max(1, math.ceil(window.until - now)))

            self.tokens = min(float(cfg.burst), self.tokens + (now - self.refilled_at) * cfg.rps)
            self.refilled_at = now
            if self.tokens < 1.0:
                seconds = max(1, math.ceil((1.0 - self.tokens) / cfg.rps))
                self._new_window(now, seconds, "rate_limit")
                return Decision("429", retry_after=seconds)
            self.tokens -= 1.0

            kind = "ok"
            if cfg.faults and (endpoint == "send" or cfg.faults_on_sent):
                kind = self._draw_fault(send=endpoint == "send")
            if kind == "429":
                seconds = self.fault_rng.randint(cfg.retry_after_min, cfg.retry_after_max)
                self._new_window(now, seconds, "fault")
                return Decision("429", retry_after=seconds)
            if kind in ("500", "drop_before"):
                return Decision(kind)

            receipt = None
            if endpoint == "send":
                assert payload is not None
                receipt = {
                    "receipt_id": f"rcpt_{uuid.uuid4().hex}",
                    "action_id": payload["action_id"],
                    "recipient": payload["recipient"],
                    "sent_at": _utcnow_iso(),
                }
                self.receipts.append(receipt)
            return Decision(kind, receipt=receipt)

    def _draw_fault(self, send: bool) -> str:
        cfg = self.cfg
        r = self.fault_rng.random()
        edges = [("429", cfg.rate_429), ("500", cfg.rate_500)]
        if send:
            edges += [
                ("hang", cfg.rate_hang),
                ("drop", cfg.rate_drop),
                ("drop_before", cfg.rate_drop_before),
            ]
        acc = 0.0
        for kind, rate in edges:
            acc += rate
            if r < acc:
                return kind
        return "ok"

    def _open_window(self, now: float) -> Window | None:
        if self.windows and self.windows[-1].until > now:
            return self.windows[-1]
        return None

    def _new_window(self, now: float, seconds: int, reason: str) -> None:
        self.windows.append(Window(_utcnow_iso(), now, seconds, reason))

    def latency(self) -> float:
        with self.lock:
            ms = self.latency_rng.uniform(self.cfg.latency_min_ms, self.cfg.latency_max_ms)
        return ms / 1000.0

    def page(self, since: int, limit: int) -> tuple[list[dict], int]:
        with self.lock:
            page = self.receipts[since : since + limit]
            return list(page), since + len(page)

    def count_response(self, kind: str) -> None:
        with self.lock:
            self.responses[kind] += 1

    def stats(self) -> dict:
        with self.lock:
            now = time.monotonic()
            self.requests["stats"] += 1
            open_window = self._open_window(now)
            per_action = Counter(r["action_id"] for r in self.receipts)
            return {
                "requests": dict(self.requests),
                "responses": dict(self.responses),
                "sends_recorded": len(self.receipts),
                "distinct_action_ids": len(per_action),
                "violations": sum(w.violations for w in self.windows),
                "max_violations_per_window": max((w.violations for w in self.windows), default=0),
                "windows": [
                    {
                        "opened_at": w.opened_at,
                        "seconds": w.seconds,
                        "reason": w.reason,
                        "violations": w.violations,
                        "violation_offsets_ms": w.violation_offsets_ms,
                    }
                    for w in self.windows
                ],
                "retry_after_open": open_window is not None,
                "retry_after_remaining": round(open_window.until - now, 3) if open_window else 0.0,
                "uptime_seconds": round(now - self.started_mono, 1),
                "config": {k: v for k, v in asdict(self.cfg).items() if k not in ("host", "log_requests")},
            }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MockProvider/1.0"
    provider: Provider  # set on the subclass in main()

    _aborted = False

    def log_message(self, fmt: str, *args) -> None:
        if self.provider.cfg.log_requests:
            sys.stderr.write("%s %s\n" % (_utcnow_iso(), fmt % args))

    def finish(self) -> None:
        if self._aborted:
            return
        super().finish()

    # -- routing -----------------------------------------------------------

    def do_POST(self) -> None:
        url = urlparse(self.path)
        if url.path != "/send":
            self._drain_body()
            return self._json(404, {"error": "not found"})
        raw = self._drain_body()
        try:
            payload = json.loads(raw or b"null")
            ok = isinstance(payload, dict) and all(
                isinstance(payload.get(k), str) and payload.get(k) for k in ("action_id", "recipient", "body")
            )
        except (ValueError, UnicodeDecodeError):
            ok = False
        if not ok:
            self.provider.count_response("400")
            return self._json(400, {"error": "expected JSON {action_id, recipient, body}, all non-empty strings"})
        decision = self.provider.admit("send", payload)
        self._respond(decision, lambda: {"receipt_id": decision.receipt["receipt_id"], "action_id": payload["action_id"]})

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/healthz":
            return self._json(200, {"ok": True})
        if url.path == "/stats":
            return self._json(200, self.provider.stats())
        if url.path == "/sent":
            return self._sent(parse_qs(url.query))
        return self._json(404, {"error": "not found"})

    def _sent(self, query: dict[str, list[str]]) -> None:
        cfg = self.provider.cfg
        since_raw = (query.get("since") or [""])[0]
        limit_raw = (query.get("limit") or [str(cfg.page_size)])[0]
        try:
            since = int(since_raw) if since_raw else 0
            limit = int(limit_raw)
            if since < 0 or not 1 <= limit <= cfg.page_size:
                raise ValueError
        except ValueError:
            self.provider.count_response("400")
            return self._json(400, {"error": f"since must be a cursor from 'next'; limit in 1..{cfg.page_size}"})
        decision = self.provider.admit("sent")

        def body() -> dict:
            receipts, nxt = self.provider.page(since, limit)
            return {"receipts": receipts, "next": str(nxt)}

        self._respond(decision, body)

    # -- responses ---------------------------------------------------------

    def _respond(self, decision: Decision, body) -> None:
        self.provider.count_response(decision.kind)
        if decision.kind == "429":
            return self._json(429, {"error": "rate limited"}, {"Retry-After": str(decision.retry_after)})
        if decision.kind == "500":
            return self._json(500, {"error": "internal error, nothing was sent"})
        if decision.kind == "drop_before":
            return self._abort()
        time.sleep(self.provider.latency())
        if decision.kind == "hang":
            return self._hang()
        if decision.kind == "drop":
            return self._abort()
        return self._json(200, body())

    def _json(self, status: int, payload: dict, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(payload).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _drain_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length > 0 else b""

    def _hang(self) -> None:
        """Hold the connection open, never answer, until the client gives up."""
        sock = self.connection
        deadline = time.monotonic() + self.provider.cfg.hang_max_s
        while time.monotonic() < deadline:
            try:
                readable, _, _ = select.select([sock], [], [], 1.0)
                if readable and not sock.recv(65536):
                    break  # client closed its side
            except OSError:
                break
        self._abort()

    def _abort(self) -> None:
        """Reset the connection without sending a response."""
        self._aborted = True
        self.close_connection = True
        try:
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256
    allow_reuse_address = True


def main() -> None:
    cfg = Config.from_env()
    handler = type("BoundHandler", (Handler,), {"provider": Provider(cfg)})
    server = Server((cfg.host, cfg.port), handler)
    print(f"mock provider listening on {cfg.host}:{cfg.port} config={asdict(cfg)}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
