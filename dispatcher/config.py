"""Runtime configuration. Everything comes from the environment with the defaults
the README allows (127.0.0.1 services) and the numbers justified in DECISIONS.md."""

import os
import socket
from dataclasses import dataclass


def _f(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _i(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Config:
    provider_url: str
    database_url: str
    worker_id: str

    lease_seconds: float        # how long a claim is valid without a heartbeat
    heartbeat_seconds: float    # how often a live worker extends its lease
    client_timeout: float       # connect + read timeout; normal latency is 20-120 ms, a hang holds 300 s
    max_attempts: int           # 500s and not-found-in-/sent retries, 429 excluded
    min_send_interval: float    # per-worker pacing so 3 workers stay under the bucket rate
    idle_exit_seconds: float    # exit when nothing claimable for this long (0 = never)

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            provider_url=os.environ.get("PROVIDER_URL", "http://127.0.0.1:8080").rstrip("/"),
            database_url=os.environ.get(
                "DATABASE_URL", "postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch"
            ),
            worker_id=os.environ.get("WORKER_ID", f"{socket.gethostname()}:{os.getpid()}"),
            lease_seconds=_f("LEASE_SECONDS", 15.0),
            heartbeat_seconds=_f("HEARTBEAT_SECONDS", 3.0),
            client_timeout=_f("CLIENT_TIMEOUT", 2.0),
            max_attempts=_i("MAX_ATTEMPTS", 8),
            # 12 rps bucket shared by 3 workers and /sent pages. The interval
            # includes the request's own latency, and pauses/timeouts eat more,
            # so 4 nominal/s per worker stays under the bucket in practice.
            min_send_interval=_f("MIN_SEND_INTERVAL", 0.25),
            idle_exit_seconds=_f("IDLE_EXIT_SECONDS", 10.0),
        )
