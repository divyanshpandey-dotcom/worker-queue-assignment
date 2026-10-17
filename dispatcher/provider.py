"""HTTP client for the provider. Knows nothing about the database.

Its whole job is to turn whatever happens on the wire into one of five
observations the state machine understands:

    SENT       200 with a receipt: the email went out.
    NOT_SENT   500: the provider promises nothing was sent. Safe to retry.
    PAUSED     429: nothing sent, and everyone must stop for Retry-After.
    REJECTED   400: nothing sent, and never will be. Terminal.
    UNKNOWN    timeout or connection reset. May or may not have sent.
               Only this one needs the /sent scan.

Library-level retries are off. One request per attempt, owned by the worker.
"""

import enum
from dataclasses import dataclass

import httpx


class Kind(enum.Enum):
    SENT = "sent"
    NOT_SENT = "not_sent"
    PAUSED = "paused"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Outcome:
    kind: Kind
    receipt_id: str | None = None
    retry_after: float = 0.0
    detail: str = ""


@dataclass(frozen=True)
class Page:
    receipts: list[dict]
    next: int


class ProviderClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base = base_url
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            # retries=0 is the default; stated so nobody "fixes" it later.
            transport=httpx.HTTPTransport(retries=0),
            # Close after each response. A connection the provider reset with
            # RST would otherwise sit in the pool and fail the next request,
            # costing a needless /sent scan.
            headers={"Connection": "close"},
        )

    def close(self) -> None:
        self.client.close()

    # -- POST /send --------------------------------------------------------

    def send(self, action_id: str, recipient: str, body: str) -> Outcome:
        try:
            resp = self.client.post(
                f"{self.base}/send",
                json={"action_id": action_id, "recipient": recipient, "body": body},
            )
        except httpx.TimeoutException:
            return Outcome(Kind.UNKNOWN, detail="timeout")
        except httpx.TransportError as e:  # reset, remote disconnect, refused...
            return Outcome(Kind.UNKNOWN, detail=f"{type(e).__name__}")

        if resp.status_code == 200:
            return Outcome(Kind.SENT, receipt_id=resp.json()["receipt_id"])
        if resp.status_code == 429:
            return Outcome(Kind.PAUSED, retry_after=_retry_after(resp))
        if resp.status_code == 400:
            return Outcome(Kind.REJECTED, detail=resp.text[:200])
        if resp.status_code >= 500:
            return Outcome(Kind.NOT_SENT, detail=f"HTTP {resp.status_code}")
        # Anything else is unspecified by the provider; treat as unknown and
        # let the scan decide rather than guess.
        return Outcome(Kind.UNKNOWN, detail=f"HTTP {resp.status_code}")

    # -- GET /sent ---------------------------------------------------------

    def sent_page(self, since: int, limit: int = 100) -> Page | Outcome:
        """Returns a Page, or an Outcome of kind PAUSED / NOT_SENT / UNKNOWN
        when the call itself failed and should be retried."""
        try:
            resp = self.client.get(f"{self.base}/sent", params={"since": since, "limit": limit})
        except httpx.TimeoutException:
            return Outcome(Kind.UNKNOWN, detail="timeout")
        except httpx.TransportError as e:
            return Outcome(Kind.UNKNOWN, detail=type(e).__name__)

        if resp.status_code == 200:
            data = resp.json()
            return Page(receipts=data["receipts"], next=int(data["next"]))
        if resp.status_code == 429:
            return Outcome(Kind.PAUSED, retry_after=_retry_after(resp))
        return Outcome(Kind.NOT_SENT, detail=f"HTTP {resp.status_code}")


def _retry_after(resp: httpx.Response) -> float:
    try:
        return max(0.0, float(resp.headers.get("Retry-After", "1")))
    except ValueError:
        return 1.0
