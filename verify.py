#!/usr/bin/env python3
"""Acceptance report for the dispatcher take-home.

Reads seed/actions.json and the provider's /stats and /sent. Nothing else.
Run it after your workers have stopped:

    python verify.py [--provider http://127.0.0.1:8080] [--seed seed/actions.json] [--json]

It reports what reached the provider, and warns when something is out of bounds:
  - an action_id appears in /sent more than once;
  - a seeded action is missing from /sent, or was sent to another recipient;
  - more than 3 requests arrived inside a single Retry-After window;
  - more than 3 requests, over the whole run, arrived more than 100 ms into a
    Retry-After window. A request that was already on its way when the window
    opened lands within milliseconds.

Warnings are not a verdict. We read them together with your DECISIONS.md, and
a warning you have explained is worth more than a clean run you can't.

Exit code 0 means the report was produced, whatever it says. Exit code 2 means
the provider could not be read.

Standard library only, Python 3.12.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

MAX_VIOLATIONS_PER_WINDOW = 3
LATE_VIOLATION_MS = 100
MAX_LATE_VIOLATIONS = 3


class ProviderUnavailable(Exception):
    pass


def get_json(base: str, path: str, attempts: int = 60) -> dict:
    """GET with polite retries: honours Retry-After, retries 5xx and network errors."""
    last: Exception | None = None
    for _ in range(attempts):
        try:
            with urllib.request.urlopen(base + path, timeout=10) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(float(e.headers.get("Retry-After") or 1) + 0.1)
                continue
            if e.code >= 500:
                time.sleep(0.5)
                continue
            raise ProviderUnavailable(f"GET {path} -> HTTP {e.code}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
            time.sleep(0.5)
    raise ProviderUnavailable(f"GET {path} failed after {attempts} attempts: {last}")


def fetch_all_receipts(base: str) -> list[dict]:
    receipts: list[dict] = []
    cursor = ""
    while True:
        page = get_json(base, f"/sent?since={cursor}")
        receipts.extend(page["receipts"])
        if not page["receipts"]:
            return receipts
        cursor = page["next"]


def check(seed: list[dict], stats: dict, receipts: list[dict]) -> dict:
    seeded = {a["id"]: a["recipient"] for a in seed}
    counts = Counter(r["action_id"] for r in receipts)

    duplicates = {aid: n for aid, n in counts.items() if n > 1}
    present = [aid for aid in seeded if aid in counts]
    missing = [aid for aid in seeded if aid not in counts]
    wrong_recipient = sorted(
        {
            r["action_id"]
            for r in receipts
            if r["action_id"] in seeded and r["recipient"] != seeded[r["action_id"]]
        }
    )
    unknown = sorted({aid for aid in counts if aid not in seeded})

    windows = stats.get("windows", [])
    if "max_violations_per_window" in stats:
        max_window = stats["max_violations_per_window"]
    else:  # a provider that only reports the total
        max_window = stats.get("violations", 0)
    offsets = [o for w in windows for o in w.get("violation_offsets_ms", [])]
    late = sum(1 for o in offsets if o > LATE_VIOLATION_MS)

    warnings = []
    if duplicates:
        extra = sum(duplicates.values()) - len(duplicates)
        warnings.append(f"{len(duplicates)} action_id(s) reached /sent more than once ({extra} extra sends)")
    if missing:
        warnings.append(f"{len(missing)} of {len(seeded)} seeded actions never reached /sent")
    if wrong_recipient:
        warnings.append(f"{len(wrong_recipient)} action(s) sent to a recipient other than the seeded one")
    if max_window > MAX_VIOLATIONS_PER_WINDOW:
        warnings.append(
            f"{max_window} requests arrived inside one Retry-After window (expected at most {MAX_VIOLATIONS_PER_WINDOW})"
        )
    if late > MAX_LATE_VIOLATIONS:
        warnings.append(
            f"{late} requests arrived more than {LATE_VIOLATION_MS} ms into a Retry-After window"
            f" (expected at most {MAX_LATE_VIOLATIONS} over the run)"
        )
    if unknown:
        warnings.append(f"{len(unknown)} action_id(s) in /sent are not in the seed")

    return {
        "clean": not warnings,
        "warnings": warnings,
        "seeded": len(seeded),
        "present": len(present),
        "missing": missing,
        "duplicates": duplicates,
        "wrong_recipient": wrong_recipient,
        "unknown_action_ids": unknown,
        "sends_recorded": len(receipts),
        "violations_total": stats.get("violations", 0),
        "max_violations_per_window": max_window,
        "late_violations": late,
        "retry_after_windows": len(windows),
        "provider_requests": stats.get("requests", {}),
        "provider_responses": stats.get("responses", {}),
    }


def render(result: dict) -> str:
    lines = [
        f"seeded actions            {result['seeded']}",
        f"present in /sent          {result['present']}",
        f"sends recorded            {result['sends_recorded']}",
        f"duplicated action_ids     {len(result['duplicates'])}",
        f"wrong recipient           {len(result['wrong_recipient'])}",
        f"Retry-After windows       {result['retry_after_windows']}",
        f"violations (total)        {result['violations_total']}",
        f"violations (worst window) {result['max_violations_per_window']}",
        f"{f'violations (>{LATE_VIOLATION_MS} ms late)':<26}{result['late_violations']}",
        f"provider requests         {result['provider_requests']}",
        f"provider responses        {result['provider_responses']}",
    ]
    if result["duplicates"]:
        lines.append("duplicates: " + ", ".join(f"{k} x{v}" for k, v in list(result["duplicates"].items())[:10]))
    if result["missing"]:
        lines.append("missing: " + ", ".join(result["missing"][:10]) + (" ..." if len(result["missing"]) > 10 else ""))
    if result["wrong_recipient"]:
        lines.append("wrong recipient: " + ", ".join(result["wrong_recipient"][:10]))
    lines.append("")
    if result["clean"]:
        lines.append("Nothing out of bounds.")
    else:
        lines += ["WARNINGS"] + [f"  - {w}" for w in result["warnings"]]
    return "\n".join(lines)


def main() -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--provider", default=os.environ.get("PROVIDER_URL", "http://127.0.0.1:8080"))
    ap.add_argument("--seed", default=str(here / "seed" / "actions.json"))
    ap.add_argument("--json", action="store_true", help="print the full result as JSON")
    args = ap.parse_args()
    base = args.provider.rstrip("/")

    seed = json.loads(Path(args.seed).read_text(encoding="utf-8"))
    try:
        # /stats first, so this script's own requests can never count against you.
        stats = get_json(base, "/stats")
        if stats.get("retry_after_open"):
            time.sleep(float(stats.get("retry_after_remaining", 0)) + 0.1)
        receipts = fetch_all_receipts(base)
    except ProviderUnavailable as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    result = check(seed, stats, receipts)
    print(json.dumps(result, indent=2) if args.json else render(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
