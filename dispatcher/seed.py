"""`make seed`: create the schema if missing and load seed/actions.json.
Idempotent; running it twice inserts nothing new."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from . import db
from .config import Config


def main() -> int:
    cfg = Config.from_env()
    seed_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "seed" / "actions.json"
    actions = json.loads(seed_path.read_text(encoding="utf-8"))

    with db.connect(cfg.database_url) as conn:
        db.ensure_schema(conn)
        inserted = db.insert_actions(conn, ((a["id"], a["recipient"], a["body"]) for a in actions))
        total = conn.execute("SELECT count(*) AS n FROM actions").fetchone()["n"]

    print(f"seed: {len(actions)} actions in file, {inserted} inserted, {total} in table")
    return 0


if __name__ == "__main__":
    sys.exit(main())
