"""External trigger for timed wakes: run the `waker` graph once.

Use this from any scheduler you already have (system cron, CI, an event bus) instead of, or as well
as, an Agent Server cron.

    uv run python scripts/trigger_waker.py                     # wake anything that is due now
    uv run python scripts/trigger_waker.py --advance 10d       # DEMO ONLY: pretend 10 days passed
    uv run python scripts/trigger_waker.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os

from langgraph_sdk import get_sync_client


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("LANGGRAPH_URL", "http://127.0.0.1:2024"))
    ap.add_argument("--advance", help="demo clock skew, e.g. 10d")
    ap.add_argument("--simulate-now", help="demo clock override (ISO-8601)")
    ap.add_argument("--thread", action="append", dest="thread_ids")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client = get_sync_client(
        url=args.url,
        api_key=os.environ.get("LANGSMITH_API_KEY"),
        headers={"X-Tenant-Id": ws} if (ws := os.environ.get("LANGSMITH_WORKSPACE_ID")) else None,
    )
    payload = {k: v for k, v in {
        "advance": args.advance,
        "simulate_now": args.simulate_now,
        "thread_ids": args.thread_ids,
        "dry_run": args.dry_run or None,
    }.items() if v}
    print(json.dumps(client.runs.wait(None, "waker", input=payload), indent=2))


if __name__ == "__main__":
    main()
