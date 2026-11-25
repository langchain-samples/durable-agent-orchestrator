"""Register (or remove) the Agent Server cron that runs the `waker` graph.

    uv run python scripts/create_cron.py                         # every minute
    uv run python scripts/create_cron.py --schedule "*/5 * * * *"
    uv run python scripts/create_cron.py --list
    uv run python scripts/create_cron.py --delete-all

Agent Server stores the cron and fires it on schedule; each tick is a normal traced run.
"""

from __future__ import annotations

import argparse
import json
import os

from langgraph_sdk import get_sync_client


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("LANGGRAPH_URL", "http://127.0.0.1:2024"))
    ap.add_argument("--schedule", default="* * * * *")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--delete-all", action="store_true")
    args = ap.parse_args()

    client = get_sync_client(
        url=args.url,
        api_key=os.environ.get("LANGSMITH_API_KEY"),
        headers={"X-Tenant-Id": ws} if (ws := os.environ.get("LANGSMITH_WORKSPACE_ID")) else None,
    )
    waker_ids = [a["assistant_id"] for a in client.assistants.search(graph_id="waker")]
    crons = [c for aid in waker_ids for c in client.crons.search(assistant_id=aid)]
    if args.list:
        print(json.dumps([{k: c.get(k) for k in ("cron_id", "schedule", "next_run_date")} for c in crons], indent=2))
        return
    if args.delete_all:
        for c in crons:
            client.crons.delete(c["cron_id"])
        print(f"deleted {len(crons)} waker cron(s)")
        return
    cron = client.crons.create("waker", schedule=args.schedule, input={})
    print(json.dumps({k: cron.get(k) for k in ("cron_id", "schedule", "next_run_date")}, indent=2))


if __name__ == "__main__":
    main()
