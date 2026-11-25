"""Hard-kill demo: the process really dies (os._exit) mid-node, then a NEW process resumes the thread.

Uses SQLite for the checkpointer and store so state survives process death on a laptop. On Agent
Server the same checkpoints live in Postgres.

    uv run python scripts/hard_kill_demo.py run      # dies with exit code 137 during P2
    uv run python scripts/hard_kill_demo.py resume   # new process picks up from the last checkpoint
    uv run python scripts/hard_kill_demo.py status
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.store.sqlite import SqliteStore

from durable_orchestrator.graph import EXECUTIONS_NS, build_graph
from durable_orchestrator.registry import load_registry

INPUT = {
    "goal": "Upgrade the AI platform cluster to 1.9.0",
    "params": {"cluster_id": "aip-hardkill", "target_version": "1.9.0", "maintenance_window_delay": "10d"},
    "faults": {"kill": {"step": "P2_backup_verify", "before_tool": "platform_record_known_good", "mode": "process"}},
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["run", "resume", "status"])
    ap.add_argument("--db", default=".demo/hardkill.sqlite")
    ap.add_argument("--thread", default="hardkill-1")
    args = ap.parse_args()
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    if args.action == "run":
        for suffix in ("", "-wal", "-shm"):
            Path(args.db + suffix).unlink(missing_ok=True)

    with SqliteSaver.from_conn_string(args.db) as saver, SqliteStore.from_conn_string(args.db) as store:
        store.setup()
        graph = build_graph(load_registry(), checkpointer=saver, store=store)
        config = {"configurable": {"thread_id": args.thread}}
        if args.action == "run":
            print("starting run; the kill switch will os._exit(137) inside P2_backup_verify ...", flush=True)
            graph.invoke(INPUT, config, durability="sync")
        elif args.action == "resume":
            print("new process: resuming thread from its last checkpoint ...", flush=True)
            graph.invoke(None, config, durability="sync")
        snap = graph.get_state(config)
        executions = {i.key: i.value["count"] for i in store.search((EXECUTIONS_NS, args.thread), limit=100)}
        print(
            json.dumps(
                {
                    "next": list(snap.next),
                    "completed_steps": sorted((snap.values.get("results") or {})),
                    "waiting_on": [i.value.get("step_id") for i in snap.interrupts],
                    "executions": executions,
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    sys.exit(main())
