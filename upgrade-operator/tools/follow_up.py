"""Goal-oriented follow-through with MDA runtime schedules.

A workflow can wait days for a maintenance window or an approval. Instead of a person remembering
to check back, the agent schedules its own progress checks on the same thread and stops them once
the workflow reaches a terminal state.
"""

from __future__ import annotations

from langchain.tools import tool
from managed_deepagents import ManagedToolRuntime

OWNER = {"type": "agent"}


@tool(parse_docstring=True)
async def watch_workflow(thread_id: str, cron: str, runtime: ManagedToolRuntime) -> str:
    """Schedule recurring progress checks of an orchestrator workflow on this conversation thread.

    Args:
        thread_id: The orchestrator thread ID to watch.
        cron: Five-field cron expression, no more often than every 30 minutes (e.g. "0 * * * *").
    """
    minute = cron.split()[0]
    if minute == "*" or minute.startswith("*/") and int(minute[2:] or 1) < 30:
        return "Refused: checks must be at least 30 minutes apart; each check is a model run."
    # Agent-owned schedules can continue the same thread only for channel (e.g. Slack) conversations.
    # Elsewhere each check runs on a fresh thread and keeps continuity through agent memory.
    in_channel = getattr(getattr(runtime, "thread", None), "origin", None) is not None
    prompt = (
        f"Scheduled check of AI platform workflow {thread_id}. Call get_workflow_status. "
        f"Compare with the latest entry for {thread_id} in /memories/agent/watch-log.md and append one "
        "timestamped line only if something changed. If it has waited on an approval for more than a day, "
        "say who needs to act. If it reached a terminal state (succeeded, rolled_back, failed, "
        "compensation_failed), report the outcome, follow the rollback-triage skill if it did not succeed, "
        "and call stop_watching."
    )
    item = await runtime.schedules.create(
        owner=OWNER,
        cron=cron,
        new_thread=not in_channel,
        prompt=prompt,
        metadata={"orchestrator_thread_id": thread_id},
    )
    await runtime.schedules.update(
        item["id"], {"prompt": f"{prompt} This schedule's ID is {item['id']}."}, owner=OWNER
    )
    where = "replies in this conversation" if in_channel else "each check runs on its own thread and logs changes to /memories/agent/watch-log.md"
    return f"Watching {thread_id} with schedule {item['id']} ({cron}); {where}."


@tool(parse_docstring=True)
async def stop_watching(schedule_id: str, runtime: ManagedToolRuntime) -> str:
    """Stop the recurring progress checks for a workflow that finished or was abandoned.

    Args:
        schedule_id: The schedule ID returned by watch_workflow.
    """
    await runtime.schedules.delete(schedule_id, owner=OWNER)
    return f"Stopped schedule {schedule_id}."


@tool
async def list_watches(runtime: ManagedToolRuntime) -> list[dict]:
    """List this agent's active workflow watches and declared schedules."""
    page = await runtime.schedules.list(owner=OWNER, limit=50)
    return [
        {k: s.get(k) for k in ("id", "cron", "at", "paused", "metadata", "next_run_date")}
        for s in page.get("items", [])
    ]
