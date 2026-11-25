"""In-process helpers (tests, evals, the hard-kill script). On Agent Server use the SDK instead."""

from __future__ import annotations

from typing import Any

from langgraph.types import Command

from durable_orchestrator.timeutil import parse_ts
from durable_orchestrator.waker import due_timer_resumes

APPROVE = {"approved": True, "approver": "auto-approver", "comment": "eval run"}


def pending_interrupts(graph, config) -> list[dict[str, Any]]:
    snap = graph.get_state(config)
    return [{"id": i.id, "value": i.value} for i in snap.interrupts]


def drive(
    graph,
    inputs: dict[str, Any] | None,
    *,
    thread_id: str,
    approvals: dict[str, dict] | None = None,
    max_resumes: int = 10,
) -> dict[str, Any]:
    """Run to completion, answering approvals from `approvals` (default approve) and waking timers
    on a simulated clock (jump straight to wake_at)."""
    config = {"configurable": {"thread_id": thread_id}}
    graph.invoke(inputs, config, durability="sync")
    for _ in range(max_resumes):
        pending = pending_interrupts(graph, config)
        if not pending:
            break
        resume: dict[str, Any] = {}
        for intr in pending:
            v = intr["value"]
            if v.get("type") == "approval":
                resume[intr["id"]] = (approvals or {}).get(v["step_id"], APPROVE)
            elif v.get("type") == "timer":
                resume.update(due_timer_resumes([intr], parse_ts(v["wake_at"]), by="simulated-clock")[0])
        graph.invoke(Command(resume=resume), config, durability="sync")
    return graph.get_state(config).values
