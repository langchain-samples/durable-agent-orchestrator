"""Timed wake for long waits (e.g. a maintenance window 10 days out).

interrupt() only resumes when someone calls the thread with Command(resume=...). Nothing inside
the paused thread can wake itself up. So the wake-up comes from outside:

  Agent Server cron  --(every N minutes)-->  `waker` graph  --(SDK)-->  Command(resume={id: wake})

The waker is stateless: it searches for interrupted orchestrator threads, finds timer interrupts
whose wake_at has passed, and resumes exactly those interrupt ids on the same thread. Approval
interrupts on the same thread are left alone, so parallel gates resume independently.

The same graph can be run by any external trigger (system cron, CI, an event bus):
`scripts/trigger_waker.py`. `simulate_now` / `advance` let a demo jump the clock forward; leave
them unset in production.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph_sdk import get_client

from durable_orchestrator.timeutil import iso, parse_duration, parse_ts, utcnow


class WakerState(TypedDict, total=False):
    graph_id: str
    simulate_now: str
    advance: str
    dry_run: bool
    thread_ids: list[str]
    now: str
    checked: int
    woken: list[dict[str, Any]]
    waiting: list[dict[str, Any]]


def due_timer_resumes(interrupts: list[dict[str, Any]], now: datetime, by: str = "waker") -> tuple[dict, list]:
    """interrupts: [{"id": ..., "value": {...}}]. Returns ({interrupt_id: resume_value}, still_waiting)."""
    resume, waiting = {}, []
    for intr in interrupts:
        value = intr.get("value") or {}
        if not isinstance(value, dict) or value.get("type") != "timer":
            continue
        if parse_ts(value["wake_at"]) <= now:
            resume[intr["id"]] = {"action": "wake", "now": iso(now), "by": by}
        else:
            waiting.append({"interrupt_id": intr["id"], "step_id": value.get("step_id"), "wake_at": value["wake_at"]})
    return resume, waiting


def _flatten(thread_interrupts: Any) -> list[dict[str, Any]]:
    if isinstance(thread_interrupts, dict):
        return [i for lst in thread_interrupts.values() for i in lst]
    return list(thread_interrupts or [])


async def sweep(state: WakerState) -> dict:
    now = parse_ts(state["simulate_now"]) if state.get("simulate_now") else utcnow()
    if state.get("advance"):
        now = now + parse_duration(state["advance"])
    graph_id = state.get("graph_id", "orchestrator")
    client = get_client()  # no URL: talks to this same Agent Server deployment in-process

    threads: list[dict] = []
    offset = 0
    while True:
        page = await client.threads.search(
            status="interrupted", metadata={"graph_id": graph_id}, limit=100, offset=offset
        )
        threads += page
        if len(page) < 100:
            break
        offset += 100
    if state.get("thread_ids"):
        threads = [t for t in threads if t["thread_id"] in state["thread_ids"]]

    woken, waiting = [], []
    for t in threads:
        resume, still = due_timer_resumes(_flatten(t.get("interrupts")), now)
        waiting += [{"thread_id": t["thread_id"], **w} for w in still]
        if not resume:
            continue
        if not state.get("dry_run"):
            await client.runs.create(
                t["thread_id"],
                graph_id,
                command={"resume": resume},
                multitask_strategy="enqueue",
                durability="sync",
            )
        woken.append({"thread_id": t["thread_id"], "interrupt_ids": list(resume)})
    return {"now": iso(now), "checked": len(threads), "woken": woken, "waiting": waiting}


_g = StateGraph(WakerState)
_g.add_node("sweep", sweep)
_g.add_edge(START, "sweep")
_g.add_edge("sweep", END)
graph = _g.compile(name="waker")
