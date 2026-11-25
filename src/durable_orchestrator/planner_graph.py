"""Read-only `planner` graph: preview a plan, or list the registry's goals, without starting a workflow.

Used by the Managed Deep Agents operator (upgrade-operator/) so it never needs a copy of the registry.
"""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from durable_orchestrator.planner import PlanningError, build_plan
from durable_orchestrator.registry import load_registry
from durable_orchestrator.timeutil import utcnow

REGISTRY = load_registry()


class PlannerState(TypedDict, total=False):
    goal: str
    target: str | None
    params: dict[str, Any]
    goals: list[dict[str, Any]]
    plan: dict[str, Any]
    error: str


def preview(state: PlannerState) -> dict:
    goals = [
        {"artifact": g.artifact, "description": g.description, "required_params": list(g.required_params)}
        for g in REGISTRY.goals.values()
    ]
    if not state.get("goal") and not state.get("target"):
        return {"goals": goals}
    try:
        plan = build_plan(
            REGISTRY,
            goal=state.get("goal", ""),
            target=state.get("target"),
            params=state.get("params", {}),
            now=utcnow(),
        )
    except PlanningError as e:
        return {"goals": goals, "error": str(e)}
    return {"goals": goals, "plan": plan}


_g = StateGraph(PlannerState)
_g.add_node("preview", preview)
_g.add_edge(START, "preview")
_g.add_edge("preview", END)
graph = _g.compile(name="planner")
