"""Registry-driven planner.

1. Pick the goal artifact: explicit `target` > LLM choice (if PLANNER_MODEL is set) > keyword match.
2. Backward-chain from the goal to every agent needed to produce it.
3. Dependency gates = edges from each agent to the providers of the artifacts it requires.
4. Topologically layer the DAG. Steps in the same layer can run in parallel.

Nothing here is specific to the AI platform.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .registry import Registry
from .timeutil import iso, parse_duration, parse_ts


class PlanningError(ValueError):
    pass


class _GoalChoice(BaseModel):
    artifact: str = Field(description="One of the goal artifacts from the registry")
    rationale: str


def choose_target(registry: Registry, goal: str, target: str | None) -> tuple[str, str]:
    if target:
        if target not in registry.goals:
            raise PlanningError(f"Unknown target {target!r}. Known goals: {list(registry.goals)}")
        return target, "explicit"

    model_name = os.environ.get("PLANNER_MODEL")
    if model_name:
        from langchain.chat_models import init_chat_model

        llm = init_chat_model(model_name).with_structured_output(_GoalChoice)
        choice = llm.invoke(
            "Pick the registry goal that satisfies the request. Answer with one goal artifact.\n\n"
            f"{registry.describe()}\n\nREQUEST: {goal}"
        )
        if choice.artifact in registry.goals:
            return choice.artifact, "llm"

    words = set(re.findall(r"[a-z]+", goal.lower()))
    scored = sorted(
        ((len(words & set(g.keywords)), g.artifact) for g in registry.goals.values()), reverse=True
    )
    if not scored or scored[0][0] == 0:
        raise PlanningError(f"Could not map goal {goal!r} to a registry goal; pass `target`.")
    return scored[0][1], "keyword"


def build_plan(
    registry: Registry,
    *,
    goal: str,
    target: str | None,
    params: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    target, planner = choose_target(registry, goal, target)

    missing = [p for p in registry.goals[target].required_params if p not in params]
    if missing:
        raise PlanningError(f"Goal {target!r} needs params {missing}")

    # Backward chaining from the goal artifact.
    steps: dict[str, dict[str, Any]] = {}
    pending = [target]
    seen_artifacts: set[str] = set()
    while pending:
        artifact = pending.pop()
        if artifact in seen_artifacts:
            continue
        seen_artifacts.add(artifact)
        providers = registry.providers_of(artifact)
        if len(providers) != 1:
            raise PlanningError(f"Artifact {artifact!r} needs exactly one provider, found {len(providers)}")
        agent = providers[0]
        if agent.id in steps:
            continue
        deps = sorted({registry.providers_of(r)[0].id for r in agent.requires})
        step: dict[str, Any] = {
            "id": agent.id,
            "name": agent.name,
            "kind": agent.kind,
            "phase": agent.phase,
            "deps": deps,
            "requires": list(agent.requires),
            "provides": list(agent.provides),
            "compensated_by": agent.compensated_by,
            "compensate_on_failure": agent.compensate_on_failure,
        }
        if agent.kind == "gate":
            step["gate"] = _resolve_gate(agent.id, dict(agent.gate or {}), params, now)
        steps[agent.id] = step
        pending.extend(agent.requires)

    layers = _layers(steps)
    return {
        "goal": goal,
        "target": target,
        "planner": planner,
        "planned_at": iso(now),
        "steps": steps,
        "layers": layers,
    }


def _resolve_gate(step_id: str, gate: dict[str, Any], params: dict[str, Any], now: datetime) -> dict:
    """Timer gates get an absolute wake_at, fixed once at planning time (deterministic on replay)."""
    if gate["type"] == "timer":
        at_param, delay_param = gate.get("at_param"), gate.get("delay_param")
        if at_param and params.get(at_param):
            gate["wake_at"] = iso(parse_ts(params[at_param]))
        elif delay_param and params.get(delay_param) is not None:
            gate["wake_at"] = iso(now + parse_duration(params[delay_param]))
        else:
            raise PlanningError(f"{step_id}: timer gate needs param {at_param!r} or {delay_param!r}")
    return gate


def _layers(steps: dict[str, dict[str, Any]]) -> list[list[str]]:
    indeg = {s: len(v["deps"]) for s, v in steps.items()}
    children: dict[str, list[str]] = {s: [] for s in steps}
    for s, v in steps.items():
        for d in v["deps"]:
            children[d].append(s)
    layer = sorted(s for s, n in indeg.items() if n == 0)
    layers, placed = [], 0
    while layer:
        layers.append(layer)
        placed += len(layer)
        nxt = []
        for s in layer:
            for c in children[s]:
                indeg[c] -= 1
                if indeg[c] == 0:
                    nxt.append(c)
        layer = sorted(nxt)
    if placed != len(steps):
        raise PlanningError("Dependency cycle in registry")
    return layers


def plan_to_mermaid(plan: dict[str, Any]) -> str:
    lines = ["flowchart LR"]
    for s in plan["steps"].values():
        label = f"{s['id']}"
        shape = f"{{{{{label}}}}}" if s["kind"] == "gate" else f"[{label}]"
        lines.append(f"  {s['id']}{shape}")
        for d in s["deps"]:
            lines.append(f"  {d} --> {s['id']}")
    return "\n".join(lines)
