"""Domain-agnostic durable orchestrator.

    START -> plan -> dispatch --(Send per ready step)--> run_step --> dispatch ... -> finalize -> END

* plan      reads the registry and builds a DAG with dependency gates (planner.py).
* dispatch  is the scheduler. Each pass it fans out every step whose dependencies have all
            succeeded, using Send. Steps in the same pass run in parallel in one super-step.
            After a failure it switches to saga mode and sends compensations one at a time.
* run_step  is the single generic executor: a gate (interrupt), a sub-agent, or a compensator.
* finalize  writes the report.

Durability comes from LangGraph checkpoints (Postgres on Agent Server). Each super-step is a
checkpoint, and each finished parallel task's writes are saved as soon as it finishes. If a worker
dies mid-super-step, resuming the thread re-runs only the tasks that had not finished.
"""

from __future__ import annotations

import operator
from typing import Annotated, Any, Literal, TypedDict

from langgraph.config import get_config, get_store
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy, Send

from durable_orchestrator.context import StepContext
from durable_orchestrator.errors import StepFailure, TransientInfraError
from durable_orchestrator.gates import run_gate
from durable_orchestrator.planner import PlanningError, build_plan
from durable_orchestrator.registry import Registry, load_registry
from durable_orchestrator.saga import compensation_blocked, compensation_stack, next_compensation
from durable_orchestrator.subagent import run_subagent
from durable_orchestrator.timeutil import iso, utcnow

EXECUTIONS_NS = "demo_executions"


def _merge(a: dict | None, b: dict | None) -> dict:
    return {**(a or {}), **(b or {})}


class InputState(TypedDict, total=False):
    goal: str
    target: str | None
    params: dict[str, Any]
    faults: dict[str, Any]


class OrchestratorState(InputState, total=False):
    plan: dict[str, Any]
    results: Annotated[dict[str, Any], _merge]
    compensations: Annotated[dict[str, Any], _merge]
    events: Annotated[list[dict[str, Any]], operator.add]
    round: int
    status: str
    error: str
    report: dict[str, Any]


class StepTask(TypedDict):
    mode: Literal["forward", "compensate"]
    step: dict[str, Any]
    round: int
    params: dict[str, Any]
    faults: dict[str, Any]
    artifacts: dict[str, Any]


def _event(round_: int, step: str, event: str, **detail) -> dict[str, Any]:
    return {"ts": iso(utcnow()), "round": round_, "step": step, "event": event, **detail}


def _artifacts(results: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for r in results.values():
        if r["status"] == "succeeded":
            out.update(r.get("artifacts", {}))
    return out


def _count_execution(store, thread_id: str, step_key: str) -> int:
    """Demo instrumentation: how many times has this step's code actually executed?"""
    ns = (EXECUTIONS_NS, thread_id)
    item = store.get(ns, step_key)
    n = (item.value["count"] if item else 0) + 1
    store.put(ns, step_key, {"count": n, "last_started_at": iso(utcnow())})
    return n


def build_graph(registry: Registry, *, checkpointer=None, store=None):
    def plan(state: OrchestratorState) -> dict:
        try:
            p = build_plan(
                registry,
                goal=state.get("goal", ""),
                target=state.get("target"),
                params=state.get("params", {}),
                now=utcnow(),
            )
        except PlanningError as e:
            return {"status": "failed", "error": str(e), "events": [_event(0, "plan", "planning_failed", error=str(e))]}
        return {
            "plan": p,
            "status": "running",
            "round": 0,
            "events": [_event(0, "plan", "planned", target=p["target"], planner=p["planner"], layers=p["layers"])],
        }

    def dispatch(state: OrchestratorState) -> dict:
        return {"round": state.get("round", 0) + 1}

    def route(state: OrchestratorState):
        plan_ = state.get("plan")
        if not plan_:
            return "finalize"
        results = state.get("results", {})
        comps = state.get("compensations", {})
        base = {
            "round": state["round"],
            "params": state.get("params", {}),
            "faults": state.get("faults", {}),
            "artifacts": _artifacts(results),
        }
        if any(r["status"] == "failed" for r in results.values()):
            if compensation_blocked(comps):
                return "finalize"
            nxt = next_compensation(plan_, results, comps)
            if nxt is None:
                return "finalize"
            return [Send("run_step", {**base, "mode": "compensate", "step": plan_["steps"][nxt]})]

        ready = [
            s
            for s in plan_["steps"].values()
            if s["id"] not in results
            and all(results.get(d, {}).get("status") == "succeeded" for d in s["deps"])
        ]
        if not ready:
            return "finalize"
        return [Send("run_step", {**base, "mode": "forward", "step": s}) for s in ready]

    def run_step(task: StepTask) -> dict:
        config = get_config()
        thread_id = config.get("configurable", {}).get("thread_id", "local")
        store = get_store()
        step, mode = task["step"], task["mode"]
        forward = mode == "forward"
        agent = registry.agents[step["id"] if forward else step["compensated_by"]]
        step_key = step["id"] if forward else f"undo:{step['id']}"
        attempt = _count_execution(store, thread_id, step_key)
        started_at = iso(utcnow())
        ctx = StepContext(
            thread_id=thread_id,
            step_id=step["id"],
            step_key=step_key,
            agent_id=agent.id,
            params=task["params"],
            artifacts=task["artifacts"],
            faults=task["faults"],
            store=store,
        )
        try:
            if agent.kind == "gate":
                payload = run_gate(step, task["artifacts"])
                out = {"summary": f"gate {step['gate']['type']} passed", "tool_results": {}}
            else:
                out = run_subagent(agent, registry, ctx)
                payload = {}
                for r in out["tool_results"].values():
                    if isinstance(r, dict):
                        payload.update({k: v for k, v in r.items() if k != "idempotent_replay"})
            res = {
                "status": "succeeded",
                "artifacts": {a: payload for a in step["provides"]} if forward else {},
                "summary": out["summary"],
                "tool_results": out["tool_results"],
            }
        except StepFailure as e:
            res = {"status": "failed", "error": str(e)}
        res.update(
            agent=agent.id,
            kind=agent.kind,
            mode=mode,
            round=task["round"],
            attempt=attempt,
            started_at=started_at,
            finished_at=iso(utcnow()),
        )
        channel = "results" if forward else "compensations"
        label = ("" if forward else "compensation_") + res["status"]
        return {
            channel: {step["id"]: res},
            "events": [_event(task["round"], step_key, label, attempt=attempt, error=res.get("error"))],
        }

    def finalize(state: OrchestratorState) -> dict:
        plan_ = state.get("plan")
        if not plan_:
            return {"status": "failed", "report": {"status": "failed", "error": state.get("error")}}
        results = state.get("results", {})
        comps = state.get("compensations", {})
        failed = [s for s, r in results.items() if r["status"] == "failed"]
        if not failed and all(results.get(s, {}).get("status") == "succeeded" for s in plan_["steps"]):
            status = "succeeded"
        elif compensation_blocked(comps):
            status = "compensation_failed"
        elif failed and comps and all(s in comps for s in compensation_stack(plan_, results)):
            status = "rolled_back"
        else:
            status = "failed"
        report = {
            "status": status,
            "goal": plan_["goal"],
            "target": plan_["target"],
            "failed_steps": {s: results[s].get("error") for s in failed},
            "steps": [
                {
                    "id": s,
                    "status": results.get(s, {}).get("status", "not_run"),
                    "round": results.get(s, {}).get("round"),
                    "attempt": results.get(s, {}).get("attempt"),
                }
                for layer in plan_["layers"]
                for s in layer
            ],
            "compensations": [
                {"undo": s, "agent": c["agent"], "status": c["status"], "tool_results": c.get("tool_results")}
                for s, c in comps.items()
            ],
        }
        return {"status": status, "report": report, "events": [_event(state.get("round", 0), "finalize", status)]}

    g = StateGraph(OrchestratorState, input_schema=InputState)
    g.add_node("plan", plan)
    g.add_node("dispatch", dispatch)
    g.add_node(
        "run_step",
        run_step,
        retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.5, backoff_factor=2.0, retry_on=TransientInfraError),
    )
    g.add_node("finalize", finalize)
    g.add_edge(START, "plan")
    g.add_edge("plan", "dispatch")
    g.add_conditional_edges("dispatch", route, ["run_step", "finalize"])
    g.add_edge("run_step", "dispatch")
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer, store=store, name="orchestrator")


graph = build_graph(load_registry())
