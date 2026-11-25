"""Tools that operate the durable upgrade orchestrator (a separate Agent Server deployment) over the SDK.

The orchestrator owns durability: plans, parallel phases, interrupt() gates, timers, retries and
saga rollback. This agent is the conversational and proactive layer on top. It never touches
infrastructure directly, only the orchestrator's API.

Config:
  ORCHESTRATOR_URL                       e.g. http://127.0.0.1:2024 or https://<deployment>.langgraph.app
  connection "orchestrator-api" (agent) API key for a hosted orchestrator (mda connections create).
                                         Locally: MDA_DEV_ORCHESTRATOR_API, or leave unset for langgraph dev.
"""

from __future__ import annotations

import os
from typing import Any, Literal

from langchain.tools import tool
from langgraph_sdk import get_client
from managed_deepagents import ManagedToolRuntime, connections

ASSISTANT = "orchestrator"

DEMO_FAULTS: dict[str, dict[str, Any]] = {
    "none": {},
    "kill_mid_backup": {"kill": {"step": "P2_backup_verify", "before_tool": "platform_record_known_good"}},
    "lose_upgrade_response": {"lose_response": {"tool": "platform_upgrade_control_plane", "times": 1}},
    "fail_upgrade": {"fail": {"tool": "platform_upgrade_workers"}},
}


async def _client():
    try:
        api_key = await connections.get("orchestrator-api", {"type": "agent"})
    except Exception:
        api_key = os.environ.get("ORCHESTRATOR_API_KEY") or None
    headers = {}
    if os.environ.get("ORCHESTRATOR_WORKSPACE_ID"):
        headers["X-Tenant-Id"] = os.environ["ORCHESTRATOR_WORKSPACE_ID"]
    return get_client(
        url=os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:2024"),
        api_key=api_key,
        headers=headers or None,
    )


def _params(cluster_id, target_version, maintenance_window_delay, maintenance_window_start) -> dict:
    p = {
        "cluster_id": cluster_id,
        "target_version": target_version,
        "maintenance_window_delay": maintenance_window_delay,
        "maintenance_window_start": maintenance_window_start,
    }
    return {k: v for k, v in p.items() if v}


def _pending(thread: dict) -> list[dict]:
    out = []
    for lst in (thread.get("interrupts") or {}).values():
        for i in lst:
            v = i.get("value") or {}
            out.append(
                {
                    "step_id": v.get("step_id"),
                    "type": v.get("type"),
                    "prompt": v.get("prompt"),
                    "approver_role": v.get("approver_role"),
                    "wake_at": v.get("wake_at"),
                    "error": v.get("error"),
                }
            )
    return out


def _summary(thread: dict) -> dict:
    values = thread.get("values") or {}
    results = values.get("results") or {}
    report = values.get("report") or {}
    return {
        "thread_id": thread["thread_id"],
        "thread_status": thread["status"],
        "workflow_status": values.get("status"),
        "goal": values.get("goal"),
        "cluster_id": (values.get("params") or {}).get("cluster_id"),
        "completed_steps": {k: r["status"] for k, r in results.items()},
        "pending_gates": _pending(thread),
        "failed_steps": report.get("failed_steps") or {k: r.get("error") for k, r in results.items() if r["status"] == "failed"},
        "compensations": {k: c["status"] for k, c in (values.get("compensations") or {}).items()},
    }


async def _wait_and_summarize(client, thread_id: str, run: dict) -> dict:
    try:
        await client.runs.join(thread_id, run["run_id"])
    except Exception as e:  # a crashed run (kill switch) surfaces here; the thread keeps its checkpoint
        summary = _summary(await client.threads.get(thread_id))
        summary["run_error"] = str(e)[:300]
        return summary
    return _summary(await client.threads.get(thread_id))


@tool
async def list_workflow_goals() -> list[dict]:
    """List the goals the orchestrator's agent registry can plan for, with their required parameters."""
    client = await _client()
    out = await client.runs.wait(None, "planner", input={})
    return out["goals"]


@tool(parse_docstring=True)
async def preview_plan(
    goal: str,
    cluster_id: str,
    target_version: str | None = None,
    maintenance_window_delay: str | None = None,
    maintenance_window_start: str | None = None,
) -> dict:
    """Preview the dependency-gated plan the orchestrator would run, without starting anything.

    Args:
        goal: What to do, in plain language, e.g. "Upgrade the AI platform cluster to 1.9.0".
        cluster_id: Target cluster, e.g. "aip-prod-01".
        target_version: Target version for upgrades, e.g. "1.9.0".
        maintenance_window_delay: Time until the maintenance window opens, e.g. "10d", "2h", "90s".
        maintenance_window_start: Absolute window start (ISO-8601). Use instead of the delay.
    """
    client = await _client()
    params = _params(cluster_id, target_version, maintenance_window_delay, maintenance_window_start)
    out = await client.runs.wait(None, "planner", input={"goal": goal, "params": params})
    if out.get("error"):
        return {"error": out["error"], "goals": out["goals"]}
    plan = out["plan"]
    return {
        "target": plan["target"],
        "layers": plan["layers"],
        "gates": [
            {"step_id": s["id"], **{k: s["gate"].get(k) for k in ("type", "wake_at", "approver_role")}}
            for s in plan["steps"].values()
            if s["kind"] == "gate"
        ],
        "compensated_steps": {s["id"]: s["compensated_by"] for s in plan["steps"].values() if s.get("compensated_by")},
    }


@tool(parse_docstring=True)
async def start_workflow(
    goal: str,
    cluster_id: str,
    runtime: ManagedToolRuntime,
    target_version: str | None = None,
    maintenance_window_delay: str | None = None,
    maintenance_window_start: str | None = None,
    demo_fault: Literal["none", "kill_mid_backup", "lose_upgrade_response", "fail_upgrade"] = "none",
) -> dict:
    """Start a durable orchestrator workflow. Runs until its first human or timer gate, then returns.

    Args:
        goal: What to do, in plain language, e.g. "Upgrade the AI platform cluster to 1.9.0".
        cluster_id: Target cluster, e.g. "aip-prod-01".
        target_version: Target version for upgrades.
        maintenance_window_delay: Time until the maintenance window opens, e.g. "10d".
        maintenance_window_start: Absolute window start (ISO-8601). Use instead of the delay.
        demo_fault: DEMO ONLY. Inject a failure to show recovery. Use "none" unless the user asks for a failure demo.
    """
    client = await _client()
    params = _params(cluster_id, target_version, maintenance_window_delay, maintenance_window_start)
    operator_thread = runtime.thread.id if getattr(runtime, "thread", None) else None
    thread = await client.threads.create(metadata={"started_by": "upgrade-operator", "operator_thread_id": operator_thread})
    run = await client.runs.create(
        thread["thread_id"],
        ASSISTANT,
        input={"goal": goal, "params": params, "faults": DEMO_FAULTS[demo_fault]},
        metadata={"started_by": "upgrade-operator", "operator_thread_id": operator_thread},
        durability="sync",
    )
    return await _wait_and_summarize(client, thread["thread_id"], run)


@tool(parse_docstring=True)
async def get_workflow_status(thread_id: str) -> dict:
    """Get the current status of an orchestrator workflow: completed steps, pending gates, failures, rollbacks.

    Args:
        thread_id: The orchestrator thread ID returned by start_workflow.
    """
    client = await _client()
    return _summary(await client.threads.get(thread_id))


@tool(parse_docstring=True)
async def get_workflow_events(thread_id: str) -> dict:
    """Get the full event timeline, final report and compensation details of a workflow (for investigations).

    Args:
        thread_id: The orchestrator thread ID.
    """
    client = await _client()
    values = (await client.threads.get(thread_id)).get("values") or {}
    return {
        "events": values.get("events", []),
        "report": values.get("report"),
        "compensations": values.get("compensations"),
        "plan_layers": (values.get("plan") or {}).get("layers"),
    }


@tool(parse_docstring=True)
async def list_active_workflows(limit: int = 20) -> list[dict]:
    """List orchestrator workflows that are waiting on a gate, running, or crashed.

    Args:
        limit: Maximum number of workflows per status to return.
    """
    client = await _client()
    out = []
    for status in ("interrupted", "busy", "error"):
        threads = await client.threads.search(metadata={"graph_id": ASSISTANT}, status=status, limit=limit)
        out += [_summary(t) for t in threads]
    return out


async def _resume_gate(thread_id: str, step_id: str, gate_type: str, value: dict) -> dict:
    client = await _client()
    thread = await client.threads.get(thread_id)
    match = [
        i
        for lst in (thread.get("interrupts") or {}).values()
        for i in lst
        if (i.get("value") or {}).get("step_id") == step_id and i["value"].get("type") == gate_type
    ]
    if not match:
        return {"error": f"No pending {gate_type} gate {step_id!r} on {thread_id}", **_summary(thread)}
    run = await client.runs.create(
        thread_id, ASSISTANT, command={"resume": {match[0]["id"]: value}}, multitask_strategy="enqueue", durability="sync"
    )
    return await _wait_and_summarize(client, thread_id, run)


@tool(parse_docstring=True)
async def decide_approval_gate(thread_id: str, step_id: str, approved: bool, approver: str, comment: str) -> dict:
    """Approve or reject a pending approval gate (e.g. the change-advisory-board gate) on a workflow.

    Args:
        thread_id: The orchestrator thread ID.
        step_id: The approval gate's step ID, e.g. "P4a_change_approval".
        approved: True to approve, False to reject (a rejection triggers rollback of committed steps).
        approver: Name of the human who made the decision.
        comment: Change ticket and rationale, e.g. "CHG-4711 approved by CAB".
    """
    return await _resume_gate(
        thread_id, step_id, "approval", {"approved": approved, "approver": approver, "comment": comment}
    )


@tool(parse_docstring=True)
async def open_maintenance_window_now(thread_id: str, step_id: str, opened_by: str, reason: str) -> dict:
    """Open a timer gate (maintenance window) early instead of waiting for its scheduled time.

    Args:
        thread_id: The orchestrator thread ID.
        step_id: The timer gate's step ID, e.g. "P4b_maintenance_window".
        opened_by: Name of the human authorising the early start.
        reason: Why the window is opened early.
    """
    return await _resume_gate(thread_id, step_id, "timer", {"action": "override", "by": f"{opened_by}: {reason}"})


@tool(parse_docstring=True)
async def cancel_maintenance_window(thread_id: str, step_id: str, cancelled_by: str, reason: str) -> dict:
    """Cancel a pending timer gate. The workflow fails that step and rolls back committed steps.

    Args:
        thread_id: The orchestrator thread ID.
        step_id: The timer gate's step ID.
        cancelled_by: Name of the human cancelling.
        reason: Why.
    """
    return await _resume_gate(thread_id, step_id, "timer", {"action": "cancel", "by": f"{cancelled_by}: {reason}"})


@tool(parse_docstring=True)
async def resume_crashed_workflow(thread_id: str) -> dict:
    """Resume a workflow whose run crashed (thread status "error") from its last checkpoint.

    Finished steps are not re-run; idempotent tools absorb any repeated side effects.

    Args:
        thread_id: The orchestrator thread ID.
    """
    client = await _client()
    thread = await client.threads.get(thread_id)
    if thread["status"] != "error":
        return {"error": f"Thread is {thread['status']!r}, not 'error'; nothing to resume.", **_summary(thread)}
    run = await client.runs.create(thread_id, ASSISTANT, input=None, durability="sync")
    return await _wait_and_summarize(client, thread_id, run)


@tool(parse_docstring=True)
async def inspect_cluster(cluster_id: str) -> dict:
    """Read the (mock) AI platform cluster state: versions, health, staged bundle, known-good snapshot.

    Args:
        cluster_id: The cluster ID.
    """
    client = await _client()
    item = await client.store.get_item(["mock_platform", "clusters"], cluster_id)
    if not item:
        return {
            "cluster_id": cluster_id,
            "status": "not_inventoried",
            "note": "No inventory recorded yet (mock AI platform). P0 discovery inventories the cluster when a workflow starts.",
        }
    v = item["value"]
    return {
        "version": v["version"],
        "control_plane_version": v["control_plane_version"],
        "node_versions": {n["name"]: n["version"] for n in v["nodes"]},
        "health": v["health"],
        "staged_bundle": v["staged_bundle"],
        "known_good": v["known_good"],
    }


READ_TOOLS = [list_workflow_goals, preview_plan, get_workflow_status, get_workflow_events, list_active_workflows, inspect_cluster]
WRITE_TOOLS = [start_workflow, decide_approval_gate, open_maintenance_window_now, cancel_maintenance_window, resume_crashed_workflow]
