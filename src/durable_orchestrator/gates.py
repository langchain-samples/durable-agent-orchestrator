"""Human-in-the-loop and timer gates, built on interrupt().

A gate step pauses the thread with interrupt(payload). The run ends, the thread status becomes
"interrupted", and nothing is consuming a worker while it waits. Any client resumes it later on the
same thread with Command(resume=...). When several gates are paused in parallel, each one has its
own interrupt id and is resumed independently with Command(resume={interrupt_id: value}).

Resume payloads:
  approval: {"approved": true|false, "approver": "...", "comment": "..."}
  timer:    {"action": "wake", "now": "<ISO>", "by": "waker"}     sent by the scheduled waker
            {"action": "override", "by": "<who>"}                   open the window early
            {"action": "cancel", "by": "<who>"}                     abandon the wait (step fails)

interrupt() alone never wakes up on its own. Timer gates are woken by the `waker` graph, run on
an Agent Server cron (or any external trigger); see waker.py.
"""

from __future__ import annotations

from typing import Any

from langgraph.types import interrupt

from .errors import StepFailure
from .timeutil import iso, parse_ts, utcnow


def run_gate(step: dict[str, Any], artifacts: dict[str, Any]) -> dict[str, Any]:
    gate = step["gate"]
    if gate["type"] == "approval":
        return _approval(step, gate, artifacts)
    if gate["type"] == "timer":
        return _timer(step, gate)
    raise StepFailure(f"Unknown gate type {gate['type']!r}")


def _approval(step, gate, artifacts) -> dict[str, Any]:
    payload = {
        "type": "approval",
        "step_id": step["id"],
        "prompt": gate.get("prompt", f"Approve {step['id']}?"),
        "approver_role": gate.get("approver_role"),
        "evidence": sorted(artifacts),
        "resume_with": {"approved": True, "approver": "<name>", "comment": "<optional>"},
    }
    while True:  # each interrupt() call in a loop is a fresh wait; invalid answers re-ask
        answer = interrupt(payload)
        if isinstance(answer, dict) and isinstance(answer.get("approved"), bool):
            break
        payload = {**payload, "error": f"Invalid resume value {answer!r}"}
    if not answer["approved"]:
        raise StepFailure(f"{step['id']} rejected by {answer.get('approver', 'unknown')}: {answer.get('comment', '')}")
    return {
        "approved": True,
        "approver": answer.get("approver"),
        "comment": answer.get("comment"),
        "decided_at": iso(utcnow()),
    }


def _timer(step, gate) -> dict[str, Any]:
    wake_at = parse_ts(gate["wake_at"])
    payload = {
        "type": "timer",
        "step_id": step["id"],
        "wake_at": gate["wake_at"],
        "resume_with": {"action": "wake", "now": "<ISO timestamp>", "by": "waker"},
    }
    while True:
        answer = interrupt(payload)
        action = answer.get("action") if isinstance(answer, dict) else None
        if action == "cancel":
            raise StepFailure(f"{step['id']} wait cancelled by {answer.get('by', 'unknown')}")
        if action == "override":
            return {"opened_at": iso(utcnow()), "opened_by": answer.get("by"), "override": True}
        if action == "wake" and parse_ts(answer["now"]) >= wake_at:
            return {"opened_at": answer["now"], "opened_by": answer.get("by", "waker"), "override": False}
        payload = {**payload, "error": f"Woken before wake_at ({answer!r}); still waiting"}
