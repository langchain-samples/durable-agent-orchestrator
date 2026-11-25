"""Saga compensation.

================================================================================================
DESIGN PATTERN, NOT A BUILT-IN FEATURE.
LangGraph and Agent Server do not ship a saga / compensation primitive. What they provide is the
durable substrate that makes the pattern easy to build correctly:
  * every completed step is checkpointed, so we know exactly which steps committed;
  * compensations run as ordinary checkpointed steps, so a crash during rollback resumes the
    rollback rather than restarting or skipping it;
  * idempotent tools make re-running a half-finished compensation safe.
The policy below (LIFO, one at a time, stop on a failed compensation) is ours, written in
~40 lines of application code, and can be changed freely.
================================================================================================

Policy:
1. A forward step fails with StepFailure (non-retryable).
2. Stop scheduling forward work.
3. Collect every step that committed side effects and declares `compensated_by`:
   - succeeded steps, plus
   - the failed step itself if it declares `compensate_on_failure` (partial side effects).
4. Run their compensators in reverse completion order (last done, first undone), one per
   super-step, each checkpointed.
5. If a compensator fails, stop and surface the thread for a human (status "compensation_failed").
"""

from __future__ import annotations

from typing import Any


def compensation_stack(plan: dict[str, Any], results: dict[str, Any]) -> list[str]:
    """Forward step ids to undo, in the order they should be undone."""
    candidates = []
    for step_id, res in results.items():
        step = plan["steps"].get(step_id)
        if not step or not step.get("compensated_by"):
            continue
        if res["status"] == "succeeded" or (res["status"] == "failed" and step.get("compensate_on_failure")):
            candidates.append((res["round"], res.get("finished_at") or "", step_id))
    return [s for _, _, s in sorted(candidates, reverse=True)]


def next_compensation(plan: dict[str, Any], results: dict[str, Any], compensations: dict[str, Any]) -> str | None:
    for step_id in compensation_stack(plan, results):
        if step_id not in compensations:
            return step_id
    return None


def compensation_blocked(compensations: dict[str, Any]) -> bool:
    return any(c["status"] == "failed" for c in compensations.values())
