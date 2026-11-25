"""Change policy as code, enforced on every tool call regardless of what the model was told.

* Approving a gate requires a change ticket (CHG-<number>) in the comment.
* Opening a maintenance window early requires a written reason.

Runs after the human-in-the-loop approval (interrupt_on), so even an approved call is rejected if it
violates policy.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable

from langchain.agents.middleware import wrap_tool_call
from langchain.messages import ToolMessage
from langchain.tools.tool_node import ToolCallRequest
from langgraph.types import Command

TICKET = re.compile(r"\bCHG-\d+\b")


def _violation(name: str, args: dict) -> str | None:
    if name == "decide_approval_gate" and args.get("approved") and not TICKET.search(args.get("comment", "")):
        return "Policy violation: approvals must reference a change ticket (CHG-<number>) in the comment."
    if name == "open_maintenance_window_now" and len(args.get("reason", "").strip()) < 15:
        return "Policy violation: opening a maintenance window early needs a written reason (15+ characters)."
    return None


@wrap_tool_call
async def enforce_change_policy(
    request: ToolCallRequest,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
) -> ToolMessage | Command:
    call = request.tool_call
    problem = _violation(call["name"], call.get("args") or {})
    if problem:
        return ToolMessage(content=problem, tool_call_id=call["id"], name=call["name"], status="error")
    return await handler(request)
