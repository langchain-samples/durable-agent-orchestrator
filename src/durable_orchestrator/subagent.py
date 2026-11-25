"""Generic sub-agent factory: one small tool-calling agent per registry entry.

Model selection:
* SUBAGENT_MODEL set (e.g. "anthropic:claude-haiku-4-5", "openai:gpt-4.1-mini") -> a real LLM agent.
* unset -> ScriptedToolCaller, a deterministic stand-in that calls each tool once, in order.
  Keeps the durability demo reproducible and runnable with no model keys.

A step is the unit of durability (like a Temporal activity): the sub-agent runs with
checkpointer=False inside the orchestrator's `run_step` node. If the step is retried or resumed
it runs again from the top, and the idempotent tools turn repeated side effects into no-ops.
"""

from __future__ import annotations

import json
import os
import uuid
import warnings
from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from . import faults
from .context import StepContext
from .errors import StepFailure
from .registry import AgentSpec, Registry, ToolSpec

warnings.filterwarnings("ignore", message="`durability` has no effect")


class ScriptedToolCaller(BaseChatModel):
    """Deterministic chat model: calls every bound tool once, in order, then summarises."""

    tool_names: list[str] = []

    @property
    def _llm_type(self) -> str:
        return "scripted-tool-caller"

    def bind_tools(self, tools, **kwargs):  # type: ignore[override]
        names = [t.name if hasattr(t, "name") else t["name"] for t in tools]
        return self.model_copy(update={"tool_names": names})

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs) -> ChatResult:
        done = [m.name for m in messages if isinstance(m, ToolMessage)]
        remaining = [t for t in self.tool_names if t not in done]
        if remaining:
            msg = AIMessage(
                content="",
                tool_calls=[{"name": remaining[0], "args": {}, "id": f"call_{uuid.uuid4().hex[:12]}"}],
            )
        else:
            outputs = {m.name: m.content for m in messages if isinstance(m, ToolMessage)}
            msg = AIMessage(content="Completed: " + "; ".join(f"{k} -> {v}" for k, v in outputs.items()))
        return ChatResult(generations=[ChatGeneration(message=msg)])


class _NoArgs(BaseModel):
    pass


def get_model() -> BaseChatModel:
    name = os.environ.get("SUBAGENT_MODEL")
    if name:
        from langchain.chat_models import init_chat_model

        return init_chat_model(name, temperature=0)
    return ScriptedToolCaller()


def _make_tool(spec: ToolSpec, ctx: StepContext) -> StructuredTool:
    def call() -> str:
        faults.before_tool(ctx, spec.name)
        result = spec.fn(ctx)
        faults.after_tool(ctx, spec.name)
        return json.dumps(result)

    return StructuredTool.from_function(
        func=call, name=spec.name, description=spec.description, args_schema=_NoArgs
    )


def run_subagent(agent: AgentSpec, registry: Registry, ctx: StepContext) -> dict[str, Any]:
    tools = [_make_tool(registry.tools[t], ctx) for t in agent.tools]
    sub = create_agent(
        get_model(),
        tools=tools,
        system_prompt=agent.instructions or agent.description,
        checkpointer=False,
        name=agent.id,
    )
    brief = {
        "task": agent.description,
        "params": ctx.params,
        "upstream_artifacts": sorted(ctx.artifacts),
    }
    out = sub.invoke(
        {"messages": [HumanMessage(content=json.dumps(brief))]},
        config={
            "run_name": f"agent:{agent.id}",
            "tags": ["sub-agent", agent.id] + ([agent.phase] if agent.phase else []),
            "metadata": {"step_key": ctx.step_key, "agent_id": agent.id},
            "recursion_limit": 4 * len(tools) + 10,
        },
        durability="exit",  # the sub-agent is not checkpointed; the step is the unit of durability
    )
    tool_results: dict[str, Any] = {}
    for m in out["messages"]:
        if isinstance(m, ToolMessage):
            try:
                tool_results[m.name] = json.loads(m.content)
            except (TypeError, json.JSONDecodeError):
                tool_results[m.name] = m.content
    missing = [t for t in agent.tools if t not in tool_results]
    if missing:
        raise StepFailure(f"{agent.id} finished without calling required tools: {missing}")
    summary = next((m.content for m in reversed(out["messages"]) if isinstance(m, AIMessage)), "")
    return {"tool_results": tool_results, "summary": summary if isinstance(summary, str) else str(summary)}
