"""Agent registry: the only place the orchestrator learns what agents exist.

The orchestrator is domain-agnostic. Everything domain-specific (AI platform phases, tools,
gates, compensators) is declared in a registry file and a tool module.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

import yaml

AgentKind = Literal["agent", "gate", "compensator"]

DEFAULT_REGISTRY = Path(__file__).resolve().parents[2] / "registry" / "agents.yaml"


@dataclass(frozen=True)
class ToolSpec:
    """A tool exposed to sub-agents. `fn(ctx) -> dict`. Side-effecting tools are made idempotent."""

    name: str
    description: str
    fn: Callable[[Any], dict]
    side_effect: bool = False


@dataclass(frozen=True)
class AgentSpec:
    id: str
    name: str
    description: str
    kind: AgentKind = "agent"
    phase: str | None = None
    requires: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()
    instructions: str = ""
    gate: dict[str, Any] | None = None
    compensated_by: str | None = None
    compensate_on_failure: bool = False


@dataclass(frozen=True)
class GoalSpec:
    artifact: str
    description: str
    keywords: tuple[str, ...] = ()
    required_params: tuple[str, ...] = ()


@dataclass
class Registry:
    agents: dict[str, AgentSpec]
    goals: dict[str, GoalSpec]
    tools: dict[str, ToolSpec] = field(default_factory=dict)

    def forward_agents(self) -> list[AgentSpec]:
        return [a for a in self.agents.values() if a.kind != "compensator"]

    def providers_of(self, artifact: str) -> list[AgentSpec]:
        return [a for a in self.forward_agents() if artifact in a.provides]

    def describe(self) -> str:
        """Compact text description, used as LLM planner context."""
        lines = ["GOALS:"]
        lines += [f"- {g.artifact}: {g.description}" for g in self.goals.values()]
        lines.append("AGENTS:")
        for a in self.forward_agents():
            lines.append(
                f"- {a.id} ({a.kind}): {a.description} requires={list(a.requires)} provides={list(a.provides)}"
            )
        return "\n".join(lines)

    def validate(self) -> None:
        errors: list[str] = []
        for a in self.agents.values():
            if a.kind == "gate" and not a.gate:
                errors.append(f"{a.id}: gate agents need a `gate` block")
            if a.kind == "gate" and a.gate and a.gate.get("type") not in {"approval", "timer"}:
                errors.append(f"{a.id}: unknown gate type {a.gate.get('type')!r}")
            if a.kind in {"agent", "compensator"}:
                for t in a.tools:
                    if t not in self.tools:
                        errors.append(f"{a.id}: unknown tool {t!r}")
            if a.compensated_by:
                c = self.agents.get(a.compensated_by)
                if c is None or c.kind != "compensator":
                    errors.append(f"{a.id}: compensated_by {a.compensated_by!r} is not a compensator")
            for r in a.requires:
                if not self.providers_of(r):
                    errors.append(f"{a.id}: nothing provides required artifact {r!r}")
        for g in self.goals.values():
            if not self.providers_of(g.artifact):
                errors.append(f"goal {g.artifact!r}: no agent provides it")
        if errors:
            raise ValueError("Invalid registry:\n  " + "\n  ".join(errors))


def _load_tools(modules: list[str]) -> dict[str, ToolSpec]:
    tools: dict[str, ToolSpec] = {}
    for mod_name in modules:
        mod = importlib.import_module(mod_name)
        for spec in getattr(mod, "TOOLS"):
            tools[spec.name] = spec
    return tools


def load_registry(path: str | os.PathLike | None = None) -> Registry:
    path = Path(path or os.environ.get("AGENT_REGISTRY", DEFAULT_REGISTRY))
    raw = yaml.safe_load(path.read_text())
    agents = {}
    for a in raw.get("agents", []):
        agents[a["id"]] = AgentSpec(
            id=a["id"],
            name=a.get("name", a["id"]),
            description=a.get("description", ""),
            kind=a.get("kind", "agent"),
            phase=a.get("phase"),
            requires=tuple(a.get("requires", [])),
            provides=tuple(a.get("provides", [])),
            tools=tuple(a.get("tools", [])),
            instructions=a.get("instructions", ""),
            gate=a.get("gate"),
            compensated_by=a.get("compensated_by"),
            compensate_on_failure=bool(a.get("compensate_on_failure", False)),
        )
    goals = {
        g["artifact"]: GoalSpec(
            artifact=g["artifact"],
            description=g.get("description", ""),
            keywords=tuple(g.get("keywords", [])),
            required_params=tuple(g.get("required_params", [])),
        )
        for g in raw.get("goals", [])
    }
    reg = Registry(agents=agents, goals=goals, tools=_load_tools(raw.get("tool_modules", [])))
    reg.validate()
    return reg
