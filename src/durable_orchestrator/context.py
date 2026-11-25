from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from langgraph.store.base import BaseStore


@dataclass
class StepContext:
    """Everything a tool needs. Built fresh for each step execution and closed over by the tools."""

    thread_id: str
    step_id: str
    step_key: str  # "P5_upgrade" for forward steps, "undo:P5_upgrade" for compensations
    agent_id: str
    params: dict[str, Any]
    artifacts: dict[str, Any]
    faults: dict[str, Any]
    store: BaseStore
    extra: dict[str, Any] = field(default_factory=dict)

    def idempotency_key(self, operation: str) -> str:
        """Deterministic per (thread, step, operation): a retried or resumed step reuses the same key,
        so the downstream system can de-duplicate the request."""
        return f"{self.thread_id}/{self.step_key}/{operation}"

    def fault_targets(self, kind: str, tool: str) -> bool:
        f = self.faults.get(kind) or {}
        return f.get("tool") == tool and f.get("step", self.step_id) == self.step_id
