"""Fault injection (demo only). Passed in the run input as `faults`:

    {
      "kill":          {"step": "P2_backup_verify", "before_tool": "platform_record_known_good",
                        "mode": "raise" | "process"},
      "lose_response": {"tool": "platform_upgrade_control_plane", "times": 1},
      "fail":          {"tool": "platform_upgrade_workers", "message": "..."}
    }

* kill          - the worker dies mid-node. "raise" raises SimulatedCrash (safe on a server);
                  "process" calls os._exit(137) (hard kill, used by scripts/hard_kill_demo.py).
                  Fires once per thread so the resumed run gets past it.
* lose_response - the side effect is committed downstream, then the response is lost
                  (TransientInfraError). Fires `times` times per thread.
* fail          - a non-retryable business failure (StepFailure). Fires every time.

"Fired" markers live in the store so they survive crashes and resumes.
"""

from __future__ import annotations

import os

from .context import StepContext
from .errors import SimulatedCrash, StepFailure, TransientInfraError

_NS = "demo_faults"


def _fired(ctx: StepContext, name: str) -> int:
    item = ctx.store.get((_NS, ctx.thread_id), name)
    return item.value["fired"] if item else 0


def _mark(ctx: StepContext, name: str) -> None:
    ctx.store.put((_NS, ctx.thread_id), name, {"fired": _fired(ctx, name) + 1})


def before_tool(ctx: StepContext, tool: str) -> None:
    kill = ctx.faults.get("kill") or {}
    if kill.get("step") == ctx.step_id and kill.get("before_tool") == tool and not _fired(ctx, "kill"):
        _mark(ctx, "kill")
        if kill.get("mode") == "process":
            os._exit(137)
        raise SimulatedCrash(f"kill switch: worker died in {ctx.step_id} before calling {tool}")


def after_tool(ctx: StepContext, tool: str) -> None:
    lose = ctx.faults.get("lose_response") or {}
    if lose.get("tool") == tool and _fired(ctx, "lose_response") < int(lose.get("times", 1)):
        _mark(ctx, "lose_response")
        raise TransientInfraError(
            f"{tool}: connection reset after the request was sent (response lost; side effect may have applied)"
        )
    if ctx.fault_targets("fail", tool):
        raise StepFailure((ctx.faults["fail"].get("message")) or f"{tool} failed")
