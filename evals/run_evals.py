"""Run the scenario suite as a LangSmith experiment.

    uv run python evals/run_evals.py            # upload dataset + experiment to LangSmith
    uv run python evals/run_evals.py --local    # no LangSmith; print scores

Each example runs the real orchestrator graph in-process (fresh checkpointer + store), injects the
example's faults, answers approvals, jumps timers on a simulated clock, auto-resumes after a
simulated crash, then hands the final state to the evaluators.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

os.environ.setdefault("MOCK_LATENCY_SCALE", "0.1")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402
from langgraph.store.memory import InMemoryStore  # noqa: E402

from durable_orchestrator.driver import drive  # noqa: E402
from durable_orchestrator.errors import SimulatedCrash  # noqa: E402
from durable_orchestrator.graph import EXECUTIONS_NS, build_graph  # noqa: E402
from durable_orchestrator.registry import load_registry  # noqa: E402
from evaluators import EVALUATORS  # noqa: E402
from mock_platform.infra import CLUSTERS, LEDGER  # noqa: E402

DATASET = "durable-orchestrator-scenarios"
REGISTRY = load_registry()

_UPGRADE = {"target_version": "1.9.0", "maintenance_window_delay": "10d"}
_ROLLED_BACK = {"version": "1.8.2", "health": "green", "staged_bundle": None}
_UPGRADED = {"version": "1.9.0", "control_plane_version": "1.9.0", "health": "green"}

SCENARIOS = [
    {
        "name": "happy_path",
        "inputs": {"goal": "Upgrade the AI platform cluster to 1.9.0", "params": {"cluster_id": "eval-happy", **_UPGRADE}},
        "outputs": {"status": "succeeded", "cluster": _UPGRADED},
    },
    {
        "name": "kill_mid_node_then_resume",
        "inputs": {
            "goal": "Upgrade the AI platform cluster to 1.9.0",
            "params": {"cluster_id": "eval-kill", **_UPGRADE},
            "faults": {"kill": {"step": "P2_backup_verify", "before_tool": "platform_record_known_good"}},
        },
        "outputs": {"status": "succeeded", "cluster": _UPGRADED},
    },
    {
        "name": "lost_response_retry",
        "inputs": {
            "goal": "Upgrade the AI platform cluster to 1.9.0",
            "params": {"cluster_id": "eval-lost", **_UPGRADE},
            "faults": {"lose_response": {"tool": "platform_upgrade_control_plane", "times": 1}},
        },
        "outputs": {"status": "succeeded", "cluster": _UPGRADED},
    },
    {
        "name": "failed_upgrade_saga_rollback",
        "inputs": {
            "goal": "Upgrade the AI platform cluster to 1.9.0",
            "params": {"cluster_id": "eval-saga", **_UPGRADE},
            "faults": {"fail": {"tool": "platform_upgrade_workers"}},
        },
        "outputs": {"status": "rolled_back", "cluster": _ROLLED_BACK},
    },
    {
        "name": "cert_rotation_second_use_case",
        "inputs": {"goal": "Rotate the TLS certificates", "params": {"cluster_id": "eval-certs"}},
        "outputs": {"status": "succeeded", "cluster": {"version": "1.8.2"}},
    },
    {
        "name": "cert_rotation_rejected",
        "inputs": {
            "goal": "Rotate the TLS certificates",
            "params": {"cluster_id": "eval-certs-rejected"},
            "approvals": {"CR2_rotation_approval": {"approved": False, "approver": "security", "comment": "freeze"}},
        },
        "outputs": {"status": "failed", "cluster": {"version": "1.8.2"}},
    },
]


def run_scenario(inputs: dict) -> dict:
    store = InMemoryStore()
    graph = build_graph(REGISTRY, checkpointer=InMemorySaver(), store=store)
    tid = f"eval-{uuid.uuid4().hex[:8]}"
    approvals = inputs.get("approvals")
    orchestrator_input = {k: v for k, v in inputs.items() if k != "approvals"}
    crashes = 0
    try:
        final = drive(graph, orchestrator_input, thread_id=tid, approvals=approvals)
    except SimulatedCrash:
        crashes += 1
        final = drive(graph, None, thread_id=tid, approvals=approvals)
    cluster = store.get(CLUSTERS, inputs["params"]["cluster_id"])
    return {
        "status": final.get("status"),
        "report": final.get("report"),
        "plan": final.get("plan"),
        "results": {
            k: {kk: v.get(kk) for kk in ("status", "round", "attempt", "error")}
            for k, v in (final.get("results") or {}).items()
        },
        "ledger": {i.key: i.value for i in store.search(LEDGER, filter={"thread_id": tid}, limit=500)},
        "executions": {i.key: i.value["count"] for i in store.search((EXECUTIONS_NS, tid), limit=100)},
        "cluster": cluster.value if cluster else {},
        "crashes_recovered": crashes,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="run without LangSmith")
    args = ap.parse_args()

    if args.local:
        failures = 0
        for sc in SCENARIOS:
            out = run_scenario(sc["inputs"])
            scores = [ev(out, sc["outputs"]) for ev in EVALUATORS]
            failures += sum(1 for s in scores if not s["score"])
            print(f"\n{sc['name']}: status={out['status']} crashes_recovered={out['crashes_recovered']}")
            for s in scores:
                print(f"  {'PASS' if s['score'] else 'FAIL'}  {s['key']:28s} {s['comment']}")
        sys.exit(1 if failures else 0)

    from langsmith import Client, evaluate

    client = Client()
    if not client.has_dataset(dataset_name=DATASET):
        ds = client.create_dataset(DATASET, description="Durable orchestrator failure / recovery scenarios")
        client.create_examples(
            dataset_id=ds.id,
            examples=[
                {"inputs": sc["inputs"], "outputs": sc["outputs"], "metadata": {"scenario": sc["name"]}}
                for sc in SCENARIOS
            ],
        )
    results = evaluate(
        run_scenario,
        data=DATASET,
        evaluators=EVALUATORS,
        experiment_prefix="durable-orchestrator",
        max_concurrency=4,
    )
    print(results)


if __name__ == "__main__":
    main()
