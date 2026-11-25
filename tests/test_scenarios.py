import os

os.environ.setdefault("MOCK_LATENCY_SCALE", "0.1")
os.environ.pop("SUBAGENT_MODEL", None)
os.environ.pop("PLANNER_MODEL", None)

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from langgraph.types import Command

from durable_orchestrator.driver import drive, pending_interrupts
from durable_orchestrator.errors import SimulatedCrash
from durable_orchestrator.graph import EXECUTIONS_NS, build_graph
from durable_orchestrator.planner import build_plan
from durable_orchestrator.registry import load_registry
from durable_orchestrator.timeutil import utcnow
from mock_platform.infra import CLUSTERS, LEDGER

REG = load_registry()
UPGRADE = {
    "goal": "Upgrade the AI platform cluster to 1.9.0",
    "params": {"cluster_id": "c1", "target_version": "1.9.0", "maintenance_window_delay": "10d"},
}


@pytest.fixture
def env():
    store = InMemoryStore()
    return build_graph(REG, checkpointer=InMemorySaver(), store=store), store


def executions(store, tid):
    return {i.key: i.value["count"] for i in store.search((EXECUTIONS_NS, tid), limit=100)}


def ledger(store, tid):
    return {i.key.split("/")[-1]: i.value for i in store.search(LEDGER, filter={"thread_id": tid}, limit=100)}


def cluster(store, cid="c1"):
    return store.get(CLUSTERS, cid).value


def test_plan_is_derived_from_registry():
    p = build_plan(REG, goal="upgrade the platform cluster", target=None, params=UPGRADE["params"], now=utcnow())
    assert p["target"] == "upgrade.verified" and p["planner"] == "keyword"
    assert p["layers"] == [
        ["P0_discovery"],
        ["P1_precheck", "P2_backup_verify"],
        ["P3_prepare_stage"],
        ["P4a_change_approval", "P4b_maintenance_window"],
        ["P5_upgrade"],
        ["P6_verify_health"],
    ]
    c = build_plan(REG, goal="rotate tls certificates", target=None, params={"cluster_id": "c"}, now=utcnow())
    assert c["layers"] == [["CR1_cert_inventory"], ["CR2_rotation_approval"], ["CR3_rotate"], ["CR4_verify_tls"]]


def test_parallel_gates_resume_independently(env):
    graph, store = env
    cfg = {"configurable": {"thread_id": "t-gates"}}
    graph.invoke(UPGRADE, cfg, durability="sync")
    pending = pending_interrupts(graph, cfg)
    assert sorted(p["value"]["type"] for p in pending) == ["approval", "timer"]

    approval = next(p for p in pending if p["value"]["type"] == "approval")
    graph.invoke(Command(resume={approval["id"]: {"approved": True, "approver": "cab"}}), cfg, durability="sync")
    pending = pending_interrupts(graph, cfg)
    assert [p["value"]["type"] for p in pending] == ["timer"]
    assert graph.get_state(cfg).values["results"]["P4a_change_approval"]["status"] == "succeeded"

    timer = pending[0]
    graph.invoke(Command(resume={timer["id"]: {"action": "wake", "now": utcnow().isoformat()}}), cfg)
    assert [p["value"]["type"] for p in pending_interrupts(graph, cfg)] == ["timer"], "early wake must keep waiting"

    timer = pending_interrupts(graph, cfg)[0]
    final = graph.invoke(
        Command(resume={timer["id"]: {"action": "wake", "now": timer["value"]["wake_at"]}}), cfg, durability="sync"
    )
    assert final["status"] == "succeeded"
    assert cluster(store)["version"] == "1.9.0"
    ex = executions(store, "t-gates")
    assert all(ex[s] == 1 for s in ["P0_discovery", "P1_precheck", "P2_backup_verify", "P3_prepare_stage", "P5_upgrade"])


def test_kill_mid_node_resumes_without_rerunning_finished_parallel_step(env):
    graph, store = env
    cfg = {"configurable": {"thread_id": "t-kill"}}
    inp = {**UPGRADE, "faults": {"kill": {"step": "P2_backup_verify", "before_tool": "platform_record_known_good"}}}
    with pytest.raises(SimulatedCrash):
        graph.invoke(inp, cfg, durability="sync")
    assert executions(store, "t-kill") == {"P0_discovery": 1, "P1_precheck": 1, "P2_backup_verify": 1}

    graph.invoke(None, cfg, durability="sync")
    ex = executions(store, "t-kill")
    assert ex["P1_precheck"] == 1 and ex["P2_backup_verify"] == 2
    snap = ledger(store, "t-kill")["take_snapshot"]
    assert snap["requests"] == 2 and snap["applied"] == 1
    assert sum(s["kind"] == "pre-upgrade" for s in cluster(store)["snapshots"]) == 1


def test_lost_response_is_retried_idempotently(env):
    graph, store = env
    inp = {**UPGRADE, "faults": {"lose_response": {"tool": "platform_upgrade_control_plane", "times": 1}}}
    final = drive(graph, inp, thread_id="t-lost")
    assert final["status"] == "succeeded"
    assert executions(store, "t-lost")["P5_upgrade"] == 2
    entry = ledger(store, "t-lost")["upgrade_control_plane"]
    assert entry["requests"] == 2 and entry["applied"] == 1
    assert ledger(store, "t-lost")["drain_nodes"]["requests"] == 2


def test_failed_upgrade_runs_saga_compensation(env):
    graph, store = env
    inp = {**UPGRADE, "faults": {"fail": {"tool": "platform_upgrade_workers"}}}
    final = drive(graph, inp, thread_id="t-saga")
    assert final["status"] == "rolled_back"
    order = sorted(final["compensations"].values(), key=lambda c: c["round"])
    assert [c["agent"] for c in order] == ["C5_rollback", "C3_unstage"]
    c = cluster(store)
    assert c["version"] == "1.8.2" and c["health"] == "green" and c["staged_bundle"] is None
    assert all(n["version"] == "1.8.2" for n in c["nodes"])
    assert "P6_verify_health" not in final["results"]


def test_rejection_with_parallel_wait_cancelled(env):
    graph, store = env
    cfg = {"configurable": {"thread_id": "t-reject"}}
    graph.invoke(UPGRADE, cfg, durability="sync")
    resume = {}
    for p in pending_interrupts(graph, cfg):
        if p["value"]["type"] == "approval":
            resume[p["id"]] = {"approved": False, "approver": "cab", "comment": "freeze"}
        else:
            resume[p["id"]] = {"action": "cancel", "by": "cab"}
    final = graph.invoke(Command(resume=resume), cfg, durability="sync")
    assert final["status"] == "rolled_back"
    assert list(final["compensations"]) == ["P3_prepare_stage"]
    assert cluster(store)["staged_bundle"] is None


def test_second_use_case_same_orchestrator(env):
    graph, store = env
    final = drive(graph, {"goal": "rotate the TLS certificates", "params": {"cluster_id": "c2"}}, thread_id="t-certs")
    assert final["status"] == "succeeded"
    assert cluster(store, "c2")["certs"]["previous"] == "certs-2025-q1"


def test_planner_graph_previews_without_side_effects():
    from durable_orchestrator.planner_graph import graph as planner

    goals = planner.invoke({})["goals"]
    assert {g["artifact"] for g in goals} == {"upgrade.verified", "certs.verified"}
    out = planner.invoke({"goal": "upgrade the platform", "params": UPGRADE["params"]})
    assert out["plan"]["layers"][3] == ["P4a_change_approval", "P4b_maintenance_window"]
    assert "error" in planner.invoke({"goal": "upgrade the platform", "params": {"cluster_id": "x"}})
