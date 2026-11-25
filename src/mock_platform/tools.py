"""Mocked AI platform tools. Each takes the StepContext and returns a JSON-able dict.

Side-effecting tools route through MockPlatform.apply() with ctx.idempotency_key(...), so a retried or
resumed step never repeats a side effect.
"""

from __future__ import annotations

import hashlib
from typing import Any

from durable_orchestrator.context import StepContext
from durable_orchestrator.errors import StepFailure
from durable_orchestrator.registry import ToolSpec

from .infra import MockPlatform, latency


def _api(ctx: StepContext) -> MockPlatform:
    return MockPlatform(ctx.store)


def _cid(ctx: StepContext) -> str:
    return ctx.params["cluster_id"]


def _target(ctx: StepContext) -> str:
    return ctx.params.get("target_version", "1.9.0")


def _apply(ctx: StepContext, op: str, mutate) -> dict[str, Any]:
    return _api(ctx).apply(
        key=ctx.idempotency_key(op), op=op, cluster_id=_cid(ctx), thread_id=ctx.thread_id, mutate=mutate
    )


def _short(key: str) -> str:
    return hashlib.sha1(key.encode()).hexdigest()[:8]


# ------------------------------------------------------------------ P0 discovery
def inventory_cluster(ctx):
    latency(0.2)
    c = _api(ctx).cluster(_cid(ctx))
    return {"cluster_id": c["cluster_id"], "nodes": [n["name"] for n in c["nodes"]], "node_count": len(c["nodes"])}


def get_versions(ctx):
    latency(0.1)
    c = _api(ctx).cluster(_cid(ctx))
    return {
        "version": c["version"],
        "control_plane_version": c["control_plane_version"],
        "node_versions": {n["name"]: n["version"] for n in c["nodes"]},
    }


# ------------------------------------------------------------------ P1 pre-check
def check_health(ctx):
    latency(0.2)
    c = _api(ctx).cluster(_cid(ctx))
    return {"health": c["health"], "etcd": "ok", "api_server": "ok", "gpu_operator": "ok"}


def check_compatibility(ctx):
    latency(0.2)
    c = _api(ctx).cluster(_cid(ctx))
    cur, tgt = c["version"].split("."), _target(ctx).split(".")
    if cur[0] != tgt[0]:
        raise StepFailure(f"No supported upgrade path {c['version']} -> {_target(ctx)}")
    return {"compatible": True, "upgrade_path": f"{c['version']} -> {_target(ctx)}"}


def check_capacity(ctx):
    latency(0.2)
    return {"capacity_headroom_pct": 38, "capacity_ok": True}


# ------------------------------------------------------------------ P2 backup
def list_snapshots(ctx):
    latency(0.2)
    return {"existing_snapshots": [s["id"] for s in _api(ctx).cluster(_cid(ctx))["snapshots"]]}


def take_snapshot(ctx):
    latency(0.2)
    snap_id = f"snap-pre-upgrade-{_short(ctx.idempotency_key('take_snapshot'))}"

    def mutate(c):
        c["snapshots"].append({"id": snap_id, "version": c["version"], "kind": "pre-upgrade"})
        return {"snapshot_id": snap_id, "snapshot_version": c["version"]}

    return _apply(ctx, "take_snapshot", mutate)


def verify_snapshot_restorable(ctx):
    latency(1.5)  # a restore test is slow; this is where the kill switch lands in the demo
    snap = _api(ctx).cluster(_cid(ctx))["snapshots"][-1]
    return {"verified_snapshot": snap["id"], "restorable": True}


def record_known_good(ctx):
    latency(0.1)

    def mutate(c):
        snap = c["snapshots"][-1]
        c["known_good"] = {"snapshot_id": snap["id"], "version": snap["version"]}
        return {"known_good_snapshot": snap["id"], "known_good_version": snap["version"]}

    return _apply(ctx, "record_known_good", mutate)


# ------------------------------------------------------------------ P3 prepare & stage
def download_bundle(ctx):
    latency(0.3)
    return {"bundle": f"ai-platform-{_target(ctx)}.tar.zst", "size_gb": 14.2}


def verify_checksum(ctx):
    latency(0.1)
    return {"sha256_ok": True}


def stage_bundle(ctx):
    latency(0.3)

    def mutate(c):
        c["staged_bundle"] = _target(ctx)
        return {"staged_version": _target(ctx), "staged_on": [n["name"] for n in c["nodes"]]}

    return _apply(ctx, "stage_bundle", mutate)


def unstage_bundle(ctx):
    latency(0.2)

    def mutate(c):
        removed, c["staged_bundle"] = c["staged_bundle"], None
        return {"unstaged": removed}

    return _apply(ctx, "unstage_bundle", mutate)


# ------------------------------------------------------------------ P5 upgrade
def drain_nodes(ctx):
    latency(0.3)

    def mutate(c):
        for n in c["nodes"]:
            n["cordoned"] = True
        return {"drained": [n["name"] for n in c["nodes"]]}

    return _apply(ctx, "drain_nodes", mutate)


def upgrade_control_plane(ctx):
    latency(0.4)

    def mutate(c):
        c["control_plane_version"] = _target(ctx)
        return {"control_plane_version": _target(ctx)}

    return _apply(ctx, "upgrade_control_plane", mutate)


def upgrade_workers(ctx):
    latency(0.4)
    if ctx.fault_targets("fail", "platform_upgrade_workers"):
        # Partial side effect, then a hard failure: the saga has to unwind this.
        api = _api(ctx)
        c = api.cluster(_cid(ctx))
        c["nodes"][0]["version"] = _target(ctx)
        c["health"] = "degraded"
        api.save(c)
        raise StepFailure(
            ctx.faults["fail"].get("message") or f"{c['nodes'][1]['name']} failed to rejoin after upgrade"
        )

    def mutate(c):
        for n in c["nodes"]:
            n["version"] = _target(ctx)
        c["version"] = _target(ctx)
        return {"workers_upgraded": [n["name"] for n in c["nodes"]], "version": _target(ctx)}

    return _apply(ctx, "upgrade_workers", mutate)


def uncordon_nodes(ctx):
    latency(0.2)

    def mutate(c):
        for n in c["nodes"]:
            n["cordoned"] = False
        return {"uncordoned": [n["name"] for n in c["nodes"]]}

    return _apply(ctx, "uncordon_nodes", mutate)


# ------------------------------------------------------------------ P6 verify
def run_smoke_tests(ctx):
    latency(0.3)
    c = _api(ctx).cluster(_cid(ctx))
    if c["version"] != _target(ctx) or c["health"] != "green":
        raise StepFailure(f"Smoke tests failed: version={c['version']} health={c['health']}")
    return {"smoke_tests": "passed", "version": c["version"]}


# ------------------------------------------------------------------ C5 rollback (compensation)
def restore_snapshot(ctx):
    latency(0.5)

    def mutate(c):
        kg = c["known_good"]
        if not kg:
            raise StepFailure("No known-good snapshot recorded; cannot roll back automatically")
        c["version"] = c["control_plane_version"] = kg["version"]
        for n in c["nodes"]:
            n["version"], n["cordoned"] = kg["version"], False
        c["health"] = "green"
        return {"restored_snapshot": kg["snapshot_id"], "version": kg["version"]}

    return _apply(ctx, "restore_snapshot", mutate)


def confirm_known_good(ctx):
    latency(0.2)
    c = _api(ctx).cluster(_cid(ctx))
    kg = c["known_good"] or {}
    ok = c["version"] == kg.get("version") and c["health"] == "green"
    if not ok:
        raise StepFailure(f"Cluster not at known-good state: {c['version']} / {c['health']}")
    return {"at_known_good": True, "version": c["version"], "health": c["health"]}


# ------------------------------------------------------------------ cert rotation (2nd use case)
def certs_list(ctx):
    latency(0.2)
    return {"certs": _api(ctx).cluster(_cid(ctx))["certs"]}


def certs_issue(ctx):
    latency(0.3)
    new_id = f"certs-{_short(ctx.idempotency_key('certs_issue'))}"

    def mutate(c):
        c["certs"]["pending"] = new_id
        return {"issued": new_id}

    return _apply(ctx, "certs_issue", mutate)


def certs_install(ctx):
    latency(0.3)

    def mutate(c):
        certs = c["certs"]
        certs["previous"], certs["current"], certs["pending"] = certs["current"], certs["pending"], None
        certs["expires_in_days"] = 397
        return {"installed": certs["current"], "previous": certs["previous"]}

    return _apply(ctx, "certs_install", mutate)


def certs_probe_endpoints(ctx):
    latency(0.2)
    return {"tls_endpoints_ok": 4, "tls_endpoints_failed": 0}


def certs_restore_previous(ctx):
    latency(0.2)

    def mutate(c):
        certs = c["certs"]
        if certs["previous"]:
            certs["current"], certs["previous"] = certs["previous"], None
        return {"restored": certs["current"]}

    return _apply(ctx, "certs_restore_previous", mutate)


TOOLS = [
    ToolSpec("platform_inventory_cluster", "Inventory the cluster's nodes.", inventory_cluster),
    ToolSpec("platform_get_versions", "Get control-plane and node versions.", get_versions),
    ToolSpec("platform_check_health", "Check cluster health.", check_health),
    ToolSpec("platform_check_compatibility", "Check the target version is a supported upgrade path.", check_compatibility),
    ToolSpec("platform_check_capacity", "Check spare capacity for a rolling upgrade.", check_capacity),
    ToolSpec("platform_list_snapshots", "List existing snapshots.", list_snapshots),
    ToolSpec("platform_take_snapshot", "Take a pre-upgrade snapshot.", take_snapshot, side_effect=True),
    ToolSpec("platform_verify_snapshot_restorable", "Run a restore test on the newest snapshot.", verify_snapshot_restorable),
    ToolSpec("platform_record_known_good", "Record the newest snapshot as the known-good state.", record_known_good, side_effect=True),
    ToolSpec("platform_download_bundle", "Download the upgrade bundle.", download_bundle),
    ToolSpec("platform_verify_checksum", "Verify the bundle checksum.", verify_checksum),
    ToolSpec("platform_stage_bundle", "Stage the bundle on every node.", stage_bundle, side_effect=True),
    ToolSpec("platform_unstage_bundle", "Remove the staged bundle from every node.", unstage_bundle, side_effect=True),
    ToolSpec("platform_drain_nodes", "Cordon and drain all nodes.", drain_nodes, side_effect=True),
    ToolSpec("platform_upgrade_control_plane", "Upgrade the control plane.", upgrade_control_plane, side_effect=True),
    ToolSpec("platform_upgrade_workers", "Upgrade the worker nodes.", upgrade_workers, side_effect=True),
    ToolSpec("platform_uncordon_nodes", "Uncordon all nodes.", uncordon_nodes, side_effect=True),
    ToolSpec("platform_run_smoke_tests", "Run post-upgrade smoke tests.", run_smoke_tests),
    ToolSpec("platform_restore_snapshot", "Restore the known-good snapshot.", restore_snapshot, side_effect=True),
    ToolSpec("platform_confirm_known_good", "Confirm the cluster is at its known-good state.", confirm_known_good),
    ToolSpec("certs_list", "List TLS certificates and expiry.", certs_list),
    ToolSpec("certs_issue", "Issue new certificates.", certs_issue, side_effect=True),
    ToolSpec("certs_install", "Install the issued certificates.", certs_install, side_effect=True),
    ToolSpec("certs_probe_endpoints", "Probe TLS endpoints.", certs_probe_endpoints),
    ToolSpec("certs_restore_previous", "Reinstall the previous certificates.", certs_restore_previous, side_effect=True),
]
