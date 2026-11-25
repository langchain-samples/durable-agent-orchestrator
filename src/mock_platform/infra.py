"""A fake AI platform control plane. No real infrastructure is touched.

State lives in the LangGraph Store (Postgres on Agent Server, in-memory/SQLite locally) so it
survives worker crashes and can be inspected from the notebook via the SDK:

    ("mock_platform", "clusters")  cluster_id      -> cluster state
    ("mock_platform", "ledger")    idempotency key -> {requests, applied, result, ...}

`apply()` behaves like an idempotent external API (Stripe-style idempotency keys): the first
request with a key performs the mutation, later requests with the same key return the stored
result without touching the cluster again.
"""

from __future__ import annotations

import os
import time
from copy import deepcopy
from typing import Any, Callable

from langgraph.store.base import BaseStore

from durable_orchestrator.timeutil import iso, utcnow

CLUSTERS = ("mock_platform", "clusters")
LEDGER = ("mock_platform", "ledger")
BASE_VERSION = "1.8.2"


def latency(seconds: float) -> None:
    time.sleep(seconds * float(os.environ.get("MOCK_LATENCY_SCALE", "1")))


def _seed(cluster_id: str) -> dict[str, Any]:
    return {
        "cluster_id": cluster_id,
        "version": BASE_VERSION,
        "control_plane_version": BASE_VERSION,
        "nodes": [{"name": f"{cluster_id}-w{i}", "version": BASE_VERSION, "cordoned": False} for i in (1, 2, 3)],
        "health": "green",
        "staged_bundle": None,
        "snapshots": [{"id": "snap-nightly", "version": BASE_VERSION, "kind": "scheduled"}],
        "known_good": None,
        "certs": {"current": "certs-2025-q1", "previous": None, "pending": None, "expires_in_days": 12},
    }


class MockPlatform:
    def __init__(self, store: BaseStore):
        self.store = store

    def cluster(self, cluster_id: str) -> dict[str, Any]:
        item = self.store.get(CLUSTERS, cluster_id)
        if item is None:
            state = _seed(cluster_id)
            self.store.put(CLUSTERS, cluster_id, state)
            return deepcopy(state)
        return deepcopy(item.value)

    def save(self, cluster: dict[str, Any]) -> None:
        self.store.put(CLUSTERS, cluster["cluster_id"], cluster)

    def apply(
        self,
        *,
        key: str,
        op: str,
        cluster_id: str,
        thread_id: str,
        mutate: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> dict[str, Any]:
        seen = self.store.get(LEDGER, key)
        if seen is not None:
            entry = dict(seen.value)
            entry["requests"] += 1
            self.store.put(LEDGER, key, entry)
            return {**entry["result"], "idempotent_replay": True}

        cluster = self.cluster(cluster_id)
        result = mutate(cluster)
        self.save(cluster)
        self.store.put(
            LEDGER,
            key,
            {
                "op": op,
                "cluster_id": cluster_id,
                "thread_id": thread_id,
                "requests": 1,
                "applied": 1,
                "result": result,
                "applied_at": iso(utcnow()),
            },
        )
        return result
