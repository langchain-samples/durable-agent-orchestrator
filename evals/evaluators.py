"""Code evaluators for orchestrator runs. Each takes (outputs, reference_outputs) and returns a score.

They check the properties a durable workflow engine is supposed to guarantee, not the wording of
the model's answers:

* outcome_matches_expected   - the run ended in the expected terminal status
* dependency_gates_respected - no step started before all of its dependencies had succeeded
* side_effects_exactly_once  - every side-effecting request was applied once, despite retries/resumes
* reached_known_good_state   - the cluster ended where it should: target version, or rolled back
"""

from __future__ import annotations


def outcome_matches_expected(outputs: dict, reference_outputs: dict) -> dict:
    ok = outputs["status"] == reference_outputs["status"]
    return {"key": "outcome_matches_expected", "score": int(ok), "comment": f"got {outputs['status']}"}


def dependency_gates_respected(outputs: dict, reference_outputs: dict) -> dict:
    steps, results = outputs["plan"]["steps"], outputs["results"]
    violations = []
    for sid, res in results.items():
        for dep in steps[sid]["deps"]:
            d = results.get(dep)
            if d is None or d["status"] != "succeeded" or d["round"] >= res["round"]:
                violations.append(f"{sid} ran before {dep} succeeded")
    return {
        "key": "dependency_gates_respected",
        "score": int(not violations),
        "comment": "; ".join(violations) or f"{len(results)} steps in dependency order",
    }


def side_effects_exactly_once(outputs: dict, reference_outputs: dict) -> dict:
    ledger = outputs["ledger"]
    dupes = [k for k, v in ledger.items() if v["applied"] != 1]
    replays = sum(v["requests"] - 1 for v in ledger.values())
    return {
        "key": "side_effects_exactly_once",
        "score": int(not dupes),
        "comment": f"{len(ledger)} side effects, {replays} idempotent replays absorbed, duplicates={dupes}",
    }


def reached_known_good_state(outputs: dict, reference_outputs: dict) -> dict:
    expected = reference_outputs.get("cluster") or {}
    actual = outputs["cluster"]
    diffs = {k: (actual.get(k), v) for k, v in expected.items() if actual.get(k) != v}
    return {
        "key": "reached_known_good_state",
        "score": int(not diffs),
        "comment": f"mismatches (actual, expected): {diffs}" if diffs else f"cluster matches {expected}",
    }


EVALUATORS = [
    outcome_matches_expected,
    dependency_gates_respected,
    side_effects_exactly_once,
    reached_known_good_state,
]
