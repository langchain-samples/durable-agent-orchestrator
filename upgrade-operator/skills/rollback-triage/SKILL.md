---
name: rollback-triage
description: Triage an AI platform workflow that crashed, failed, or was rolled back, and capture the lesson.
---

# Rollback triage

Check the workflow with `get_workflow_status` and branch on what you find.

## Thread status "error" (the run crashed mid-step)

The orchestrator's checkpoint is intact. Call `resume_crashed_workflow`. Finished steps do not re-run,
and idempotent tools absorb any repeated side effects. Report which step was re-executed.

## Workflow status "rolled_back"

The saga already returned the cluster to its known-good snapshot.
1. Confirm with `inspect_cluster`: version equals `known_good.version` and health is green.
2. Start the `incident-investigator` async subagent with the thread ID. Keep talking to the user
   while it works.
3. When its postmortem arrives, save it to `/memories/agent/incidents/<thread_id>.md`, and add a
   one-line lesson to `/memories/agent/AGENTS.md` linking to it.
4. Recommend a next step (retry in the next window, hold the version, open a vendor case). Do not
   restart the upgrade without the user's go-ahead.

## Workflow status "compensation_failed" or "failed"

A rollback step itself failed, or nothing could be compensated. Escalate to a human immediately:
name the failed step and error, and the current cluster state from `inspect_cluster`. Do not retry.
