---
name: platform-upgrade-runbook
description: Plan, start and follow an AI platform cluster upgrade through the durable orchestrator, from parameters to verified health.
---

# AI platform upgrade runbook

1. **Collect parameters.** You need `cluster_id`, `target_version`, and either a
   `maintenance_window_delay` (e.g. "10d", "2h") or an absolute `maintenance_window_start`.
   Check `/memories/agent/AGENTS.md` for the cluster's usual window and owners before asking.
2. **Check the cluster.** Call `inspect_cluster`. If health is not green, or a bundle is already
   staged, stop and tell the user.
3. **Preview.** Call `preview_plan` and present:
   - the phases in order, and which run in parallel (same layer),
   - the gates: the CAB approval (`P4a_change_approval`) and the maintenance window
     (`P4b_maintenance_window`, with its `wake_at`),
   - what gets rolled back on failure (`compensated_steps`).
4. **Confirm, then start.** Call `start_workflow`. It returns once the workflow pauses at the gates.
   Report the orchestrator `thread_id`.
5. **Follow through.** Call `watch_workflow` for the thread. Tell the user that the CAB approval is
   needed (and from whom) and when the window opens. The window is opened automatically by the
   orchestrator's scheduled waker; you do not need to do anything for it.
6. **Approvals.** When the user relays a CAB decision, call `decide_approval_gate` with the approver's
   name and the CHG ticket in the comment.
7. **Outcome.** On `succeeded`, confirm the new version with `inspect_cluster`, save a one-line entry to
   `/memories/agent/AGENTS.md` (cluster, version, date, thread), and stop the watch.
   Otherwise switch to the `rollback-triage` skill.
