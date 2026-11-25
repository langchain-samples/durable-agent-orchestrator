import os

from langchain.chat_models import init_chat_model
from managed_deepagents import define_deep_agent

from middleware.change_policy import enforce_change_policy
from tools.follow_up import list_watches, stop_watching, watch_workflow
from tools.orchestrator import (
    cancel_maintenance_window,
    decide_approval_gate,
    get_workflow_events,
    get_workflow_status,
    inspect_cluster,
    list_active_workflows,
    list_workflow_goals,
    open_maintenance_window_now,
    preview_plan,
    resume_crashed_workflow,
    start_workflow,
)

MODEL = "langsmith:openai/gpt-5.5"  # LLM Gateway, billed to the workspace's OpenAI provider key


def _model():
    # Org-scoped LangSmith keys must name the workspace on gateway calls.
    workspace = os.environ.get("LANGSMITH_WORKSPACE_ID")
    if workspace:
        return init_chat_model(MODEL, default_headers={"X-Tenant-Id": workspace})
    return MODEL


agent = define_deep_agent(
    name="upgrade-operator",
    model=_model(),
    tools=[
        list_workflow_goals,
        preview_plan,
        start_workflow,
        get_workflow_status,
        get_workflow_events,
        list_active_workflows,
        inspect_cluster,
        decide_approval_gate,
        open_maintenance_window_now,
        cancel_maintenance_window,
        resume_crashed_workflow,
        watch_workflow,
        stop_watching,
        list_watches,
    ],
    middleware=[enforce_change_policy],
    interrupt_on={
        "start_workflow": True,
        "decide_approval_gate": True,
        "open_maintenance_window_now": True,
        "cancel_maintenance_window": True,
    },
    subagents=[
        {
            "name": "incident-investigator",
            "description": (
                "Investigates a failed or rolled-back AI platform workflow in the background and returns a short "
                "postmortem. Give it the orchestrator thread ID."
            ),
            "system_prompt": (
                "You investigate AI platform change workflows that did not succeed. Use get_workflow_events and "
                "inspect_cluster. Return a postmortem of at most 12 lines: what failed (step, error), what the "
                "saga rolled back and in which order, the cluster's final state versus its known-good snapshot, "
                "and one recommendation. State facts from the tools only."
            ),
            "tools": [get_workflow_events, get_workflow_status, inspect_cluster],
            "async": True,
        },
    ],
)
