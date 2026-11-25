from managed_deepagents import define_schedule

schedule = define_schedule(
    cron="0 8 * * 1-5",
    timezone="UTC",
    prompt=(
        "Write the AI platform change digest. Call list_active_workflows. Report: workflows waiting on approval "
        "(and for how long), maintenance windows opening in the next 48 hours, crashed workflows that need "
        "resume_crashed_workflow, and anything rolled back. Keep it under 15 lines. Record notable outcomes "
        "in /memories/agent/upgrade-log.md."
    ),
)
