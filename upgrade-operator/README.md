# upgrade-operator: a Managed Deep Agent on top of the durable orchestrator

The conversational and proactive layer for AI platform changes. It plans changes with people, starts
workflows on the durable orchestrator, answers gates on behalf of named humans, watches progress
on its own schedules, investigates rollbacks in the background, and remembers what it learned.
It never executes infrastructure changes itself: every action goes through the orchestrator's API.

See `../docs/managed-deep-agents.png` and the "Managed Deep Agents layer" section of `../README.md`.

## Layout

```text
agent.py                         define_deep_agent: model, tools, middleware, interrupt_on, async subagent
instructions.md                  operator rules + memory policy (synced to Context Hub)
skills/platform-upgrade-runbook/     upgrade procedure (Context Hub)
skills/rollback-triage/          what to do when a workflow crashes / rolls back (Context Hub)
tools/orchestrator.py            SDK tools: preview_plan, start_workflow, decide_approval_gate, ...
tools/follow_up.py               watch_workflow / stop_watching / list_watches (runtime schedules)
middleware/change_policy.py      CHG ticket + written-reason policy, enforced on every tool call
schedules/weekday_change_digest.py   weekday 08:00 UTC digest
memory.py                        agent memory layer (/memories/agent/)
identity.py                      LangSmith API key auth
tests/                           policy unit tests (uv run --with pytest pytest)
```

## Configuration (`.env`)

| Variable | Purpose |
|---|---|
| `LANGSMITH_API_KEY` | Deploy + local auth |
| `LANGSMITH_WORKSPACE_ID` | Required for org-scoped service keys (deploy routing, LLM Gateway header locally) |
| `ORCHESTRATOR_URL` | Orchestrator Agent Server, e.g. `http://127.0.0.1:2024` or the hosted URL |
| `ORCHESTRATOR_WORKSPACE_ID` | Sent as `X-Tenant-Id` to a hosted orchestrator when the key is org-scoped |

The orchestrator's API key is an **agent-owned connection**, not an env var:

```bash
uv run mda connections create orchestrator-api --secret-from-env SOME_ENV_VAR
```

Against a local `langgraph dev` orchestrator no key is needed.

## Run

```bash
uv sync
uv run mda dev        # local Agent Server + Studio (runtime schedules need a deployment)
uv run mda deploy     # hosted; reconciles schedules/, syncs instructions + skills to Context Hub
```

Model: `langsmith:openai/gpt-5.5` through the LangSmith LLM Gateway, using the workspace's OpenAI
provider key. No provider key lives in this project.

## Gotchas found while building it

- Org-scoped `lsv2_sk_` keys need the workspace named everywhere: tracing (`LANGSMITH_WORKSPACE_ID`),
  `mda deploy`, the LLM Gateway (`X-Tenant-Id`, added in `agent.py` when the env var is set), and
  calls to the hosted orchestrator.
- Agent-owned runtime schedules can continue the *same* thread only for channel (Slack, etc.)
  conversations. `watch_workflow` falls back to one fresh thread per check and keeps continuity in
  `/memories/agent/watch-log.md`.
- When the model issues several gated tool calls in one turn, the HITL resume must contain one
  decision per call (`{"decisions": [..., ...]}`). A wrong count fails the run and the bad resume
  value stays recorded on that thread; start a new thread.
- Restarting `mda dev` drops local threads. Local memory lives in `.mda/__contexthub__/`.
