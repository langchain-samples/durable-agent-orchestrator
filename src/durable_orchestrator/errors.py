"""Error taxonomy. How a step fails decides what the orchestrator does next.

| Exception            | Meaning                                  | Handled by                                   |
|----------------------|------------------------------------------|----------------------------------------------|
| TransientInfraError  | timeout / lost response / 5xx            | LangGraph RetryPolicy re-runs the step       |
| StepFailure          | business failure (upgrade failed, denied)| orchestrator -> saga compensation            |
| SimulatedCrash       | the worker died mid-step (kill switch)   | run errors; resume the thread from checkpoint|
"""


class TransientInfraError(ConnectionError):
    """Retryable. Tools must be idempotent because the whole step is re-run."""


class StepFailure(Exception):
    """Non-retryable business failure. Triggers compensation."""


class SimulatedCrash(RuntimeError):
    """Raised by the kill switch to simulate a worker dying mid-node."""
