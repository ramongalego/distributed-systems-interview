"""What the API reports. This shape is fixed: the checks read it.

Your Executor returns RunView. Keep the field names and the status values. How
you store a run, and what other states you keep internally, is up to you.
"""

from dataclasses import dataclass, field
from datetime import datetime

RUN_PENDING = "pending"
RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"
RUN_STATUSES = (RUN_PENDING, RUN_RUNNING, RUN_COMPLETED, RUN_FAILED, RUN_CANCELLED)

STEP_PENDING = "pending"
STEP_RUNNING = "running"
STEP_COMPLETED = "completed"
STEP_FAILED = "failed"
STEP_CANCELLED = "cancelled"
STEP_STATUSES = (STEP_PENDING, STEP_RUNNING, STEP_COMPLETED, STEP_FAILED, STEP_CANCELLED)


@dataclass(frozen=True)
class StepView:
    name: str
    device_id: str
    depends_on: list[str]
    status: str
    # When the step started and finished on its device, as far as you know.
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    # Where the step's data is stored, as s3://bucket/key. Only steps that
    # produce data (the plate reader) have one.
    data_ref: str | None = None


@dataclass(frozen=True)
class RunView:
    id: str
    workflow_name: str
    status: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    steps: list[StepView] = field(default_factory=list)
