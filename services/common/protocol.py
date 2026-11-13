"""What the executor and a driver say to each other.

One question, sent by request/reply on ``lab.jobs.<device_id>`` and repeated
until it is answered: "tell me about this job". A driver that has never heard
of the job starts it first, if it is still allowed to. Whatever it answers
comes from the instrument, so asking twice can never run a job twice.
"""

import json
from dataclasses import asdict, dataclass
from typing import Self

# The driver's instrument has no record of the job: it never ran.
UNKNOWN = "unknown"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"


def subject(device_id: str) -> str:
    return f"lab.jobs.{device_id}"


def job_id(run_id: str, step_name: str, attempt: int) -> str:
    """Never reused: a second attempt at a step is a different job."""
    return f"{run_id}/{step_name}/{attempt}"


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    run_id: str
    step_name: str
    # The job may be started until this moment (epoch seconds), and not after:
    # by then the executor has stopped waiting and may have changed its mind.
    # None asks only for a report.
    start_by: float | None
    # How long to hold the reply open for a running job to end, in seconds.
    wait: float

    def encode(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def decode(cls, data: bytes) -> Self:
        return cls(**json.loads(data))


@dataclass(frozen=True)
class JobReport:
    state: str
    # Why the job is unknown, when the driver was asked to start it and did not.
    detail: str = ""
    # When the instrument started and stopped, by its own clock (epoch seconds).
    started_at: float | None = None
    finished_at: float | None = None
    error: str = ""
    # Whether the instrument considers the step safe to run again.
    retryable: bool = False
    data_ref: str | None = None
    data_sha256: str | None = None

    def encode(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def decode(cls, data: bytes) -> Self:
        return cls(**json.loads(data))
