"""The decisions the record makes, without a database."""

import pytest

from services.common.protocol import COMPLETED, FAILED, RUNNING, UNKNOWN, JobReport
from services.executor.store import (
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_PENDING,
    JOB_REVOKED,
    JOB_RUNNING,
    MAX_ATTEMPTS,
    VANISHED,
    Candidate,
    to_start,
    verdict,
)

SAFE_TO_REPEAT = JobReport(FAILED, error="door open", retryable=True)
NOT_SAFE_TO_REPEAT = JobReport(FAILED, error="tip lost", retryable=False)


def decide(state: str, report: JobReport, *, attempt: int = 1, ending: bool = False) -> object:
    decided = verdict(state, attempt, ending, report)
    return decided and (decided.job_state, decided.step_status, decided.stops_run)


@pytest.mark.parametrize(
    ("state", "report", "decided"),
    [
        (JOB_PENDING, JobReport(RUNNING), (JOB_RUNNING, "running", False)),
        (JOB_REVOKED, JobReport(RUNNING), (JOB_RUNNING, "running", False)),
        (JOB_RUNNING, JobReport(RUNNING), None),
        (JOB_PENDING, JobReport(COMPLETED), (JOB_COMPLETED, "completed", False)),
        (JOB_RUNNING, JobReport(COMPLETED), (JOB_COMPLETED, "completed", False)),
        (JOB_PENDING, JobReport(UNKNOWN), None),
        (JOB_REVOKED, JobReport(UNKNOWN), (JOB_CANCELLED, "cancelled", False)),
    ],
)
def test_what_a_report_means(state: str, report: JobReport, decided: object) -> None:
    assert decide(state, report) == decided


def test_a_step_is_tried_again_only_when_its_instrument_calls_that_safe() -> None:
    assert decide(JOB_RUNNING, SAFE_TO_REPEAT) == (JOB_FAILED, "pending", False)
    assert decide(JOB_RUNNING, NOT_SAFE_TO_REPEAT) == (JOB_FAILED, "failed", True)


def test_a_step_is_not_tried_for_ever() -> None:
    last = decide(JOB_RUNNING, SAFE_TO_REPEAT, attempt=MAX_ATTEMPTS)
    assert last == (JOB_FAILED, "failed", True)


def test_nothing_is_tried_again_in_a_run_that_is_ending() -> None:
    assert decide(JOB_RUNNING, SAFE_TO_REPEAT, ending=True) == (JOB_FAILED, "failed", True)


def test_a_retry_that_never_started_leaves_its_step_failed_not_cancelled() -> None:
    assert decide(JOB_REVOKED, JobReport(UNKNOWN), attempt=2) == (JOB_CANCELLED, "failed", False)


def test_a_running_job_the_instrument_has_forgotten_fails_and_is_not_repeated() -> None:
    decided = verdict(JOB_RUNNING, 1, False, JobReport(UNKNOWN))
    assert decided is not None
    assert (decided.job_state, decided.error, decided.stops_run) == (JOB_FAILED, VANISHED, True)


def step(run_id: str, name: str, device_id: str, *depends_on: str, **changes: object) -> Candidate:
    candidate = Candidate(run_id, name, device_id, list(depends_on), "pending", 0, False)
    return candidate._replace(**changes)  # type: ignore[arg-type]


def started(candidates: list[Candidate], busy: set[str] | None = None) -> list[tuple[str, str]]:
    return [(c.run_id, c.name) for c in to_start(candidates, busy if busy is not None else set())]


def test_an_instrument_is_given_to_one_run_at_a_time_oldest_first() -> None:
    candidates = [step("run-1", "fill", "handler"), step("run-2", "fill", "handler")]

    assert started(candidates) == [("run-1", "fill")]


def test_an_instrument_already_on_a_job_is_not_given_another() -> None:
    candidates = [step("run-1", "fill", "handler"), step("run-1", "warm", "incubator")]

    assert started(candidates, busy={"handler"}) == [("run-1", "warm")]


def test_a_step_with_an_open_job_holds_its_instrument_whatever_its_status() -> None:
    candidates = [
        step("run-1", "fill", "handler", has_open_job=True),
        step("run-1", "refill", "handler"),
    ]

    assert started(candidates) == []


def test_a_step_waits_for_what_it_depends_on() -> None:
    candidates = [
        step("run-1", "fill", "handler", status="completed"),
        step("run-1", "warm", "incubator", "fill"),
        step("run-1", "read", "reader", "warm"),
    ]

    assert started(candidates) == [("run-1", "warm")]
