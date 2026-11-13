"""The run record, in Postgres: what the executor intends, and what it has been told.

Every method is one transaction. Anything that changes a run, its steps or its
jobs locks the run's row first, so the three always change together.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import groupby
from typing import Any, NamedTuple

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from ..common import protocol
from ..common.protocol import JobReport
from .models import (
    RUN_CANCELLED,
    RUN_FAILED,
    RUN_PENDING,
    RUN_RUNNING,
    STEP_CANCELLED,
    STEP_COMPLETED,
    STEP_FAILED,
    STEP_PENDING,
    STEP_RUNNING,
    RunView,
    StepView,
)
from .scheduler import StepState, next_steps
from .workflows import StepTemplate

# See db/init.sql for what each state means.
JOB_PENDING = "pending"
JOB_REVOKED = "revoked"
JOB_RUNNING = "running"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_CANCELLED = "cancelled"

# How many times a step is put on its instrument before its failure stands.
MAX_ATTEMPTS = 3

VANISHED = "the instrument has no record of a job it had started"

_RUNS = """
    SELECT r.id, r.workflow_name, r.status, r.created_at, r.started_at, r.finished_at,
           s.name, s.device_id, s.depends_on, s.status,
           j.id, j.attempt, j.state, j.started_at, j.finished_at,
           j.error, j.retryable, j.data_ref, j.data_sha256
      FROM ({runs}) r
      LEFT JOIN steps s ON s.run_id = r.id
      LEFT JOIN jobs j ON j.run_id = s.run_id AND j.step_name = s.name
     ORDER BY r.created_at DESC, r.id, s.position, j.attempt
"""
_RUN_BY_ID = _RUNS.format(runs="SELECT * FROM runs WHERE id = %s")
_RUNS_BY_STATUS = _RUNS.format(
    runs="SELECT * FROM runs WHERE %s::text IS NULL OR status = %s"
    " ORDER BY created_at DESC, id LIMIT %s"
)

_FINISH_QUIET_RUNS = """
    UPDATE runs r
       SET status = COALESCE(r.ending_as, 'completed'), finished_at = now()
     WHERE r.status = 'running'
       AND NOT EXISTS (
               SELECT FROM jobs j
                WHERE j.run_id = r.id AND j.state IN ('pending', 'revoked', 'running'))
       AND (r.ending_as IS NOT NULL
            OR NOT EXISTS (
               SELECT FROM steps s WHERE s.run_id = r.id AND s.status <> 'completed'))
    RETURNING r.id
"""

_STEPS_OF_RUNS_GOING_ON = """
    SELECT s.run_id, s.name, s.device_id, s.depends_on, s.status,
           (SELECT count(*) FROM jobs j
             WHERE j.run_id = s.run_id AND j.step_name = s.name),
           EXISTS (SELECT FROM jobs j
                    WHERE j.run_id = s.run_id AND j.step_name = s.name
                      AND j.state IN ('pending', 'revoked', 'running'))
      FROM steps s
      JOIN runs r ON r.id = s.run_id
     WHERE r.status = 'running' AND r.ending_as IS NULL
     ORDER BY r.started_at, r.id, s.position
"""

_OPEN_JOBS = "SELECT {column} FROM jobs WHERE state IN ('pending', 'revoked', 'running')"
_OPEN_JOB_IDS = _OPEN_JOBS.format(column="id")
_BUSY_DEVICES = _OPEN_JOBS.format(column="device_id")


class RunNotFound(Exception):
    """No run with that id."""


@dataclass(frozen=True)
class Attempt:
    """One job: one time a step was put on its instrument."""

    job_id: str
    number: int
    state: str
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None
    retryable: bool | None
    data_ref: str | None
    data_sha256: str | None


@dataclass(frozen=True)
class StepRecord:
    name: str
    device_id: str
    depends_on: list[str]
    status: str
    attempts: list[Attempt]

    @property
    def latest(self) -> Attempt | None:
        """The last attempt to reach the instrument: what the step's own times,
        error and data are taken from."""
        return next((a for a in reversed(self.attempts) if a.started_at), None)

    def view(self) -> StepView:
        latest = self.latest
        return StepView(
            name=self.name,
            device_id=self.device_id,
            depends_on=self.depends_on,
            status=self.status,
            started_at=latest.started_at if latest else None,
            finished_at=latest.finished_at if latest else None,
            error=latest.error if latest else None,
            data_ref=latest.data_ref if latest else None,
        )


@dataclass(frozen=True)
class RunRecord:
    id: str
    workflow_name: str
    status: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    steps: list[StepRecord]

    @property
    def is_over(self) -> bool:
        return self.status not in (RUN_PENDING, RUN_RUNNING)

    def view(self) -> RunView:
        return RunView(
            id=self.id,
            workflow_name=self.workflow_name,
            status=self.status,
            created_at=self.created_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            steps=[step.view() for step in self.steps],
        )


class Candidate(NamedTuple):
    """A step of a run that is going on, as the scheduler needs to see it."""

    run_id: str
    name: str
    device_id: str
    depends_on: list[str]
    status: str
    attempts: int
    has_open_job: bool


def to_start(candidates: Sequence[Candidate], busy: set[str]) -> list[Candidate]:
    """The steps to put on their instruments now, oldest run first.

    The scheduler keeps a device to one step within a run. `busy` keeps it to
    one step across runs, and is added to as devices are taken.
    """
    chosen: list[Candidate] = []
    for _, of_run in groupby(candidates, key=lambda step: step.run_id):
        by_name = {step.name: step for step in of_run}
        states = [
            # A step with an open job occupies its device, whatever its status.
            StepState(
                step.name,
                step.device_id,
                step.depends_on,
                STEP_RUNNING if step.has_open_job else step.status,
            )
            for step in by_name.values()
        ]
        for name in next_steps(states):
            step = by_name[name]
            if step.device_id not in busy:
                busy.add(step.device_id)
                chosen.append(step)
    return chosen


@dataclass(frozen=True)
class OpenJob:
    """A job whose instrument may still have something to say about it."""

    id: str
    run_id: str
    step_name: str
    device_id: str
    state: str


@dataclass(frozen=True)
class Verdict:
    job_state: str
    step_status: str
    error: str | None = None
    retryable: bool | None = None
    stops_run: bool = False


def verdict(state: str, attempt: int, run_is_ending: bool, report: JobReport) -> Verdict | None:
    """What a driver's report means for a job and its step. None: nothing new."""
    match report.state:
        case protocol.RUNNING if state != JOB_RUNNING:
            return Verdict(JOB_RUNNING, STEP_RUNNING)
        case protocol.COMPLETED:
            return Verdict(JOB_COMPLETED, STEP_COMPLETED)
        case protocol.FAILED:
            return _failure(attempt, run_is_ending, report.error, report.retryable)
        case protocol.UNKNOWN if state == JOB_REVOKED:
            # A second attempt is only ever made at a step that has failed.
            return Verdict(JOB_CANCELLED, STEP_CANCELLED if attempt == 1 else STEP_FAILED)
        case protocol.UNKNOWN if state == JOB_RUNNING:
            return _failure(attempt, run_is_ending, VANISHED, retryable=False)
    return None


def _failure(attempt: int, run_is_ending: bool, error: str, retryable: bool) -> Verdict:
    """A step is tried again only when its instrument says that is safe."""
    again = retryable and attempt < MAX_ATTEMPTS and not run_is_ending
    return Verdict(
        JOB_FAILED,
        STEP_PENDING if again else STEP_FAILED,
        error=error,
        retryable=retryable,
        stops_run=not again,
    )


class Store:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def create_run(
        self, run_id: str, workflow_name: str, steps: Sequence[StepTemplate]
    ) -> None:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO runs (id, workflow_name) VALUES (%s, %s)", (run_id, workflow_name)
            )
            await cur.executemany(
                "INSERT INTO steps (run_id, name, position, device_id, depends_on)"
                " VALUES (%s, %s, %s, %s, %s)",
                [
                    (run_id, step.name, position, step.device_id, list(step.depends_on))
                    for position, step in enumerate(steps)
                ],
            )

    async def start_run(self, run_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = 'running', started_at = now()"
                " WHERE id = %s AND status = 'pending'",
                (run_id,),
            )

    async def cancel_run(self, run_id: str) -> None:
        """Nothing more is started. What is already on an instrument still ends
        as it ends, and the run with it."""
        async with self._pool.connection() as conn:
            run = await _one(conn, "SELECT status FROM runs WHERE id = %s FOR UPDATE", run_id)
            if run is None:
                raise RunNotFound(run_id)
            if run[0] == RUN_PENDING:
                await conn.execute(
                    "UPDATE runs SET status = 'cancelled', finished_at = now() WHERE id = %s",
                    (run_id,),
                )
                await conn.execute(
                    "UPDATE steps SET status = 'cancelled' WHERE run_id = %s", (run_id,)
                )
            elif run[0] == RUN_RUNNING:
                await _stop(conn, run_id, RUN_CANCELLED)

    async def run(self, run_id: str) -> RunRecord:
        async with self._pool.connection() as conn:
            rows = await _all(conn, _RUN_BY_ID, run_id)
        if not rows:
            raise RunNotFound(run_id)
        return _records(rows)[0]

    async def runs(self, status: str | None = None, limit: int | None = None) -> list[RunRecord]:
        """Newest first."""
        async with self._pool.connection() as conn:
            return _records(await _all(conn, _RUNS_BY_STATUS, status, status, limit))

    async def reconcile(self) -> tuple[set[str], list[str]]:
        """Makes the record say what should happen next: ends every run that
        has gone quiet, and opens a job for every step that can start.

        Returns the runs it changed, and every job that is now open.
        """
        async with self._pool.connection() as conn:
            await conn.execute("SELECT FROM runs WHERE status = 'running' FOR UPDATE")
            changed = {row[0] for row in await _all(conn, _FINISH_QUIET_RUNS)}

            candidates = [Candidate(*row) for row in await _all(conn, _STEPS_OF_RUNS_GOING_ON)]
            busy = {row[0] for row in await _all(conn, _BUSY_DEVICES)}
            for step in to_start(candidates, busy):
                attempt = step.attempts + 1
                await conn.execute(
                    "INSERT INTO jobs (id, run_id, step_name, attempt, device_id)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (
                        protocol.job_id(step.run_id, step.name, attempt),
                        step.run_id,
                        step.name,
                        attempt,
                        step.device_id,
                    ),
                )
                changed.add(step.run_id)
            open_jobs = [row[0] for row in await _all(conn, _OPEN_JOB_IDS)]
        return changed, open_jobs

    async def open_job(self, job_id: str) -> OpenJob | None:
        async with self._pool.connection() as conn:
            row = await _one(
                conn,
                "SELECT id, run_id, step_name, device_id, state FROM jobs"
                " WHERE id = %s AND state IN ('pending', 'revoked', 'running')",
                job_id,
            )
        return OpenJob(*row) if row else None

    async def record(self, job: OpenJob, report: JobReport) -> bool:
        """Writes down what a driver said about a job. False if it was not news."""
        async with self._pool.connection() as conn:
            run = await _one(
                conn, "SELECT ending_as FROM runs WHERE id = %s FOR UPDATE", job.run_id
            )
            row = await _one(conn, "SELECT state, attempt FROM jobs WHERE id = %s", job.id)
            assert run is not None and row is not None, f"job {job.id} has left the record"
            state, attempt = row
            if state not in (JOB_PENDING, JOB_REVOKED, JOB_RUNNING):
                return False
            decided = verdict(state, attempt, run[0] is not None, report)
            if decided is None:
                return False

            await conn.execute(
                "UPDATE jobs SET state = %s, started_at = COALESCE(%s, started_at),"
                " finished_at = %s, error = %s, retryable = %s, data_ref = %s, data_sha256 = %s"
                " WHERE id = %s",
                (
                    decided.job_state,
                    _moment(report.started_at),
                    _moment(report.finished_at),
                    decided.error,
                    decided.retryable,
                    report.data_ref,
                    report.data_sha256,
                    job.id,
                ),
            )
            await conn.execute(
                "UPDATE steps SET status = %s WHERE run_id = %s AND name = %s",
                (decided.step_status, job.run_id, job.step_name),
            )
            if decided.stops_run:
                await _stop(conn, job.run_id, RUN_FAILED)
        return True


async def _stop(conn: AsyncConnection[Any], run_id: str, ending_as: str) -> None:
    """Decides how a run will end, and takes back whatever has not started.

    A job an instrument was asked to start is only revoked: it may have started
    all the same, and its step stays open until its driver says which. A step
    that was waiting to be tried again has failed; it is not called cancelled.
    """
    await conn.execute(
        "UPDATE runs SET ending_as = COALESCE(ending_as, %s) WHERE id = %s", (ending_as, run_id)
    )
    await conn.execute(
        "UPDATE jobs SET state = 'revoked' WHERE run_id = %s AND state = 'pending'", (run_id,)
    )
    await conn.execute(
        "UPDATE steps s SET status = CASE WHEN EXISTS ("
        "   SELECT FROM jobs j WHERE j.run_id = s.run_id AND j.step_name = s.name"
        "      AND j.state = 'failed') THEN 'failed' ELSE 'cancelled' END"
        " WHERE s.run_id = %s AND s.status = 'pending'"
        " AND NOT EXISTS (SELECT FROM jobs j WHERE j.run_id = s.run_id"
        " AND j.step_name = s.name AND j.state = 'revoked')",
        (run_id,),
    )


def _records(rows: Sequence[Sequence[Any]]) -> list[RunRecord]:
    """Rows of run, step and job columns, one per job, into one record per run."""
    runs: dict[str, RunRecord] = {}
    steps: dict[tuple[str, str], StepRecord] = {}
    for run_id, workflow_name, status, created_at, started_at, finished_at, *rest in rows:
        if run_id not in runs:
            runs[run_id] = RunRecord(
                run_id, workflow_name, status, created_at, started_at, finished_at, []
            )
        name, device_id, depends_on, step_status, *job = rest
        if name is None:
            continue
        if (run_id, name) not in steps:
            steps[run_id, name] = StepRecord(name, device_id, depends_on, step_status, [])
            runs[run_id].steps.append(steps[run_id, name])
        if job[0] is not None:
            steps[run_id, name].attempts.append(Attempt(*job))
    return list(runs.values())


def _moment(epoch_seconds: float | None) -> datetime | None:
    return None if epoch_seconds is None else datetime.fromtimestamp(epoch_seconds, UTC)


async def _all(conn: AsyncConnection[Any], query: str, *params: Any) -> list[Any]:
    return await (await conn.execute(query, params)).fetchall()


async def _one(conn: AsyncConnection[Any], query: str, *params: Any) -> Any:
    return await (await conn.execute(query, params)).fetchone()
