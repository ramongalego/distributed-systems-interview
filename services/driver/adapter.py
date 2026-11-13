"""The driver side: connects one instrument to the executor.

It answers one question about one job (see common/protocol.py), and what it
says comes from the instrument: the ledger for what has happened, the status
for what is happening now. It keeps nothing of its own that it cannot afford
to lose, so a restart does not change what it will report.
"""

import asyncio
import logging
import time
from dataclasses import dataclass

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

from ..common.protocol import COMPLETED, FAILED, RUNNING, UNKNOWN, JobReport, JobRequest, subject
from ..common.storage import Storage, StorageError
from .instrument import OUTCOME_COMPLETED, OUTCOME_HUNG, Execution, Instrument, Job, Outcome

log = logging.getLogger(__name__)

# How often a job that has not returned is checked against the instrument's
# own status. A hung job never returns; only the status says that it is over.
WATCH_INTERVAL = 1.0
STORE_RETRY_DELAY = 1.0

LOST = "the instrument completed, but its measurement was lost before it could be stored"


async def serve(instrument: Instrument, nc: NATSClient, storage: Storage) -> None:
    driver = Driver(instrument, storage)
    answering: set[asyncio.Task[None]] = set()

    async def on_request(msg: Msg) -> None:
        # Not awaited: the client hands over one message at a time, and an
        # answer can take as long as a job.
        task = asyncio.create_task(_answer(driver, msg))
        answering.add(task)
        task.add_done_callback(answering.discard)

    subscription = await nc.subscribe(subject(instrument.device_id), cb=on_request)
    log.info("driver %s: answering on %s", instrument.device_id, subscription.subject)
    try:
        await asyncio.Event().wait()
    finally:
        for task in answering:
            task.cancel()
        driver.close()
        await subscription.unsubscribe()


async def _answer(driver: "Driver", msg: Msg) -> None:
    """No reply is an answer too: the executor asks again."""
    try:
        request = JobRequest.decode(msg.data)
        report = await driver.report(request)
        await msg.respond(report.encode())
    except StorageError as exc:
        log.warning("cannot answer %r yet: %s", msg.data, exc)
    except Exception:
        log.exception("could not answer %r", msg.data)


@dataclass(frozen=True)
class Flight:
    """A job this process started and has not finished reporting on."""

    # Running, or unknown if the instrument refused the job.
    departure: asyncio.Future[JobReport]
    # How it ended. For a job with data, not before the data is stored.
    landing: asyncio.Task[JobReport]


class Driver:
    def __init__(self, instrument: Instrument, storage: Storage) -> None:
        self._instrument = instrument
        self._storage = storage
        self._flights: dict[str, Flight] = {}

    async def report(self, request: JobRequest) -> JobReport:
        """What the instrument has to say about a job, starting it first if
        that is what was asked and it has never been started."""
        flight = self._flights.get(request.job_id)
        if flight is None:
            execution = self._find(request.job_id)
            if execution is not None:
                return await self._report(execution, outcome=None)
            if request.start_by is None or time.time() > request.start_by:
                return JobReport(UNKNOWN, detail="not asked to start it, or asked too late")
            flight = self._take_off(Job(request.job_id, request.run_id, request.step_name))

        departure = await asyncio.shield(flight.departure)
        if departure.state != RUNNING:
            return departure
        landed, _ = await asyncio.wait({flight.landing}, timeout=request.wait)
        return flight.landing.result() if landed else departure

    def close(self) -> None:
        for flight in self._flights.values():
            flight.landing.cancel()

    def _take_off(self, job: Job) -> Flight:
        departure: asyncio.Future[JobReport] = asyncio.get_running_loop().create_future()
        flight = Flight(departure, asyncio.create_task(self._fly(job, departure)))
        self._flights[job.job_id] = flight
        return flight

    async def _fly(self, job: Job, departure: asyncio.Future[JobReport]) -> JobReport:
        work = asyncio.ensure_future(self._instrument.run(job))
        execution = None
        while execution is None and not work.done():
            # run() writes the ledger before it first waits, or raises instead.
            await asyncio.sleep(0)
            execution = self._find(job.job_id)
        if execution is None:
            report = JobReport(UNKNOWN, detail=str(work.exception()))
            departure.set_result(report)
        else:
            departure.set_result(JobReport(RUNNING, started_at=execution.started_at))
            outcome = await self._outcome(job, work)
            report = await self._report(self._found(job.job_id), outcome)
        # Only once there is an answer: a flight that fails stays, so nothing
        # is reported about the job until this process is restarted.
        del self._flights[job.job_id]
        return report

    async def _outcome(self, job: Job, work: asyncio.Future[Outcome]) -> Outcome | None:
        """What run() returned, or None if the job hung: then it never returns.

        Any other end shows in the ledger a moment before run() returns it.
        """
        while True:
            returned, _ = await asyncio.wait({work}, timeout=WATCH_INTERVAL)
            if returned:
                return work.result()
            if self._found(job.job_id).outcome == OUTCOME_HUNG:
                work.cancel()
                return None

    async def _report(self, execution: Execution, outcome: Outcome | None) -> JobReport:
        """How an execution ended. `outcome` is None when all that is left of it
        is the ledger: it hung, or this process has restarted since."""
        if execution.outcome != OUTCOME_COMPLETED:
            retryable = outcome.retryable if outcome else self._instrument.failures_retryable
            return _ended(execution, FAILED, error=execution.error, retryable=retryable)
        if execution.data_sha256 is None:
            return _ended(execution, COMPLETED)

        key = f"{execution.job_id}.csv"
        if outcome is not None and outcome.data is not None:
            await self._store(key, outcome.data)
        elif not await self._storage.exists(key):
            retryable = self._instrument.failures_retryable
            return _ended(execution, FAILED, error=LOST, retryable=retryable)
        return _ended(
            execution,
            COMPLETED,
            data_ref=self._storage.ref(key),
            data_sha256=execution.data_sha256,
        )

    async def _store(self, key: str, data: bytes) -> None:
        """Until it is stored: this process holds the only copy."""
        while True:
            try:
                await self._storage.put(key, data)
                return
            except StorageError as exc:
                log.warning("%s is not stored yet: %s", key, exc)
                await asyncio.sleep(STORE_RETRY_DELAY)

    def _find(self, job_id: str) -> Execution | None:
        executions = self._instrument.status().executions
        return next((e for e in reversed(executions) if e.job_id == job_id), None)

    def _found(self, job_id: str) -> Execution:
        execution = self._find(job_id)
        assert execution is not None, f"{job_id} has left the ledger"
        return execution


def _ended(
    execution: Execution,
    state: str,
    *,
    error: str = "",
    retryable: bool = False,
    data_ref: str | None = None,
    data_sha256: str | None = None,
) -> JobReport:
    return JobReport(
        state,
        started_at=execution.started_at,
        finished_at=execution.finished_at,
        error=error,
        retryable=retryable,
        data_ref=data_ref,
        data_sha256=data_sha256,
    )
