"""The executor side: keeps the record, and makes it come true.

Two things run for as long as the executor does. One loop reads the record and
does what it implies: it ends the runs that have gone quiet and opens a job for
every step that can start. And for each open job, one task asks the job's
driver about it until there is nothing left to learn. Neither remembers
anything the record does not hold, so a restart carries on from the record.
"""

import asyncio
import contextlib
import logging
import secrets
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator, Sequence

import nats.errors
import psycopg
from nats.aio.client import Client as NATSClient
from psycopg_pool import AsyncConnectionPool

from ..common.protocol import UNKNOWN, JobReport, JobRequest, subject
from ..common.storage import Storage
from .models import RunView
from .store import JOB_PENDING, JOB_REVOKED, JOB_RUNNING, OpenJob, RunNotFound, RunRecord, Store
from .workflows import StepTemplate

__all__ = ["Executor", "RunNotFound"]

log = logging.getLogger(__name__)

# A request to start a job is good for START_WINDOW, and its reply is waited
# for a little longer. So once the executor has given up on a reply, that
# request can no longer start anything.
START_WINDOW = 1.5
REPLY_TIMEOUT = 2.0
# How long a driver holds its reply open for a running job to end. A reply
# lost to a partition costs this much before the question is asked again.
HOLD = 5.0
RETRY_DELAY = 1.0
# The record is re-read this often even if nothing said it had changed.
SWEEP_INTERVAL = 5.0


class Executor:
    def __init__(self, pool: AsyncConnectionPool, nc: NATSClient, storage: Storage) -> None:
        self.pool = pool
        self.nc = nc
        self.storage = storage
        self._store = Store(pool)
        self._stale = asyncio.Event()
        self._reconciling: asyncio.Task[None] | None = None
        self._supervising: dict[str, asyncio.Task[None]] = {}
        self._following: defaultdict[str, set[asyncio.Event]] = defaultdict(set)

    async def start(self) -> None:
        """Called once, when the executor process starts."""
        self._reconciling = asyncio.create_task(self._reconcile_forever())

    async def close(self) -> None:
        """Called once, when the executor process stops."""
        tasks = [*self._supervising.values()]
        if self._reconciling is not None:
            tasks.append(self._reconciling)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def create_run(self, workflow_name: str, steps: Sequence[StepTemplate]) -> RunView:
        return (await self.create(workflow_name, steps)).view()

    async def start_run(self, run_id: str) -> RunView:
        return (await self.begin(run_id)).view()

    async def cancel_run(self, run_id: str) -> RunView:
        return (await self.cancel(run_id)).view()

    async def get_run(self, run_id: str) -> RunView:
        return (await self.run(run_id)).view()

    async def list_runs(self) -> list[RunView]:
        return [record.view() for record in await self.runs()]

    async def create(self, workflow_name: str, steps: Sequence[StepTemplate]) -> RunRecord:
        run_id = f"run-{secrets.token_hex(6)}"
        await self._store.create_run(run_id, workflow_name, steps)
        return await self.run(run_id)

    async def begin(self, run_id: str) -> RunRecord:
        await self._store.start_run(run_id)
        self._changed(run_id)
        return await self.run(run_id)

    async def cancel(self, run_id: str) -> RunRecord:
        await self._store.cancel_run(run_id)
        self._changed(run_id)
        return await self.run(run_id)

    async def run(self, run_id: str) -> RunRecord:
        return await self._store.run(run_id)

    async def runs(self, status: str | None = None, limit: int | None = None) -> list[RunRecord]:
        return await self._store.runs(status, limit)

    async def follow(self, run_id: str) -> AsyncIterator[RunRecord]:
        """The run as it is, then again each time it changes, until it is over."""
        with self._watching(run_id) as changed:
            seen = None
            while seen is None or not seen.is_over:
                changed.clear()
                record = await self.run(run_id)
                if record != seen:
                    yield record
                    seen = record
                if not record.is_over:
                    await changed.wait()

    async def _reconcile_forever(self) -> None:
        while True:
            self._stale.clear()
            try:
                changed, open_jobs = await self._store.reconcile()
            except Exception as exc:
                # Whatever it was, this loop is what moves every run forward.
                unexpected = not isinstance(exc, psycopg.Error)
                log.warning("could not reconcile the record: %r", exc, exc_info=unexpected)
                await asyncio.sleep(RETRY_DELAY)
                continue
            for run_id in changed:
                self._notify(run_id)
            for job_id in open_jobs:
                if job_id not in self._supervising:
                    self._supervising[job_id] = asyncio.create_task(self._supervise(job_id))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stale.wait(), SWEEP_INTERVAL)

    async def _supervise(self, job_id: str) -> None:
        try:
            await self._resolve(job_id)
        except Exception:
            log.exception("lost track of job %s, picking it up again", job_id)
            await asyncio.sleep(RETRY_DELAY)
        finally:
            del self._supervising[job_id]
            self._stale.set()

    async def _resolve(self, job_id: str) -> None:
        """Asks a job's driver about it until there is nothing left to learn."""
        while True:
            try:
                job = await self._store.open_job(job_id)
                if job is None:
                    return
                if job.state == JOB_REVOKED:
                    # Whatever request to start it is still on its way will
                    # have expired, so "unknown" will mean it never ran.
                    await asyncio.sleep(START_WINDOW)
                report = await self._ask(job)
                if await self._store.record(job, report):
                    log.info("job %s: %s %s", job.id, report.state, report.error)
                    self._changed(job.run_id)
                elif report.state == UNKNOWN:
                    log.info("job %s: not started: %s", job.id, report.detail)
                    await asyncio.sleep(RETRY_DELAY)
            except (nats.errors.Error, psycopg.Error) as exc:
                log.warning("job %s: asking again after %r", job_id, exc)
                await asyncio.sleep(RETRY_DELAY)

    async def _ask(self, job: OpenJob) -> JobReport:
        request = JobRequest(
            job.id,
            job.run_id,
            job.step_name,
            start_by=time.time() + START_WINDOW if job.state == JOB_PENDING else None,
            wait=HOLD if job.state == JOB_RUNNING else 0.0,
        )
        reply = await self.nc.request(
            subject(job.device_id), request.encode(), timeout=request.wait + REPLY_TIMEOUT
        )
        return JobReport.decode(reply.data)

    def _changed(self, run_id: str) -> None:
        self._stale.set()
        self._notify(run_id)

    def _notify(self, run_id: str) -> None:
        for changed in self._following.get(run_id, ()):
            changed.set()

    @contextlib.contextmanager
    def _watching(self, run_id: str) -> Iterator[asyncio.Event]:
        changed = asyncio.Event()
        self._following[run_id].add(changed)
        try:
            yield changed
        finally:
            self._following[run_id].discard(changed)
            if not self._following[run_id]:
                del self._following[run_id]
