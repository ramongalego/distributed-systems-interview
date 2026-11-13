"""The instrument: the physical piece of lab equipment behind a driver.

This file is fixed. Do not change it. It is the only part of the system that
stands for physical reality, and the checks read its ledger to find out what
really happened. A submission that changes this file cannot be checked.

What it does:

  - It does one job at a time. Offered a job while busy, it raises
    InstrumentBusy straight away. It does not queue.
  - A job either completes, fails (FAIL_PCT), or hangs (HANG_PCT). A hung job
    never returns from run(). The instrument frees itself after HANG_DURATION, but
    nothing reports what happened.
  - Every physical execution is written to the ledger the moment it starts, and
    updated when it ends. The ledger survives restarts.
  - status() tells you what it is doing now and what it has done.

Whether a job fails or hangs is decided from CHAOS_SEED, the step name and how
many times this instrument has run that step for that run before. The same seed
gives the same faults.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger("instrument")

OUTCOME_RUNNING = "running"
OUTCOME_COMPLETED = "completed"
OUTCOME_FAILED = "failed"
OUTCOME_HUNG = "hung"


class InstrumentBusy(Exception):
    """The instrument is already running a job."""


@dataclass(frozen=True)
class Job:
    """One piece of physical work.

    job_id is yours to choose. The instrument records it in the ledger and
    reports it back in status(). run_id and step_name say which step of which
    run this work is for.
    """

    job_id: str
    run_id: str
    step_name: str


@dataclass(frozen=True)
class Outcome:
    """How a job ended. error is empty on success.

    retryable is the instrument's own view of whether the same step could
    sensibly be run again. It only means anything when error is set.

    data is the measurement, for instruments that produce one (the plate
    reader). It is None otherwise, and None when the job failed.
    """

    job_id: str
    error: str = ""
    retryable: bool = False
    data: bytes | None = None


@dataclass
class Execution:
    """One entry in the ledger: the instrument physically started a job."""

    execution_id: str
    job_id: str
    run_id: str
    step_name: str
    started_at: float
    outcome: str = OUTCOME_RUNNING
    finished_at: float | None = None
    error: str = ""
    data_sha256: str | None = None


@dataclass(frozen=True)
class InstrumentStatus:
    device_id: str
    busy: bool
    current_job_id: str
    executions: list[Execution] = field(default_factory=list)


class Instrument:
    def __init__(
        self,
        device_id: str,
        *,
        duration: float,
        fail_pct: int,
        hang_pct: int,
        hang_secs: float,
        failures_retryable: bool,
        produces_data: bool,
        seed: str,
        ledger_dir: Path,
    ) -> None:
        self.device_id = device_id
        self.duration = duration
        self.fail_pct = fail_pct
        self.hang_pct = hang_pct
        self.hang_secs = hang_secs
        self.failures_retryable = failures_retryable
        self.produces_data = produces_data
        self._seed = seed
        self._ledger_path = ledger_dir / f"{device_id}.jsonl"
        self._executions: dict[str, Execution] = self._load_ledger()
        self._busy = False
        self._current_job_id = ""
        self._tasks: set[asyncio.Task[None]] = set()

    async def run(self, job: Job) -> Outcome:
        """Physically run one job, and return when it ends.

        Raises InstrumentBusy straight away if a job is already running, and
        OSError if the ledger cannot be written; in both cases nothing ran.
        A job that hangs never returns.
        """
        # No await between the check and the set, so no other coroutine can
        # slip in between them.
        if self._busy:
            raise InstrumentBusy(f"{self.device_id} is busy with {self._current_job_id}")

        attempt = sum(
            1
            for e in self._executions.values()
            if e.run_id == job.run_id and e.step_name == job.step_name
        )
        execution = Execution(
            execution_id=uuid.uuid4().hex,
            job_id=job.job_id,
            run_id=job.run_id,
            step_name=job.step_name,
            started_at=time.time(),
        )
        # Nothing physical happens unless the ledger has it first.
        self._record(execution)
        self._busy = True
        self._current_job_id = job.job_id
        log.info("started %s (job=%s run=%s)", job.step_name, job.job_id, job.run_id)

        fault = self._fault(job.step_name, attempt)
        # The physical work runs in its own task: a caller that stops waiting
        # does not stop the instrument, just as in a lab.
        done: asyncio.Future[Outcome] = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(self._work(job, execution, fault, done))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return await asyncio.shield(done)

    def status(self) -> InstrumentStatus:
        """What the instrument is doing now, and everything it has ever done."""
        return InstrumentStatus(
            device_id=self.device_id,
            busy=self._busy,
            current_job_id=self._current_job_id,
            executions=[Execution(**asdict(e)) for e in self._executions.values()],
        )

    async def _work(
        self, job: Job, execution: Execution, fault: str, done: asyncio.Future[Outcome]
    ) -> None:
        if fault == OUTCOME_HUNG:
            log.info("HUNG %s (job=%s) -- no report will follow", job.step_name, job.job_id)
            await asyncio.sleep(self.hang_secs)
            self._finish(execution, OUTCOME_HUNG, error="instrument hung")
            log.info("recovered from hang on %s (job=%s)", job.step_name, job.job_id)
            # done is never resolved: whoever is waiting waits for ever.
            return

        await asyncio.sleep(self.duration)

        if fault == OUTCOME_FAILED:
            error = f"instrument error during {job.step_name}"
            self._finish(execution, OUTCOME_FAILED, error=error)
            log.info("FAILED %s (job=%s)", job.step_name, job.job_id)
            done.set_result(
                Outcome(job_id=job.job_id, error=error, retryable=self.failures_retryable)
            )
            return

        data = self._measure(job) if self.produces_data else None
        self._finish(
            execution,
            OUTCOME_COMPLETED,
            data_sha256=hashlib.sha256(data).hexdigest() if data is not None else None,
        )
        log.info("completed %s (job=%s)", job.step_name, job.job_id)
        done.set_result(Outcome(job_id=job.job_id, data=data))

    def _finish(
        self,
        execution: Execution,
        outcome: str,
        *,
        error: str = "",
        data_sha256: str | None = None,
    ) -> None:
        execution.outcome = outcome
        execution.finished_at = time.time()
        execution.error = error
        execution.data_sha256 = data_sha256
        self._busy = False
        self._current_job_id = ""
        try:
            self._record(execution)
        except OSError as exc:
            # The work has ended whatever the disk says. status() still has the
            # end; only the ledger file is behind.
            log.error(
                "ledger %s: could not record the end of %s (job=%s, outcome=%s): %s",
                self._ledger_path,
                execution.step_name,
                execution.job_id,
                outcome,
                exc,
            )

    def _fault(self, step_name: str, attempt: int) -> str:
        roll = _roll(self._seed, self.device_id, step_name, attempt)
        if roll < self.fail_pct:
            return OUTCOME_FAILED
        if roll < self.fail_pct + self.hang_pct:
            return OUTCOME_HUNG
        return OUTCOME_COMPLETED

    @staticmethod
    def _measure(job: Job) -> bytes:
        """A 96-well absorbance reading."""
        digest = hashlib.sha256(job.job_id.encode()).digest()
        rows = ["well,absorbance"]
        for i in range(96):
            well = f"{'ABCDEFGH'[i // 12]}{i % 12 + 1}"
            value = digest[i % len(digest)] / 255 * 2.5
            rows.append(f"{well},{value:.4f}")
        return ("\n".join(rows) + "\n").encode()

    def _record(self, execution: Execution) -> None:
        self._append(execution)
        self._executions[execution.execution_id] = execution

    def _append(self, execution: Execution) -> None:
        with self._ledger_path.open("a") as fh:
            fh.write(json.dumps(asdict(execution)) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _load_ledger(self) -> dict[str, Execution]:
        executions: dict[str, Execution] = {}
        if not self._ledger_path.exists():
            return executions
        for line in self._ledger_path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                entry = Execution(**json.loads(line))
            except (json.JSONDecodeError, TypeError) as exc:
                log.error("ledger %s: unreadable line %r: %s", self._ledger_path, line, exc)
                continue
            executions[entry.execution_id] = entry
        # An execution still "running" in the ledger was cut off by a restart.
        # Write that down too, so that the ledger and status() agree.
        for entry in executions.values():
            if entry.outcome == OUTCOME_RUNNING:
                entry.outcome = OUTCOME_HUNG
                entry.finished_at = time.time()
                entry.error = "instrument restarted during the job"
                self._append(entry)
        return executions


def _roll(seed: str, device_id: str, step_name: str, attempt: int) -> int:
    key = f"{seed}:{device_id}:{step_name}:{attempt}".encode()
    return int.from_bytes(hashlib.sha256(key).digest()[:4], "big") % 100


async def serve_status(instrument: Instrument, port: int) -> asyncio.Server:
    """A read-only side channel, used by the checks: GET /status.

    It does not go through NATS, so it does not depend on your protocol.
    """

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            while (await reader.readline()) not in (b"\r\n", b"\n", b""):
                pass
            parts = request_line.decode(errors="replace").split()
            if len(parts) >= 2 and parts[0] == "GET" and parts[1] == "/status":
                payload: Any = _status_payload(instrument)
                code = "200 OK"
            else:
                payload = {"error": "only GET /status is served"}
                code = "404 Not Found"
            body = json.dumps(payload).encode()
            writer.write(
                f"HTTP/1.1 {code}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError) as exc:
            log.warning("status side channel: %s", exc)
        finally:
            writer.close()

    return await asyncio.start_server(handle, host="0.0.0.0", port=port)


def _status_payload(instrument: Instrument) -> dict[str, Any]:
    st = instrument.status()
    return {
        "device_id": st.device_id,
        "busy": st.busy,
        "current_job_id": st.current_job_id,
        "failures_retryable": instrument.failures_retryable,
        "fail_pct": instrument.fail_pct,
        "hang_pct": instrument.hang_pct,
        "executions": [asdict(e) for e in st.executions],
    }
