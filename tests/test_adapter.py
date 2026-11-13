"""The driver's answers, against the real instrument and its ledger."""

import asyncio
import hashlib
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from services.common.protocol import COMPLETED, FAILED, RUNNING, UNKNOWN, JobReport, JobRequest
from services.common.storage import Storage, StorageError
from services.driver import adapter
from services.driver.adapter import LOST, Driver
from services.driver.instrument import Instrument

JOB = "run-1/read_plate/1"
STEP_SECONDS = 0.05
HANG_SECONDS = 0.2

MakeInstrument = Callable[..., Instrument]


class MemoryStorage(Storage):
    def __init__(self) -> None:
        self.bucket = "lab-data"
        self.objects: dict[str, bytes] = {}
        self.down = False

    async def put(self, key: str, data: bytes) -> str:
        self._check()
        self.objects[key] = data
        return self.ref(key)

    async def exists(self, key: str) -> bool:
        self._check()
        return key in self.objects

    def _check(self) -> None:
        if self.down:
            raise StorageError("storage is down")


@pytest.fixture(autouse=True)
def quick(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(adapter, "WATCH_INTERVAL", 0.02)
    monkeypatch.setattr(adapter, "STORE_RETRY_DELAY", 0.02)


@pytest.fixture
def storage() -> MemoryStorage:
    return MemoryStorage()


@pytest.fixture
def instrument(tmp_path: Path) -> MakeInstrument:
    """Builds the instrument. Building it again is a restart: same ledger."""

    def build(**overrides: object) -> Instrument:
        settings: dict[str, object] = {
            "duration": STEP_SECONDS,
            "fail_pct": 0,
            "hang_pct": 0,
            "hang_secs": HANG_SECONDS,
            "failures_retryable": True,
            "produces_data": False,
            "seed": "0",
            "ledger_dir": tmp_path,
        }
        return Instrument("plate-reader-1", **(settings | overrides))  # type: ignore[arg-type]

    return build


def start(job_id: str = JOB, *, wait: float = 0.0) -> JobRequest:
    return JobRequest(job_id, "run-1", "read_plate", start_by=time.time() + 5, wait=wait)


def ask(job_id: str = JOB, *, wait: float = 0.0) -> JobRequest:
    return JobRequest(job_id, "run-1", "read_plate", start_by=None, wait=wait)


async def ended(driver: Driver, job_id: str = JOB) -> JobReport:
    return await asyncio.wait_for(driver.report(ask(job_id, wait=5)), timeout=5)


async def test_a_job_runs_once_however_often_it_is_asked_for(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    device = instrument()
    driver = Driver(device, storage)

    first, again = await asyncio.gather(driver.report(start()), driver.report(start()))
    end = await ended(driver)
    after = await driver.report(start())

    assert (first.state, again.state, end.state, after.state) == (
        RUNNING,
        RUNNING,
        COMPLETED,
        COMPLETED,
    )
    assert len(device.status().executions) == 1


async def test_the_times_reported_are_the_instruments_own(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    device = instrument()
    driver = Driver(device, storage)

    running = await driver.report(start())
    end = await ended(driver)

    [execution] = device.status().executions
    assert running.started_at == end.started_at == execution.started_at
    assert end.finished_at == execution.finished_at


async def test_the_reply_is_held_until_a_running_job_ends(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    driver = Driver(instrument(), storage)
    await driver.report(start())

    assert (await driver.report(ask(wait=0))).state == RUNNING
    began = time.monotonic()
    assert (await driver.report(ask(wait=5))).state == COMPLETED
    assert time.monotonic() - began < 1


@pytest.mark.parametrize(
    "request_",
    [ask(), JobRequest(JOB, "run-1", "read_plate", start_by=time.time() - 1, wait=0)],
    ids=["not asked to start", "asked too late"],
)
async def test_nothing_starts_unless_it_is_still_wanted(
    instrument: MakeInstrument, storage: MemoryStorage, request_: JobRequest
) -> None:
    device = instrument()

    report = await Driver(device, storage).report(request_)

    assert report.state == UNKNOWN
    assert device.status().executions == []


async def test_a_busy_instrument_refuses_and_can_be_asked_again(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    device = instrument()
    driver = Driver(device, storage)
    await driver.report(start("run-1/fill/1"))

    refused = await driver.report(start("run-2/fill/1"))
    await ended(driver, "run-1/fill/1")
    accepted = await driver.report(start("run-2/fill/1"))

    assert (refused.state, accepted.state) == (UNKNOWN, RUNNING)
    assert "busy" in refused.detail
    assert [e.job_id for e in device.status().executions] == ["run-1/fill/1", "run-2/fill/1"]


@pytest.mark.parametrize("retryable", [True, False])
async def test_a_failure_carries_the_instruments_view_of_running_it_again(
    instrument: MakeInstrument, storage: MemoryStorage, retryable: bool
) -> None:
    driver = Driver(instrument(fail_pct=100, failures_retryable=retryable), storage)
    await driver.report(start())

    report = await ended(driver)

    assert (report.state, report.retryable) == (FAILED, retryable)
    assert report.error == "instrument error during read_plate"


async def test_a_hang_is_reported_once_the_instrument_gives_up(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    device = instrument(hang_pct=100, failures_retryable=False)
    driver = Driver(device, storage)
    await driver.report(start())

    assert (await driver.report(ask(wait=HANG_SECONDS / 2))).state == RUNNING
    report = await ended(driver)

    assert (report.state, report.error, report.retryable) == (FAILED, "instrument hung", False)
    assert not device.status().busy


async def test_a_job_that_ends_just_as_it_is_checked_is_not_taken_for_a_hang(
    instrument: MakeInstrument, storage: MemoryStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ledger shows the end a moment before run() returns."""
    monkeypatch.setattr(adapter, "WATCH_INTERVAL", STEP_SECONDS / 2)
    device = instrument(produces_data=True)
    driver = Driver(device, storage)
    await driver.report(start())

    report = await ended(driver)

    assert (report.state, report.error) == (COMPLETED, "")
    assert f"{JOB}.csv" in storage.objects


async def test_after_a_restart_the_ledger_answers(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    driver = Driver(instrument(), storage)
    await driver.report(start())
    before = await ended(driver)

    restarted = instrument()
    after = await Driver(restarted, storage).report(start())

    assert after == before
    assert len(restarted.status().executions) == 1


async def test_a_job_cut_off_by_a_restart_is_reported_as_failed(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    await Driver(instrument(duration=60), storage).report(start())

    report = await Driver(instrument(), storage).report(start())

    assert report.state == FAILED
    assert report.error == "instrument restarted during the job"


async def test_a_measurement_is_stored_before_the_job_is_called_complete(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    device = instrument(produces_data=True)
    driver = Driver(device, storage)
    await driver.report(start())

    report = await ended(driver)

    stored = storage.objects[f"{JOB}.csv"]
    assert report.data_ref == f"s3://lab-data/{JOB}.csv"
    assert report.data_sha256 == hashlib.sha256(stored).hexdigest()
    assert report.data_sha256 == device.status().executions[0].data_sha256


async def test_a_measurement_waits_for_storage_to_come_back(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    storage.down = True
    driver = Driver(instrument(produces_data=True), storage)
    await driver.report(start())

    still = await driver.report(ask(wait=STEP_SECONDS * 3))
    storage.down = False
    report = await ended(driver)

    assert (still.state, report.state) == (RUNNING, COMPLETED)
    assert f"{JOB}.csv" in storage.objects


async def test_a_measurement_lost_in_a_restart_is_a_failure_not_a_reference(
    instrument: MakeInstrument, storage: MemoryStorage
) -> None:
    storage.down = True
    driver = Driver(instrument(produces_data=True), storage)
    await driver.report(start())
    await driver.report(ask(wait=STEP_SECONDS * 3))
    driver.close()

    storage.down = False
    report = await Driver(instrument(produces_data=True), storage).report(start())

    assert (report.state, report.error, report.retryable) == (FAILED, LOST, True)
    assert report.data_ref is None
