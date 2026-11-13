"""Starts one driver: the instrument, its status side channel, and your adapter."""

import asyncio
import logging
import os
import re
import signal
import sys
from pathlib import Path

import nats

from ..common.connection import NATS
from ..common.storage import Storage
from . import adapter
from .instrument import Instrument, serve_status

log = logging.getLogger("driver")

STATUS_PORT = 7000

_DURATION_UNITS = {"ms": 1e-3, "s": 1.0, "m": 60.0}
_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m)")


def parse_duration(name: str, text: str) -> float:
    """Parse a duration such as '2s', '500ms' or '1m30s' into seconds."""
    parts = _DURATION_PART.findall(text.strip())
    if not parts or "".join(n + u for n, u in parts) != text.strip():
        sys.exit(f"{name} is not a duration: {text!r}")
    return sum(float(n) * _DURATION_UNITS[u] for n, u in parts)


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        sys.exit(f"{name} environment variable is required")
    return value


def _flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if not raw:
        return default
    lowered = raw.strip().lower()
    if lowered in ("true", "1", "yes"):
        return True
    if lowered in ("false", "0", "no"):
        return False
    sys.exit(f"{name} must be true or false, got {raw!r}")


def _pct(name: str) -> int:
    raw = os.getenv(name)
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        sys.exit(f"{name} must be a whole number from 0 to 100, got {raw!r}")
    if not 0 <= value <= 100:
        sys.exit(f"{name} must be a whole number from 0 to 100, got {raw!r}")
    return value


async def run() -> None:
    device_id = _required("WORKER_ID")
    nats_url = _required("NATS_URL")
    storage = Storage(_required("S3_ENDPOINT"), _required("S3_BUCKET"))

    fail_pct = _pct("FAIL_PCT")
    hang_pct = _pct("HANG_PCT")
    if fail_pct + hang_pct > 100:
        sys.exit(f"FAIL_PCT + HANG_PCT must not be more than 100, got {fail_pct + hang_pct}")

    instrument = Instrument(
        device_id,
        duration=parse_duration("STEP_DURATION", os.getenv("STEP_DURATION") or "2s"),
        fail_pct=fail_pct,
        hang_pct=hang_pct,
        hang_secs=parse_duration("HANG_DURATION", os.getenv("HANG_DURATION") or "30s"),
        failures_retryable=_flag("FAILURES_RETRYABLE", default=True),
        produces_data=_flag("PRODUCES_DATA", default=False),
        seed=os.getenv("CHAOS_SEED") or "0",
        ledger_dir=Path(os.getenv("LEDGER_DIR") or "/ledger"),
    )
    status_server = await serve_status(instrument, STATUS_PORT)
    log.info(
        "instrument %s ready (step %gs, fail %d%%, hang %d%%)",
        device_id,
        instrument.duration,
        fail_pct,
        hang_pct,
    )

    try:
        nc = await nats.connect(nats_url, **NATS)
    except OSError as exc:
        sys.exit(f"connect nats {nats_url}: {exc}")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    serving = asyncio.create_task(adapter.serve(instrument, nc, storage))
    stopping = asyncio.create_task(stop.wait())
    done, _ = await asyncio.wait({serving, stopping}, return_when=asyncio.FIRST_COMPLETED)
    failed = serving in done
    if failed and (error := serving.exception()) is not None:
        log.error("driver %s: adapter.serve stopped with an error: %r", device_id, error)
    elif failed:
        log.error(
            "driver %s: adapter.serve returned; it is expected to run until shutdown", device_id
        )

    log.info("driver %s shutting down", device_id)
    stopping.cancel()
    serving.cancel()
    # Let serve finish its own clean-up before the connection goes away.
    await asyncio.gather(serving, return_exceptions=True)
    status_server.close()
    await status_server.wait_closed()
    await nc.close()
    if failed:
        # A non-zero exit, so that Docker restarts the driver.
        sys.exit(1)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
