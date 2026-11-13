import asyncio
import logging
import os
import sys

import nats
import uvicorn
from psycopg_pool import AsyncConnectionPool

from ..common.connection import NATS
from ..common.storage import Storage, StorageError
from .api import create_app
from .service import Executor
from .workflows import WorkflowError, load_workflows

log = logging.getLogger(__name__)

PORT = 5001
DEFAULT_WORKFLOWS_FILE = "/app/config/workflows.yaml"

# A partition leaves connections that look open and carry nothing, and TCP
# alone takes minutes to give them up. These make each end do it in seconds:
# the client drops a connection that goes unanswered, and the server ends a
# transaction whose client has gone, so that it does not sit on its locks.
POSTGRES = {
    "connect_timeout": 3,
    "keepalives_idle": 5,
    "keepalives_interval": 2,
    "keepalives_count": 2,
    "tcp_user_timeout": 5000,
    "options": "-c idle_in_transaction_session_timeout=5000"
    " -c tcp_keepalives_idle=5 -c tcp_keepalives_interval=2 -c tcp_keepalives_count=2",
}


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        sys.exit(f"{name} environment variable is required")
    return value


async def run() -> None:
    database_url = _required("DATABASE_URL")
    nats_url = _required("NATS_URL")
    s3_endpoint = _required("S3_ENDPOINT")
    s3_bucket = _required("S3_BUCKET")
    workflows_file = os.getenv("WORKFLOWS_FILE") or DEFAULT_WORKFLOWS_FILE

    # Fail fast if the definitions are broken, rather than at the first run.
    try:
        wf_set = load_workflows(workflows_file)
    except WorkflowError as exc:
        sys.exit(f"Failed to load workflows: {exc}")
    log.info("loaded %d workflow(s), default %r", len(wf_set.names), wf_set.default)

    pool = AsyncConnectionPool(
        database_url,
        min_size=2,
        max_size=10,
        open=False,
        kwargs=POSTGRES,
        check=AsyncConnectionPool.check_connection,
    )
    await pool.open()
    await pool.wait()

    try:
        nc = await nats.connect(nats_url, **NATS)
    except OSError as exc:
        sys.exit(f"connect nats {nats_url}: {exc}")

    storage = Storage(s3_endpoint, s3_bucket)
    bucket = asyncio.create_task(_ensure_bucket(storage))

    executor = Executor(pool, nc, storage)
    app = create_app(executor, workflows_file)

    log.info("starting executor on :%d", PORT)
    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="warning", access_log=False)
    )
    try:
        await server.serve()
    finally:
        bucket.cancel()
        await nc.close()
        await pool.close()


async def _ensure_bucket(storage: Storage) -> None:
    """The executor does not need object storage to run, so it does not wait
    for it: a driver with data to store retries until the bucket is there."""
    while True:
        try:
            await storage.ensure_bucket()
            return
        except StorageError as exc:
            log.warning("object storage is not ready: %s", exc)
            await asyncio.sleep(1)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
