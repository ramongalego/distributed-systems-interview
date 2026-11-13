import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .models import RunView
from .schema import graphql_router
from .service import Executor, RunNotFound
from .workflows import WorkflowError, load_workflows

log = logging.getLogger(__name__)


class CreateRunRequest(BaseModel):
    workflow_name: str | None = None


def _error(code: int, msg: str) -> JSONResponse:
    return JSONResponse(status_code=code, content={"error": msg})


def _ms(later: datetime, earlier: datetime) -> int:
    return int((later - earlier).total_seconds() * 1000)


def create_app(executor: Executor, workflows_file: str) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await executor.start()
        log.info("executor ready")
        try:
            yield
        finally:
            await executor.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.include_router(graphql_router(executor, workflows_file), prefix="/graphql")

    @app.exception_handler(RunNotFound)
    async def _on_run_not_found(request: Request, exc: Exception) -> JSONResponse:
        return _error(404, "run not found")

    @app.exception_handler(NotImplementedError)
    async def _on_not_implemented(request: Request, exc: Exception) -> JSONResponse:
        return _error(501, str(exc) or "not implemented")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "healthy"}

    @app.get("/workflows")
    async def list_workflows() -> Any:
        """Report what is currently defined in workflows.yaml."""
        try:
            wf_set = load_workflows(workflows_file)
        except WorkflowError as exc:
            return _error(500, str(exc))
        return [
            {
                "name": name,
                "is_default": name == wf_set.default,
                "steps": [st.name for st in wf_set.get(name)],
            }
            for name in wf_set.names
        ]

    @app.get("/runs")
    async def list_runs() -> list[RunView]:
        return await executor.list_runs()

    @app.post("/runs", status_code=201)
    async def create_run(body: CreateRunRequest | None = None) -> Any:
        # Read fresh so edits to workflows.yaml take effect without a restart.
        try:
            wf_set = load_workflows(workflows_file)
        except WorkflowError as exc:
            return _error(500, str(exc))
        name = (body.workflow_name if body else None) or wf_set.default
        try:
            template = wf_set.get(name)
        except WorkflowError as exc:
            return _error(400, str(exc))
        return await executor.create_run(name, template)

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> RunView:
        return await executor.get_run(run_id)

    @app.post("/runs/{run_id}/start")
    async def start_run(run_id: str) -> RunView:
        return await executor.start_run(run_id)

    @app.post("/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> RunView:
        return await executor.cancel_run(run_id)

    @app.get("/runs/{run_id}/timeline")
    async def timeline(run_id: str) -> Any:
        """When each step occupied its device, relative to the start of the run."""
        run = await executor.get_run(run_id)
        origin = run.started_at or run.created_at
        return {
            "run_id": run.id,
            "status": run.status,
            "total_duration_ms": _ms(run.finished_at, origin) if run.finished_at else None,
            "steps": [
                {
                    "name": st.name,
                    "device_id": st.device_id,
                    "status": st.status,
                    "depends_on": st.depends_on,
                    "start_offset_ms": _ms(st.started_at, origin) if st.started_at else None,
                    "end_offset_ms": _ms(st.finished_at, origin) if st.finished_at else None,
                    "error": st.error,
                    "data_ref": st.data_ref,
                }
                for st in run.steps
            ],
        }

    return app
