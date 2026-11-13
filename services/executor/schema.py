"""The run record as GraphQL, served at /graphql.

Reading mirrors the record: a run, its steps, and every attempt made at each
step, under the job id its instrument knows it by. Writing is the three things
a tool can do to a run. Following a run is a subscription that ends when the
run does.
"""

from collections.abc import AsyncGenerator
from datetime import datetime
from enum import Enum

import strawberry
from graphql import GraphQLError
from strawberry.fastapi import GraphQLRouter
from strawberry.types import ExecutionContext

from . import store
from .service import Executor, RunNotFound
from .workflows import WorkflowError, WorkflowSet, load_workflows

MAX_RUNS = 100


@strawberry.enum
class RunStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@strawberry.enum
class StepStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@strawberry.enum
class AttemptStatus(Enum):
    PENDING = strawberry.enum_value(
        "pending", description="The instrument was asked to start it and has not confirmed."
    )
    RUNNING = strawberry.enum_value("running", description="The instrument confirmed the start.")
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = strawberry.enum_value(
        "cancelled", description="The instrument has no record of it: it never ran."
    )


@strawberry.type(description="A measurement, where it is stored and what it must hash to.")
class Data:
    ref: str = strawberry.field(description="s3://bucket/key")
    sha256: str = strawberry.field(description="Of the bytes the instrument produced.")

    @classmethod
    def of(cls, attempt: store.Attempt | None) -> "Data | None":
        if attempt is None or attempt.data_ref is None or attempt.data_sha256 is None:
            return None
        return cls(ref=attempt.data_ref, sha256=attempt.data_sha256)


@strawberry.type(description="One time a step was put on its instrument.")
class Attempt:
    job_id: str = strawberry.field(description="What the instrument's own ledger calls it.")
    number: int
    status: AttemptStatus
    started_at: datetime | None = strawberry.field(description="By the instrument's clock.")
    finished_at: datetime | None = strawberry.field(description="By the instrument's clock.")
    error: str | None
    retryable: bool | None = strawberry.field(
        description="After a failure: whether the instrument calls the step safe to run again."
    )
    data: Data | None

    @classmethod
    def of(cls, attempt: store.Attempt) -> "Attempt":
        revoked = attempt.state == store.JOB_REVOKED
        return cls(
            job_id=attempt.job_id,
            number=attempt.number,
            status=AttemptStatus.PENDING if revoked else AttemptStatus(attempt.state),
            started_at=attempt.started_at,
            finished_at=attempt.finished_at,
            error=attempt.error,
            retryable=attempt.retryable,
            data=Data.of(attempt),
        )


@strawberry.type(
    description="One node of the workflow, for one run. Its times, error and data are"
    " those of the last attempt to reach the instrument."
)
class Step:
    name: str
    device_id: str
    depends_on: list[str]
    status: StepStatus
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None
    data: Data | None
    attempts: list[Attempt] = strawberry.field(description="Every attempt, oldest first.")

    @classmethod
    def of(cls, step: store.StepRecord) -> "Step":
        latest = step.latest
        return cls(
            name=step.name,
            device_id=step.device_id,
            depends_on=step.depends_on,
            status=StepStatus(step.status),
            started_at=latest.started_at if latest else None,
            finished_at=latest.finished_at if latest else None,
            error=latest.error if latest else None,
            data=Data.of(latest),
            attempts=[Attempt.of(attempt) for attempt in step.attempts],
        )


@strawberry.type
class Run:
    id: strawberry.ID
    workflow_name: str
    status: RunStatus = strawberry.field(
        description="A run only ends once no instrument is still working for it."
    )
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    steps: list[Step] = strawberry.field(description="In workflow order.")

    @classmethod
    def of(cls, run: store.RunRecord) -> "Run":
        return cls(
            id=strawberry.ID(run.id),
            workflow_name=run.workflow_name,
            status=RunStatus(run.status),
            created_at=run.created_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            steps=[Step.of(step) for step in run.steps],
        )


@strawberry.type
class WorkflowStep:
    name: str
    device_id: str
    depends_on: list[str]


@strawberry.type
class Workflow:
    name: str
    is_default: bool
    steps: list[WorkflowStep]


class Schema(strawberry.Schema):
    def process_errors(
        self, errors: list[GraphQLError], execution_context: ExecutionContext | None = None
    ) -> None:
        """A refusal that carries a code is an answer, not a fault to log."""
        faults = [error for error in errors if "code" not in (error.extensions or {})]
        super().process_errors(faults, execution_context)


def _not_found(run_id: str) -> GraphQLError:
    return GraphQLError(f"no run {run_id}", extensions={"code": "RUN_NOT_FOUND"})


def graphql_router(executor: Executor, workflows_file: str) -> GraphQLRouter[None, None]:
    @strawberry.type
    class Query:
        @strawberry.field(description="Null if there is no such run.")
        async def run(self, id: strawberry.ID) -> Run | None:
            try:
                return Run.of(await executor.run(id))
            except RunNotFound:
                return None

        @strawberry.field(description=f"Newest first, at most {MAX_RUNS} at a time.")
        async def runs(self, status: RunStatus | None = None, limit: int = 20) -> list[Run]:
            records = await executor.runs(
                status.value if status else None, max(1, min(limit, MAX_RUNS))
            )
            return [Run.of(record) for record in records]

        @strawberry.field(description="What workflows.yaml defines now.")
        def workflows(self) -> list[Workflow]:
            defined = _workflows(workflows_file)
            return [
                Workflow(
                    name=name,
                    is_default=name == defined.default,
                    steps=[
                        WorkflowStep(
                            name=step.name, device_id=step.device_id, depends_on=step.depends_on
                        )
                        for step in defined.get(name)
                    ],
                )
                for name in defined.names
            ]

    @strawberry.type
    class Mutation:
        @strawberry.mutation(description="Creates a run of a workflow: the default one, if none.")
        async def create_run(self, workflow_name: str | None = None) -> Run:
            defined = _workflows(workflows_file)
            try:
                steps = defined.get(workflow_name or defined.default)
            except WorkflowError as exc:
                raise GraphQLError(str(exc), extensions={"code": "UNKNOWN_WORKFLOW"}) from exc
            return Run.of(await executor.create(workflow_name or defined.default, steps))

        @strawberry.mutation(description="Starts a pending run. Any other run is left as it is.")
        async def start_run(self, id: strawberry.ID) -> Run:
            try:
                return Run.of(await executor.begin(id))
            except RunNotFound:
                raise _not_found(id) from None

        @strawberry.mutation(
            description="Nothing more is started. A step already on its instrument ends as"
            " it ends, and the run with it."
        )
        async def cancel_run(self, id: strawberry.ID) -> Run:
            try:
                return Run.of(await executor.cancel(id))
            except RunNotFound:
                raise _not_found(id) from None

    @strawberry.type
    class Subscription:
        @strawberry.subscription(
            description="The run as it is, then again each time it changes, until it is over."
        )
        async def run(self, id: strawberry.ID) -> AsyncGenerator[Run]:
            try:
                async for record in executor.follow(id):
                    yield Run.of(record)
            except RunNotFound:
                raise _not_found(id) from None

    return GraphQLRouter(Schema(query=Query, mutation=Mutation, subscription=Subscription))


def _workflows(workflows_file: str) -> WorkflowSet:
    try:
        return load_workflows(workflows_file)
    except WorkflowError as exc:
        raise GraphQLError(str(exc), extensions={"code": "WORKFLOWS_UNREADABLE"}) from exc
