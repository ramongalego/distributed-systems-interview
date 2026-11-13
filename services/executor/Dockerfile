FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.8.23 /uv /bin/uv
WORKDIR /src
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
# Dependencies first, exactly as locked, so a code change does not reinstall them.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project
COPY services ./services
RUN uv sync --frozen --no-editable

FROM python:3.12-slim AS runtime
RUN useradd --uid 10001 --create-home app
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /app
COPY config /app/config
USER app
EXPOSE 5001
ENTRYPOINT ["python", "-m", "services.executor.main"]
