FROM ghcr.io/astral-sh/uv:0.12.2 AS uv
FROM python:3.13-slim

COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ src/
COPY alembic.ini ./
COPY migrations/ migrations/
RUN uv sync --frozen --no-dev --no-editable
ENV PATH="/app/.venv/bin:$PATH"
USER 10001:10001
