FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY shared/ ./shared/
COPY services/ingestion_consumer/ ./services/ingestion_consumer/

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "services.ingestion_consumer.app.main"]
