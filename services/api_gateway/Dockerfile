# ─── Stage 1: Install dependencies ───────────────────────────────
FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy only dependency files first (better layer caching)
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
RUN uv sync --frozen --no-dev --no-editable

# ─── Stage 2: Runtime ─────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY shared/ ./shared/
COPY services/api_gateway/ ./services/api_gateway/

# Ensure the venv's Python is used
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["uvicorn", "services.api_gateway.app.main:app", "--host", "0.0.0.0", "--port", "8000"]

