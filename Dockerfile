# Dockerfile for the Flow-of-Funds MCP demo

FROM python:3.11-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

COPY . .

CMD uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000}