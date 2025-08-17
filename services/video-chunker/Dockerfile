# Use Python 3.11 slim image as base
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.8.6 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV PYTHONPATH=/app

COPY uv.lock pyproject.toml readme.md ./
RUN uv sync --frozen --no-dev

COPY video_chunker ./video_chunker

EXPOSE 8000

# Run the application
CMD ["uv", "run", "--frozen", "--no-dev", "video_chunker"]
