# Use Python 3.11 slim image as base
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install ffmpeg libsm6 libxext6  -y

COPY --from=ghcr.io/astral-sh/uv:0.8.6 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV PYTHONPATH=/app

COPY uv.lock pyproject.toml readme.md ./
RUN uv sync --frozen --no-dev

COPY scene_detector ./scene_detector

# Run the application
CMD ["uv", "run", "--frozen", "--no-dev", "scene_detector"]
