FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.8.6 /uv /uvx /bin/


WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV PYTHONPATH=/app

COPY uv.lock pyproject.toml readme.md ./
RUN uv sync --frozen --no-dev

COPY system_view ./system_view

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uv", "run", "--frozen", "--no-dev", "uvicorn", "system_view.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
