SHELL := /bin/bash

# Convenience variables
UVX := uvx
RUFF := $(UVX) ruff
PYTEST := uv run pytest
MYPY := uv run mypy

fmt:
	$(RUFF) format
	$(RUFF) check --fix

lint:
	$(RUFF) check
	# $(MYPY) video_chunker

unit-tests:
	$(PYTEST) tests/unit

integration-tests:
	$(PYTEST) tests/integration

e2e-tests:
	$(PYTEST) tests/e2e


