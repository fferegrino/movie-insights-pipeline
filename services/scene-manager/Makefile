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

test:
	$(PYTEST) tests
