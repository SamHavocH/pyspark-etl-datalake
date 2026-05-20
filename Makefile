.PHONY: install run test lint format check docker-build docker-run clean

install:
	uv sync --all-extras

run:
	uv run datalake run

test:
	uv run pytest

lint:
	uv run ruff check src tests

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

check: lint test

docker-build:
	docker compose build

docker-run:
	docker compose run --rm datalake datalake run

clean:
	python scripts/clean_outputs.py
