.PHONY: install up down sample-data ingest transform gold quality-check pipeline test lint type-check format clean

install:
	uv sync --extra dev

up:
	docker compose up -d

down:
	docker compose down

sample-data:
	uv run medallion sample-data

ingest:
	uv run medallion ingest

transform:
	uv run medallion transform

gold:
	uv run medallion gold

quality-check:
	uv run medallion quality-check

pipeline:
	uv run medallion pipeline

test:
	uv run pytest

lint:
	uv run ruff check src tests

type-check:
	uv run mypy

format:
	uv run ruff format src tests

clean:
	python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/bronze','data/silver','data/gold','data/rejected_records','data/quality_reports','data/spark-warehouse','.pytest_cache','.ruff_cache']]"
