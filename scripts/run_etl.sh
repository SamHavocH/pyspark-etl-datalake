#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs data/processed

if [[ -d ".venv" ]]; then
  source .venv/bin/activate
fi

if [[ -f ".env" ]]; then
  set -a
  source .env
  set +a
fi

python -m src.cli
