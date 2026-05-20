import argparse

from datalake.configs.settings import get_settings
from datalake.jobs.analytics import build_duckdb_serving_layer
from datalake.jobs.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(prog="datalake", description="PySpark weather data lake")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("run", help="Run the full medallion pipeline")
    subparsers.add_parser("build-serving", help="Refresh DuckDB views over silver and gold data")
    args = parser.parse_args()

    settings = get_settings()
    if args.command in (None, "run"):
        return run_pipeline(settings)
    if args.command == "build-serving":
        build_duckdb_serving_layer(settings.duckdb_path, settings.silver_dir, settings.gold_dir)
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
