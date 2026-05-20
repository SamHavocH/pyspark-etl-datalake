import argparse

from pyspark_medallion.config import load_settings
from pyspark_medallion.ingestion.synthetic import generate_ecommerce_dataset
from pyspark_medallion.jobs import build_gold, ingest_bronze, quality_check, run_pipeline, transform_silver
from pyspark_medallion.monitoring.logging import configure_logging, get_logger


def main() -> int:
    parser = argparse.ArgumentParser(prog="medallion")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("sample-data")
    subparsers.add_parser("ingest")
    subparsers.add_parser("transform")
    subparsers.add_parser("gold")
    subparsers.add_parser("quality-check")
    subparsers.add_parser("pipeline")
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    if args.command == "sample-data":
        generate_ecommerce_dataset(settings.raw_dir)
        logger.info("sample data generated", extra={"event": "sample_data_generated"})
    elif args.command == "ingest":
        ingest_bronze.run(settings)
    elif args.command == "transform":
        transform_silver.run(settings)
    elif args.command == "gold":
        build_gold.run(settings)
    elif args.command == "quality-check":
        quality_check.run(settings)
    elif args.command == "pipeline":
        run_pipeline.run(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
