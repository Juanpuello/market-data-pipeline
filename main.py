"""
Market Data Pipeline - Main Entry Point

This script provides a simple CLI for running the market data ETL pipeline
with different modes and configurations.
"""

import argparse
import sys
from datetime import date, datetime, timedelta

from config.logging_config import get_logger, setup_logging
from src.core.database import create_database_engine, create_tables
from src.models import RunModeEnum
from src.pipeline.orchestrator import run_pipeline

# Setup logging
setup_logging()
logger = get_logger(__name__)


def setup_database():
    """Initialize database tables."""
    engine, _ = create_database_engine("sqlite:///market_data.db")
    create_tables(engine)
    logger.info("Database tables created/verified")


def run_demo():
    """Run a demonstration of the pipeline with sample data."""
    logger.info("Running Market Data Pipeline Demo")
    logger.info("=" * 50)

    setup_database()

    end_date = date.today() + timedelta(days=1)
    start_date = end_date + timedelta(days=0)
    logger.info(f"Demo date range: {start_date} to {end_date}")

    logger.info("1. Running DEFAULT mode (new codes only)...")
    default_metrics = run_pipeline(
        start_date=start_date, end_date=end_date, ingestion_mode=RunModeEnum.DEFAULT
    )

    logger.info("2. Running OLD_CODES mode (includes old codes)...")
    old_codes_metrics = run_pipeline(
        start_date=start_date, end_date=end_date, ingestion_mode=RunModeEnum.OLD_CODES
    )

    logger.info("=" * 50)
    logger.info("Demo completed successfully!")


def run_pipeline_cli():
    """Run pipeline with command line arguments."""
    parser = argparse.ArgumentParser(description="Market Data Pipeline")
    parser.add_argument(
        "--mode",
        choices=["default", "old_codes", "historical"],
        default="default",
        help="Pipeline mode",
    )
    parser.add_argument(
        "--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument("--setup-db", action="store_true", help="Setup database tables")

    args = parser.parse_args()

    if args.setup_db:
        setup_database()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    mode_mapping = {
        "default": RunModeEnum.DEFAULT,
        "old_codes": RunModeEnum.OLD_CODES,
        "historical": RunModeEnum.HISTORICAL,
    }

    metrics = run_pipeline(
        start_date=start_date, end_date=end_date, ingestion_mode=mode_mapping[args.mode]
    )

    if not metrics["success"]:
        logger.error("Pipeline execution failed")
        sys.exit(1)
    else:
        logger.info("Pipeline execution completed successfully")


def main():
    """Main entry point."""
    if len(sys.argv) == 1:
        run_demo()
    else:
        run_pipeline_cli()


if __name__ == "__main__":
    main()
