"""
Pipeline orchestrator for coordinating the extract-transform-load process.

This module provides:
- High-level orchestration of ETL pipeline
- Error handling and retry logic
- Metrics collection and reporting
- Support for different ingestion modes
"""

import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from prefect import flow, task

from config.logging_config import get_logger
from src.core.database import get_shared_engine
from src.models import CleanData, RunModeEnum
from src.pipeline.extract.extractor import DataExtractor
from src.pipeline.load.loader import DataLoader
from src.pipeline.transform.transformer import DataTransformer

logger = get_logger(__name__)


@flow(
    name="market-data-pipeline",
    description="Complete ETL pipeline for market data processing",
)
def run_pipeline(
    start_date: date,
    end_date: date,
    ingestion_mode: RunModeEnum = RunModeEnum.DEFAULT,
    expressions: Optional[List[str]] = None,
    db_connection_string: str = "sqlite:///market_data.db",
) -> Dict[str, Any]:
    """
    Run the complete ETL pipeline.

    Args:
        start_date: Start date for data extraction
        end_date: End date for data extraction
        ingestion_mode: Type of ingestion run
        expressions: Optional list of specific expressions to process
        db_connection_string: Database connection string for ETL components

    Returns:
        Dictionary with comprehensive pipeline metrics
    """
    engine = get_shared_engine(db_connection_string)

    extractor = DataExtractor(engine=engine)
    transformer = DataTransformer(engine=engine)
    loader = DataLoader(engine=engine)

    pipeline_start = datetime.now(timezone.utc)

    pipeline_metrics = {
        "run_id": f"{ingestion_mode.value}_{start_date}_{end_date}_{int(time.time())}",
        "ingestion_mode": ingestion_mode.value,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "start_time": pipeline_start.isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "success": False,
        "extract_metrics": {},
        "transform_metrics": {},
        "load_metrics": {},
        "errors": [],
    }

    try:
        logger.info(
            f"Starting {ingestion_mode.value} pipeline run for {start_date} to {end_date}"
        )

        logger.info("Phase 1: Extracting data from API...")
        extract_metrics = _run_extract_phase(
            start_date, end_date, ingestion_mode, expressions, extractor
        )
        pipeline_metrics["extract_metrics"] = extract_metrics

        logger.info("Phase 2: Transforming raw data...")
        clean_records, transform_metrics = _run_transform_phase(transformer)
        pipeline_metrics["transform_metrics"] = transform_metrics

        logger.info("Phase 3: Loading clean data...")
        load_metrics = _run_load_phase(clean_records, loader)
        pipeline_metrics["load_metrics"] = load_metrics

        pipeline_metrics["success"] = True
        pipeline_end = datetime.now(timezone.utc)
        pipeline_metrics["end_time"] = pipeline_end.isoformat()
        pipeline_metrics["duration_seconds"] = (
            pipeline_end - pipeline_start
        ).total_seconds()

        _print_pipeline_summary(pipeline_metrics)

    except (ValueError, ConnectionError, RuntimeError) as e:
        pipeline_metrics["errors"].append(str(e))
        pipeline_metrics["success"] = False
        pipeline_end = datetime.now(timezone.utc)
        pipeline_metrics["end_time"] = pipeline_end.isoformat()
        pipeline_metrics["duration_seconds"] = (
            pipeline_end - pipeline_start
        ).total_seconds()
        logger.error(f"Pipeline failed: {str(e)}")

    return pipeline_metrics


@task(name="extract-data", description="Extract data from API sources")
def _run_extract_phase(
    start_date: date,
    end_date: date,
    ingestion_mode: RunModeEnum,
    expressions: Optional[List[str]],
    extractor: DataExtractor,
) -> Dict[str, int]:
    """Run the extraction phase."""
    if expressions is None:
        expressions = extractor.get_expressions_for_mode(ingestion_mode)

    return extractor.extract_data(expressions, start_date, end_date, ingestion_mode)


@task(name="transform-data", description="Transform raw data into clean format")
def _run_transform_phase(
    transformer: DataTransformer,
) -> Tuple[List[CleanData], Dict[str, int]]:
    """Run the transformation phase."""
    return transformer.transform_raw_data()


@task(name="load-data", description="Load clean data into target storage")
def _run_load_phase(
    clean_records: List[CleanData], loader: DataLoader
) -> Dict[str, int]:
    """Run the loading phase."""
    return loader.load_clean_data(clean_records)


def _print_pipeline_summary(metrics: Dict[str, Any]) -> None:
    """Print a summary of pipeline execution."""
    logger.info("\n" + "=" * 60)
    logger.info(f"Pipeline Summary - Run ID: {metrics['run_id']}")
    logger.info("=" * 60)

    logger.info(f"Mode: {metrics['ingestion_mode']}")
    logger.info(f"Date Range: {metrics['start_date']} to {metrics['end_date']}")
    logger.info(f"Duration: {metrics['duration_seconds']:.2f} seconds")
    logger.info(f"Success: {metrics['success']}")

    if metrics.get("extract_metrics"):
        extract = metrics["extract_metrics"]
        logger.info("\nExtraction:")
        logger.info(
            f"  - Expressions processed: {extract.get('expressions_processed', 0)}"
        )
        logger.info(f"  - Rows fetched: {extract.get('rows_fetched', 0)}")
        logger.info(f"  - Rows inserted: {extract.get('rows_inserted', 0)}")
        logger.info(f"  - Duplicates detected: {extract.get('duplicates_detected', 0)}")
        logger.info(f"  - Errors: {extract.get('errors', 0)}")

    if metrics.get("transform_metrics"):
        transform = metrics["transform_metrics"]
        logger.info("\nTransformation:")
        logger.info(f"  - Rows processed: {transform.get('rows_processed', 0)}")
        logger.info(f"  - Rows transformed: {transform.get('rows_transformed', 0)}")
        logger.info(f"  - Rows rejected: {transform.get('rows_rejected', 0)}")
        logger.info(f"  - Validation errors: {transform.get('validation_errors', 0)}")

    if metrics.get("load_metrics"):
        load = metrics["load_metrics"]
        logger.info("\nLoading:")
        logger.info(f"  - Records processed: {load.get('records_processed', 0)}")
        logger.info(f"  - Records inserted: {load.get('records_inserted', 0)}")
        logger.info(f"  - Records updated: {load.get('records_updated', 0)}")
        logger.info(f"  - Records failed: {load.get('records_failed', 0)}")

    if metrics.get("errors"):
        logger.error("\nErrors:")
        for error in metrics["errors"]:
            logger.error(f"  - {error}")

    logger.info("=" * 60)


def run_daily_pipeline(
    run_date: date, db_connection_string: str = "sqlite:///market_data.db"
) -> Dict[str, Any]:
    """Run default daily pipeline for a single business day."""
    return run_pipeline.fn(
        start_date=run_date,
        end_date=run_date,
        ingestion_mode=RunModeEnum.DEFAULT,
        db_connection_string=db_connection_string,
    )


def run_old_codes_ingestion(
    start_date: date,
    end_date: date,
    db_connection_string: str = "sqlite:///market_data.db",
) -> Dict[str, Any]:
    """Run ingestion including old codes."""
    return run_pipeline.fn(
        start_date=start_date,
        end_date=end_date,
        ingestion_mode=RunModeEnum.OLD_CODES,
        db_connection_string=db_connection_string,
    )


def run_historical_backfill(
    start_date: date,
    end_date: date,
    db_connection_string: str = "sqlite:///market_data.db",
) -> Dict[str, Any]:
    """Run historical backfill for old codes only."""
    return run_pipeline.fn(
        start_date=start_date,
        end_date=end_date,
        ingestion_mode=RunModeEnum.HISTORICAL,
        db_connection_string=db_connection_string,
    )


def validate_data_integrity(
    db_connection_string: str = "sqlite:///market_data.db",
) -> Dict[str, Any]:
    """Run data integrity validation across the pipeline."""
    engine = get_shared_engine(db_connection_string)
    return DataLoader(engine=engine).validate_clean_data_integrity()


if __name__ == "__main__":
    run_pipeline.deploy(
        name="market-data-daily-pipeline",
        work_pool_name="azure-container-instance-pool",
        description="Daily market data ETL pipeline for business days",
        version="1.0.0",
        tags=["market-data", "etl", "daily", "production"],
        cron="0 8 * * 1-5",
        parameters={
            "start_date": date.today(),
            "end_date": date.today(),
            "ingestion_mode": RunModeEnum.DEFAULT,
            "expressions": None,
            "db_connection_string": "sqlite:///market_data.db",
        },
    )
