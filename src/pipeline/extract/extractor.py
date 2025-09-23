"""
Data extraction module for fetching market data from API and storing raw responses.

This module handles:
- Fetching data from MarketData API
- Simulating blob storage for raw responses
- Inserting raw data into database with versioning
- Supporting different ingestion modes (default, old_codes, historical)
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from config.logging_config import get_logger
from src.core.database import create_database_engine
from src.market_data_api import MarketData, create_sample_expressions
from src.models import APIRequest, APIResponse, RawData, RunModeEnum

logger = get_logger(__name__)


class DataExtractor:
    """Handles data extraction from API and storage to raw_data table."""

    def __init__(
        self,
        db_connection_string: Optional[str] = None,
        engine: Optional[Engine] = None,
    ):
        """
        Initialize extractor with database connection.

        Args:
            db_connection_string: Database connection string (legacy, will create new engine)
            engine: Shared SQLAlchemy engine instance
        """
        if engine is not None:
            self.engine = engine
        elif db_connection_string is not None:
            self.engine = create_database_engine(db_connection_string)
        else:
            raise ValueError("Either provide engine or db_connection_string")

        self.api_client = MarketData()
        self.blob_storage_path = Path("blob_storage")
        self.blob_storage_path.mkdir(exist_ok=True)

    def extract_data(
        self,
        expressions: List[str],
        start_date: date,
        end_date: date,
        ingestion_mode: RunModeEnum = RunModeEnum.DEFAULT,
    ) -> Dict[str, int]:
        """
        Extract data for given expressions and date range.

        Args:
            expressions: List of API expression codes
            start_date: Start date for data extraction
            end_date: End date for data extraction
            ingestion_mode: Type of ingestion run

        Returns:
            Dictionary with extraction metrics
        """
        if ingestion_mode in [RunModeEnum.DEFAULT, RunModeEnum.OLD_CODES]:
            if start_date != end_date:
                raise ValueError(
                    f"{ingestion_mode.value} mode requires start_date and end_date to be the same. "
                    f"Got start_date={start_date}, end_date={end_date}"
                )

        metrics = {
            "expressions_processed": 0,
            "rows_fetched": 0,
            "rows_inserted": 0,
            "duplicates_detected": 0,
            "errors": 0,
        }

        with Session(self.engine) as session:
            for expression in expressions:
                try:
                    # Validate API request
                    api_request = APIRequest(
                        expression=expression, start_date=start_date, end_date=end_date
                    )

                    if not self._should_fetch_data(
                        session, expression, start_date, end_date, ingestion_mode
                    ):
                        continue

                    df = self.api_client.get_historical_data(
                        expression, start_date, end_date
                    )
                    if df.empty:
                        continue

                    blob_uri = self._store_blob(df, expression, start_date, end_date)

                    rows_inserted = self._insert_raw_data(
                        session, df, expression, blob_uri, ingestion_mode
                    )

                    metrics["expressions_processed"] += 1
                    metrics["rows_fetched"] += len(df)
                    metrics["rows_inserted"] += rows_inserted

                except Exception as e:
                    logger.error(f"Error processing {expression}: {str(e)}")
                    metrics["errors"] += 1
                    continue

            session.commit()

        return metrics

    def _should_fetch_data(
        self,
        session: Session,
        expression: str,
        start_date: date,
        end_date: date,
        ingestion_mode: RunModeEnum,
    ) -> bool:
        """Check if data should be fetched based on existing records."""
        # Always fetch for HISTORICAL mode (allows overwrites)
        if ingestion_mode == RunModeEnum.HISTORICAL:
            return True

        # For DEFAULT and OLD_CODES modes, check version limit (max 3 records per expression+date)
        if ingestion_mode in [RunModeEnum.DEFAULT, RunModeEnum.OLD_CODES]:
            stmt = select(RawData).where(
                RawData.expression == expression, RawData.date == start_date
            )
            return len(session.exec(stmt).all()) < 3

        raise ValueError(f"Unsupported ingestion mode: {ingestion_mode}")

    def _store_blob(
        self, df: pd.DataFrame, expression: str, start_date: date, end_date: date
    ) -> str:
        """Store raw API response as JSON file and return URI."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = (
            f"{expression.replace('(', '').replace(')', '').replace(',', '_')}_"
            f"{start_date}_{end_date}_{timestamp}.json"
        )
        file_path = self.blob_storage_path / filename

        # Add metadata data for blob storage
        blob_data = {
            "expression": expression,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            "data": df.to_dict(orient="records"),
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(blob_data, f, indent=2, default=str)
        return f"blob://market-data/{filename}"

    def _insert_raw_data(
        self,
        session: Session,
        df: pd.DataFrame,
        expression: str,
        blob_uri: str,
        ingestion_mode: RunModeEnum,
    ) -> int:
        """Insert raw data records into database with individual versioning per expression+date."""
        rows_inserted = 0
        fetch_timestamp = datetime.now(timezone.utc)

        for _, row in df.iterrows():
            api_response = APIResponse(date=row["date"], value=float(row["value"]))

            stmt = select(func.coalesce(func.max(RawData.version), 0) + 1).where(
                RawData.expression == expression,
                RawData.date == api_response.date,
            )
            version = session.exec(stmt).first() or 1

            raw_record = RawData(
                expression=expression,
                date=api_response.date,
                value=api_response.value,
                fetch_timestamp=fetch_timestamp,
                version=version,
                ingestion_mode=ingestion_mode.value,
                source_file_uri=blob_uri,
            )

            session.add(raw_record)
            rows_inserted += 1

        return rows_inserted

    def get_expressions_for_mode(self, mode: RunModeEnum) -> List[str]:
        """Get appropriate expressions for the given ingestion mode."""
        sample_expressions = create_sample_expressions()

        if mode == RunModeEnum.DEFAULT:
            return sample_expressions["new_codes"]
        if mode == RunModeEnum.OLD_CODES:
            return sample_expressions["all_codes"]
        if mode == RunModeEnum.HISTORICAL:
            return sample_expressions["old_codes"]
        raise ValueError(f"Unsupported ingestion mode: {mode}")
