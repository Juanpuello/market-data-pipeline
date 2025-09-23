"""
Data loading module for upserting clean data with deduplication and versioning.

This module handles:
- Upserting clean data records with conflict resolution
- Maintaining data consistency and idempotency
- Handling versioning and deduplication
- Providing metrics for loaded data
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import and_
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

# Add parent directory to path for imports when running directly
if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.parent))

from src.core.database import create_database_engine
from src.models import CleanData, RawData


class DataLoader:
    """Handles loading of clean data with upsert capabilities."""

    def __init__(
        self,
        db_connection_string: Optional[str] = None,
        engine: Optional[Engine] = None,
    ):
        """
        Initialize loader with database connection.

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

    def load_clean_data(self, clean_records: List[CleanData]) -> Dict[str, int]:
        """
        Load clean data records with upsert logic.

        The transformer ensures we only receive records that need processing,
        so this method can focus on efficient upserts.

        Args:
            clean_records: List of CleanData records to load

        Returns:
            Dictionary with loading metrics
        """
        metrics = {
            "records_processed": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "records_failed": 0,
        }

        with Session(self.engine) as session:
            for clean_record in clean_records:
                try:
                    metrics["records_processed"] += 1
                    result = self._upsert_clean_record(session, clean_record)

                    if result == "inserted":
                        metrics["records_inserted"] += 1
                    elif result == "updated":
                        metrics["records_updated"] += 1

                except Exception as exc:
                    print(f"Error loading clean record: {exc}")
                    metrics["records_failed"] += 1
                    continue

            session.commit()

        return metrics

    def _upsert_clean_record(self, session: Session, clean_record: CleanData) -> str:
        """
        Upsert a single clean data record.

        Since the transformer already ensures we only receive records that either:
        1. Don't exist in clean_data, or
        2. Exist but are from an older version

        We can safely upsert without complex version checking.

        Returns:
            String indicating action taken: 'inserted' or 'updated'
        """
        existing_stmt = select(CleanData).where(
            CleanData.expression == clean_record.expression,
            CleanData.date == clean_record.date,
        )
        existing_record = session.exec(existing_stmt).first()

        if not existing_record:
            session.add(clean_record)
            return "inserted"
        existing_record.currency = clean_record.currency
        existing_record.x = clean_record.x
        existing_record.y = clean_record.y
        existing_record.ref = clean_record.ref
        existing_record.value = clean_record.value
        existing_record.raw_data_id = clean_record.raw_data_id
        session.add(existing_record)
        return "updated"

    def validate_clean_data_integrity(self) -> Dict[str, Any]:
        """
        Validate clean data integrity by checking for duplicates.

        Uses efficient JOIN with latest fetch_timestamp to avoid large WHERE clauses.

        Returns:
            Dictionary with validation results
        """
        integrity_report = {
            "valid": True,
            "issues": [],
            "duplicate_combinations": 0,
            "validation_fetch_timestamp": None,
            "records_checked": 0,
        }

        with Session(self.engine) as session:
            latest_fetch_stmt = (
                select(col(RawData.fetch_timestamp))
                .order_by(col(RawData.fetch_timestamp).desc())
                .limit(1)
            )
            latest_fetch_timestamp = session.exec(latest_fetch_stmt).first()

            if not latest_fetch_timestamp:
                integrity_report["issues"].append("No raw data records found")
                return integrity_report

            integrity_report["validation_fetch_timestamp"] = latest_fetch_timestamp

            latest_clean_data_stmt = (
                select(CleanData)
                .join(
                    RawData,
                    and_(
                        col(CleanData.expression) == col(RawData.expression),
                        col(CleanData.date) == col(RawData.date),
                    ),
                )
                .where(col(RawData.fetch_timestamp) == latest_fetch_timestamp)
            )

            latest_clean_data = list(session.exec(latest_clean_data_stmt).all())
            integrity_report["records_checked"] = len(latest_clean_data)

            expressions_seen = set()
            duplicates = 0

            for record in latest_clean_data:
                expression_date_combo = (record.expression, record.date)
                if expression_date_combo in expressions_seen:
                    duplicates += 1
                else:
                    expressions_seen.add(expression_date_combo)

            if duplicates > 0:
                integrity_report["valid"] = False
                integrity_report["duplicate_combinations"] = duplicates
                integrity_report["issues"].append(
                    f"Found {duplicates} duplicate expression+date combinations "
                    f"for fetch_timestamp {latest_fetch_timestamp}"
                )

        return integrity_report


def main():
    """Test the validate_clean_data_integrity method."""
    print("Testing DataLoader.validate_clean_data_integrity method")
    print("=" * 60)

    loader = DataLoader()

    try:
        integrity_report = loader.validate_clean_data_integrity()

        print("✅ Validation completed!")
        print(f"Valid: {integrity_report['valid']}")
        print(
            f"Validation fetch_timestamp: {integrity_report['validation_fetch_timestamp']}"
        )
        print(f"Records checked: {integrity_report['records_checked']}")
        print(f"Duplicate combinations: {integrity_report['duplicate_combinations']}")

        if integrity_report["issues"]:
            print("Issues found:")
            for issue in integrity_report["issues"]:
                print(f"  - {issue}")
        else:
            print("No issues found!")

    except Exception as exc:
        print(f"❌ Error during validation: {exc}")

    print("\nValidation testing completed!")


if __name__ == "__main__":
    main()
