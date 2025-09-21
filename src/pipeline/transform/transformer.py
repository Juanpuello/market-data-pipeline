"""
Data transformation module for processing raw market data into clean format.

This module handles:
- Parsing API expressions to extract currency, tenors, and reference rates
- Mapping currencies to reference rates based on API specification
- Applying USD SOFR transformation (sqrt(252) multiplier)
- Validating transformed data using Pydantic models
"""

import logging
import math
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pydantic import ValidationError
from sqlalchemy import and_
from sqlmodel import Session, col, select

# Add parent directory to path for imports when running directly
if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.parent))

from src.core.database import create_database_engine
from src.models import CleanData, OptionExpiryEnum, RawData, SwapTenorEnum


class DataTransformer:
    """Handles transformation of raw data into clean, validated format."""

    def __init__(self, db_connection_string: str = "sqlite:///market_data.db"):
        """Initialize transformer with database connection."""
        self.engine, self.SessionLocal = create_database_engine(db_connection_string)

        # Currency to reference rate mapping based on requirements
        self.currency_ref_mapping = {
            # USD mappings
            "USD": "Libor",  # Old USD code
            "USDD": "SOFR",  # New USD code
            # EUR mapping
            "EUR": "Euribor",
            # GBP mappings
            "GBP": {
                "default": "Libor",  # Old GBP code
                "SONIA": "SOFR",  # New GBP code with SONIA in expression
            },
            # CHF mappings
            "CHF": {
                "default": "Libor",  # Old CHF code
                "SARON": "SARON",  # New CHF code with SARON in expression
            },
        }

    def transform_raw_data(
        self, raw_data_ids: Optional[List[int]] = None
    ) -> Tuple[List[CleanData], Dict[str, int]]:
        """
        Transform raw data records into clean data without database operations.

        Args:
            raw_data_ids: Specific raw data IDs to process. If None, processes unprocessed data.

        Returns:
            Tuple of (list of CleanData objects, metrics dictionary)
        """
        metrics = {
            "rows_processed": 0,
            "rows_transformed": 0,
            "rows_rejected": 0,
            "validation_errors": 0,
        }

        clean_records = []
        with Session(self.engine) as session:
            raw_records = self._get_raw_data_to_process(session, raw_data_ids)
            for raw_record in raw_records:
                try:
                    metrics["rows_processed"] += 1

                    currency, x_tenor, y_tenor, ref_rate = self._parse_expression(
                        raw_record.expression
                    )

                    transformed_value = self._apply_value_transformations(
                        raw_record.value, currency, ref_rate
                    )

                    if raw_record.raw_data_id is None:
                        raise ValueError(
                            f"raw_data_id is None for raw_record with expression "
                            f"{raw_record.expression} and date {raw_record.date}"
                        )
                    clean_record = CleanData(
                        expression=raw_record.expression,
                        date=raw_record.date,
                        currency=currency,
                        x=x_tenor,
                        y=y_tenor,
                        ref=ref_rate,
                        value=transformed_value,
                        raw_data_id=raw_record.raw_data_id,
                    )

                    if clean_record:
                        clean_records.append(clean_record)
                        metrics["rows_transformed"] += 1
                    else:
                        metrics["rows_rejected"] += 1

                except ValidationError as exc:
                    print(
                        f"Validation error for raw_data_id {raw_record.raw_data_id}: {exc}"
                    )
                    metrics["validation_errors"] += 1
                except Exception as exc:
                    print(
                        f"Error transforming raw_data_id {raw_record.raw_data_id}: {exc}"
                    )
                    metrics["rows_rejected"] += 1

        return clean_records, metrics

    def _get_raw_data_to_process(
        self, session: Session, raw_data_ids: Optional[List[int]]
    ) -> List[RawData]:
        """Get raw data records that need processing using efficient SQLModel joins."""
        if raw_data_ids:
            stmt = select(RawData).where(col(RawData.raw_data_id).in_(raw_data_ids))
            return list(session.exec(stmt).all())

        # This finds raw_data records that either:
        # 1. Have no matching clean_data (clean_data_id will be NULL)
        # 2. Have matching clean_data but raw_data_id is newer than what's in clean_data
        stmt = (
            select(RawData, CleanData)
            .join(
                CleanData,
                and_(
                    col(CleanData.expression) == col(RawData.expression),
                    col(CleanData.date) == col(RawData.date),
                ),
                isouter=True,
            )
            .where(
                # Case 1: No matching clean_data exists (unprocessed)
                (col(CleanData.clean_data_id).is_(None))
                |
                # Case 2: Raw data is newer than existing clean_data
                (
                    (col(CleanData.raw_data_id).is_not(None))
                    & (col(RawData.raw_data_id) > col(CleanData.raw_data_id))
                )
            )
            .order_by(col(RawData.raw_data_id))
        )

        results = session.exec(stmt)
        raw_data_records = []

        for raw_data, clean_data in results:
            raw_data_records.append(raw_data)

        return raw_data_records

    def _parse_expression(self, expression: str) -> Tuple[str, str, str, str]:
        """
        Parse API expression to extract currency, tenors, and reference rate.

        Expected formats:
        - DB(COV,VOLSWAPTION,USD,<y>,<x>,PAYER,VOLBPVOL) -> USD old
        - DB(COV,VOLSWAPTION,USDD,<y>,<x>,PAYER,VOLBPVOL) -> USD new
        - DB(COV,VOLSWAPTION,EUR,<y>,<x>,PAYER,VOLBPVOL) -> EUR
        - DB(COV,VOLSWAPTION,GBP,<y>,<x>,PAYER,VOLBPVOL) -> GBP old
        - DB(COV,VOLSWAPTION,GBP,SONIA,<y>,<x>,PAYER,VOLBPVOL) -> GBP new
        - DB(COV,VOLSWAPTION,CHF,<y>,<x>,PAYER,VOLBPVOL) -> CHF old
        - DB(COV,VOLSWAPTION,CHF,SARON,<y>,<x>,PAYER,VOLBPVOL) -> CHF new

        Returns:
            Tuple of (currency, x_tenor, y_tenor, reference_rate)

        Raises:
            ValueError: If expression cannot be parsed or any field is missing
        """
        if not expression.startswith("DB(") or not expression.endswith(")"):
            raise ValueError(
                f"Expression must start with 'DB(' and end with ')': {expression}"
            )

        parts = expression[3:-1].split(",")

        if parts[0] != "COV" or parts[1] != "VOLSWAPTION":
            raise ValueError(
                f"Expression must follow DB(COV,VOLSWAPTION,...) format: {expression}"
            )

        if len(parts) == 7:
            # Standard format: DB(COV,VOLSWAPTION,<currency>,<y>,<x>,PAYER,VOLBPVOL)
            currency = parts[2]
            y_tenor = parts[3]
            x_tenor = parts[4]
        elif len(parts) == 8:
            # Extended format: DB(COV,VOLSWAPTION,<currency>,<ref_indicator>,<y>,<x>,PAYER,VOLBPVOL)
            currency = parts[2]
            ref_indicator = parts[3]  # SONIA, SARON, etc.
            y_tenor = parts[4]
            x_tenor = parts[5]

            if currency in ["GBP", "CHF"] and ref_indicator not in ["SONIA", "SARON"]:
                raise ValueError(
                    f"Invalid reference indicator '{ref_indicator}' for currency "
                    f"'{currency}' in expression: {expression}"
                )
        else:
            raise ValueError(
                f"Expression must have 7 or 8 comma-separated parts, got {len(parts)}: {expression}"
            )

        if x_tenor not in [e.value for e in OptionExpiryEnum]:
            raise ValueError(f"Invalid x_tenor '{x_tenor}' in expression: {expression}")
        if y_tenor not in [e.value for e in SwapTenorEnum]:
            raise ValueError(f"Invalid y_tenor '{y_tenor}' in expression: {expression}")

        ref_rate = self._map_reference_rate(currency, parts)
        if not ref_rate:
            raise ValueError(
                f"Could not map reference rate for currency {currency} with parts {parts}"
            )

        # Normalize currency (USDD -> USD for clean data)
        normalized_currency = "USD" if currency == "USDD" else currency

        if not all([normalized_currency, x_tenor, y_tenor, ref_rate]):
            raise ValueError(
                f"Missing required fields after parsing expression {expression}: "
                f"currency={normalized_currency}, x={x_tenor}, y={y_tenor}, ref={ref_rate}"
            )

        return normalized_currency, x_tenor, y_tenor, ref_rate

    def _map_reference_rate(
        self, currency: str, expression_parts: List[str]
    ) -> Optional[str]:
        """Map currency and expression to reference rate."""
        if currency in ["USD", "USDD", "EUR"]:
            return self.currency_ref_mapping[currency]

        if currency in ["GBP", "CHF"]:
            currency_mapping = self.currency_ref_mapping[currency]
            if len(expression_parts) == 8:
                ref_indicator = expression_parts[3]
                return currency_mapping[ref_indicator]

            return currency_mapping["default"]

        return None

    def _apply_value_transformations(
        self, value: float, currency: str, ref_rate: str
    ) -> float:
        """Apply currency-specific transformations to the value."""
        # USD new code (SOFR) values must be multiplied by sqrt(252)
        if currency == "USD" and ref_rate == "SOFR":
            return value * math.sqrt(252)

        return value


def main():
    """Test the _get_raw_data_to_process method."""
    # Configure logging to show SQLAlchemy INFO messages
    logging.basicConfig(level=logging.INFO)
    sqlalchemy_logger = logging.getLogger("sqlalchemy.engine")
    sqlalchemy_logger.setLevel(logging.INFO)

    transformer = DataTransformer()
    with Session(transformer.engine) as session:
        try:
            print("Test: Getting all records that need processing...")
            records = transformer._get_raw_data_to_process(session, None)
            print(f"✅ Found {len(records)} records to process")
            if records:
                print("Sample records:")
                for i, record in enumerate(records[:3]):
                    print(
                        f"  {i+1}. ID: {record.raw_data_id}, "
                        f"Expression: {record.expression}, Date: {record.date}"
                    )
            else:
                print("No records found (database might be empty)")
        except Exception as exc:
            print(f"❌ Error during testing: {exc}")
    print("\nTesting completed!")


if __name__ == "__main__":
    main()
