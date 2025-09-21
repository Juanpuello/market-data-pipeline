"""
Database schema definitions using SQLModel.

This module provides database engine creation and table management
for the market data pipeline using SQLModel tables from models.py:
- raw_data: Append-only storage of API responses with versioning
- clean_data: Normalized and validated data with provenance tracking

SQLModel provides seamless integration between Pydantic validation
and SQLAlchemy table creation with automatic migration support.
"""

from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from config.logging_config import get_logger

logger = get_logger(__name__)


def utc_now():
    """Get current UTC timestamp with timezone awareness."""
    return datetime.now(timezone.utc)


def create_database_engine(connection_string: str):
    """
    Create database engine with appropriate connection pooling.

    Args:
        connection_string: Database connection string

    Returns:
        Tuple of (engine, SessionLocal class)
    """
    if connection_string.startswith("sqlite"):
        engine = create_engine(connection_string, echo=False, pool_pre_ping=True)
    else:
        engine = create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            echo=False,
            pool_pre_ping=True,
        )

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return engine, SessionLocal


def create_tables(engine):
    """
    Create all database tables using SQLModel.

    Args:
        engine: SQLAlchemy engine instance
    """
    SQLModel.metadata.create_all(bind=engine)


def get_table_info() -> dict:
    """Get basic information about all tables."""
    table_info = {
        "raw_data": "Append-only storage of individual API response data points with versioning",
        "clean_data": "Normalized and validated data with unique ID and reference to raw_data",
    }
    return table_info


if __name__ == "__main__":

    from sqlmodel import Session

    from src.models import (
        CleanData,
        CurrencyEnum,
        OptionExpiryEnum,
        RawData,
        RunModeEnum,
        SwapTenorEnum,
    )

    engine, SessionLocal = create_database_engine("sqlite:///./market_data.db")
    create_tables(engine)

    with Session(engine) as session:
        raw_record = RawData(
            expression="DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 15),
            value=125.5,
            source_file_uri="s3://bucket/file.parquet",
            version=1,
            ingestion_mode=RunModeEnum.DEFAULT.value,
        )
        session.add(raw_record)
        session.commit()
        session.refresh(raw_record)

        if raw_record.raw_data_id is None:
            raise ValueError("raw_data_id should not be None after commit")
        clean_record = CleanData(
            expression="DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 15),
            currency=CurrencyEnum.EUR.value,
            x=OptionExpiryEnum.ONE_YEAR.value,
            y=SwapTenorEnum.FIVE_YEAR.value,
            ref="EURIBOR",
            value=125.5,
            raw_data_id=raw_record.raw_data_id,
        )
        session.add(clean_record)
        session.commit()

        logger.info("Successfully inserted test data!")
        logger.debug(f"Raw data ID: {raw_record.raw_data_id}")
        logger.debug(f"Raw data ingestion mode: {raw_record.ingestion_mode}")
        logger.debug(f"Clean data record: {clean_record}")
        logger.debug(f"Clean data ID: {clean_record.clean_data_id}")
