"""
Data models using Pydantic and SQLModel for validation and schema enforcement.

This module defines simplified data models focused solely on data type validation:
- Enum definitions for supported values
- API request and response schemas (Pydantic)
- Database record schemas (SQLModel)
- Configuration and metrics models

These models ensure data conforms to expected types and basic constraints
without implementing business logic or transformation rules.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date as Date
from datetime import datetime
from enum import Enum
from typing import Self

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import CheckConstraint, Index, UniqueConstraint
from sqlmodel import Field as SQLField
from sqlmodel import SQLModel

# Context variable and fix for SQLModel validation with table=True
_ONGOING_MODEL_VALIDATE: ContextVar[bool] = ContextVar("_ONGOING_MODEL_VALIDATE")


@contextmanager
def set_ongoing_model_validate():
    """Context manager to track ongoing model validation."""
    token = _ONGOING_MODEL_VALIDATE.set(True)
    yield
    _ONGOING_MODEL_VALIDATE.reset(token)


class ValidatedSQLModel(SQLModel):
    """
    Custom SQLModel base class that ensures validation works even with table=True.

    This fixes the SQLModel issue where field validators are silenced when table=True.
    """

    def __init__(self, **data):
        if getattr(self, "__tablename__", None) and not _ONGOING_MODEL_VALIDATE.get(
            False
        ):
            self_copy = self.model_copy()
            self.__pydantic_validator__.validate_python(data, self_instance=self_copy)
            data = self_copy.model_dump()
            self.__dict__ |= self_copy.__dict__
        super().__init__(**data)

    @classmethod
    def model_validate(cls: type[Self], *args, **kwargs) -> Self:
        with set_ongoing_model_validate():
            return super().model_validate(*args, **kwargs)


class CurrencyEnum(str, Enum):
    """
    Supported currency codes in the system.

    Used to validate that currency values in API responses and database
    records are one of the expected currency types.
    """

    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    CHF = "CHF"


class OptionExpiryEnum(str, Enum):
    """
    Permitted option expiry values (x_tenor).

    Based on API specification: x ∈ ['1y', '5y']
    """

    ONE_YEAR = "1y"
    FIVE_YEAR = "5y"


class SwapTenorEnum(str, Enum):
    """
    Permitted swap tenor values (y_tenor).

    Based on API specification: y ∈ ['2y', '5y', '10y']
    """

    TWO_YEAR = "2y"
    FIVE_YEAR = "5y"
    TEN_YEAR = "10y"


class RunModeEnum(str, Enum):
    """
    Supported pipeline execution modes.

    Used to track which type of data ingestion run created
    raw data records for operational monitoring.
    """

    DEFAULT = "default"
    OLD_CODES = "old_codes"
    HISTORICAL = "historical"


class APIRequest(BaseModel):
    """
    Model for API request parameters sent to the market data API.

    Validates that request parameters conform to expected types and basic
    constraints before sending to the external API. Includes validation
    for permitted expression format based on the API specification.
    """

    expression: str = Field(..., description="API expression code")
    start_date: Date = Field(..., description="Start date (inclusive)")
    end_date: Date = Field(..., description="End date (inclusive)")

    @field_validator("expression")
    @classmethod
    def validate_expression_format(cls, v):
        """Validate expression follows DB() format with correct structure."""
        if not v.startswith("DB(") or not v.endswith(")"):
            raise ValueError("Expression must start with 'DB(' and end with ')'")

        parts = v[3:-1].split(",")
        if len(parts) not in [7, 8]:
            raise ValueError("Expression must have 7 or 8 comma-separated parts")

        if parts[0] != "COV" or parts[1] != "VOLSWAPTION":
            raise ValueError("Expression must follow DB(COV,VOLSWAPTION,...) format")

        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info):
        """Ensure end_date >= start_date for basic data consistency."""
        data = info.data
        if "start_date" in data and v < data["start_date"]:
            raise ValueError("end_date must be >= start_date")
        return v


class APIResponse(BaseModel):
    """
    Model for individual data points received from the market data API.

    Validates that each data point has the expected structure and
    basic value constraints to ensure clean database storage.
    Note: Reference rate is NOT returned by API - it will be mapped
    during transformation based on the expression used.
    """

    date: Date = Field(..., description="Data date")
    value: float = Field(..., gt=0, description="Volatility value in basis points")


class RawData(ValidatedSQLModel, table=True):
    """
    Raw data table - append-only storage of API responses.

    This table stores all raw API responses with metadata for provenance
    and replay capabilities. Includes versioning for handling duplicate
    fetches of the same expression/date range.
    Schema: (raw_data_id, expression, date, value, fetch_timestamp, version, source_file_uri)
    """

    __tablename__ = "raw_data"  # type: ignore

    raw_data_id: int | None = SQLField(default=None, primary_key=True)
    expression: str = SQLField(
        index=True,
        max_length=200,
        description="API expression code used for the request",
    )
    date: Date = SQLField(
        index=True,
        description="Data date from API response",
    )
    value: float = SQLField(
        gt=0,
        description="Volatility value from API response (basis points)",
    )
    fetch_timestamp: datetime = SQLField(
        default_factory=datetime.utcnow,
        index=True,
        description="When the API call was made",
    )
    version: int = SQLField(
        default=1,
        ge=1,
        description="Version number for this expression/date combination",
    )
    ingestion_mode: str = SQLField(
        default="default",
        index=True,
        description="Run mode: default, old_codes, or historical",
    )
    source_file_uri: str = SQLField(
        max_length=500, description="Azure blob URI for the raw response file"
    )

    @field_validator("version")
    @classmethod
    def validate_version(cls, v):
        """Validate version is positive integer."""
        if v < 1:
            raise ValueError("version must be >= 1")
        return v

    @field_validator("ingestion_mode")
    @classmethod
    def validate_ingestion_mode(cls, v):
        """Validate ingestion_mode matches RunModeEnum values."""
        valid_modes = [mode.value for mode in RunModeEnum]
        if v not in valid_modes:
            raise ValueError(f"ingestion_mode must be one of {valid_modes}, got {v}")
        return v

    __table_args__ = (
        CheckConstraint("date IS NOT NULL", name="valid_date"),
        CheckConstraint("value > 0", name="positive_value"),
        CheckConstraint("version >= 1", name="positive_version"),
        Index("idx_raw_data_expression_date", "expression", "date"),
        Index("idx_raw_data_fetch_time", "fetch_timestamp"),
        Index("idx_raw_data_version", "version"),
        Index("idx_raw_data_mode_version", "ingestion_mode", "version"),
        UniqueConstraint(
            "expression",
            "date",
            "version",
            name="uq_raw_data_expression_date_version",
        ),
    )


class CleanData(ValidatedSQLModel, table=True):
    """
    Clean data table - normalized and validated market data.

    This table contains the final, cleaned market data with a reference
    to the latest raw_data record that was used to generate it.
    Schema: (clean_data_id, expression, date, currency, x, y, ref, value, raw_data_id)
    """

    __tablename__ = "clean_data"  # type: ignore

    clean_data_id: int | None = SQLField(default=None, primary_key=True)
    expression: str = SQLField(
        index=True,
        max_length=200,
        description="API expression code from the raw data",
    )
    date: Date = SQLField(
        index=True,
        description="Data date (YYYY-MM-DD)",
    )
    currency: str = SQLField(
        index=True,
        description="Currency code (USD, EUR, GBP, CHF)",
        max_length=10,
    )
    x: str = SQLField(
        index=True, description="Option expiry tenor (1y, 5y)", max_length=10
    )
    y: str = SQLField(index=True, description="Swap tenor (2y, 5y, 10y)", max_length=10)
    ref: str = SQLField(
        index=True,
        max_length=20,
        description="Reference rate (Libor, SOFR, SONIA, etc.)",
    )
    value: float = SQLField(
        gt=0, description="Validated volatility value (basis points)"
    )
    raw_data_id: int = SQLField(
        foreign_key="raw_data.raw_data_id",
        description="ID of the latest raw_data record used to generate this data",
    )

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v):
        """Validate currency matches CurrencyEnum values."""
        valid_currencies = [curr.value for curr in CurrencyEnum]
        if v not in valid_currencies:
            raise ValueError(f"currency must be one of {valid_currencies}, got {v}")
        return v

    @field_validator("x")
    @classmethod
    def validate_x(cls, v):
        """Validate x matches OptionExpiryEnum values."""
        valid_x = [opt.value for opt in OptionExpiryEnum]
        if v not in valid_x:
            raise ValueError(f"x must be one of {valid_x}, got {v}")
        return v

    @field_validator("y")
    @classmethod
    def validate_y(cls, v):
        """Validate y matches SwapTenorEnum values."""
        valid_y = [tenor.value for tenor in SwapTenorEnum]
        if v not in valid_y:
            raise ValueError(f"y must be one of {valid_y}, got {v}")
        return v

    __table_args__ = (
        CheckConstraint("value > 0", name="positive_value"),
        CheckConstraint(
            "value >= 1.0 AND value <= 2000.0", name="reasonable_value_range"
        ),
        Index("idx_clean_data_expression_date", "expression", "date"),
        Index("idx_clean_data_date", "date"),
        Index("idx_clean_data_currency_ref", "currency", "ref"),
        Index("idx_clean_data_tenors", "x", "y"),
        Index("idx_clean_data_raw_data_id", "raw_data_id"),
        UniqueConstraint(
            "expression",
            "date",
            name="uq_clean_data_combination",
        ),
    )
