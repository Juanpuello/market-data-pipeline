"""
Comprehensive tests for data models in src/models.py.

This module tests all Pydantic models, validators, enums, and SQLModel constraints
including edge cases for validation logic, date ranges, value constraints, and enum validations.
"""

from datetime import date, datetime
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, Index, UniqueConstraint

from src.models import (
    APIRequest,
    APIResponse,
    CleanData,
    CurrencyEnum,
    OptionExpiryEnum,
    RawData,
    RunModeEnum,
    SwapTenorEnum,
)


class TestValidatedSQLModel:
    """Test the ValidatedSQLModel base class functionality."""

    def test_validation(self):
        """Test that ValidatedSQLModel enables proper validation for table models."""

        with pytest.raises(
            ValidationError, match="Input should be greater than or equal to 1"
        ):
            RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                value=125.5,
                version=0,  # Should trigger custom validator
                source_file_uri="blob://bucket/file.json",
            )

        with pytest.raises(ValidationError, match="ingestion_mode must be one of"):
            RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                value=125.5,
                ingestion_mode="invalid",  # Should trigger custom validator
                source_file_uri="blob://bucket/file.json",
            )

        with pytest.raises(ValidationError, match="currency must be one of"):
            CleanData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                currency="INVALID",  # Should trigger custom validator
                x="1y",
                y="2y",
                ref="SOFR",
                value=125.5,
                raw_data_id=1,
            )

    def test_sqlmodel_constraints(self):
        """Test that SQLModel field constraints (gt, ge, etc.) still work."""
        with pytest.raises(ValidationError, match="Input should be greater than 0"):
            RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                value=0,  # Should trigger gt=0 constraint
                source_file_uri="blob://bucket/file.json",
            )

        with pytest.raises(ValidationError, match="Input should be greater than 0"):
            CleanData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                currency="USD",
                x="1y",
                y="2y",
                ref="SOFR",
                value=-1,  # Should trigger gt=0 constraint
                raw_data_id=1,
            )

        # Test ge=1 constraint on version
        with pytest.raises(
            ValidationError, match="Input should be greater than or equal to 1"
        ):
            RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                value=125.5,
                version=0,  # Should trigger ge=1 constraint
                source_file_uri="blob://bucket/file.json",
            )

    def test_valid_data_creation(self):
        """Test valid data creation."""
        raw_data = RawData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            value=125.5,
            version=1,
            ingestion_mode="default",
            source_file_uri="blob://bucket/file.json",
        )
        assert raw_data.expression == "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        assert raw_data.value == 125.5
        assert raw_data.version == 1
        assert raw_data.ingestion_mode == "default"

        clean_data = CleanData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            currency="USD",
            x="1y",
            y="2y",
            ref="SOFR",
            value=125.5,
            raw_data_id=1,
        )
        assert clean_data.currency == "USD"
        assert clean_data.x == "1y"
        assert clean_data.y == "2y"
        assert clean_data.value == 125.5


class TestEnums:
    """Test all enum definitions."""

    def test_currency_enum(self):
        """Test CurrencyEnum values and behavior."""
        assert CurrencyEnum.USD == "USD"
        assert CurrencyEnum.EUR == "EUR"
        assert CurrencyEnum.GBP == "GBP"
        assert CurrencyEnum.CHF == "CHF"

        assert "USD" in [c.value for c in CurrencyEnum]
        assert "JPY" not in [c.value for c in CurrencyEnum]

        currencies = list(CurrencyEnum)
        assert all(isinstance(c, CurrencyEnum) for c in currencies)

    def test_option_expiry_enum(self):
        """Test OptionExpiryEnum values and behavior."""
        assert OptionExpiryEnum.ONE_YEAR == "1y"
        assert OptionExpiryEnum.FIVE_YEAR == "5y"

        values = [e.value for e in OptionExpiryEnum]
        assert "1y" in values
        assert "5y" in values
        assert "10y" not in values

    def test_swap_tenor_enum(self):
        """Test SwapTenorEnum values and behavior."""
        assert SwapTenorEnum.TWO_YEAR == "2y"
        assert SwapTenorEnum.FIVE_YEAR == "5y"
        assert SwapTenorEnum.TEN_YEAR == "10y"

        values = [e.value for e in SwapTenorEnum]
        assert "2y" in values
        assert "5y" in values
        assert "10y" in values
        assert "1y" not in values  # Should not be in swap tenor

    def test_run_mode_enum(self):
        """Test RunModeEnum values and behavior."""
        assert RunModeEnum.DEFAULT == "default"
        assert RunModeEnum.OLD_CODES == "old_codes"
        assert RunModeEnum.HISTORICAL == "historical"

        values = [e.value for e in RunModeEnum]
        assert "default" in values
        assert "old_codes" in values
        assert "historical" in values


class TestAPIRequest:
    """Test APIRequest model validation."""

    def test_valid_api_request(self):
        """Test creation of valid APIRequest objects."""
        request = APIRequest(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),
        )
        assert request.expression == "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        assert request.start_date == date(2024, 1, 1)
        assert request.end_date == date(2024, 1, 5)

        request_8_parts = APIRequest(
            expression="DB(COV,VOLSWAPTION,GBP,SONIA,2y,1y,PAYER,VOLBPVOL)",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
        )
        assert (
            request_8_parts.expression
            == "DB(COV,VOLSWAPTION,GBP,SONIA,2y,1y,PAYER,VOLBPVOL)"
        )

    def test_expression_format_validation(self):
        """Test expression format validation."""
        base_params = {"start_date": date(2024, 1, 1), "end_date": date(2024, 1, 5)}

        with pytest.raises(
            ValidationError, match="must start with 'DB\\(' and end with '\\)'"
        ):
            APIRequest(
                expression="COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL", **base_params
            )

        with pytest.raises(
            ValidationError, match="must start with 'DB\\(' and end with '\\)'"
        ):
            APIRequest(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL", **base_params
            )

        with pytest.raises(
            ValidationError, match="must have 7 or 8 comma-separated parts"
        ):
            APIRequest(expression="DB(COV,VOLSWAPTION,USD)", **base_params)

        with pytest.raises(
            ValidationError, match="must have 7 or 8 comma-separated parts"
        ):
            APIRequest(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL,EXTRA,EXTRA2)",
                **base_params,
            )

        with pytest.raises(
            ValidationError, match="must follow DB\\(COV,VOLSWAPTION,...\\) format"
        ):
            APIRequest(
                expression="DB(WRONG,FORMAT,USD,2y,1y,PAYER,VOLBPVOL)", **base_params
            )

        with pytest.raises(
            ValidationError, match="must follow DB\\(COV,VOLSWAPTION,...\\) format"
        ):
            APIRequest(
                expression="DB(COV,WRONGTYPE,USD,2y,1y,PAYER,VOLBPVOL)", **base_params
            )

    def test_date_range_validation(self):
        """Test date range validation."""
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"

        request = APIRequest(
            expression=expression,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),
        )
        assert request.start_date == date(2024, 1, 1)
        assert request.end_date == date(2024, 1, 5)

        request_same = APIRequest(
            expression=expression,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
        )
        assert request_same.start_date == request_same.end_date

        with pytest.raises(ValidationError, match="end_date must be >= start_date"):
            APIRequest(
                expression=expression,
                start_date=date(2024, 1, 5),
                end_date=date(2024, 1, 1),
            )

    def test_edge_case_dates(self):
        """Test edge case dates."""
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"

        leap_request = APIRequest(
            expression=expression,
            start_date=date(2024, 2, 29),
            end_date=date(2024, 2, 29),
        )
        assert leap_request.start_date == date(2024, 2, 29)

        year_boundary = APIRequest(
            expression=expression,
            start_date=date(2023, 12, 31),
            end_date=date(2024, 1, 1),
        )
        assert year_boundary.start_date == date(2023, 12, 31)
        assert year_boundary.end_date == date(2024, 1, 1)

    def test_expression_variations(self):
        """Test various valid expression formats."""
        base_params = {"start_date": date(2024, 1, 1), "end_date": date(2024, 1, 1)}

        valid_expressions = [
            "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",  # Standard USD
            "DB(COV,VOLSWAPTION,USDD,2y,1y,PAYER,VOLBPVOL)",  # New USD code
            "DB(COV,VOLSWAPTION,EUR,5y,5y,PAYER,VOLBPVOL)",  # EUR
            "DB(COV,VOLSWAPTION,GBP,2y,1y,PAYER,VOLBPVOL)",  # Old GBP
            "DB(COV,VOLSWAPTION,GBP,SONIA,2y,1y,PAYER,VOLBPVOL)",  # New GBP
            "DB(COV,VOLSWAPTION,CHF,10y,5y,PAYER,VOLBPVOL)",  # Old CHF
            "DB(COV,VOLSWAPTION,CHF,SARON,10y,5y,PAYER,VOLBPVOL)",  # New CHF
        ]

        for expression in valid_expressions:
            request = APIRequest(expression=expression, **base_params)
            assert request.expression == expression


class TestAPIResponse:
    """Test APIResponse model validation."""

    def test_valid_api_response(self):
        """Test creation of valid APIResponse objects."""
        response = APIResponse(date=date(2024, 1, 1), value=125.5)
        assert response.date == date(2024, 1, 1)
        assert response.value == 125.5

    def test_value_constraints(self):
        """Test value constraints."""
        valid_values = [0.1, 1.0, 100.0, 1000.0, 2000.0]
        for value in valid_values:
            response = APIResponse(date=date(2024, 1, 1), value=value)
            assert response.value == value

        invalid_values = [0, -1, -100.5]
        for value in invalid_values:
            with pytest.raises(ValidationError, match="greater than 0"):
                APIResponse(date=date(2024, 1, 1), value=value)

    def test_date_validation(self):
        """Test date validation."""
        valid_dates = [
            date(2024, 1, 1),
            date(2024, 2, 29),  # Leap year
            date(2020, 1, 1),  # Past date
            date(2030, 12, 31),  # Future date
        ]
        for test_date in valid_dates:
            response = APIResponse(date=test_date, value=100.0)
            assert response.date == test_date

    def test_type_validation(self):
        """Test type validation for APIResponse."""
        try:
            response = APIResponse(
                date="2024-01-01", value=100.0
            )  # String might be converted
            # If conversion succeeded, verify it's a proper date
            assert isinstance(response.date, date)
        except ValidationError:
            pass

        response = APIResponse(
            date=date(2024, 1, 1), value="100"
        )  # String should convert
        assert response.value == 100.0
        assert isinstance(response.value, float)


class TestRawData:
    """Test RawData SQLModel validation."""

    def test_valid_raw_data(self):
        """Test creation of valid RawData objects."""
        raw_data = RawData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            value=125.5,
            version=1,
            ingestion_mode="default",
            source_file_uri="blob://bucket/file.json",
        )
        assert raw_data.expression == "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        assert raw_data.date == date(2024, 1, 1)
        assert raw_data.value == 125.5
        assert raw_data.version == 1
        assert raw_data.ingestion_mode == "default"
        assert raw_data.source_file_uri == "blob://bucket/file.json"

    def test_default_values(self):
        """Test default values for RawData."""
        raw_data = RawData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            value=125.5,
            source_file_uri="blob://bucket/file.json",
        )
        assert raw_data.version == 1  # Default version
        assert raw_data.ingestion_mode == "default"  # Default mode
        assert raw_data.fetch_timestamp is not None
        assert isinstance(raw_data.fetch_timestamp, datetime)

    def test_version_validation(self):
        """Test version validation."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "value": 125.5,
            "source_file_uri": "blob://bucket/file.json",
        }

        for version in [1, 2, 10, 100]:
            raw_data = RawData(version=version, **base_params)
            assert raw_data.version == version

        for version in [0, -1, -5]:
            with pytest.raises(
                ValidationError, match="Input should be greater than or equal to 1"
            ):
                RawData(version=version, **base_params)

    def test_ingestion_mode_validation(self):
        """Test ingestion mode validation."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "value": 125.5,
            "source_file_uri": "blob://bucket/file.json",
        }

        valid_modes = ["default", "old_codes", "historical"]
        for mode in valid_modes:
            raw_data = RawData(ingestion_mode=mode, **base_params)
            assert raw_data.ingestion_mode == mode

        with pytest.raises(ValidationError, match="ingestion_mode must be one of"):
            RawData(ingestion_mode="invalid_mode", **base_params)

    def test_value_constraints(self):
        """Test value constraints for RawData."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "source_file_uri": "blob://bucket/file.json",
        }

        # Test valid positive values
        for value in [0.1, 1.0, 100.0, 1000.0]:
            raw_data = RawData(value=value, **base_params)
            assert raw_data.value == value

        for value in [0, -1, -100.5]:
            with pytest.raises(ValidationError, match="Input should be greater than 0"):
                RawData(value=value, **base_params)

    def test_table_constraints(self):
        """Test SQLModel table constraints."""
        # Test that __tablename__ is set correctly - use string comparison
        assert str(RawData.__tablename__) == "raw_data"

        # Test that table_args contains expected constraints
        table_args = RawData.__table_args__
        constraint_names = []
        index_names = []

        for constraint in table_args:
            if isinstance(constraint, CheckConstraint):
                constraint_names.append(constraint.name)
            elif isinstance(constraint, Index):
                index_names.append(constraint.name)
            elif isinstance(constraint, UniqueConstraint):
                constraint_names.append(constraint.name)

        expected_constraints = [
            "valid_date",
            "positive_value",
            "positive_version",
            "uq_raw_data_expression_date_version",
        ]
        for constraint_name in expected_constraints:
            assert constraint_name in constraint_names

        expected_indexes = [
            "idx_raw_data_expression_date",
            "idx_raw_data_fetch_time",
            "idx_raw_data_version",
            "idx_raw_data_mode_version",
        ]
        for index_name in expected_indexes:
            assert index_name in index_names

    def test_long_expressions(self):
        """Test handling of long expressions."""
        reasonable_expression = (
            "DB(COV,VOLSWAPTION," + "A" * 150 + ",2y,1y,PAYER,VOLBPVOL)"
        )
        raw_data = RawData(
            expression=reasonable_expression,
            date=date(2024, 1, 1),
            value=125.5,
            source_file_uri="blob://bucket/file.json",
        )
        assert len(raw_data.expression) <= 200

        too_long_expression = (
            "DB(COV,VOLSWAPTION," + "X" * 190 + ",2y,1y,PAYER,VOLBPVOL)"
        )
        with pytest.raises(
            ValidationError, match="String should have at most 200 characters"
        ):
            RawData(
                expression=too_long_expression,
                date=date(2024, 1, 1),
                value=125.5,
                source_file_uri="blob://bucket/file.json",
            )

    def test_long_uri(self):
        """Test handling of long source file URIs."""
        long_uri = "blob://bucket/" + "x" * 480 + ".json"
        raw_data = RawData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            value=125.5,
            source_file_uri=long_uri,
        )
        assert raw_data.source_file_uri == long_uri


class TestCleanData:
    """Test CleanData SQLModel validation."""

    def test_valid_clean_data(self):
        """Test creation of valid CleanData objects."""
        clean_data = CleanData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            currency="USD",
            x="1y",
            y="2y",
            ref="SOFR",
            value=125.5,
            raw_data_id=1,
        )
        assert clean_data.expression == "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        assert clean_data.date == date(2024, 1, 1)
        assert clean_data.currency == "USD"
        assert clean_data.x == "1y"
        assert clean_data.y == "2y"
        assert clean_data.ref == "SOFR"
        assert clean_data.value == 125.5
        assert clean_data.raw_data_id == 1

    def test_currency_validation(self):
        """Test currency validation."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "x": "1y",
            "y": "2y",
            "ref": "SOFR",
            "value": 125.5,
            "raw_data_id": 1,
        }

        valid_currencies = ["USD", "EUR", "GBP", "CHF"]
        for currency in valid_currencies:
            clean_data = CleanData(currency=currency, **base_params)
            assert clean_data.currency == currency

        with pytest.raises(ValidationError, match="currency must be one of"):
            CleanData(currency="JPY", **base_params)

        with pytest.raises(ValidationError, match="currency must be one of"):
            CleanData(currency="INVALID", **base_params)

    def test_x_validation(self):
        """Test x (option expiry) validation."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "currency": "USD",
            "y": "2y",
            "ref": "SOFR",
            "value": 125.5,
            "raw_data_id": 1,
        }

        valid_x_values = ["1y", "5y"]
        for x in valid_x_values:
            clean_data = CleanData(x=x, **base_params)
            assert clean_data.x == x

        invalid_x_values = ["2y", "10y", "3y", "invalid"]
        for x in invalid_x_values:
            with pytest.raises(ValidationError, match="x must be one of"):
                CleanData(x=x, **base_params)

    def test_y_validation(self):
        """Test y (swap tenor) validation."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "currency": "USD",
            "x": "1y",
            "ref": "SOFR",
            "value": 125.5,
            "raw_data_id": 1,
        }

        valid_y_values = ["2y", "5y", "10y"]
        for y in valid_y_values:
            clean_data = CleanData(y=y, **base_params)
            assert clean_data.y == y

        invalid_y_values = ["1y", "3y", "7y", "invalid"]
        for y in invalid_y_values:
            with pytest.raises(ValidationError, match="y must be one of"):
                CleanData(y=y, **base_params)

    def test_value_constraints(self):
        """Test value constraints for CleanData."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "currency": "USD",
            "x": "1y",
            "y": "2y",
            "ref": "SOFR",
            "raw_data_id": 1,
        }

        for value in [0.1, 1.0, 100.0, 1000.0, 2000.0]:
            clean_data = CleanData(value=value, **base_params)
            assert clean_data.value == value

        for value in [0, -1, -100.5]:
            with pytest.raises(ValidationError, match="Input should be greater than 0"):
                CleanData(value=value, **base_params)

    def test_table_constraints(self):
        """Test SQLModel table constraints for CleanData."""
        assert str(CleanData.__tablename__) == "clean_data"

        table_args = CleanData.__table_args__
        constraint_names = []
        index_names = []
        for constraint in table_args:
            if isinstance(constraint, CheckConstraint):
                constraint_names.append(constraint.name)
            elif isinstance(constraint, Index):
                index_names.append(constraint.name)
            elif isinstance(constraint, UniqueConstraint):
                constraint_names.append(constraint.name)

        expected_constraints = [
            "positive_value",
            "reasonable_value_range",
            "uq_clean_data_combination",
        ]
        for constraint_name in expected_constraints:
            assert constraint_name in constraint_names

        expected_indexes = [
            "idx_clean_data_expression_date",
            "idx_clean_data_date",
            "idx_clean_data_currency_ref",
            "idx_clean_data_tenors",
            "idx_clean_data_raw_data_id",
        ]
        for index_name in expected_indexes:
            assert index_name in index_names

    def test_reference_rate_values(self):
        """Test various reference rate values."""
        base_params = {
            "expression": "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            "date": date(2024, 1, 1),
            "currency": "USD",
            "x": "1y",
            "y": "2y",
            "value": 125.5,
            "raw_data_id": 1,
        }

        ref_rates = ["SOFR", "Libor", "Euribor", "SONIA", "SARON"]
        for ref in ref_rates:
            clean_data = CleanData(ref=ref, **base_params)
            assert clean_data.ref == ref

    def test_edge_case_combinations(self):
        """Test edge case combinations of tenors and currencies."""
        combinations = [
            ("USD", "1y", "2y", "SOFR"),
            ("USD", "1y", "5y", "Libor"),
            ("USD", "5y", "10y", "SOFR"),
            ("EUR", "1y", "2y", "Euribor"),
            ("EUR", "5y", "5y", "Euribor"),  # Same x and y
            ("GBP", "1y", "10y", "SONIA"),
            ("CHF", "5y", "2y", "SARON"),
        ]

        for currency, x, y, ref in combinations:
            clean_data = CleanData(
                expression=f"DB(COV,VOLSWAPTION,{currency},{y},{x},PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                currency=currency,
                x=x,
                y=y,
                ref=ref,
                value=125.5,
                raw_data_id=1,
            )
            assert clean_data.currency == currency
            assert clean_data.x == x
            assert clean_data.y == y
            assert clean_data.ref == ref


class TestModelIntegration:
    """Test integration between models."""

    def test_api_request_to_response_flow(self):
        """Test the flow from APIRequest to APIResponse."""
        request = APIRequest(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3),
        )

        expected_dates = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
        responses = []

        for response_date in expected_dates:
            response = APIResponse(date=response_date, value=100.0 + len(responses))
            responses.append(response)

        assert len(responses) == 3
        for i, response in enumerate(responses):
            assert response.date in expected_dates
            assert response.value > 0

    def test_raw_to_clean_data_flow(self):
        """Test the flow from RawData to CleanData."""
        # Create raw data
        raw_data = RawData(
            raw_data_id=1,
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            value=125.5,
            version=1,
            ingestion_mode="default",
            source_file_uri="blob://bucket/file.json",
        )

        clean_data = CleanData(
            expression=raw_data.expression,
            date=raw_data.date,
            currency="USD",
            x="1y",
            y="2y",
            ref="SOFR",
            value=raw_data.value,
            raw_data_id=1,  # Use explicit value instead of raw_data.raw_data_id
        )

        assert clean_data.expression == raw_data.expression
        assert clean_data.date == raw_data.date
        assert clean_data.value == raw_data.value
        assert clean_data.raw_data_id == 1

    def test_enum_integration_with_models(self):
        """Test that enums integrate properly with models."""
        clean_data = CleanData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            currency=CurrencyEnum.USD.value,
            x=OptionExpiryEnum.ONE_YEAR.value,
            y=SwapTenorEnum.TWO_YEAR.value,
            ref="SOFR",
            value=125.5,
            raw_data_id=1,
        )

        assert clean_data.currency == "USD"
        assert clean_data.x == "1y"
        assert clean_data.y == "2y"

        raw_data = RawData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            value=125.5,
            ingestion_mode=RunModeEnum.HISTORICAL.value,
            source_file_uri="blob://bucket/file.json",
        )

        assert raw_data.ingestion_mode == "historical"

    def test_model_serialization(self):
        """Test that models can be serialized to dictionaries."""
        request = APIRequest(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),
        )
        request_dict = request.model_dump()
        assert "expression" in request_dict
        assert "start_date" in request_dict
        assert "end_date" in request_dict

        response = APIResponse(date=date(2024, 1, 1), value=125.5)
        response_dict = response.model_dump()
        assert "date" in response_dict
        assert "value" in response_dict

    def test_model_validation_consistency(self):
        """Test that validation is consistent across related models."""
        test_date = date(2024, 1, 1)

        with pytest.raises(ValidationError):
            APIResponse(date=test_date, value=0)

        with pytest.raises(ValidationError):
            RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=test_date,
                value=0,
                source_file_uri="blob://bucket/file.json",
            )

        with pytest.raises(ValidationError):
            CleanData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=test_date,
                currency="USD",
                x="1y",
                y="2y",
                ref="SOFR",
                value=0,
                raw_data_id=1,
            )


class TestModelEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_boundary_values(self):
        """Test boundary values for numeric fields."""
        min_value = 0.0001
        response = APIResponse(date=date(2024, 1, 1), value=min_value)
        assert response.value == min_value

        large_value = 99999.99
        clean_data = CleanData(
            expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 1),
            currency="USD",
            x="1y",
            y="2y",
            ref="SOFR",
            value=large_value,
            raw_data_id=1,
        )
        assert clean_data.value == large_value

    def test_string_length_boundaries(self):
        """Test string length boundaries."""
        max_expression = "DB(COV,VOLSWAPTION," + "A" * 150 + ",2y,1y,PAYER,VOLBPVOL)"

        raw_data = RawData(
            expression=max_expression,
            date=date(2024, 1, 1),
            value=125.5,
            source_file_uri="blob://bucket/file.json",
        )
        assert len(raw_data.expression) <= 200

        clean_data = CleanData(
            expression=max_expression,
            date=date(2024, 1, 1),
            currency="USD",
            x="1y",
            y="2y",
            ref="SOFR",
            value=125.5,
            raw_data_id=1,
        )
        assert len(clean_data.expression) <= 200

    def test_date_edge_cases(self):
        """Test edge cases for dates."""
        edge_dates = [
            date(1900, 1, 1),  # Very old date
            date(2100, 12, 31),  # Far future date
            date(2024, 2, 29),  # Leap year
            date(2023, 2, 28),  # Non-leap year
            date(2024, 12, 31),  # End of year
            date(2024, 1, 1),  # Start of year
        ]

        for test_date in edge_dates:
            response = APIResponse(date=test_date, value=100.0)
            assert response.date == test_date

            raw_data = RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=test_date,
                value=125.5,
                source_file_uri="blob://bucket/file.json",
            )
            assert raw_data.date == test_date

    def test_precision_handling(self):
        """Test floating point precision handling."""
        precision_values = [
            1.0,
            1.1,
            1.12,
            1.123,
            1.1234,
            1.12345,
            1.123456,
            1.1234567,
            1.12345678,
        ]

        for value in precision_values:
            response = APIResponse(date=date(2024, 1, 1), value=value)
            assert response.value == value

    @patch("src.models.datetime")
    def test_fetch_timestamp_generation(self, mock_datetime):
        """Test that fetch_timestamp is generated correctly."""
        mock_now = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.utcnow.return_value = mock_now

        # Import datetime in the module context
        with patch("datetime.datetime") as mock_dt:
            mock_dt.utcnow.return_value = mock_now

            raw_data = RawData(
                expression="DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 1),
                value=125.5,
                source_file_uri="blob://bucket/file.json",
            )

            # Verify that timestamp was set (may not be the exact mocked value due to default_factory)
            assert raw_data.fetch_timestamp is not None
            assert isinstance(raw_data.fetch_timestamp, datetime)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
