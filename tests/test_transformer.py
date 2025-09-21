"""
Test suite for data transformation in src/pipeline/transform/transformer.py.

Tests data transformation logic, expression parsing, value transformations,
and database operations while mocking external dependencies.
"""

import math
from datetime import date
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Engine

from src.models import CleanData, OptionExpiryEnum, RawData, SwapTenorEnum
from src.pipeline.transform.transformer import DataTransformer


class TestDataTransformerInit:
    """Test DataTransformer initialization."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_init_with_default_connection(self, mock_create_engine):
        """Test initialization with default database connection."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        transformer = DataTransformer()

        mock_create_engine.assert_called_once_with("sqlite:///market_data.db")
        assert transformer.engine == mock_engine
        assert transformer.SessionLocal == mock_session_local

        assert isinstance(transformer.currency_ref_mapping, dict)
        assert "USD" in transformer.currency_ref_mapping
        assert "EUR" in transformer.currency_ref_mapping

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_init_with_custom_connection(self, mock_create_engine):
        """Test initialization with custom database connection string."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        custom_connection = "postgresql://user:pass@host/db"
        transformer = DataTransformer(custom_connection)

        mock_create_engine.assert_called_once_with(custom_connection)


class TestParseExpression:
    """Test expression parsing functionality."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_usd_old_expression(self, mock_create_engine):
        """Test parsing USD old format expression."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(expression)

        assert currency == "USD"
        assert x_tenor == "1y"
        assert y_tenor == "2y"
        assert ref_rate == "Libor"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_usd_new_expression(self, mock_create_engine):
        """Test parsing USD new format (USDD) expression."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        expression = "DB(COV,VOLSWAPTION,USDD,10y,5y,PAYER,VOLBPVOL)"
        currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(expression)

        assert currency == "USD"  # Normalized to USD
        assert x_tenor == "5y"
        assert y_tenor == "10y"
        assert ref_rate == "SOFR"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_eur_expression(self, mock_create_engine):
        """Test parsing EUR expression."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        expression = "DB(COV,VOLSWAPTION,EUR,5y,1y,PAYER,VOLBPVOL)"
        currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(expression)

        assert currency == "EUR"
        assert x_tenor == "1y"  # Valid OptionExpiryEnum value
        assert y_tenor == "5y"
        assert ref_rate == "Euribor"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_gbp_old_expression(self, mock_create_engine):
        """Test parsing GBP old format expression."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        expression = "DB(COV,VOLSWAPTION,GBP,2y,1y,PAYER,VOLBPVOL)"
        currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(expression)

        assert currency == "GBP"
        assert x_tenor == "1y"  # Valid OptionExpiryEnum value
        assert y_tenor == "2y"
        assert ref_rate == "Libor"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_gbp_sonia_expression(self, mock_create_engine):
        """Test parsing GBP SONIA format expression."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        expression = "DB(COV,VOLSWAPTION,GBP,SONIA,2y,1y,PAYER,VOLBPVOL)"
        currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(expression)

        assert currency == "GBP"
        assert x_tenor == "1y"
        assert y_tenor == "2y"
        assert ref_rate == "SOFR"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_chf_saron_expression(self, mock_create_engine):
        """Test parsing CHF SARON format expression."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        expression = "DB(COV,VOLSWAPTION,CHF,SARON,10y,5y,PAYER,VOLBPVOL)"
        currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(expression)

        assert currency == "CHF"
        assert x_tenor == "5y"
        assert y_tenor == "10y"
        assert ref_rate == "SARON"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_invalid_format_expression(self, mock_create_engine):
        """Test parsing invalid expression format."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        with pytest.raises(
            ValueError, match="Expression must start with 'DB\\(' and end with '\\)'"
        ):
            transformer._parse_expression("INVALID_EXPRESSION")

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_wrong_prefix_expression(self, mock_create_engine):
        """Test parsing expression with wrong prefix."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        with pytest.raises(
            ValueError,
            match="Expression must follow DB\\(COV,VOLSWAPTION,...\\) format",
        ):
            transformer._parse_expression("DB(WRONG,PREFIX,USD,2y,1y,PAYER,VOLBPVOL)")

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_invalid_tenor_expression(self, mock_create_engine):
        """Test parsing expression with invalid tenor."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        with pytest.raises(ValueError, match="Invalid x_tenor 'invalid' in expression"):
            transformer._parse_expression(
                "DB(COV,VOLSWAPTION,USD,2y,invalid,PAYER,VOLBPVOL)"
            )

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_parse_invalid_part_count_expression(self, mock_create_engine):
        """Test parsing expression with wrong number of parts."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        with pytest.raises(
            ValueError, match="Expression must have 7 or 8 comma-separated parts"
        ):
            transformer._parse_expression("DB(COV,VOLSWAPTION,USD,2y)")


class TestMapReferenceRate:
    """Test reference rate mapping functionality."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_map_usd_reference_rate(self, mock_create_engine):
        """Test USD reference rate mapping."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        # USD old format
        result = transformer._map_reference_rate(
            "USD", ["COV", "VOLSWAPTION", "USD", "2y", "1y", "PAYER", "VOLBPVOL"]
        )
        assert result == "Libor"

        # USD new format
        result = transformer._map_reference_rate(
            "USDD", ["COV", "VOLSWAPTION", "USDD", "2y", "1y", "PAYER", "VOLBPVOL"]
        )
        assert result == "SOFR"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_map_eur_reference_rate(self, mock_create_engine):
        """Test EUR reference rate mapping."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        result = transformer._map_reference_rate(
            "EUR", ["COV", "VOLSWAPTION", "EUR", "2y", "1y", "PAYER", "VOLBPVOL"]
        )
        assert result == "Euribor"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_map_gbp_reference_rates(self, mock_create_engine):
        """Test GBP reference rate mapping."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        # GBP old format
        result = transformer._map_reference_rate(
            "GBP", ["COV", "VOLSWAPTION", "GBP", "2y", "1y", "PAYER", "VOLBPVOL"]
        )
        assert result == "Libor"

        # GBP SONIA format
        result = transformer._map_reference_rate(
            "GBP",
            ["COV", "VOLSWAPTION", "GBP", "SONIA", "2y", "1y", "PAYER", "VOLBPVOL"],
        )
        assert result == "SOFR"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_map_chf_reference_rates(self, mock_create_engine):
        """Test CHF reference rate mapping."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        # CHF old format
        result = transformer._map_reference_rate(
            "CHF", ["COV", "VOLSWAPTION", "CHF", "2y", "1y", "PAYER", "VOLBPVOL"]
        )
        assert result == "Libor"

        # CHF SARON format
        result = transformer._map_reference_rate(
            "CHF",
            ["COV", "VOLSWAPTION", "CHF", "SARON", "2y", "1y", "PAYER", "VOLBPVOL"],
        )
        assert result == "SARON"

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_map_unknown_currency(self, mock_create_engine):
        """Test mapping for unknown currency."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        result = transformer._map_reference_rate(
            "UNKNOWN",
            ["COV", "VOLSWAPTION", "UNKNOWN", "2y", "1y", "PAYER", "VOLBPVOL"],
        )
        assert result is None


class TestApplyValueTransformations:
    """Test value transformation functionality."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_usd_sofr_transformation(self, mock_create_engine):
        """Test USD SOFR value transformation (multiply by sqrt(252))."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        original_value = 100.0
        result = transformer._apply_value_transformations(original_value, "USD", "SOFR")
        expected = original_value * math.sqrt(252)

        assert result == expected
        assert result != original_value  # Verify transformation was applied

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_usd_libor_no_transformation(self, mock_create_engine):
        """Test USD Libor value (no transformation)."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        original_value = 100.0
        result = transformer._apply_value_transformations(
            original_value, "USD", "Libor"
        )

        assert result == original_value  # No transformation

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_eur_no_transformation(self, mock_create_engine):
        """Test EUR value (no transformation)."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        original_value = 125.5
        result = transformer._apply_value_transformations(
            original_value, "EUR", "Euribor"
        )

        assert result == original_value  # No transformation

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_gbp_no_transformation(self, mock_create_engine):
        """Test GBP value (no transformation)."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        original_value = 150.0
        result = transformer._apply_value_transformations(
            original_value, "GBP", "Libor"
        )

        assert result == original_value  # No transformation


class TestTransformRawData:
    """Test main transformation functionality."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    @patch("src.pipeline.transform.transformer.Session")
    def test_transform_raw_data_success(self, mock_session_class, mock_create_engine):
        """Test successful raw data transformation."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        transformer = DataTransformer()

        mock_raw_record = Mock(spec=RawData)
        mock_raw_record.raw_data_id = 1
        mock_raw_record.expression = "DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)"
        mock_raw_record.date = date(2024, 1, 15)
        mock_raw_record.value = 125.5

        with patch.object(
            transformer, "_get_raw_data_to_process", return_value=[mock_raw_record]
        ):
            clean_records, metrics = transformer.transform_raw_data()

            expected_metrics = {
                "rows_processed": 1,
                "rows_transformed": 1,
                "rows_rejected": 0,
                "validation_errors": 0,
            }
            assert metrics == expected_metrics

            assert len(clean_records) == 1
            clean_record = clean_records[0]
            assert isinstance(clean_record, CleanData)
            assert clean_record.expression == mock_raw_record.expression
            assert clean_record.date == mock_raw_record.date
            assert clean_record.currency == "EUR"
            assert clean_record.x == "1y"
            assert clean_record.y == "2y"
            assert clean_record.ref == "Euribor"
            assert clean_record.value == 125.5  # No transformation for EUR
            assert clean_record.raw_data_id == 1

    @patch("src.pipeline.transform.transformer.create_database_engine")
    @patch("src.pipeline.transform.transformer.Session")
    def test_transform_usd_sofr_with_transformation(
        self, mock_session_class, mock_create_engine
    ):
        """Test USD SOFR transformation with value multiplication."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        transformer = DataTransformer()

        mock_raw_record = Mock(spec=RawData)
        mock_raw_record.raw_data_id = 2
        mock_raw_record.expression = "DB(COV,VOLSWAPTION,USDD,10y,5y,PAYER,VOLBPVOL)"
        mock_raw_record.date = date(2024, 1, 15)
        mock_raw_record.value = 100.0

        with patch.object(
            transformer, "_get_raw_data_to_process", return_value=[mock_raw_record]
        ):
            clean_records, metrics = transformer.transform_raw_data()

            assert len(clean_records) == 1
            clean_record = clean_records[0]
            assert clean_record.currency == "USD"  # Normalized from USDD
            assert clean_record.ref == "SOFR"
            assert clean_record.value == 100.0 * math.sqrt(252)  # Transformed value

    @patch("src.pipeline.transform.transformer.create_database_engine")
    @patch("src.pipeline.transform.transformer.Session")
    def test_transform_parsing_error(self, mock_session_class, mock_create_engine):
        """Test handling of expression parsing errors."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        transformer = DataTransformer()

        mock_raw_record = Mock(spec=RawData)
        mock_raw_record.raw_data_id = 3
        mock_raw_record.expression = "INVALID_EXPRESSION"
        mock_raw_record.date = date(2024, 1, 15)
        mock_raw_record.value = 125.5

        with patch.object(
            transformer, "_get_raw_data_to_process", return_value=[mock_raw_record]
        ):
            clean_records, metrics = transformer.transform_raw_data()

            # Verify error handling
            expected_metrics = {
                "rows_processed": 1,
                "rows_transformed": 0,
                "rows_rejected": 1,
                "validation_errors": 0,
            }
            assert metrics == expected_metrics
            assert len(clean_records) == 0

    @patch("src.pipeline.transform.transformer.create_database_engine")
    @patch("src.pipeline.transform.transformer.Session")
    def test_transform_validation_error(self, mock_session_class, mock_create_engine):
        """Test handling of CleanData validation errors."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        transformer = DataTransformer()

        # Mock raw data record with None raw_data_id (will cause validation error)
        mock_raw_record = Mock(spec=RawData)
        mock_raw_record.raw_data_id = None  # This will trigger ValueError
        mock_raw_record.expression = "DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)"
        mock_raw_record.date = date(2024, 1, 15)
        mock_raw_record.value = 125.5

        with patch.object(
            transformer, "_get_raw_data_to_process", return_value=[mock_raw_record]
        ):
            clean_records, metrics = transformer.transform_raw_data()

            # Verify validation error handling
            expected_metrics = {
                "rows_processed": 1,
                "rows_transformed": 0,
                "rows_rejected": 1,
                "validation_errors": 0,
            }
            assert metrics == expected_metrics
            assert len(clean_records) == 0

    @patch("src.pipeline.transform.transformer.create_database_engine")
    @patch("src.pipeline.transform.transformer.Session")
    def test_transform_empty_input(self, mock_session_class, mock_create_engine):
        """Test transformation with no raw data."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        transformer = DataTransformer()

        with patch.object(transformer, "_get_raw_data_to_process", return_value=[]):
            clean_records, metrics = transformer.transform_raw_data()

            # Verify empty result
            expected_metrics = {
                "rows_processed": 0,
                "rows_transformed": 0,
                "rows_rejected": 0,
                "validation_errors": 0,
            }
            assert metrics == expected_metrics
            assert len(clean_records) == 0


class TestGetRawDataToProcess:
    """Test raw data retrieval functionality."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_get_raw_data_with_specific_ids(self, mock_create_engine):
        """Test getting specific raw data records by IDs."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        transformer = DataTransformer()
        mock_session = Mock()

        mock_records = [Mock(spec=RawData), Mock(spec=RawData)]
        mock_session.exec.return_value.all.return_value = mock_records

        result = transformer._get_raw_data_to_process(mock_session, [1, 2, 3])

        assert result == mock_records
        mock_session.exec.assert_called_once()

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_get_raw_data_unprocessed(self, mock_create_engine):
        """Test getting unprocessed raw data records."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        transformer = DataTransformer()
        mock_session = Mock()

        # Mock return data - simulate tuples from join query
        mock_raw_data_1 = Mock(spec=RawData)
        mock_raw_data_2 = Mock(spec=RawData)
        mock_session.exec.return_value = [
            (mock_raw_data_1, None),  # No matching clean_data
            (mock_raw_data_2, Mock()),  # Has matching clean_data but raw_data is newer
        ]

        result = transformer._get_raw_data_to_process(mock_session, None)

        assert len(result) == 2
        assert result[0] == mock_raw_data_1
        assert result[1] == mock_raw_data_2
        mock_session.exec.assert_called_once()


class TestDataTransformerEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_expression_with_all_valid_tenors(self, mock_create_engine):
        """Test parsing expressions with all valid tenor combinations."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        # Test valid combinations
        valid_expressions = [
            "DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
            "DB(COV,VOLSWAPTION,EUR,5y,1y,PAYER,VOLBPVOL)",
            "DB(COV,VOLSWAPTION,EUR,10y,5y,PAYER,VOLBPVOL)",
            "DB(COV,VOLSWAPTION,EUR,5y,5y,PAYER,VOLBPVOL)",
        ]

        for expression in valid_expressions:
            # Should not raise exception
            currency, x_tenor, y_tenor, ref_rate = transformer._parse_expression(
                expression
            )
            assert currency == "EUR"
            assert ref_rate == "Euribor"
            assert x_tenor in [e.value for e in OptionExpiryEnum]
            assert y_tenor in [e.value for e in SwapTenorEnum]

    @patch("src.pipeline.transform.transformer.create_database_engine")
    def test_value_transformation_edge_values(self, mock_create_engine):
        """Test value transformations with edge case values."""
        mock_create_engine.return_value = (Mock(), Mock())
        transformer = DataTransformer()

        # Test edge values
        test_values = [0.0, 0.001, 999999.999, -100.0]
        sqrt_252 = math.sqrt(252)

        for value in test_values:
            # USD SOFR transformation
            result = transformer._apply_value_transformations(value, "USD", "SOFR")
            assert result == value * sqrt_252

            # No transformation cases
            result = transformer._apply_value_transformations(value, "EUR", "Euribor")
            assert result == value

    @patch("src.pipeline.transform.transformer.create_database_engine")
    @patch("src.pipeline.transform.transformer.Session")
    def test_transform_mixed_currency_batch(
        self, mock_session_class, mock_create_engine
    ):
        """Test transformation of mixed currency batch."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        transformer = DataTransformer()

        # Mock multiple raw data records with different currencies
        mock_records = [
            Mock(
                raw_data_id=1,
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                value=100.0,
            ),
            Mock(
                raw_data_id=2,
                expression="DB(COV,VOLSWAPTION,USDD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                value=100.0,
            ),
            Mock(
                raw_data_id=3,
                expression="DB(COV,VOLSWAPTION,GBP,SONIA,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                value=100.0,
            ),
        ]

        with patch.object(
            transformer, "_get_raw_data_to_process", return_value=mock_records
        ):
            clean_records, metrics = transformer.transform_raw_data()

            # Verify all records processed
            assert metrics["rows_processed"] == 3
            assert metrics["rows_transformed"] == 3
            assert len(clean_records) == 3

            # Verify currency-specific transformations
            eur_record = next(r for r in clean_records if r.currency == "EUR")
            assert eur_record.value == 100.0  # No transformation
            assert eur_record.ref == "Euribor"

            usd_record = next(r for r in clean_records if r.currency == "USD")
            assert usd_record.value == 100.0 * math.sqrt(252)  # SOFR transformation
            assert usd_record.ref == "SOFR"

            gbp_record = next(r for r in clean_records if r.currency == "GBP")
            assert gbp_record.value == 100.0  # No transformation
            assert gbp_record.ref == "SOFR"  # SONIA maps to SOFR
