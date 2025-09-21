"""
Test suite for data extraction in src/pipeline/extract/extractor.py.

Tests data extraction logic, API client integration, blob storage,
and database operations while mocking external dependencies.
"""

from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pandas as pd
import pytest
from sqlalchemy import Engine

from src.market_data_api import MarketData
from src.models import RawData, RunModeEnum
from src.pipeline.extract.extractor import DataExtractor


class TestDataExtractorInit:
    """Test DataExtractor initialization."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_init_with_default_connection(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test initialization with default database connection."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_api_client = Mock(spec=MarketData)
        mock_market_data.return_value = mock_api_client
        mock_blob_path = Mock(spec=Path)
        mock_path.return_value = mock_blob_path

        extractor = DataExtractor()

        mock_create_engine.assert_called_once_with("sqlite:///market_data.db")
        mock_market_data.assert_called_once()
        mock_path.assert_called_once_with("blob_storage")
        mock_blob_path.mkdir.assert_called_once_with(exist_ok=True)

        assert extractor.engine == mock_engine
        assert extractor.SessionLocal == mock_session_local
        assert extractor.api_client == mock_api_client
        assert extractor.blob_storage_path == mock_blob_path

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_init_with_custom_connection(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test initialization with custom database connection string."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_api_client = Mock(spec=MarketData)
        mock_market_data.return_value = mock_api_client

        custom_connection = "postgresql://user:pass@host/db"

        extractor = DataExtractor(custom_connection)

        mock_create_engine.assert_called_once_with(custom_connection)


class TestExtractDataBasic:
    """Test basic extract_data functionality."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.Session")
    def test_extract_data_success(
        self, mock_session_class, mock_path, mock_market_data, mock_create_engine
    ):
        """Test successful data extraction."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_api_client = Mock()
        mock_market_data.return_value = mock_api_client

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        extractor = DataExtractor()

        mock_df = pd.DataFrame({"date": [date(2024, 1, 15)], "value": [125.5]})
        mock_api_client.get_historical_data.return_value = mock_df

        with (
            patch.object(extractor, "_should_fetch_data", return_value=True),
            patch.object(
                extractor, "_store_blob", return_value="blob://test/file.json"
            ),
            patch.object(extractor, "_insert_raw_data", return_value=1),
        ):

            expressions = ["DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)"]
            start_date = end_date = date(2024, 1, 15)

            result = extractor.extract_data(expressions, start_date, end_date)

            expected_metrics = {
                "expressions_processed": 1,
                "rows_fetched": 1,
                "rows_inserted": 1,
                "duplicates_detected": 0,
                "errors": 0,
            }
            assert result == expected_metrics

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_extract_data_invalid_date_range_default_mode(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test validation of date ranges for DEFAULT mode."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()

        expressions = ["DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)"]
        start_date = date(2024, 1, 15)
        end_date = date(2024, 1, 16)  # Different from start_date

        with pytest.raises(
            ValueError,
            match="default mode requires start_date and end_date to be the same",
        ):
            extractor.extract_data(
                expressions, start_date, end_date, RunModeEnum.DEFAULT
            )

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_extract_data_invalid_date_range_old_codes_mode(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test validation of date ranges for OLD_CODES mode."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()

        expressions = ["DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)"]
        start_date = date(2024, 1, 15)
        end_date = date(2024, 1, 16)  # Different from start_date

        with pytest.raises(
            ValueError,
            match="old_codes mode requires start_date and end_date to be the same",
        ):
            extractor.extract_data(
                expressions, start_date, end_date, RunModeEnum.OLD_CODES
            )

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.Session")
    def test_extract_data_empty_expressions_list(
        self, mock_session_class, mock_path, mock_market_data, mock_create_engine
    ):
        """Test extraction with empty expressions list."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()

        result = extractor.extract_data([], date(2024, 1, 15), date(2024, 1, 15))

        expected_metrics = {
            "expressions_processed": 0,
            "rows_fetched": 0,
            "rows_inserted": 0,
            "duplicates_detected": 0,
            "errors": 0,
        }
        assert result == expected_metrics


class TestShouldFetchData:
    """Test _should_fetch_data logic."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_should_fetch_historical_mode(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test that HISTORICAL mode always allows fetching."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()
        mock_session = Mock()

        result = extractor._should_fetch_data(
            mock_session,
            "expr",
            date(2024, 1, 15),
            date(2024, 1, 16),
            RunModeEnum.HISTORICAL,
        )

        assert result is True

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_should_fetch_default_mode_under_limit(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test DEFAULT mode when under version limit."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()
        mock_session = Mock()
        mock_session.exec.return_value.all.return_value = [
            1,
            2,
        ]  # 2 existing records < 3 limit

        result = extractor._should_fetch_data(
            mock_session,
            "expr",
            date(2024, 1, 15),
            date(2024, 1, 15),
            RunModeEnum.DEFAULT,
        )

        assert result is True

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    def test_should_fetch_default_mode_at_limit(
        self, mock_path, mock_market_data, mock_create_engine
    ):
        """Test DEFAULT mode when at version limit."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()
        mock_session = Mock()
        mock_session.exec.return_value.all.return_value = [
            1,
            2,
            3,
        ]  # 3 existing records = limit

        result = extractor._should_fetch_data(
            mock_session,
            "expr",
            date(2024, 1, 15),
            date(2024, 1, 15),
            RunModeEnum.DEFAULT,
        )

        assert result is False


class TestStoreBlob:
    """Test blob storage functionality."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.datetime")
    @patch("builtins.open", new_callable=mock_open)
    def test_store_blob_creates_json_file(
        self, mock_file, mock_datetime, mock_path, mock_market_data, mock_create_engine
    ):
        """Test that blob storage creates JSON file with correct content."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        mock_datetime.utcnow.return_value.strftime.return_value = "20240115_123000"
        mock_datetime.now.return_value = datetime(
            2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc
        )

        extractor = DataExtractor()

        df = pd.DataFrame({"date": [date(2024, 1, 15)], "value": [125.5]})

        result = extractor._store_blob(
            df,
            "DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)",
            date(2024, 1, 15),
            date(2024, 1, 15),
        )

        expected_uri = "blob://market-data/DBCOV_VOLSWAPTION_EUR_1y_5y_PAYER_VOLBPVOL_2024-01-15_2024-01-15_20240115_123000.json"
        assert result == expected_uri

        mock_file.assert_called_once()


class TestInsertRawData:
    """Test raw data insertion functionality."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.datetime")
    @patch("src.pipeline.extract.extractor.select")
    def test_insert_raw_data_single_row(
        self,
        mock_select,
        mock_datetime,
        mock_path,
        mock_market_data,
        mock_create_engine,
    ):
        """Test insertion of single raw data row."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()
        mock_session = Mock()

        mock_session.exec.return_value.first.return_value = 2

        fixed_datetime = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_datetime

        df = pd.DataFrame({"date": [date(2024, 1, 15)], "value": [125.5]})

        result = extractor._insert_raw_data(
            mock_session, df, "test_expr", "blob://test/file.json", RunModeEnum.DEFAULT
        )

        assert result == 1
        mock_session.add.assert_called_once()

        added_record = mock_session.add.call_args[0][0]
        assert isinstance(added_record, RawData)
        assert added_record.expression == "test_expr"
        assert added_record.date == date(2024, 1, 15)
        assert added_record.value == 125.5
        assert added_record.version == 2
        assert added_record.ingestion_mode == RunModeEnum.DEFAULT.value
        assert added_record.source_file_uri == "blob://test/file.json"
        assert added_record.fetch_timestamp == fixed_datetime


class TestGetExpressionsForMode:
    """Test expression selection for different modes."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.create_sample_expressions")
    def test_get_expressions_default_mode(
        self, mock_create_expressions, mock_path, mock_market_data, mock_create_engine
    ):
        """Test expression selection for DEFAULT mode."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()

        mock_create_expressions.return_value = {
            "new_codes": ["expr1", "expr2"],
            "old_codes": ["expr3", "expr4"],
            "all_codes": ["expr1", "expr2", "expr3", "expr4"],
        }

        result = extractor.get_expressions_for_mode(RunModeEnum.DEFAULT)
        assert result == ["expr1", "expr2"]

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.create_sample_expressions")
    def test_get_expressions_historical_mode(
        self, mock_create_expressions, mock_path, mock_market_data, mock_create_engine
    ):
        """Test expression selection for HISTORICAL mode."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)
        mock_market_data.return_value = Mock()

        extractor = DataExtractor()

        mock_create_expressions.return_value = {
            "new_codes": ["expr1", "expr2"],
            "old_codes": ["expr3", "expr4"],
            "all_codes": ["expr1", "expr2", "expr3", "expr4"],
        }

        result = extractor.get_expressions_for_mode(RunModeEnum.HISTORICAL)
        assert result == ["expr3", "expr4"]


class TestDataExtractorEdgeCases:
    """Test edge cases and error scenarios."""

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.Session")
    def test_extract_data_api_error_handling(
        self, mock_session_class, mock_path, mock_market_data, mock_create_engine
    ):
        """Test handling of API errors during extraction."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_api_client = Mock()
        mock_market_data.return_value = mock_api_client

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        extractor = DataExtractor()

        # Mock API error
        mock_api_client.get_historical_data.side_effect = Exception("API Error")

        with patch.object(extractor, "_should_fetch_data", return_value=True):
            expressions = ["DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)"]
            start_date = end_date = date(2024, 1, 15)

            result = extractor.extract_data(expressions, start_date, end_date)

            # Should record error
            expected_metrics = {
                "expressions_processed": 0,
                "rows_fetched": 0,
                "rows_inserted": 0,
                "duplicates_detected": 0,
                "errors": 1,
            }
            assert result == expected_metrics

    @patch("src.pipeline.extract.extractor.create_database_engine")
    @patch("src.pipeline.extract.extractor.MarketData")
    @patch("src.pipeline.extract.extractor.Path")
    @patch("src.pipeline.extract.extractor.Session")
    def test_extract_data_empty_api_response(
        self, mock_session_class, mock_path, mock_market_data, mock_create_engine
    ):
        """Test handling of empty API response."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_api_client = Mock()
        mock_market_data.return_value = mock_api_client

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        extractor = DataExtractor()

        mock_api_client.get_historical_data.return_value = pd.DataFrame()

        with patch.object(extractor, "_should_fetch_data", return_value=True):
            expressions = ["DB(COV,VOLSWAPTION,EUR,1y,5y,PAYER,VOLBPVOL)"]
            start_date = end_date = date(2024, 1, 15)

            result = extractor.extract_data(expressions, start_date, end_date)

            expected_metrics = {
                "expressions_processed": 0,
                "rows_fetched": 0,
                "rows_inserted": 0,
                "duplicates_detected": 0,
                "errors": 0,
            }
            assert result == expected_metrics
