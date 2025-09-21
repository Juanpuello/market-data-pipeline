"""
Test suite for data loading in src/pipeline/load/loader.py.

Tests data loading logic, upsert operations, integrity validation,
and database operations while mocking external dependencies.
"""

from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

from sqlalchemy import Engine

from src.models import CleanData
from src.pipeline.load.loader import DataLoader


class TestDataLoaderInit:
    """Test DataLoader initialization."""

    @patch("src.pipeline.load.loader.create_database_engine")
    def test_init_with_default_connection(self, mock_create_engine):
        """Test initialization with default database connection."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        loader = DataLoader()

        mock_create_engine.assert_called_once_with("sqlite:///market_data.db")
        assert loader.engine == mock_engine
        assert loader.session_local == mock_session_local

    @patch("src.pipeline.load.loader.create_database_engine")
    def test_init_with_custom_connection(self, mock_create_engine):
        """Test initialization with custom database connection string."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        custom_connection = "postgresql://user:pass@host/db"
        loader = DataLoader(custom_connection)

        mock_create_engine.assert_called_once_with(custom_connection)


class TestLoadCleanData:
    """Test clean data loading functionality."""

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_load_clean_data_success(self, mock_session_class, mock_create_engine):
        """Test successful loading of clean data records."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        clean_records = [
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="EUR",
                x="1y",
                y="2y",
                ref="Euribor",
                value=125.5,
                raw_data_id=1,
            ),
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="USD",
                x="5y",
                y="10y",
                ref="SOFR",
                value=200.0,
                raw_data_id=2,
            ),
        ]

        with patch.object(
            loader, "_upsert_clean_record", side_effect=["inserted", "updated"]
        ):
            metrics = loader.load_clean_data(clean_records)

            expected_metrics = {
                "records_processed": 2,
                "records_inserted": 1,
                "records_updated": 1,
                "records_failed": 0,
            }
            assert metrics == expected_metrics

            mock_session.commit.assert_called_once()

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_load_clean_data_with_failures(
        self, mock_session_class, mock_create_engine
    ):
        """Test loading with some record failures."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        clean_records = [
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="EUR",
                x="1y",
                y="2y",
                ref="Euribor",
                value=125.5,
                raw_data_id=1,
            ),
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="USD",
                x="5y",
                y="10y",
                ref="SOFR",
                value=200.0,
                raw_data_id=2,
            ),
        ]

        def mock_upsert(session, record):
            if record.raw_data_id == 1:
                return "inserted"
            else:
                raise Exception("Database error")

        with patch.object(loader, "_upsert_clean_record", side_effect=mock_upsert):
            metrics = loader.load_clean_data(clean_records)

            expected_metrics = {
                "records_processed": 2,
                "records_inserted": 1,
                "records_updated": 0,
                "records_failed": 1,
            }
            assert metrics == expected_metrics

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_load_empty_clean_data(self, mock_session_class, mock_create_engine):
        """Test loading with empty clean data list."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        metrics = loader.load_clean_data([])

        expected_metrics = {
            "records_processed": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "records_failed": 0,
        }
        assert metrics == expected_metrics

        mock_session.commit.assert_called_once()


class TestUpsertCleanRecord:
    """Test upsert functionality for individual clean records."""

    @patch("src.pipeline.load.loader.create_database_engine")
    def test_upsert_new_record_insertion(self, mock_create_engine):
        """Test inserting a new clean record (no existing record)."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        loader = DataLoader()
        mock_session = Mock()

        mock_session.exec.return_value.first.return_value = None

        clean_record = CleanData(
            clean_data_id=None,
            expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 15),
            currency="EUR",
            x="1y",
            y="2y",
            ref="Euribor",
            value=125.5,
            raw_data_id=1,
        )

        result = loader._upsert_clean_record(mock_session, clean_record)

        assert result == "inserted"
        mock_session.add.assert_called_once_with(clean_record)

    @patch("src.pipeline.load.loader.create_database_engine")
    def test_upsert_existing_record_update(self, mock_create_engine):
        """Test updating an existing clean record."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        loader = DataLoader()
        mock_session = Mock()

        existing_record = Mock(spec=CleanData)
        existing_record.currency = "OLD_CURRENCY"
        existing_record.value = 100.0
        mock_session.exec.return_value.first.return_value = existing_record

        clean_record = CleanData(
            clean_data_id=None,
            expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 15),
            currency="EUR",
            x="1y",
            y="2y",
            ref="Euribor",
            value=125.5,
            raw_data_id=1,
        )

        result = loader._upsert_clean_record(mock_session, clean_record)

        assert result == "updated"

        # Verify existing record was updated with new values
        assert existing_record.currency == "EUR"
        assert existing_record.x == "1y"
        assert existing_record.y == "2y"
        assert existing_record.ref == "Euribor"
        assert existing_record.value == 125.5
        assert existing_record.raw_data_id == 1

        mock_session.add.assert_called_once_with(existing_record)


class TestValidateCleanDataIntegrity:
    """Test clean data integrity validation functionality."""

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_validate_integrity_no_raw_data(
        self, mock_session_class, mock_create_engine
    ):
        """Test integrity validation when no raw data exists."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        # Mock no latest fetch timestamp found
        mock_session.exec.return_value.first.return_value = None

        loader = DataLoader()

        integrity_report = loader.validate_clean_data_integrity()

        expected_report = {
            "valid": True,
            "issues": ["No raw data records found"],
            "duplicate_combinations": 0,
            "validation_fetch_timestamp": None,
            "records_checked": 0,
        }

        assert integrity_report == expected_report

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_validate_integrity_no_duplicates(
        self, mock_session_class, mock_create_engine
    ):
        """Test integrity validation with no duplicates (valid state)."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        # Mock latest fetch timestamp, and clean data records with no duplicates
        latest_timestamp = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        mock_clean_records = [
            Mock(
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
            ),
            Mock(
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
            ),
            Mock(
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 16),  # Different date, so not a duplicate
            ),
        ]

        # Setup session.exec calls for different queries
        mock_session.exec.side_effect = [
            Mock(first=Mock(return_value=latest_timestamp)),  # Latest timestamp query
            Mock(all=Mock(return_value=mock_clean_records)),  # Clean data query
        ]

        integrity_report = loader.validate_clean_data_integrity()

        expected_report = {
            "valid": True,
            "issues": [],
            "duplicate_combinations": 0,
            "validation_fetch_timestamp": latest_timestamp,
            "records_checked": 3,
        }

        assert integrity_report == expected_report

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_validate_integrity_with_duplicates(
        self, mock_session_class, mock_create_engine
    ):
        """Test integrity validation with duplicates (invalid state)."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        # Mock latest fetch timestamp, and clean data records with duplicates
        latest_timestamp = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        mock_clean_records = [
            Mock(
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
            ),
            Mock(
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),  # Duplicate expression+date combination
            ),
            Mock(
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
            ),
            Mock(
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),  # Another duplicate
            ),
        ]

        # Setup session.exec calls for different queries
        mock_session.exec.side_effect = [
            Mock(first=Mock(return_value=latest_timestamp)),  # Latest timestamp query
            Mock(all=Mock(return_value=mock_clean_records)),  # Clean data query
        ]

        integrity_report = loader.validate_clean_data_integrity()

        expected_report = {
            "valid": False,
            "issues": [
                f"Found 2 duplicate expression+date combinations "
                f"for fetch_timestamp {latest_timestamp}"
            ],
            "duplicate_combinations": 2,
            "validation_fetch_timestamp": latest_timestamp,
            "records_checked": 4,
        }

        assert integrity_report == expected_report


class TestDataLoaderEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_load_clean_data_all_failures(self, mock_session_class, mock_create_engine):
        """Test loading when all records fail."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        clean_records = [
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="EUR",
                x="1y",
                y="2y",
                ref="Euribor",
                value=125.5,
                raw_data_id=1,
            ),
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="USD",
                x="5y",
                y="10y",
                ref="SOFR",
                value=200.0,
                raw_data_id=2,
            ),
        ]

        # Mock all failures
        with patch.object(
            loader, "_upsert_clean_record", side_effect=Exception("Database error")
        ):
            metrics = loader.load_clean_data(clean_records)

            # Verify all failed metrics
            expected_metrics = {
                "records_processed": 2,
                "records_inserted": 0,
                "records_updated": 0,
                "records_failed": 2,
            }
            assert metrics == expected_metrics

    @patch("src.pipeline.load.loader.create_database_engine")
    def test_upsert_with_edge_case_values(self, mock_create_engine):
        """Test upsert operation with edge case values in clean record."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        loader = DataLoader()
        mock_session = Mock()

        # Mock no existing record found
        mock_session.exec.return_value.first.return_value = None

        # Clean record with very small value (edge case but valid)
        clean_record = CleanData(
            clean_data_id=None,
            expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
            date=date(2024, 1, 15),
            currency="EUR",
            x="1y",
            y="2y",
            ref="Euribor",
            value=0.001,  # Very small but valid value (> 0)
            raw_data_id=1,
        )

        result = loader._upsert_clean_record(mock_session, clean_record)

        assert result == "inserted"
        mock_session.add.assert_called_once_with(clean_record)

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_validate_integrity_single_record(
        self, mock_session_class, mock_create_engine
    ):
        """Test integrity validation with single record (edge case)."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        # Mock latest fetch timestamp
        latest_timestamp = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

        # Mock single clean data record
        mock_clean_records = [
            Mock(
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
            ),
        ]

        # Setup session.exec calls for different queries
        mock_session.exec.side_effect = [
            Mock(first=Mock(return_value=latest_timestamp)),  # Latest timestamp query
            Mock(all=Mock(return_value=mock_clean_records)),  # Clean data query
        ]

        integrity_report = loader.validate_clean_data_integrity()

        expected_report = {
            "valid": True,
            "issues": [],
            "duplicate_combinations": 0,
            "validation_fetch_timestamp": latest_timestamp,
            "records_checked": 1,
        }

        assert integrity_report == expected_report

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_validate_integrity_empty_clean_data(
        self, mock_session_class, mock_create_engine
    ):
        """Test integrity validation with no clean data records."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        # Mock latest fetch timestamp exists but no clean data
        latest_timestamp = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

        # Setup session.exec calls for different queries
        mock_session.exec.side_effect = [
            Mock(first=Mock(return_value=latest_timestamp)),  # Latest timestamp query
            Mock(all=Mock(return_value=[])),  # Empty clean data query
        ]

        integrity_report = loader.validate_clean_data_integrity()

        expected_report = {
            "valid": True,
            "issues": [],
            "duplicate_combinations": 0,
            "validation_fetch_timestamp": latest_timestamp,
            "records_checked": 0,
        }

        assert integrity_report == expected_report

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_load_large_batch_clean_data(self, mock_session_class, mock_create_engine):
        """Test loading a large batch of clean data records."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        # Create a large batch of clean records
        clean_records = []
        for i in range(100):
            clean_records.append(
                CleanData(
                    clean_data_id=None,
                    expression=f"DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)_{i}",
                    date=date(2024, 1, 15),
                    currency="EUR",
                    x="1y",
                    y="2y",
                    ref="Euribor",
                    value=125.5 + i,
                    raw_data_id=i + 1,
                )
            )

        # Mock all records as inserts
        with patch.object(loader, "_upsert_clean_record", return_value="inserted"):
            metrics = loader.load_clean_data(clean_records)

            # Verify large batch metrics
            expected_metrics = {
                "records_processed": 100,
                "records_inserted": 100,
                "records_updated": 0,
                "records_failed": 0,
            }
            assert metrics == expected_metrics

    @patch("src.pipeline.load.loader.create_database_engine")
    @patch("src.pipeline.load.loader.Session")
    def test_load_mixed_insert_update_operations(
        self, mock_session_class, mock_create_engine
    ):
        """Test loading with mixed insert and update operations."""
        mock_engine = Mock(spec=Engine)
        mock_session_local = Mock()
        mock_create_engine.return_value = (mock_engine, mock_session_local)

        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        loader = DataLoader()

        clean_records = [
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,EUR,2y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="EUR",
                x="1y",
                y="2y",
                ref="Euribor",
                value=125.5,
                raw_data_id=1,
            ),
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="USD",
                x="5y",
                y="10y",
                ref="SOFR",
                value=200.0,
                raw_data_id=2,
            ),
            CleanData(
                clean_data_id=None,
                expression="DB(COV,VOLSWAPTION,GBP,5y,1y,PAYER,VOLBPVOL)",
                date=date(2024, 1, 15),
                currency="GBP",
                x="1y",
                y="5y",
                ref="Libor",
                value=150.0,
                raw_data_id=3,
            ),
        ]

        # Mock mixed operations: insert, update, insert
        with patch.object(
            loader,
            "_upsert_clean_record",
            side_effect=["inserted", "updated", "inserted"],
        ):
            metrics = loader.load_clean_data(clean_records)

            # Verify mixed operation metrics
            expected_metrics = {
                "records_processed": 3,
                "records_inserted": 2,
                "records_updated": 1,
                "records_failed": 0,
            }
            assert metrics == expected_metrics
