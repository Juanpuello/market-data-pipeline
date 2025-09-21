"""
Test suite for database utilities in src/core/database.py.

Tests database engine creation, table management, and utility functions
while mocking SQLAlchemy connections to avoid actual database dependencies.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from src.core.database import (
    create_database_engine,
    create_tables,
    get_table_info,
    utc_now,
)


class TestUtcNow:
    """Test UTC timestamp generation."""

    def test_utc_now_returns_aware_datetime(self):
        """Test that utc_now returns timezone-aware datetime."""
        result = utc_now()
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc

    def test_utc_now_is_current(self):
        """Test that utc_now returns current time (within reasonable margin)."""
        before = datetime.now(timezone.utc)
        result = utc_now()
        after = datetime.now(timezone.utc)

        # Should be within 1 second
        assert before <= result <= after

    def test_utc_now_consistency(self):
        """Test that multiple calls are reasonably close."""
        time1 = utc_now()
        time2 = utc_now()

        # Should be within 1 second of each other
        diff = abs((time2 - time1).total_seconds())
        assert diff < 1.0


class TestCreateDatabaseEngine:
    """Test database engine creation with different configurations."""

    @patch("src.core.database.create_engine")
    @patch("src.core.database.sessionmaker")
    def test_sqlite_engine_creation(self, mock_sessionmaker, mock_create_engine):
        """Test SQLite engine creation with appropriate settings."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_sessionmaker.return_value = mock_session_class

        connection_string = "sqlite:///test.db"
        engine, session_local = create_database_engine(connection_string)

        mock_create_engine.assert_called_once_with(
            connection_string, echo=False, pool_pre_ping=True
        )
        mock_sessionmaker.assert_called_once_with(
            autocommit=False, autoflush=False, bind=mock_engine
        )

        assert engine == mock_engine
        assert session_local == mock_session_class

    @patch("src.core.database.create_engine")
    @patch("src.core.database.sessionmaker")
    def test_postgresql_engine_creation(self, mock_sessionmaker, mock_create_engine):
        """Test PostgreSQL engine creation with connection pooling."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_sessionmaker.return_value = mock_session_class

        connection_string = "postgresql://user:pass@host:5432/db"
        engine, session_local = create_database_engine(connection_string)

        mock_create_engine.assert_called_once_with(
            connection_string,
            pool_size=10,
            max_overflow=20,
            echo=False,
            pool_pre_ping=True,
        )
        mock_sessionmaker.assert_called_once_with(
            autocommit=False, autoflush=False, bind=mock_engine
        )

        assert engine == mock_engine
        assert session_local == mock_session_class

    @patch("src.core.database.create_engine")
    @patch("src.core.database.sessionmaker")
    def test_engine_creation_returns_tuple(self, mock_sessionmaker, mock_create_engine):
        """Test that function returns both engine and session maker."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_sessionmaker.return_value = mock_session_class

        result = create_database_engine("sqlite:///test.db")

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == mock_engine
        assert result[1] == mock_session_class

    @patch("src.core.database.create_engine")
    def test_engine_creation_edge_cases(self, mock_create_engine):
        """Test edge cases in connection string handling."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        test_cases = [
            ("sqlite:///test.db", "sqlite_config"),
            ("sqlite:///:memory:", "sqlite_config"),
            ("SQLITE:///test.db", "non_sqlite_config"),
            ("SQLite:///relative.db", "non_sqlite_config"),
        ]

        for conn_str, expected_config in test_cases:
            with patch("src.core.database.sessionmaker"):
                create_database_engine(conn_str)

                if expected_config == "sqlite_config":
                    # Should use SQLite-specific config
                    mock_create_engine.assert_called_with(
                        conn_str, echo=False, pool_pre_ping=True
                    )
                else:
                    # Should use general config with pooling
                    mock_create_engine.assert_called_with(
                        conn_str,
                        pool_size=10,
                        max_overflow=20,
                        echo=False,
                        pool_pre_ping=True,
                    )


class TestCreateTables:
    """Test table creation functionality."""

    @patch.object(SQLModel.metadata, "create_all")
    def test_create_tables_calls_sqlmodel(self, mock_create_all):
        """Test that create_tables calls SQLModel.metadata.create_all."""
        mock_engine = Mock(spec=Engine)

        create_tables(mock_engine)

        mock_create_all.assert_called_once_with(bind=mock_engine)

    @patch.object(SQLModel.metadata, "create_all")
    def test_create_tables_with_none_engine(self, mock_create_all):
        """Test create_tables behavior with None engine."""
        create_tables(None)
        mock_create_all.assert_called_once_with(bind=None)

    @patch.object(SQLModel.metadata, "create_all")
    def test_create_tables_exception_handling(self, mock_create_all):
        """Test that exceptions from create_all are properly propagated."""
        mock_engine = Mock(spec=Engine)
        mock_create_all.side_effect = Exception("Database connection failed")

        with pytest.raises(Exception, match="Database connection failed"):
            create_tables(mock_engine)


class TestGetTableInfo:
    """Test table information utility."""

    def test_get_table_info_returns_dict(self):
        """Test that get_table_info returns a dictionary."""
        result = get_table_info()
        assert isinstance(result, dict)

    def test_get_table_info_contains_expected_tables(self):
        """Test that table info contains expected table names."""
        result = get_table_info()

        expected_tables = ["raw_data", "clean_data"]
        for table_name in expected_tables:
            assert table_name in result
            assert isinstance(result[table_name], str)
            assert len(result[table_name]) > 0

    def test_get_table_info_descriptions(self):
        """Test that table descriptions are meaningful."""
        result = get_table_info()

        raw_desc = result["raw_data"]
        assert "raw_data" in raw_desc.lower() or "api" in raw_desc.lower()
        assert len(raw_desc) > 20

        clean_desc = result["clean_data"]
        assert "clean_data" in clean_desc.lower() or "normalized" in clean_desc.lower()
        assert len(clean_desc) > 20

    def test_get_table_info_consistency(self):
        """Test that multiple calls return the same result."""
        result1 = get_table_info()
        result2 = get_table_info()
        assert result1 == result2


class TestDatabaseIntegration:
    """Test integration scenarios and edge cases."""

    @patch("src.core.database.create_engine")
    @patch("src.core.database.sessionmaker")
    @patch.object(SQLModel.metadata, "create_all")
    def test_full_database_setup_workflow(
        self, mock_create_all, mock_sessionmaker, mock_create_engine
    ):
        """Test complete database setup workflow."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_sessionmaker.return_value = mock_session_class

        connection_string = "sqlite:///test.db"
        engine, session_local = create_database_engine(connection_string)
        create_tables(engine)
        table_info = get_table_info()

        mock_create_engine.assert_called_once()
        mock_sessionmaker.assert_called_once()
        mock_create_all.assert_called_once_with(bind=mock_engine)
        assert isinstance(table_info, dict)
        assert len(table_info) > 0

    def test_connection_string_validation(self):
        """Test handling of various connection string formats."""
        valid_strings = [
            "sqlite:///test.db",
            "postgresql://user:pass@localhost/db",
            "mysql://user:pass@host:3306/db",
        ]

        for conn_str in valid_strings:
            with patch("src.core.database.create_engine") as mock_create:
                with patch("src.core.database.sessionmaker"):
                    mock_create.return_value = Mock(spec=Engine)
                    # Should not raise exception
                    engine, session_local = create_database_engine(conn_str)
                    assert engine is not None
                    assert session_local is not None

    @patch("src.core.database.create_engine")
    def test_engine_creation_failure_handling(self, mock_create_engine):
        """Test handling of engine creation failures."""
        mock_create_engine.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            create_database_engine("invalid://connection")


class TestDatabaseEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_utc_now_timezone_handling(self):
        """Test UTC timestamp handling across different timezones."""
        # Mock system timezone changes
        with patch("src.core.database.datetime") as mock_datetime:
            mock_now = Mock()
            mock_now.return_value = datetime(
                2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc
            )
            mock_datetime.now = mock_now
            mock_datetime.timezone = timezone

            result = utc_now()
            mock_now.assert_called_once_with(timezone.utc)

    @patch("src.core.database.create_engine")
    @patch("src.core.database.sessionmaker")
    def test_empty_connection_string(self, mock_sessionmaker, mock_create_engine):
        """Test behavior with empty connection string."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_sessionmaker.return_value = mock_session_class

        # Should still work, let SQLAlchemy handle the invalid string
        engine, session_local = create_database_engine("")
        mock_create_engine.assert_called_once()

    def test_get_table_info_immutability(self):
        """Test that table info cannot be accidentally modified."""
        result1 = get_table_info()
        original_keys = set(result1.keys())

        # Try to modify returned dict
        result1["new_table"] = "Should not affect future calls"

        result2 = get_table_info()
        assert set(result2.keys()) == original_keys
        assert "new_table" not in result2
