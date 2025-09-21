"""
Comprehensive tests for the mock MarketData API.

This module tests the functionality of the MarketData mock including
API interface, error handling, edge cases, and data validation logic.
"""

from datetime import date, timedelta
from unittest.mock import patch

import pandas as pd
import pytest

from src.market_data_api import MarketData, create_sample_expressions


class TestMarketDataAPI:
    """Test the MarketData API mock functionality."""

    def test_initialization(self):
        """Test MarketData API initialization."""
        api = MarketData()
        assert api is not None

        # Test with custom seed
        api_with_seed = MarketData(seed=123)
        assert api_with_seed is not None

    def test_basic_api_interface(self):
        """Test that the API returns the expected data structure."""
        api = MarketData()

        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 5)

        df = api.get_historical_data(expression, start_date, end_date)

        # Check basic structure
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["date", "value"]
        assert len(df) > 0

        # Check data types
        assert df["date"].dtype == "object"  # date objects
        assert pd.api.types.is_numeric_dtype(df["value"])

    def test_date_range_handling(self):
        """Test that the API handles different date ranges."""
        api = MarketData()

        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"

        # Single day
        single_day = api.get_historical_data(
            expression, date(2024, 1, 1), date(2024, 1, 1)
        )
        assert len(single_day) == 1

        # Multiple days (business days only)
        week_range = api.get_historical_data(
            expression, date(2024, 1, 1), date(2024, 1, 5)
        )
        # Should be 5 business days (Mon-Fri)
        assert len(week_range) == 5

    def test_different_expressions(self):
        """Test API with different expression formats."""
        api = MarketData()
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 3)

        expressions = [
            "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",  # Old format
            "DB(COV,VOLSWAPTION,USDD,2y,1y,PAYER,VOLBPVOL)",  # New format
            "DB(COV,VOLSWAPTION,EUR,5y,5y,PAYER,VOLBPVOL)",  # Different tenors
        ]

        for expression in expressions:
            df = api.get_historical_data(expression, start_date, end_date)
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert "date" in df.columns
            assert "value" in df.columns

    def test_value_range(self):
        """Test that values are in a reasonable range."""
        api = MarketData()

        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        df = api.get_historical_data(expression, date(2024, 1, 1), date(2024, 1, 10))

        # Values should be numeric and in a reasonable range
        assert df["value"].min() > 0  # Positive values
        assert df["value"].max() < 200  # Reasonable upper bound

    def test_reproducibility(self):
        """Test that the same seed produces the same results."""
        api1 = MarketData(seed=42)
        api2 = MarketData(seed=42)

        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 5)

        df1 = api1.get_historical_data(expression, start_date, end_date)
        df2 = api2.get_historical_data(expression, start_date, end_date)

        # Results should be identical with same seed
        pd.testing.assert_frame_equal(df1, df2)

    def test_weekend_handling(self):
        """Test that weekends are properly excluded from business days."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Date range that spans a weekend (Friday to Monday)
        friday = date(2024, 1, 5)  # Friday
        monday = date(2024, 1, 8)  # Monday
        
        df = api.get_historical_data(expression, friday, monday)
        
        # Should only include Friday and Monday (2 business days)
        assert len(df) == 2
        assert friday in df["date"].values
        assert monday in df["date"].values
        assert date(2024, 1, 6) not in df["date"].values  # Saturday
        assert date(2024, 1, 7) not in df["date"].values  # Sunday

    def test_large_date_range(self):
        """Test API with large date ranges."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # One year range
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        
        df = api.get_historical_data(expression, start_date, end_date)
        
        # Should have many business days (approximately 260-365)
        assert len(df) > 250
        assert len(df) < 370
        
        # All dates should be within range
        assert df["date"].min() >= start_date
        assert df["date"].max() <= end_date

    def test_future_dates(self):
        """Test API behavior with future dates."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Future date range
        start_date = date(2030, 1, 1)
        end_date = date(2030, 1, 5)
        
        df = api.get_historical_data(expression, start_date, end_date)
        
        # Should still return data for future dates
        assert len(df) > 0
        assert all(isinstance(d, date) for d in df["date"])

    def test_historical_dates(self):
        """Test API behavior with very old historical dates."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Very old date range
        start_date = date(1990, 1, 1)
        end_date = date(1990, 1, 5)
        
        df = api.get_historical_data(expression, start_date, end_date)
        
        # Should still return data for historical dates
        assert len(df) > 0
        assert all(isinstance(d, date) for d in df["date"])

    def test_leap_year_handling(self):
        """Test API behavior during leap years."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Leap year February 29th
        leap_day = date(2024, 2, 29)
        
        df = api.get_historical_data(expression, leap_day, leap_day)
        
        # Should handle leap year correctly
        assert len(df) == 1
        assert df["date"].iloc[0] == leap_day

    def test_single_day_multiple_calls(self):
        """Test consistency when calling API multiple times for the same day."""
        api = MarketData(seed=42)
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        target_date = date(2024, 1, 15)
        
        # Call multiple times for the same date
        df1 = api.get_historical_data(expression, target_date, target_date)
        df2 = api.get_historical_data(expression, target_date, target_date)
        df3 = api.get_historical_data(expression, target_date, target_date)
        
        # All calls should return identical results
        pd.testing.assert_frame_equal(df1, df2)
        pd.testing.assert_frame_equal(df2, df3)

    def test_different_seed_values(self):
        """Test that different seeds produce different results."""
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 5)
        
        api1 = MarketData(seed=42)
        api2 = MarketData(seed=123)
        
        df1 = api1.get_historical_data(expression, start_date, end_date)
        df2 = api2.get_historical_data(expression, start_date, end_date)
        
        # Should have same structure but different values
        assert df1.shape == df2.shape
        assert list(df1.columns) == list(df2.columns)
        pd.testing.assert_series_equal(df1["date"], df2["date"])
        
        # Values should be different (with very high probability)
        assert not df1["value"].equals(df2["value"])

    def test_expression_with_special_characters(self):
        """Test expressions with special characters and edge cases."""
        api = MarketData()
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 3)
        
        # Test various expression formats
        expressions = [
            "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)",  # Standard
            "DB(COV,VOLSWAPTION,USDD,SOFR,2y,1y,PAYER,VOLBPVOL)",  # With reference rate
            "DB(COV,VOLSWAPTION,GBP,SONIA,2y,1y,PAYER,VOLBPVOL)",  # SONIA
            "DB(COV,VOLSWAPTION,CHF,SARON,2y,1y,PAYER,VOLBPVOL)",  # SARON
            "DB(COV,VOLSWAPTION,EUR,5y,5y,PAYER,VOLBPVOL)",  # Same tenors
            "DB(COV,VOLSWAPTION,USD,10y,5y,PAYER,VOLBPVOL)",  # Long tenors
        ]
        
        for expression in expressions:
            df = api.get_historical_data(expression, start_date, end_date)
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert "date" in df.columns
            assert "value" in df.columns
            assert all(pd.notna(df["value"]))  # No NaN values

    def test_value_distribution(self):
        """Test that generated values have reasonable statistical properties."""
        api = MarketData(seed=42)  # Fixed seed for reproducible test
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Get a month of data
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        
        df = api.get_historical_data(expression, start_date, end_date)
        values = df["value"]
        
        # Basic statistical checks
        assert values.mean() > 0
        assert values.std() > 0
        assert values.min() >= 0  # Non-negative values
        assert values.max() < 1000  # Reasonable upper bound
        
        # Check for reasonable variation
        assert values.nunique() > len(values) * 0.8  # Most values should be unique

    def test_data_consistency_across_overlapping_ranges(self):
        """Test that overlapping date ranges return consistent data."""
        api = MarketData(seed=42)
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Get overlapping ranges
        df1 = api.get_historical_data(expression, date(2024, 1, 1), date(2024, 1, 10))
        df2 = api.get_historical_data(expression, date(2024, 1, 5), date(2024, 1, 15))
        
        # Find overlapping dates
        overlap_dates = set(df1["date"]).intersection(set(df2["date"]))
        assert len(overlap_dates) > 0
        
        # Values for overlapping dates should be identical
        for overlap_date in overlap_dates:
            value1 = df1[df1["date"] == overlap_date]["value"].iloc[0]
            value2 = df2[df2["date"] == overlap_date]["value"].iloc[0]
            assert value1 == value2


class TestMarketDataAPIErrorHandling:
    """Test error handling and edge cases for MarketData API."""

    def test_invalid_date_order(self):
        """Test API behavior when end_date is before start_date."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # End date before start date
        start_date = date(2024, 1, 10)
        end_date = date(2024, 1, 5)
        
        df = api.get_historical_data(expression, start_date, end_date)
        
        # Should return empty DataFrame or handle gracefully
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_same_start_end_date(self):
        """Test API when start and end dates are the same."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        target_date = date(2024, 1, 15)
        df = api.get_historical_data(expression, target_date, target_date)
        
        assert len(df) == 1
        assert df["date"].iloc[0] == target_date
        assert pd.notna(df["value"].iloc[0])

    def test_empty_expression(self):
        """Test API behavior with empty or None expression."""
        api = MarketData()
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 5)
        
        # Test with empty string
        with pytest.raises((ValueError, AttributeError, TypeError)):
            api.get_historical_data("", start_date, end_date)
        
        # Test with None (type: ignore for testing invalid input)
        with pytest.raises((ValueError, AttributeError, TypeError)):
            api.get_historical_data(None, start_date, end_date)  # type: ignore

    def test_malformed_expression(self):
        """Test API behavior with malformed expressions."""
        api = MarketData()
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 5)
        
        malformed_expressions = [
            "INVALID_EXPRESSION",
            "DB(COV,VOLSWAPTION",  # Missing closing parenthesis
            "DB(COV,VOLSWAPTION,USD)",  # Too few parameters
            "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL,EXTRA)",  # Too many parameters
            "DB(WRONG,FORMAT,HERE)",  # Wrong format
        ]
        
        for expression in malformed_expressions:
            # API should either handle gracefully or raise appropriate exception
            try:
                df = api.get_historical_data(expression, start_date, end_date)
                # If it doesn't raise exception, should return valid DataFrame
                assert isinstance(df, pd.DataFrame)
            except (ValueError, IndexError, KeyError, AttributeError):
                # These exceptions are acceptable for malformed expressions
                pass

    def test_invalid_date_types(self):
        """Test API behavior with invalid date types."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        invalid_dates = [
            "2024-01-01",  # String instead of date
            2024,  # Integer
            None,  # None
            [],  # List
        ]
        
        for invalid_date in invalid_dates:
            with pytest.raises((TypeError, AttributeError, ValueError)):
                api.get_historical_data(expression, invalid_date, date(2024, 1, 5))  # type: ignore
            
            with pytest.raises((TypeError, AttributeError, ValueError)):
                api.get_historical_data(expression, date(2024, 1, 1), invalid_date)  # type: ignore

    def test_extreme_date_ranges(self):
        """Test API with extreme date ranges."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        
        # Very large date range (10 years)
        start_date = date(2020, 1, 1)
        end_date = date(2030, 1, 1)
        
        # Should handle large ranges without errors
        df = api.get_historical_data(expression, start_date, end_date)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 1000  # Should have many data points

    def test_performance_consistency(self):
        """Test that API performance is consistent across calls."""
        api = MarketData()
        expression = "DB(COV,VOLSWAPTION,USD,2y,1y,PAYER,VOLBPVOL)"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        
        import time
        
        # Time multiple calls
        times = []
        for _ in range(5):
            start_time = time.time()
            df = api.get_historical_data(expression, start_date, end_date)
            end_time = time.time()
            times.append(end_time - start_time)
            
            # Verify data is returned
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
        
        # Performance should be reasonably consistent
        avg_time = sum(times) / len(times)
        for t in times:
            # No call should take more than 10x the average
            assert t < avg_time * 10


class TestSampleExpressions:
    """Test the sample expression generation."""

    def test_sample_expressions_structure(self):
        """Test that sample expressions are properly structured."""
        expressions = create_sample_expressions()

        # Check required keys
        required_keys = ["new_codes", "old_codes", "all_codes"]
        for key in required_keys:
            assert key in expressions
            assert isinstance(expressions[key], list)
            assert len(expressions[key]) > 0

        # Check that all_codes includes both new and old
        assert len(expressions["all_codes"]) == len(expressions["new_codes"]) + len(
            expressions["old_codes"]
        )

    def test_sample_expressions_validity(self):
        """Test that all sample expressions contain expected patterns."""
        expressions = create_sample_expressions()

        # Check that expressions contain expected patterns
        for category, expr_list in expressions.items():
            for expression in expr_list:
                assert expression.startswith("DB(COV,VOLSWAPTION,")
                assert expression.endswith(",PAYER,VOLBPVOL)")

    def test_sample_expressions_coverage(self):
        """Test that sample expressions cover main currencies."""
        expressions = create_sample_expressions()

        # New codes should include USDD, EUR, GBP with SONIA, CHF with SARON
        new_codes_str = " ".join(expressions["new_codes"])
        assert "USDD" in new_codes_str
        assert "EUR" in new_codes_str
        assert "GBP,SONIA" in new_codes_str
        assert "CHF,SARON" in new_codes_str

        # Old codes should include USD, GBP, CHF (without reference rates)
        old_codes_str = " ".join(expressions["old_codes"])
        assert "USD,2y,1y" in old_codes_str or "USD,2y,5y" in old_codes_str
        assert "GBP,2y,1y" in old_codes_str or "GBP,2y,5y" in old_codes_str
        assert "CHF,2y,1y" in old_codes_str or "CHF,2y,5y" in old_codes_str


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
