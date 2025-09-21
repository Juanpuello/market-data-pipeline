"""
Mock MarketData API for testing and development.

This module provides a simple mock of the MarketData API interface
that returns sample option volatility data for testing purposes.
"""

import random
from datetime import date
from typing import Dict, List

import pandas as pd

from config.logging_config import get_logger

logger = get_logger(__name__)


class MarketData:
    """
    Mock MarketData API client.

    This class provides a simple mock interface that mimics the real
    MarketData API without complex logic or validation.
    """

    def __init__(self, seed: int = 42):
        """
        Initialize the mock MarketData client.

        Args:
            seed: Random seed for reproducible data generation
        """
        self._seed = seed

    def get_historical_data(
        self, expression: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """
        Mock API method that returns sample option volatility data.

        This is a simple mock that returns basic data structure without
        validation or complex logic. The pipeline handles all validation
        and transformation.

        Args:
            expression: API expression code (not validated by mock)
            start_date: Start date for data (inclusive)
            end_date: End date for data (inclusive)

        Returns:
            pandas.DataFrame with columns ['date', 'value'] containing sample data
        """
        seed_value = self._seed + hash(f"{expression}{start_date}{end_date}") % 1000000
        random.seed(seed_value)

        dates = pd.bdate_range(start=start_date, end=end_date, freq="B")
        values = [80 + random.uniform(-10, 10) for _ in range(len(dates))]
        return pd.DataFrame({"date": dates.date, "value": values})


def create_sample_expressions() -> Dict[str, List[str]]:
    """
    Create sample expressions for testing different currency and tenor combinations.

    Returns:
        Dictionary mapping run modes to lists of expressions
    """
    new_codes = []
    for y in ["2y", "5y", "10y"]:
        for x in ["1y", "5y"]:
            new_codes.extend(
                [
                    f"DB(COV,VOLSWAPTION,USDD,{y},{x},PAYER,VOLBPVOL)",  # USD new
                    f"DB(COV,VOLSWAPTION,EUR,{y},{x},PAYER,VOLBPVOL)",  # EUR
                    f"DB(COV,VOLSWAPTION,GBP,SONIA,{y},{x},PAYER,VOLBPVOL)",  # GBP new
                    f"DB(COV,VOLSWAPTION,CHF,SARON,{y},{x},PAYER,VOLBPVOL)",  # CHF new
                ]
            )

    # Old code expressions
    old_codes = []
    for y in ["2y", "5y", "10y"]:
        for x in ["1y", "5y"]:
            old_codes.extend(
                [
                    f"DB(COV,VOLSWAPTION,USD,{y},{x},PAYER,VOLBPVOL)",  # USD old
                    f"DB(COV,VOLSWAPTION,GBP,{y},{x},PAYER,VOLBPVOL)",  # GBP old
                    f"DB(COV,VOLSWAPTION,CHF,{y},{x},PAYER,VOLBPVOL)",  # CHF old
                ]
            )

    return {
        "new_codes": new_codes,
        "old_codes": old_codes,
        "all_codes": new_codes + old_codes,
    }


if __name__ == "__main__":
    api = MarketData()

    EXPRESSION = "DB(COV,VOLSWAPTION,USDD,2y,1y,PAYER,VOLBPVOL)"
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 10)

    df = api.get_historical_data(EXPRESSION, start_date, end_date)
    logger.info("Sample data:")
    logger.info(f"\n{df.head()}")
    logger.info(f"Data shape: {df.shape}")
    logger.info(f"Value range: {df['value'].min():.2f} - {df['value'].max():.2f}")
