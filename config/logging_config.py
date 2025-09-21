"""
Centralized logging configuration for the market data pipeline.

This module provides a simple logging setup with both console and file handlers.
Logs are written to the logs/ directory in the project root.
"""

import logging
import logging.config
from pathlib import Path


def setup_logging(log_level: str = "INFO", log_file: str = "market_data.log") -> None:
    """
    Configure logging for the entire application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Name of the log file (will be created in logs/ directory)
    """
    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Full path to log file
    log_file_path = logs_dir / log_file

    # Logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s:%(lineno)d: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": str(log_file_path),
                "mode": "a",
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "market_data": {
                "level": "DEBUG",  # Set to DEBUG to capture all levels in file
                "handlers": ["console", "file"],
                "propagate": False,
            }
        },
        "root": {"level": log_level, "handlers": ["console", "file"]},
    }

    # Apply configuration
    logging.config.dictConfig(logging_config)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.

    Args:
        name: Logger name (typically __name__ from the calling module)

    Returns:
        Configured logger instance
    """
    # Use market_data prefix for all our loggers
    logger_name = f"market_data.{name}" if not name.startswith("market_data") else name
    return logging.getLogger(logger_name)
