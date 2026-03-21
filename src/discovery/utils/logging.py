"""Structured logging setup for discovery system."""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
    """Configure structured logging for the discovery package.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('discovery')
    logger.setLevel(getattr(logging, level.upper()))

    # Create formatter with timestamp, level, module, message
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_file}")

    return logger


# Initialize package logger
logger = setup_logging()
