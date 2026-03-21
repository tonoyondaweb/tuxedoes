"""Retry decorator and custom exceptions for discovery system."""

import functools
import logging
from typing import Callable, TypeVar, Optional, Any
import time

logger = logging.getLogger(__name__)

# Type variable for decorated functions
F = TypeVar('F')


class ExtractionError(Exception):
    """Base exception for extraction failures with object context."""
    def __init__(self, message: str, object_name: Optional[str] = None, object_type: Optional[str] = None):
        self.message = message
        self.object_name = object_name or "unknown"
        self.object_type = object_type or "unknown"
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.object_name:
            return f"{self.object_type} '{self.object_name}': {self.message}"
        return self.message


class ConfigValidationError(ExtractionError):
    """Exception for configuration validation failures."""
    pass


class ConnectionError(ExtractionError):
    """Exception for Snowflake connection failures."""
    pass


class PartialExtractionError(ExtractionError):
    """Exception raised when some objects succeed and some fail."""
    def __init__(self, message: str, success_count: int = 0, failure_count: int = 0):
        self.message = message
        self.success_count = success_count
        self.failure_count = failure_count
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} (successes: {self.success_count}, failures: {self.failure_count})"


def retry(max_attempts: int = 3, delay: float = 1, backoff: float = 2, exceptions: tuple = (Exception,)):
    """Decorator for retrying function calls on failure.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between attempts in seconds
        backoff: Multiplier for delay after each attempt
        exceptions: Tuple of exception types to catch

    Returns:
        Decorator function
    """
    def decorator(func: Callable[..., F]) -> Callable[..., F]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> F:
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.debug(f"Attempt {attempt}/{max_attempts} for {func.__name__}")
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt}/{max_attempts} failed: {type(e).__name__}: {e}")
                    if attempt < max_attempts:
                        # Calculate delay with backoff
                        wait_time = delay * (backoff ** (attempt - 1))
                        logger.info(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Max attempts ({max_attempts}) reached for {func.__name__}")
                        # Raise ExtractionError with context for orchestrator to handle
                        raise ExtractionError(
                            f"{func.__name__} failed after {max_attempts} attempts",
                            object_name=kwargs.get('object_name', 'unknown'),
                            object_type=kwargs.get('object_type', 'unknown')
                        ) from e
            # This should never be reached, but type checker needs it
            raise RuntimeError(f"Unexpected exit from retry wrapper for {func.__name__}")
        return wrapper  # type: ignore[return-value]
