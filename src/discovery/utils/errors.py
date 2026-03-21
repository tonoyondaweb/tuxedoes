"""Custom exceptions for Snowflake discovery."""

from typing import Optional


class DiscoveryError(Exception):
    """Base exception for all discovery errors."""

    def __init__(self, message: str, **context):
        super().__init__(message)
        self.context = context

    def __str__(self) -> str:
        if self.context:
            context_str = " ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{super().__str__()} ({context_str})"
        return super().__str__()


class ConfigValidationError(DiscoveryError):
    """Raised when configuration validation fails."""

    pass


class ConnectionError(DiscoveryError):
    """Raised when Snowflake connection fails."""

    pass


class ExtractionError(DiscoveryError):
    """Raised when object extraction fails."""

    def __init__(self, message: str, object_name: Optional[str] = None, object_type: Optional[str] = None, **context):
        super().__init__(message, object_name=object_name, object_type=object_type, **context)
        self.object_name = object_name
        self.object_type = object_type


class PartialExtractionError(DiscoveryError):
    """Raised when some objects succeed but some fail during extraction."""

    def __init__(self, message: str, extracted_count: int = 0, failed_count: int = 0, **context):
        super().__init__(message, extracted_count=extracted_count, failed_count=failed_count, **context)
        self.extracted_count = extracted_count
        self.failed_count = failed_count
