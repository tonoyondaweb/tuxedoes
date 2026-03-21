"""Custom validation logic for Snowflake discovery configuration."""

from pathlib import Path
from typing import Optional

from .schema import DiscoveryConfig, SNOWFLAKE_OBJECT_TYPES
from ..utils.errors import ConfigValidationError


def validate_config(config: DiscoveryConfig, config_path: Optional[str] = None) -> None:
    """
    Perform additional validation beyond Pydantic schema.

    Args:
        config: Parsed DiscoveryConfig object
        config_path: Optional path to config file (for error messages)

    Raises:
        ConfigValidationError: If validation fails
    """
    errors = []

    # Validate at least one target has schemas with object types to discover
    has_includes = any(
        any(schema.include_types for schema in target.schemas)
        for target in config.targets
    )

    if not has_includes:
        # If no explicit includes, that means "all types" - which is fine
        # But check that all schemas don't have empty exclude_lists
        pass

    # Validate output base path is valid directory name
    try:
        base_path = Path(config.output.base_path)
        if base_path.is_absolute():
            errors.append(
                f"output.base_path should be relative, not absolute: {config.output.base_path}"
            )
        # Check for invalid characters in path components
        for part in base_path.parts:
            invalid_chars = set("<>:\"|?*")
            if any(char in invalid_chars for char in part):
                errors.append(
                    f"output.base_path contains invalid characters: {part}"
                )
    except Exception as e:
        errors.append(f"Invalid output.base_path: {e}")

    # Validate variant sampling thresholds are logical
    sampling = config.variant_sampling
    if sampling.small_table_threshold >= sampling.medium_table_threshold:
        errors.append(
            "variant_sampling.small_table_threshold must be less than medium_table_threshold"
        )
    if sampling.medium_table_threshold >= sampling.large_table_threshold:
        errors.append(
            "variant_sampling.medium_table_threshold must be less than large_table_threshold"
        )

    # Raise if any validation errors
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        if config_path:
            error_msg = f"{error_msg}\n  File: {config_path}"
        raise ConfigValidationError(error_msg)


def validate_object_type(object_type: str) -> bool:
    """Check if an object type is a valid Snowflake type."""
    return object_type in SNOWFLAKE_OBJECT_TYPES
