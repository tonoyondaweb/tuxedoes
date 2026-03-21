"""YAML configuration parser for Snowflake discovery."""

from pathlib import Path
from typing import Optional

import yaml
from pydantic import ValidationError

from .schema import DiscoveryConfig
from .validator import validate_config
from ..utils.errors import ConfigValidationError


def load_config(path: str, validate: bool = True) -> DiscoveryConfig:
    """
    Load and parse YAML configuration file.

    Args:
        path: Path to YAML configuration file
        validate: Whether to run additional validation beyond Pydantic

    Returns:
        DiscoveryConfig: Parsed configuration object

    Raises:
        FileNotFoundError: If config file does not exist
        ConfigValidationError: If config is invalid (YAML or schema)
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    if not config_path.is_file():
        raise ConfigValidationError(f"Configuration path is not a file: {path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigValidationError(
            f"Invalid YAML in configuration file: {path}\n{e}"
        ) from e

    if raw_config is None:
        raise ConfigValidationError(f"Configuration file is empty: {path}")

    try:
        config = DiscoveryConfig.model_validate(raw_config)
    except ValidationError as e:
        raise ConfigValidationError(
            f"Configuration validation failed for {path}\n"
            f"{format_validation_error(e)}"
        ) from e

    if validate:
        validate_config(config, path)

    return config


def format_validation_error(error: ValidationError) -> str:
    """Format Pydantic ValidationError for human readability."""
    messages = []
    for err in error.errors():
        loc = " -> ".join(str(p) for p in err["loc"])
        messages.append(f"  {loc}: {err['msg']}")
    return "\n".join(messages)
