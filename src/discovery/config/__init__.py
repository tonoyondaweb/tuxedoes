"""Snowflake discovery configuration system."""

from .schema import (
    DiscoveryConfig,
    OutputConfig,
    SchemaConfig,
    SNOWFLAKE_OBJECT_TYPES,
    TargetConfig,
    VariantSamplingConfig,
)
from .parser import load_config
from .validator import validate_config

__all__ = [
    # Schema models
    "DiscoveryConfig",
    "OutputConfig",
    "SchemaConfig",
    "TargetConfig",
    "VariantSamplingConfig",
    "SNOWFLAKE_OBJECT_TYPES",
    # Parser
    "load_config",
    # Validator
    "validate_config",
]
