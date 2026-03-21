"""VARIANT column schema interpreter for Snowflake JSON columns.

This module provides adaptive sampling-based schema inference for Snowflake
VARIANT columns containing semi-structured JSON data.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.discovery.config.schema import VariantSamplingConfig

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class VariantSchema:
    """Schema inferred from a VARIANT column.

    Attributes:
        structure: Nested dict representing JSON structure
        confidence: Overall confidence score (0.0-1.0)
        sample_count: Number of rows sampled for inference
        field_count: Total number of fields detected
        nullable: Whether the column contains NULL values
    """

    structure: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    sample_count: int = 0
    field_count: int = 0
    nullable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "structure": self.structure,
            "confidence": self.confidence,
            "sample_count": self.sample_count,
            "field_count": self.field_count,
            "nullable": self.nullable,
        }


def get_sample_size(
    row_count: int,
    config: VariantSamplingConfig,
) -> int:
    """Calculate adaptive sample size based on table row count.

    Sample sizes:
    - < small_table_threshold: sample all rows
    - small to medium: medium_table_sample_size (default 1000)
    - medium to large: large_table_sample_size (default 5000)
    - > large: extra_large_sample_size (default 10000)

    Args:
        row_count: Total row count of the table
        config: Variant sampling configuration

    Returns:
        Number of rows to sample

    Examples:
        >>> config = VariantSamplingConfig()
        >>> get_sample_size(500, config)
        500
        >>> get_sample_size(50000, config)
        1000
        >>> get_sample_size(500000, config)
        5000
        >>> get_sample_size(5000000, config)
        10000
    """
    if row_count < config.small_table_threshold:
        # Sample all rows for small tables
        return row_count
    elif row_count < config.medium_table_threshold:
        return config.medium_table_sample_size
    elif row_count < config.large_table_threshold:
        return config.large_table_sample_size
    else:
        return config.extra_large_sample_size


def infer_type(value: Any) -> str:
    """Infer Snowflake type from Python value.

    Args:
        value: Python value to infer type from

    Returns:
        String representing inferred type
    """
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "number"
    elif isinstance(value, float):
        return "number"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return "unknown"


def merge_schemas(
    existing_schema: Dict[str, Any],
    new_value: Any,
    field_counts: Dict[str, int],
) -> None:
    """Recursively merge a new value into existing schema.

    This function modifies existing_schema and field_counts in place.
    Schema structure: dict where values are either type strings or nested dicts.

    Args:
        existing_schema: Current schema structure (modified in place)
        new_value: New value to merge into schema
        field_counts: Dictionary tracking field occurrence counts (modified in place)
    """
    inferred_type = infer_type(new_value)

    if inferred_type == "object":
        # For objects, merge all keys recursively
        for key, value in new_value.items():
            # Increment field count for this key
            field_counts[key] = field_counts.get(key, 0) + 1

            # Initialize nested structure if needed
            if key not in existing_schema:
                existing_schema[key] = {}

            # Recursively merge nested structure
            merge_schemas(existing_schema[key], value, field_counts)

    elif inferred_type == "array":
        # For arrays, sample first element to infer item type
        # Note: existing_schema for arrays stores the item schema
        if len(new_value) > 0:
            first_item = new_value[0]
            merge_schemas(existing_schema, first_item, field_counts)

    else:
        # For primitive types, check for type conflicts
        # existing_schema can contain: dict (object), str (primitive), or mixed marker
        if existing_schema and isinstance(existing_schema, dict):
            if len(existing_schema) == 0:
                # Empty dict, store primitive type directly as string
                existing_schema.clear()
                existing_schema[""] = inferred_type
            elif "" in existing_schema:
                # Existing is also a primitive (stored as {"": "type"})
                if existing_schema[""] != inferred_type:
                    existing_schema[""] = "mixed"
            else:
                # Existing is an object, new is primitive - conflict
                existing_schema.clear()
                existing_schema[""] = "mixed"
        else:
            # First time or non-dict, store as {"": type}
            existing_schema.clear()
            existing_schema[""] = inferred_type


def normalize_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Convert internal schema format to clean output format.

    Internal format uses {"": type} for primitives, output format uses type strings.

    Args:
        schema: Schema in internal format

    Returns:
        Normalized schema with primitive types as strings
    """
    normalized: Dict[str, Any] = {}
    for key, value in schema.items():
        if isinstance(value, dict):
            if "" in value:
                # Primitive type stored as {"": "type"}
                normalized[key] = value[""]
            else:
                # Nested object, recursively normalize
                normalized[key] = normalize_schema(value)
        else:
            # Already normalized (shouldn't happen but handle it)
            normalized[key] = value
    return normalized


def infer_schema(
    samples: List[Optional[Dict[str, Any]]],
    config: VariantSamplingConfig,
) -> VariantSchema:
    """Infer schema from sampled JSON objects.

    Args:
        samples: List of parsed JSON objects (may contain None values)
        config: Variant sampling configuration

    Returns:
        VariantSchema with inferred structure and confidence

    Examples:
        >>> config = VariantSamplingConfig()
        >>> samples = [
        ...     {"a": 1, "b": {"c": "x"}},
        ...     {"a": 2, "b": {"c": "y", "d": True}}
        ... ]
        >>> schema = infer_schema(samples, config)
        >>> schema.structure
        {'a': 'number', 'b': {'c': 'string', 'd': 'boolean'}}
    """
    structure: Dict[str, Any] = {}
    field_counts: Dict[str, int] = {}
    nullable = False
    valid_samples = 0

    for sample in samples:
        if sample is None:
            nullable = True
            continue

        if not isinstance(sample, dict):
            logger.warning(f"Expected dict, got {type(sample)}: {sample}")
            continue

        valid_samples += 1
        merge_schemas(structure, sample, field_counts)

    # Calculate confidence scores
    if valid_samples == 0:
        return VariantSchema(
            structure={},
            confidence=0.0,
            sample_count=len(samples),
            field_count=0,
            nullable=nullable,
        )

    filtered_structure: Dict[str, Any] = {}
    field_confidences: Dict[str, float] = {}

    for key, value in structure.items():
        field_count = field_counts.get(key, 0)
        field_confidence = field_count / valid_samples

        if field_confidence >= config.min_confidence:
            field_confidences[key] = field_confidence
            filtered_structure[key] = value

    # Normalize schema: convert {"": type} to just type strings
    normalized_structure = normalize_schema(filtered_structure)

    # Calculate overall confidence (average of field confidences)
    if normalized_structure:
        overall_confidence = sum(field_confidences.values()) / len(field_confidences)
    else:
        overall_confidence = 0.0

    return VariantSchema(
        structure=normalized_structure,
        confidence=round(overall_confidence, 3),
        sample_count=len(samples),
        field_count=len(normalized_structure),
        nullable=nullable,
    )


def interpret_variant_column(
    conn: Any,
    db: str,
    schema: str,
    table: str,
    column: str,
    row_count: int,
    config: VariantSamplingConfig,
) -> VariantSchema:
    """Interpret VARIANT column schema using adaptive sampling.

    Args:
        conn: Snowflake connection object (has execute() method)
        db: Database name
        schema: Schema name
        table: Table name
        column: Column name
        row_count: Total row count of the table
        config: Variant sampling configuration

    Returns:
        VariantSchema with inferred structure

    Examples:
        >>> config = VariantSamplingConfig()
        >>> conn = ...  # Snowflake connection
        >>> schema = interpret_variant_column(
        ...     conn, "MY_DB", "PUBLIC", "MY_TABLE", "MY_VARIANT_COL", 5000, config
        ... )
        >>> print(schema.structure)
    """
    # Calculate adaptive sample size
    sample_size = get_sample_size(row_count, config)
    logger.info(
        f"Sampling {sample_size} rows from {db}.{schema}.{table}.{column} "
        f"(total rows: {row_count})"
    )

    # Build sampling query
    if sample_size == row_count:
        # Sample all rows - no sampling clause needed
        query = f"SELECT {column} FROM {db}.{schema}.{table}"
    else:
        # Use Snowflake SAMPLE clause
        query = f"SELECT {column} FROM {db}.{schema}.{table} SAMPLE ({sample_size})"

    try:
        # Execute query
        cursor = conn.cursor()
        cursor.execute(query)

        # Parse results
        samples: List[Optional[Dict[str, Any]]] = []
        for row in cursor:
            value = row[0]

            # Handle NULL values
            if value is None:
                samples.append(None)
                continue

            # Parse JSON
            try:
                # Snowflake VARIANT columns return strings that need parsing
                if isinstance(value, str):
                    parsed = json.loads(value)
                    samples.append(parsed)
                else:
                    # Already parsed (unlikely but handle it)
                    samples.append(value)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Failed to parse JSON in {db}.{schema}.{table}.{column}: {e}"
                )
                continue

        cursor.close()

        # Infer schema from samples
        variant_schema = infer_schema(samples, config)

        logger.info(
            f"Inferred schema for {db}.{schema}.{table}.{column}: "
            f"{variant_schema.field_count} fields, "
            f"confidence={variant_schema.confidence}, "
            f"nullable={variant_schema.nullable}"
        )

        return variant_schema

    except Exception as e:
        logger.error(
            f"Error interpreting VARIANT column {db}.{schema}.{table}.{column}: {e}"
        )
        raise
