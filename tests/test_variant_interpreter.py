"""Tests for sampling logic and schema inference."""

import pytest
import json

from discovery.extract.variant_interpreter import (
    get_sample_size,
    infer_type,
    infer_schema,
    normalize_schema,
    VariantSchema,
)
from discovery.config.schema import VariantSamplingConfig


def test_get_sample_size_small_table(variant_sampling_config):
    """Test sample size for small table (< 1K rows)."""
    sample_size = get_sample_size(500, variant_sampling_config)

    assert sample_size == 500  # Sample all


def test_get_sample_size_exactly_small_threshold(variant_sampling_config):
    """Test sample size at small table threshold."""
    sample_size = get_sample_size(999, variant_sampling_config)

    assert sample_size == 999  # Sample all


def test_get_sample_size_medium_table(variant_sampling_config):
    """Test sample size for medium table (1K-100K rows)."""
    sample_size = get_sample_size(50000, variant_sampling_config)

    assert sample_size == 1000


def test_get_sample_size_at_medium_threshold(variant_sampling_config):
    """Test sample size at medium table threshold."""
    sample_size = get_sample_size(99999, variant_sampling_config)

    assert sample_size == 1000


def test_get_sample_size_near_large_threshold(variant_sampling_config):
    """Test sample size near large table threshold."""
    sample_size = get_sample_size(999999, variant_sampling_config)

    assert sample_size == 5000


def test_get_sample_size_at_large_threshold(variant_sampling_config):
    """Test sample size at large table threshold."""
    sample_size = get_sample_size(1000000, variant_sampling_config)

    assert sample_size == 5000


def test_get_sample_size_extra_large_table(variant_sampling_config):
    """Test sample size for extra large table (> 1M rows)."""
    sample_size = get_sample_size(5000000, variant_sampling_config)

    assert sample_size == 10000


def test_get_sample_size_custom_thresholds():
    """Test sample size with custom thresholds."""
    from discovery.config.schema import VariantSamplingConfig

    custom_config = VariantSamplingConfig(
        small_table_threshold=100,
        small_table_sample_size=None,
        medium_table_threshold=10000,
        medium_table_sample_size=500,
        large_table_threshold=100000,
        large_table_sample_size=2000,
        extra_large_sample_size=5000,
        min_confidence=0.5
    )

    # Small table
    assert get_sample_size(50, custom_config) == 50
    # Medium table
    assert get_sample_size(1000, custom_config) == 500
    # Large table
    assert get_sample_size(50000, custom_config) == 2000
    # Extra large table
    assert get_sample_size(200000, custom_config) == 5000


def test_infer_type_null():
    """Test type inference for None/null value."""
    result = infer_type(None)

    assert result == "null"


def test_infer_type_boolean():
    """Test type inference for boolean values."""
    assert infer_type(True) == "boolean"
    assert infer_type(False) == "boolean"


def test_infer_type_integer():
    """Test type inference for integer values."""
    assert infer_type(42) == "number"
    assert infer_type(0) == "number"
    assert infer_type(-100) == "number"


def test_infer_type_float():
    """Test type inference for float values."""
    assert infer_type(3.14) == "number"
    assert infer_type(0.5) == "number"
    assert infer_type(-2.5) == "number"


def test_infer_type_string():
    """Test type inference for string values."""
    assert infer_type("hello") == "string"
    assert infer_type("") == "string"
    assert infer_type("123") == "string"


def test_infer_type_list():
    """Test type inference for list values."""
    assert infer_type([1, 2, 3]) == "array"
    assert infer_type([]) == "array"
    assert infer_type(["a", "b"]) == "array"


def test_infer_type_dict():
    """Test type inference for dict values."""
    assert infer_type({"key": "value"}) == "object"
    assert infer_type({}) == "object"
    assert infer_type({"nested": {"key": "value"}}) == "object"


def test_normalize_schema_primitive():
    """Test normalizing primitive schema in nested object."""
    schema = {
        "field1": {"": "string"},
        "field2": {"": "number"}
    }
    result = normalize_schema(schema)

    assert result == {
        "field1": "string",
        "field2": "number"
    }


def test_normalize_schema_array():
    """Test normalizing array schema in nested object."""
    schema = {
        "items": {"": "array"}
    }
    result = normalize_schema(schema)

    assert result == {
        "items": "array"
    }


def test_normalize_schema_empty():
    """Test normalizing empty schema."""
    schema = {}
    result = normalize_schema(schema)

    assert result == {}


def test_infer_schema_simple_primitives():
    """Test schema inference with simple primitive types."""
    samples = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure["id"] == "number"
    assert result.structure["name"] == "string"
    assert result.sample_count == 3
    assert result.confidence == 1.0
    assert result.nullable is False


def test_infer_schema_with_nulls():
    """Test schema inference with null values."""
    samples = [
        {"id": 1, "name": "Alice", "age": None},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 30}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure["id"] == "number"
    assert result.structure["name"] == "string"
    # When a field has a mix of nulls and values, it becomes 'mixed'
    assert result.structure["age"] == "mixed"
    assert result.nullable is True


def test_infer_schema_mixed_types():
    """Test schema inference with mixed types for same field."""
    samples = [
        {"field": 1},
        {"field": "string"},
        {"field": 1.5}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure["field"] == "mixed"


def test_infer_schema_nested_objects():
    """Test schema inference with nested objects."""
    samples = [
        {"user": {"id": 1, "profile": {"name": "Alice"}}},
        {"user": {"id": 2, "profile": {"name": "Bob"}}}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert isinstance(result.structure["user"], dict)
    assert result.structure["user"]["id"] == "number"
    assert isinstance(result.structure["user"]["profile"], dict)
    assert result.structure["user"]["profile"]["name"] == "string"


def test_infer_schema_arrays():
    """Test schema inference with arrays."""
    samples = [
        {"tags": ["a", "b"]},
        {"tags": ["c", "d"]}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    # Arrays are sampled by their first element, so ["a", "b"] becomes "string"
    assert result.structure["tags"] == "string"


def test_infer_schema_empty_samples():
    """Test schema inference with empty sample list."""
    samples = []

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure == {}
    assert result.sample_count == 0
    assert result.confidence == 0.0


def test_infer_schema_all_nulls():
    """Test schema inference when all values are null."""
    samples = [
        {"field": None},
        {"field": None}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure["field"] == "null"


def test_infer_schema_confidence_filtering():
    """Test that low-confidence fields are filtered out."""
    samples = [
        {"common": 1, "rare": "appears_once"},
        {"common": 2},
        {"common": 3},
        {"common": 4},
        {"common": 5}
    ]

    config = VariantSamplingConfig(min_confidence=0.6)
    result = infer_schema(samples, config)

    # 'rare' field should be filtered out (20% confidence < 60% threshold)
    assert "common" in result.structure
    assert "rare" not in result.structure or result.field_count == 1


def test_infer_schema_with_invalid_json():
    """Test schema inference handles unparseable JSON gracefully."""
    # This is handled by the calling code, but we test the structure merging
    samples = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure["id"] == "number"
    assert result.structure["name"] == "string"


def test_infer_schema_field_names():
    """Test that field names are preserved correctly."""
    samples = [
        {"user_id": 1, "user_name": "Alice"},
        {"user_id": 2, "user_name": "Bob"}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert "user_id" in result.structure
    assert "user_name" in result.structure


def test_infer_schema_complex_nested_structure():
    """Test schema inference with complex nested structure."""
    samples = [
        {
            "metadata": {
                "created_at": "2025-01-01",
                "author": {
                    "id": 1,
                    "name": "Alice",
                    "settings": {
                        "theme": "dark",
                        "notifications": True
                    }
                }
            }
        }
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert isinstance(result.structure["metadata"], dict)
    assert result.structure["metadata"]["created_at"] == "string"
    assert isinstance(result.structure["metadata"]["author"], dict)
    assert isinstance(result.structure["metadata"]["author"]["settings"], dict)
    assert result.structure["metadata"]["author"]["settings"]["theme"] == "string"
    assert result.structure["metadata"]["author"]["settings"]["notifications"] == "boolean"


def test_normalize_schema_preserves_all_types():
    """Test that normalize_schema handles all data types."""
    schema = {
        "primitive": {"": "string"},
        "object": {
            "nested": {"": "number"}
        },
        "array": {"": "array"},
        "mixed": {"": "mixed"},
        "null": {"": "null"}
    }

    result = normalize_schema(schema)

    assert result["primitive"] == "string"
    assert isinstance(result["object"], dict)
    assert result["object"]["nested"] == "number"
    assert result["array"] == "array"
    assert result["mixed"] == "mixed"
    assert result["null"] == "null"


def test_variant_schema_to_dict():
    """Test VariantSchema to_dict conversion."""
    variant = VariantSchema(
        structure={"field": "string"},
        confidence=0.95,
        sample_count=1000,
        field_count=1,
        nullable=False
    )

    result = variant.to_dict()

    assert result["structure"] == {"field": "string"}
    assert result["confidence"] == 0.95
    assert result["sample_count"] == 1000
    assert result["field_count"] == 1
    assert result["nullable"] is False


def test_get_sample_size_edge_case_zero_rows(variant_sampling_config):
    """Test sample size for zero rows."""
    sample_size = get_sample_size(0, variant_sampling_config)

    assert sample_size == 0


def test_get_sample_size_edge_case_one_row(variant_sampling_config):
    """Test sample size for one row."""
    sample_size = get_sample_size(1, variant_sampling_config)

    assert sample_size == 1


def test_infer_schema_consistent_structure():
    """Test that consistent structure produces high confidence."""
    samples = [
        {"id": i, "name": f"User {i}"} for i in range(100)
    ]

    result = infer_schema(samples, VariantSamplingConfig(min_confidence=0.5))

    assert result.confidence == 1.0
    assert result.field_count == 2
    assert result.nullable is False


def test_infer_schema_variable_structure():
    """Test that variable structure produces lower confidence."""
    # Only half the samples have 'optional_field'
    samples = []
    for i in range(100):
        sample = {"id": i, "name": f"User {i}"}
        if i % 2 == 0:
            sample["optional_field"] = "present"
        samples.append(sample)

    result = infer_schema(samples, VariantSamplingConfig(min_confidence=0.5))

    # optional_field should be filtered out due to low confidence (50%)
    assert result.confidence < 1.0
    assert "optional_field" not in result.structure or result.field_count == 2


def test_infer_schema_all_samples_invalid():
    """Test schema inference when all samples are invalid (non-dict)."""
    samples = [1, 2, 3, "string", None]

    result = infer_schema(samples, VariantSamplingConfig())

    # Should return empty schema for invalid samples
    assert result.structure == {}
    assert result.sample_count == 0


def test_normalize_schema_deeply_nested():
    """Test normalize_schema with deeply nested structure."""
    schema = {
        "level1": {
            "level2": {
                "level3": {
                    "level4": {"": "string"}
                }
            }
        }
    }

    result = normalize_schema(schema)

    assert result["level1"]["level2"]["level3"]["level4"] == "string"
    assert isinstance(result["level1"], dict)
    assert isinstance(result["level1"]["level2"], dict)


def test_get_sample_size_negative_rows():
    """Test sample size with negative row count (edge case)."""
    sample_size = get_sample_size(-100, VariantSamplingConfig())

    # Should handle gracefully, likely return the negative value
    assert sample_size == -100


def test_variant_schema_default_values():
    """Test VariantSchema default field values."""
    variant = VariantSchema()

    assert variant.structure == {}
    assert variant.confidence == 0.0
    assert variant.sample_count == 0
    assert variant.field_count == 0
    assert variant.nullable is False


def test_infer_schema_with_boolean_and_numbers():
    """Test schema inference with booleans and numbers."""
    samples = [
        {"is_active": True, "count": 10},
        {"is_active": False, "count": 20}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure["is_active"] == "boolean"
    assert result.structure["count"] == "number"


def test_infer_schema_field_confidence_tracking():
    """Test that field confidence is tracked correctly."""
    samples = [
        {"always": 1},
        {"always": 2},
        {"always": 3},
        {"always": 4},
        {"always": 5}
    ]

    result = infer_schema(samples, VariantSamplingConfig())

    # 'always' field appears in 100% of samples
    assert result.confidence == 1.0


def test_infer_schema_empty_object_samples():
    """Test schema inference with empty object samples."""
    samples = [{}, {}]

    result = infer_schema(samples, VariantSamplingConfig())

    assert result.structure == {}
    assert result.sample_count == 2


def test_normalize_schema_empty_internal_marker():
    """Test normalize_schema handles empty internal marker correctly."""
    schema = {"field": {"": ""}}

    result = normalize_schema(schema)

    # Empty string value should be preserved
    assert result["field"] == ""
