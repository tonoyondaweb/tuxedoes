"""Tests for YAML config parsing and validation."""

import pytest
import yaml
from pathlib import Path

from discovery.config.parser import load_config
from discovery.config.schema import (
    DiscoveryConfig,
    TargetConfig,
    SchemaConfig,
    VariantSamplingConfig,
    OutputConfig,
    SNOWFLAKE_OBJECT_TYPES,
)
from discovery.utils.errors import ConfigValidationError


def test_load_valid_config(sample_config, sample_config_dict, tmp_discovery_dir):
    """Test loading a valid YAML config."""
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(sample_config_dict, f)

    config = load_config(str(config_path))

    assert isinstance(config, DiscoveryConfig)
    assert len(config.targets) == 1
    assert config.targets[0].database == "ANALYTICS"
    assert len(config.targets[0].schemas) == 1
    assert config.targets[0].schemas[0].name == "PUBLIC"


def test_load_config_with_yaml_content(sample_yaml_config, tmp_discovery_dir):
    """Test loading config from YAML string."""
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        f.write(sample_yaml_config)

    config = load_config(str(config_path))

    assert config.targets[0].database == "ANALYTICS"
    assert config.variant_sampling.small_table_threshold == 1000
    assert config.output.base_path == "discovery"


def test_load_config_file_not_found():
    """Test loading a non-existent config file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Configuration file not found"):
        load_config("nonexistent.yml")


def test_load_config_empty_file(tmp_discovery_dir):
    """Test loading an empty config file raises ConfigValidationError."""
    config_path = tmp_discovery_dir / "empty.yml"

    with open(config_path, "w") as f:
        f.write("")

    with pytest.raises(ConfigValidationError, match="Configuration file is empty"):
        load_config(str(config_path))


def test_load_config_invalid_yaml(tmp_discovery_dir):
    """Test loading invalid YAML raises ConfigValidationError."""
    config_path = tmp_discovery_dir / "invalid.yml"

    with open(config_path, "w") as f:
        f.write("invalid: yaml: content: :")

    with pytest.raises(ConfigValidationError, match="Invalid YAML"):
        load_config(str(config_path))


def test_invalid_config_unknown_object_type(tmp_discovery_dir):
    """Test that unknown object type raises ConfigValidationError."""
    config_dict = {
        "targets": [
            {
                "database": "ANALYTICS",
                "schemas": [
                    {
                        "name": "PUBLIC",
                        "include_types": ["UNKNOWN_TYPE"]
                    }
                ]
            }
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ConfigValidationError, match="Invalid Snowflake object types"):
        load_config(str(config_path))


def test_invalid_config_empty_targets(tmp_discovery_dir):
    """Test that empty targets list raises validation error."""
    config_dict = {
        "targets": []
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(Exception):
        load_config(str(config_path))


def test_invalid_config_duplicate_database(tmp_discovery_dir):
    """Test that duplicate database names raise validation error."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]},
            {"database": "ANALYTICS", "schemas": [{"name": "STAGING"}]}
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ValueError, match="Duplicate database names"):
        load_config(str(config_path))


def test_invalid_config_duplicate_schema(tmp_discovery_dir):
    """Test that duplicate schema names raise validation error."""
    config_dict = {
        "targets": [
            {
                "database": "ANALYTICS",
                "schemas": [
                    {"name": "PUBLIC"},
                    {"name": "PUBLIC"}
                ]
            }
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ValueError, match="Duplicate schema names"):
        load_config(str(config_path))


def test_invalid_config_type_conflict(tmp_discovery_dir):
    """Test that types cannot be both included and excluded."""
    config_dict = {
        "targets": [
            {
                "database": "ANALYTICS",
                "schemas": [
                    {
                        "name": "PUBLIC",
                        "include_types": ["TABLE"],
                        "exclude_types": ["TABLE"]
                    }
                ]
            }
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ValueError, match="Types cannot be both included and excluded"):
        load_config(str(config_path))


def test_invalid_config_thresholds_not_increasing(tmp_discovery_dir):
    """Test that thresholds must be in increasing order."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]}
        ],
        "variant_sampling": {
            "small_table_threshold": 1000000,  # Too large
            "medium_table_threshold": 100000,
            "large_table_threshold": 1000000,
            "extra_large_sample_size": 10000
        }
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ValueError, match="Thresholds must be in increasing order"):
        load_config(str(config_path))


def test_invalid_config_absolute_path(tmp_discovery_dir):
    """Test that output.base_path should be relative."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]}
        ],
        "output": {
            "base_path": "/absolute/path"
        }
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ConfigValidationError, match="should be relative, not absolute"):
        load_config(str(config_path))


def test_invalid_config_invalid_path_characters(tmp_discovery_dir):
    """Test that output.base_path cannot contain invalid characters."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]}
        ],
        "output": {
            "base_path": "invalid<path"
        }
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ConfigValidationError, match="contains invalid characters"):
        load_config(str(config_path))


def test_config_validation_with_invalid_thresholds(tmp_discovery_dir):
    """Test validator catches threshold ordering issues."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]}
        ],
        "variant_sampling": {
            "small_table_threshold": 10000,
            "medium_table_threshold": 1000,  # Less than small
            "large_table_threshold": 1000000
        }
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    with pytest.raises(ConfigValidationError,
                      match="small_table_threshold must be less than medium_table_threshold"):
        load_config(str(config_path))


def test_config_get_config_hash(sample_config):
    """Test config hash generation."""
    hash1 = sample_config.get_config_hash()
    hash2 = sample_config.get_config_hash()

    assert hash1 == hash2
    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256 hex digest


def test_config_get_config_hash_different_configs(tmp_discovery_dir):
    """Test that different configs produce different hashes."""
    config_dict1 = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]}
        ]
    }

    config_dict2 = {
        "targets": [
            {"database": "WAREHOUSE", "schemas": [{"name": "PUBLIC"}]}
        ]
    }

    config_path1 = tmp_discovery_dir / "config1.yml"
    config_path2 = tmp_discovery_dir / "config2.yml"

    with open(config_path1, "w") as f:
        yaml.dump(config_dict1, f)
    with open(config_path2, "w") as f:
        yaml.dump(config_dict2, f)

    config1 = load_config(str(config_path1))
    config2 = load_config(str(config_path2))

    hash1 = config1.get_config_hash()
    hash2 = config2.get_config_hash()

    assert hash1 != hash2


def test_valid_snowflake_object_types():
    """Test that all valid object types are defined."""
    expected_types = {
        "TABLE",
        "VIEW",
        "PROCEDURE",
        "FUNCTION",
        "STREAM",
        "TASK",
        "DYNAMIC_TABLE",
        "STAGE",
        "PIPE",
        "SEQUENCE",
        "EXTERNAL_TABLE",
    }
    assert SNOWFLAKE_OBJECT_TYPES == expected_types


def test_schema_config_defaults():
    """Test SchemaConfig default values."""
    schema = SchemaConfig(name="TEST_SCHEMA")

    assert schema.name == "TEST_SCHEMA"
    assert schema.include_types == []
    assert schema.exclude_types == []


def test_target_config_requires_schemas():
    """Test that TargetConfig requires at least one schema."""
    with pytest.raises(Exception):
        TargetConfig(database="TEST", schemas=[])


def test_variant_sampling_config_defaults():
    """Test VariantSamplingConfig default values."""
    sampling = VariantSamplingConfig()

    assert sampling.small_table_threshold == 1000
    assert sampling.small_table_sample_size is None
    assert sampling.medium_table_threshold == 100000
    assert sampling.medium_table_sample_size == 1000
    assert sampling.large_table_threshold == 1000000
    assert sampling.large_table_sample_size == 5000
    assert sampling.extra_large_sample_size == 10000
    assert sampling.min_confidence == 0.5


def test_output_config_defaults():
    """Test OutputConfig default values."""
    output = OutputConfig()

    assert output.base_path == "discovery"
    assert output.sql_comments is True
    assert output.json_metadata is True


def test_load_config_without_validation(tmp_discovery_dir):
    """Test loading config with validation disabled."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]}
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    config = load_config(str(config_path), validate=False)

    assert isinstance(config, DiscoveryConfig)
    assert config.targets[0].database == "ANALYTICS"


def test_config_with_multiple_schemas(tmp_discovery_dir):
    """Test config with multiple schemas per database."""
    config_dict = {
        "targets": [
            {
                "database": "ANALYTICS",
                "schemas": [
                    {"name": "PUBLIC", "include_types": ["TABLE"]},
                    {"name": "STAGING", "include_types": ["VIEW"]},
                    {"name": "ARCHIVE", "exclude_types": ["FUNCTION"]}
                ]
            }
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    config = load_config(str(config_path))

    assert len(config.targets[0].schemas) == 3
    assert config.targets[0].schemas[0].name == "PUBLIC"
    assert config.targets[0].schemas[0].include_types == ["TABLE"]
    assert config.targets[0].schemas[1].name == "STAGING"
    assert config.targets[0].schemas[2].name == "ARCHIVE"


def test_config_with_multiple_targets(tmp_discovery_dir):
    """Test config with multiple database targets."""
    config_dict = {
        "targets": [
            {"database": "ANALYTICS", "schemas": [{"name": "PUBLIC"}]},
            {"database": "WAREHOUSE", "schemas": [{"name": "PUBLIC"}]},
            {"database": "ARCHIVE", "schemas": [{"name": "STAGING"}]}
        ]
    }
    config_path = tmp_discovery_dir / "config.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)

    config = load_config(str(config_path))

    assert len(config.targets) == 3
    assert config.targets[0].database == "ANALYTICS"
    assert config.targets[1].database == "WAREHOUSE"
    assert config.targets[2].database == "ARCHIVE"
