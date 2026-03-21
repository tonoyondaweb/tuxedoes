"""Pytest fixtures and configuration for discovery tests."""

import pytest
from pathlib import Path


@pytest.fixture
def tmp_discovery_dir(tmp_path: Path) -> Path:
    """Provide temporary directory for discovery output.

    Args:
        tmp_path: Pytest tmp_path fixture

    Returns:
        Path to temporary discovery directory
    """
    return tmp_path / "discovery"


@pytest.fixture
def sample_table_metadata():
    """Sample TableMetadata for testing."""
    from discovery.types import TableMetadata, ColumnMetadata

    return TableMetadata(
        name="users",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE TABLE users (id INT, name VARCHAR);",
        columns=[
            ColumnMetadata(
                name="id",
                data_type="INT",
                nullable=False,
                default_value=None,
                comment="Primary key"
            ),
            ColumnMetadata(
                name="name",
                data_type="VARCHAR",
                nullable=True,
                default_value=None,
                comment="User name"
            )
        ],
        row_count=1000,
        bytes=50000,
        last_ddl="2025-01-01",
        clustering_key="id",
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=False,
        variant_schema=None
    )


@pytest.fixture
def sample_config():
    """Sample DiscoveryConfig for testing."""
    from discovery.config.schema import DiscoveryConfig, TargetConfig, SchemaConfig, OutputConfig, VariantSamplingConfig

    return DiscoveryConfig(
        targets=[
            TargetConfig(
                database="ANALYTICS",
                schemas=[
                    SchemaConfig(
                        name="PUBLIC",
                        include_types=["TABLE"],
                        exclude_types=[]
                    )
                ]
            )
        ],
        output=OutputConfig(
            base_path="discovery",
            sql_comments=True,
            json_metadata=True
        ),
        variant_sampling=VariantSamplingConfig(
            small_table_threshold=1000,
            small_table_sample_size=None,
            medium_table_threshold=100000,
            medium_table_sample_size=1000,
            large_table_threshold=1000000,
            large_table_sample_size=5000
        )
    )
