"""Pytest fixtures for Snowflake discovery tests."""

import sys
from pathlib import Path
from typing import Any, Dict, List
import tempfile
import pytest
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from discovery.types import (
    TableMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    TagAssignment,
    MaskingPolicy,
    VariantSchema,
    ViewMetadata,
    ProcedureMetadata,
    StreamMetadata,
    TaskMetadata,
    DiscoveryManifest,
    DiscoveryError,
)
from discovery.config.schema import DiscoveryConfig, VariantSamplingConfig


@pytest.fixture
def sample_column_metadata() -> ColumnMetadata:
    """Sample column metadata."""
    return ColumnMetadata(
        name="id",
        data_type="INT",
        nullable=False,
        default_value=None,
        comment="Primary key"
    )


@pytest.fixture
def sample_constraint_metadata() -> ConstraintMetadata:
    """Sample constraint metadata."""
    return ConstraintMetadata(
        name="pk_users",
        type="PK",
        columns=["id"],
        referenced_table=None,
        referenced_columns=None
    )


@pytest.fixture
def sample_tag_assignment() -> TagAssignment:
    """Sample tag assignment."""
    return TagAssignment(
        tag_name="PII",
        tag_value="HIGH",
        column_name="email"
    )


@pytest.fixture
def sample_masking_policy() -> MaskingPolicy:
    """Sample masking policy."""
    return MaskingPolicy(
        policy_name="email_mask",
        signature="VARCHAR",
        column_name="email"
    )


@pytest.fixture
def sample_variant_schema() -> VariantSchema:
    """Sample variant schema."""
    return VariantSchema(
        column_name="metadata",
        inferred_structure={
            "user_id": "number",
            "preferences": {
                "notifications": "boolean",
                "theme": "string"
            }
        },
        sample_size=1000,
        confidence=0.95
    )


@pytest.fixture
def sample_table_metadata(
    sample_column_metadata: ColumnMetadata,
    sample_constraint_metadata: ConstraintMetadata,
    sample_tag_assignment: TagAssignment,
    sample_masking_policy: MaskingPolicy,
    sample_variant_schema: VariantSchema,
) -> TableMetadata:
    """Sample table metadata."""
    return TableMetadata(
        name="users",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE TABLE users (id INT NOT NULL, name VARCHAR(100));",
        columns=[
            sample_column_metadata,
            ColumnMetadata(
                name="name",
                data_type="VARCHAR(100)",
                nullable=True,
                default_value=None,
                comment=None
            ),
            ColumnMetadata(
                name="email",
                data_type="VARCHAR(255)",
                nullable=True,
                default_value=None,
                comment="User email"
            )
        ],
        row_count=1000,
        bytes=50000,
        last_ddl="2025-01-01 10:00:00",
        clustering_key="id",
        constraints=[sample_constraint_metadata],
        tags=[sample_tag_assignment],
        masking_policies=[sample_masking_policy],
        search_optimization=True,
        variant_schema=sample_variant_schema
    )


@pytest.fixture
def sample_view_metadata(sample_column_metadata: ColumnMetadata) -> ViewMetadata:
    """Sample view metadata."""
    return ViewMetadata(
        name="active_users",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE VIEW active_users AS SELECT * FROM users WHERE active = TRUE;",
        columns=[sample_column_metadata],
        base_tables=["users"],
        last_ddl="2025-01-02 10:00:00",
        tags=[]
    )


@pytest.fixture
def sample_procedure_metadata() -> ProcedureMetadata:
    """Sample procedure metadata."""
    return ProcedureMetadata(
        name="process_data",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE PROCEDURE process_data() RETURNS VARCHAR ...",
        parameters=[
            {"name": "input_data", "type": "VARCHAR"},
            {"name": "batch_size", "type": "INT"}
        ],
        return_type="VARCHAR",
        language="SQL",
        last_ddl="2025-01-03 10:00:00"
    )


@pytest.fixture
def sample_stream_metadata() -> StreamMetadata:
    """Sample stream metadata."""
    return StreamMetadata(
        name="users_stream",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE STREAM users_stream ON TABLE users;",
        source_object="ANALYTICS.PUBLIC.users",
        mode="INCREMENTAL",
        last_ddl="2025-01-04 10:00:00"
    )


@pytest.fixture
def sample_task_metadata() -> TaskMetadata:
    """Sample task metadata."""
    return TaskMetadata(
        name="daily_sync",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE TASK daily_sync ...",
        schedule="USING CRON 0 2 * * * America/New_York",
        state="STARTED",
        predecessors=["daily_data_load"],
        last_ddl="2025-01-05 10:00:00"
    )


@pytest.fixture
def sample_config_dict() -> Dict[str, Any]:
    """Sample config dictionary."""
    return {
        "targets": [
            {
                "database": "ANALYTICS",
                "schemas": [
                    {
                        "name": "PUBLIC",
                        "include_types": ["TABLE", "VIEW"],
                        "exclude_types": []
                    }
                ]
            }
        ],
        "variant_sampling": {
            "small_table_threshold": 1000,
            "small_table_sample_size": None,
            "medium_table_threshold": 100000,
            "medium_table_sample_size": 1000,
            "large_table_threshold": 1000000,
            "large_table_sample_size": 5000,
            "extra_large_sample_size": 10000,
            "min_confidence": 0.5
        },
        "output": {
            "base_path": "discovery",
            "sql_comments": True,
            "json_metadata": True
        }
    }


@pytest.fixture
def sample_config(sample_config_dict: Dict[str, Any]) -> DiscoveryConfig:
    """Sample DiscoveryConfig object."""
    return DiscoveryConfig.model_validate(sample_config_dict)


@pytest.fixture
def variant_sampling_config() -> VariantSamplingConfig:
    """Default variant sampling config."""
    return VariantSamplingConfig()


@pytest.fixture
def tmp_discovery_dir() -> Path:
    """Temporary directory for discovery output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_cursor():
    """Mock Snowflake cursor."""
    from unittest.mock import MagicMock

    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.execute.return_value = None
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """Mock Snowflake connection."""
    from unittest.mock import MagicMock

    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    return conn


@pytest.fixture
def sample_yaml_config() -> str:
    """Sample YAML config content."""
    return """
targets:
  - database: ANALYTICS
    schemas:
      - name: PUBLIC
        include_types:
          - TABLE
          - VIEW
        exclude_types: []

variant_sampling:
  small_table_threshold: 1000
  small_table_sample_size: null
  medium_table_threshold: 100000
  medium_table_sample_size: 1000
  large_table_threshold: 1000000
  large_table_sample_size: 5000
  extra_large_sample_size: 10000
  min_confidence: 0.5

output:
  base_path: discovery
  sql_comments: true
  json_metadata: true
"""


@pytest.fixture
def sample_ddl_content() -> str:
    """Sample DDL content."""
    return """CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.users (
  id INT NOT NULL,
  name VARCHAR(100),
  email VARCHAR(255),
  department_id INT
);"""


@pytest.fixture
def sample_json_metadata(sample_table_metadata: TableMetadata) -> Dict[str, Any]:
    """Sample JSON metadata."""
    return {
        "object_type": "TABLE",
        "name": "users",
        "schema": "PUBLIC",
        "database": "ANALYTICS",
        "ddl": "CREATE TABLE users (id INT, name VARCHAR);",
        "columns": [
            {
                "name": "id",
                "data_type": "INT",
                "nullable": False,
                "default_value": None,
                "comment": "Primary key"
            }
        ],
        "row_count": 1000,
        "bytes": 50000,
        "last_ddl": "2025-01-01",
        "clustering_key": "id",
        "constraints": [],
        "tags": [],
        "masking_policies": [],
        "search_optimization": False,
        "ddl_file": "discovery/ANALYTICS/PUBLIC/tables/users.sql"
    }
