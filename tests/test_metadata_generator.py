"""Tests for JSON metadata generation."""

import pytest
import json

from discovery.generate.metadata_generator import (
    generate_metadata_json,
    _build_ddl_file_path,
    _serialize_column_metadata,
    _serialize_constraint_metadata,
    _serialize_tag_assignment,
    _serialize_masking_policy,
    _serialize_variant_schema,
)
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
)


def test_build_ddl_file_path_table():
    """Test DDL file path building for TABLE."""
    path = _build_ddl_file_path("ANALYTICS", "PUBLIC", "users", "TABLE")

    assert path == "ANALYTICS/PUBLIC/tables/users.sql"


def test_build_ddl_file_path_view():
    """Test DDL file path building for VIEW."""
    path = _build_ddl_file_path("ANALYTICS", "PUBLIC", "active_users", "VIEW")

    assert path == "ANALYTICS/PUBLIC/views/active_users.sql"


def test_build_ddl_file_path_procedure():
    """Test DDL file path building for PROCEDURE."""
    path = _build_ddl_file_path("ANALYTICS", "PUBLIC", "process_data", "PROCEDURE")

    assert path == "ANALYTICS/PUBLIC/procedures/process_data.sql"


def test_build_ddl_file_path_unknown_type():
    """Test DDL file path building for unknown object type."""
    path = _build_ddl_file_path("ANALYTICS", "PUBLIC", "my_obj", "UNKNOWN")

    assert path == "ANALYTICS/PUBLIC/unknowns/my_obj.sql"


def test_serialize_column_metadata():
    """Test column metadata serialization."""
    column = ColumnMetadata(
        name="email",
        data_type="VARCHAR(255)",
        nullable=True,
        default_value="'user@example.com'",
        comment="User email address"
    )

    result = _serialize_column_metadata(column)

    assert result["name"] == "email"
    assert result["data_type"] == "VARCHAR(255)"
    assert result["nullable"] is True
    assert result["default_value"] == "'user@example.com'"
    assert result["comment"] == "User email address"


def test_serialize_column_metadata_with_nulls():
    """Test column metadata serialization with null values."""
    column = ColumnMetadata(
        name="id",
        data_type="INT",
        nullable=False,
        default_value=None,
        comment=None
    )

    result = _serialize_column_metadata(column)

    assert result["name"] == "id"
    assert result["data_type"] == "INT"
    assert result["nullable"] is False
    assert result["default_value"] is None
    assert result["comment"] is None


def test_serialize_constraint_metadata():
    """Test constraint metadata serialization."""
    constraint = ConstraintMetadata(
        name="pk_users",
        type="PK",
        columns=["id"],
        referenced_table=None,
        referenced_columns=None
    )

    result = _serialize_constraint_metadata(constraint)

    assert result["name"] == "pk_users"
    assert result["type"] == "PK"
    assert result["columns"] == ["id"]
    assert result["referenced_table"] is None
    assert result["referenced_columns"] is None


def test_serialize_constraint_metadata_with_foreign_key():
    """Test foreign key constraint serialization."""
    constraint = ConstraintMetadata(
        name="fk_users_department",
        type="FK",
        columns=["department_id"],
        referenced_table="departments",
        referenced_columns=["id"]
    )

    result = _serialize_constraint_metadata(constraint)

    assert result["type"] == "FK"
    assert result["referenced_table"] == "departments"
    assert result["referenced_columns"] == ["id"]


def test_serialize_tag_assignment():
    """Test tag assignment serialization."""
    tag = TagAssignment(
        tag_name="PII",
        tag_value="HIGH",
        column_name="email"
    )

    result = _serialize_tag_assignment(tag)

    assert result["tag_name"] == "PII"
    assert result["tag_value"] == "HIGH"
    assert result["column_name"] == "email"


def test_serialize_tag_assignment_table_level():
    """Test table-level tag serialization."""
    tag = TagAssignment(
        tag_name="Classification",
        tag_value="Confidential",
        column_name=None
    )

    result = _serialize_tag_assignment(tag)

    assert result["tag_name"] == "Classification"
    assert result["tag_value"] == "Confidential"
    assert result["column_name"] is None


def test_serialize_masking_policy():
    """Test masking policy serialization."""
    policy = MaskingPolicy(
        policy_name="email_mask",
        signature="VARCHAR",
        column_name="email"
    )

    result = _serialize_masking_policy(policy)

    assert result["policy_name"] == "email_mask"
    assert result["signature"] == "VARCHAR"
    assert result["column_name"] == "email"


def test_serialize_variant_schema():
    """Test variant schema serialization."""
    variant = VariantSchema(
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

    result = _serialize_variant_schema(variant)

    assert result["column_name"] == "metadata"
    assert result["inferred_structure"]["user_id"] == "number"
    assert result["inferred_structure"]["preferences"]["notifications"] == "boolean"
    assert result["sample_size"] == 1000
    assert result["confidence"] == 0.95


def test_generate_metadata_json_table(sample_table_metadata):
    """Test generating JSON metadata for table."""
    result = generate_metadata_json(sample_table_metadata)

    assert result["object_type"] == "TABLE"
    assert result["name"] == "users"
    assert result["schema"] == "PUBLIC"
    assert result["database"] == "ANALYTICS"
    assert "ddl" in result
    assert len(result["columns"]) == 3
    assert result["row_count"] == 1000
    assert result["bytes"] == 50000
    assert result["last_ddl"] == "2025-01-01 10:00:00"
    assert result["clustering_key"] == "id"
    assert result["search_optimization"] is True
    assert "ddl_file" in result


def test_generate_metadata_json_table_with_variant_schema(sample_table_metadata):
    """Test that variant schema is included when present."""
    result = generate_metadata_json(sample_table_metadata)

    assert "variant_schema" in result
    assert result["variant_schema"]["column_name"] == "metadata"
    assert result["variant_schema"]["sample_size"] == 1000


def test_generate_metadata_json_table_without_variant_schema():
    """Test that variant schema is excluded when not present."""
    table = TableMetadata(
        name="simple_table",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE simple_table (id INT);",
        columns=[ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None)],
        row_count=0,
        bytes=0,
        last_ddl="2025-01-01",
        clustering_key=None,
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=False,
        variant_schema=None
    )

    result = generate_metadata_json(table)

    assert "variant_schema" not in result


def test_generate_metadata_json_table_ddl_file_path(sample_table_metadata):
    """Test that DDL file path is correctly formatted."""
    result = generate_metadata_json(sample_table_metadata, base_path="discovery")

    assert result["ddl_file"] == "discovery/ANALYTICS/PUBLIC/tables/users.sql"


def test_generate_metadata_json_view(sample_view_metadata):
    """Test generating JSON metadata for view."""
    result = generate_metadata_json(sample_view_metadata)

    assert result["object_type"] == "VIEW"
    assert result["name"] == "active_users"
    assert result["schema"] == "PUBLIC"
    assert result["database"] == "ANALYTICS"
    assert "ddl" in result
    assert "columns" in result
    assert "base_tables" in result
    assert len(result["base_tables"]) == 1
    assert result["base_tables"][0] == "users"
    assert "ddl_file" in result


def test_generate_metadata_json_procedure(sample_procedure_metadata):
    """Test generating JSON metadata for procedure."""
    result = generate_metadata_json(sample_procedure_metadata)

    assert result["object_type"] == "PROCEDURE"
    assert result["name"] == "process_data"
    assert result["schema"] == "PUBLIC"
    assert result["database"] == "ANALYTICS"
    assert "ddl" in result
    assert "parameters" in result
    assert len(result["parameters"]) == 2
    assert result["parameters"][0]["name"] == "input_data"
    assert result["return_type"] == "VARCHAR"
    assert result["language"] == "SQL"
    assert "ddl_file" in result


def test_generate_metadata_json_stream(sample_stream_metadata):
    """Test generating JSON metadata for stream."""
    result = generate_metadata_json(sample_stream_metadata)

    assert result["object_type"] == "STREAM"
    assert result["name"] == "users_stream"
    assert result["schema"] == "PUBLIC"
    assert result["database"] == "ANALYTICS"
    assert "ddl" in result
    assert "source_object" in result
    assert result["source_object"] == "ANALYTICS.PUBLIC.users"
    assert result["mode"] == "INCREMENTAL"
    assert "ddl_file" in result


def test_generate_metadata_json_task(sample_task_metadata):
    """Test generating JSON metadata for task."""
    result = generate_metadata_json(sample_task_metadata)

    assert result["object_type"] == "TASK"
    assert result["name"] == "daily_sync"
    assert result["schema"] == "PUBLIC"
    assert result["database"] == "ANALYTICS"
    assert "ddl" in result
    assert "schedule" in result
    assert result["schedule"] == "USING CRON 0 2 * * * America/New_York"
    assert result["state"] == "STARTED"
    assert "predecessors" in result
    assert len(result["predecessors"]) == 1
    assert result["predecessors"][0] == "daily_data_load"
    assert "ddl_file" in result


def test_generate_metadata_json_with_tags(sample_table_metadata):
    """Test that tags are included in JSON output."""
    result = generate_metadata_json(sample_table_metadata)

    assert "tags" in result
    assert len(result["tags"]) == 1
    assert result["tags"][0]["tag_name"] == "PII"
    assert result["tags"][0]["tag_value"] == "HIGH"


def test_generate_metadata_json_with_masking_policies(sample_table_metadata):
    """Test that masking policies are included in JSON output."""
    result = generate_metadata_json(sample_table_metadata)

    assert "masking_policies" in result
    assert len(result["masking_policies"]) == 1
    assert result["masking_policies"][0]["policy_name"] == "email_mask"
    assert result["masking_policies"][0]["signature"] == "VARCHAR"


def test_generate_metadata_json_with_constraints(sample_table_metadata):
    """Test that constraints are included in JSON output."""
    result = generate_metadata_json(sample_table_metadata)

    assert "constraints" in result
    assert len(result["constraints"]) == 1
    assert result["constraints"][0]["type"] == "PK"
    assert result["constraints"][0]["columns"] == ["id"]


def test_generate_metadata_json_serializable(sample_table_metadata):
    """Test that result is JSON serializable."""
    result = generate_metadata_json(sample_table_metadata)

    # This should not raise an exception
    json_str = json.dumps(result)

    assert isinstance(json_str, str)
    assert "users" in json_str
    assert "ANALYTICS" in json_str


def test_generate_metadata_json_all_required_fields(sample_table_metadata):
    """Test that all required fields are present for table."""
    result = generate_metadata_json(sample_table_metadata)

    required_fields = [
        "object_type", "name", "schema", "database", "ddl",
        "columns", "row_count", "bytes", "last_ddl",
        "clustering_key", "constraints", "tags",
        "masking_policies", "search_optimization", "ddl_file"
    ]

    for field in required_fields:
        assert field in result, f"Missing required field: {field}"


def test_generate_metadata_json_custom_base_path(sample_table_metadata):
    """Test that custom base path is used in DDL file reference."""
    result = generate_metadata_json(sample_table_metadata, base_path="custom/path")

    assert result["ddl_file"] == "custom/path/ANALYTICS/PUBLIC/tables/users.sql"


def test_generate_metadata_json_preserves_column_types(sample_table_metadata):
    """Test that column data types are preserved correctly."""
    result = generate_metadata_json(sample_table_metadata)

    columns = result["columns"]
    assert columns[0]["data_type"] == "INT"
    assert columns[1]["data_type"] == "VARCHAR(100)"
    assert columns[2]["data_type"] == "VARCHAR(255)"


def test_generate_metadata_json_preserves_boolean_fields():
    """Test that boolean fields are preserved correctly."""
    table = TableMetadata(
        name="test",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE test (id INT);",
        columns=[ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None)],
        row_count=0,
        bytes=0,
        last_ddl="2025-01-01",
        clustering_key=None,
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=True,
        variant_schema=None
    )

    result = generate_metadata_json(table)

    assert result["search_optimization"] is True
    assert result["columns"][0]["nullable"] is False


def test_serialize_nested_variant_structure():
    """Test that nested variant structures are serialized correctly."""
    variant = VariantSchema(
        column_name="metadata",
        inferred_structure={
            "level1": {
                "level2": {
                    "level3": "string"
                }
            }
        },
        sample_size=500,
        confidence=0.8
    )

    result = _serialize_variant_schema(variant)

    assert result["inferred_structure"]["level1"]["level2"]["level3"] == "string"


def test_metadata_json_is_dict(sample_table_metadata):
    """Test that generate_metadata_json returns a dict."""
    result = generate_metadata_json(sample_table_metadata)

    assert isinstance(result, dict)


def test_metadata_json_not_string(sample_table_metadata):
    """Test that generate_metadata_json does not return a string."""
    result = generate_metadata_json(sample_table_metadata)

    assert not isinstance(result, str)
