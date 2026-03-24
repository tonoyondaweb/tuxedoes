"""Tests for type instantiation and serialization."""

import pytest
import json
from dataclasses import asdict

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


def test_column_metadata_instantiation():
    """Test ColumnMetadata instantiation."""
    column = ColumnMetadata(
        name="id",
        data_type="INT",
        nullable=False,
        default_value=None,
        comment="Primary key"
    )

    assert column.name == "id"
    assert column.data_type == "INT"
    assert column.nullable is False
    assert column.default_value is None
    assert column.comment == "Primary key"


def test_column_metadata_serialization():
    """Test ColumnMetadata serialization to dict."""
    column = ColumnMetadata(
        name="email",
        data_type="VARCHAR(255)",
        nullable=True,
        default_value="'user@example.com'",
        comment="User email"
    )

    result = asdict(column)

    assert result["name"] == "email"
    assert result["data_type"] == "VARCHAR(255)"
    assert result["nullable"] is True
    assert result["default_value"] == "'user@example.com'"
    assert result["comment"] == "User email"


def test_constraint_metadata_instantiation():
    """Test ConstraintMetadata instantiation."""
    constraint = ConstraintMetadata(
        name="pk_users",
        type="PK",
        columns=["id"],
        referenced_table=None,
        referenced_columns=None
    )

    assert constraint.name == "pk_users"
    assert constraint.type == "PK"
    assert constraint.columns == ["id"]
    assert constraint.referenced_table is None
    assert constraint.referenced_columns is None


def test_constraint_metadata_with_foreign_key():
    """Test ConstraintMetadata with foreign key."""
    constraint = ConstraintMetadata(
        name="fk_users_department",
        type="FK",
        columns=["department_id"],
        referenced_table="departments",
        referenced_columns=["id"]
    )

    assert constraint.type == "FK"
    assert constraint.referenced_table == "departments"
    assert constraint.referenced_columns == ["id"]


def test_tag_assignment_instantiation():
    """Test TagAssignment instantiation."""
    tag = TagAssignment(
        tag_name="PII",
        tag_value="HIGH",
        column_name="email"
    )

    assert tag.tag_name == "PII"
    assert tag.tag_value == "HIGH"
    assert tag.column_name == "email"


def test_tag_assignment_without_column():
    """Test TagAssignment without column_name (table-level tag)."""
    tag = TagAssignment(
        tag_name="Classification",
        tag_value="Confidential",
        column_name=None
    )

    assert tag.column_name is None


def test_masking_policy_instantiation():
    """Test MaskingPolicy instantiation."""
    policy = MaskingPolicy(
        policy_name="email_mask",
        signature="VARCHAR",
        column_name="email"
    )

    assert policy.policy_name == "email_mask"
    assert policy.signature == "VARCHAR"
    assert policy.column_name == "email"


def test_variant_schema_instantiation():
    """Test VariantSchema instantiation."""
    variant = VariantSchema(
        column_name="metadata",
        inferred_structure={"user_id": "number", "name": "string"},
        sample_size=1000,
        confidence=0.95
    )

    assert variant.column_name == "metadata"
    assert variant.inferred_structure == {"user_id": "number", "name": "string"}
    assert variant.sample_size == 1000
    assert variant.confidence == 0.95


def test_variant_schema_serialization():
    """Test VariantSchema serialization to dict."""
    variant = VariantSchema(
        column_name="metadata",
        inferred_structure={
            "user": {
                "id": "number",
                "name": "string"
            }
        },
        sample_size=5000,
        confidence=0.85
    )

    result = asdict(variant)

    assert result["column_name"] == "metadata"
    assert result["inferred_structure"]["user"]["id"] == "number"
    assert result["sample_size"] == 5000
    assert result["confidence"] == 0.85


def test_table_metadata_instantiation(
    sample_column_metadata,
    sample_constraint_metadata,
    sample_tag_assignment,
    sample_masking_policy,
    sample_variant_schema
):
    """Test TableMetadata instantiation."""
    table = TableMetadata(
        name="users",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE TABLE users (id INT);",
        columns=[sample_column_metadata],
        row_count=1000,
        bytes=50000,
        last_ddl="2025-01-01",
        clustering_key="id",
        constraints=[sample_constraint_metadata],
        tags=[sample_tag_assignment],
        masking_policies=[sample_masking_policy],
        search_optimization=True,
        variant_schema=sample_variant_schema
    )

    assert table.name == "users"
    assert table.schema == "PUBLIC"
    assert table.database == "ANALYTICS"
    assert len(table.columns) == 1
    assert table.columns[0].name == "id"
    assert len(table.constraints) == 1
    assert len(table.tags) == 1
    assert len(table.masking_policies) == 1
    assert table.search_optimization is True
    assert table.variant_schema is not None


def test_table_metadata_without_optional_fields(sample_column_metadata):
    """Test TableMetadata with optional fields as None."""
    table = TableMetadata(
        name="simple_table",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE TABLE simple_table (id INT);",
        columns=[sample_column_metadata],
        row_count=100,
        bytes=5000,
        last_ddl="2025-01-01",
        clustering_key=None,
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=False,
        variant_schema=None
    )

    assert table.clustering_key is None
    assert len(table.constraints) == 0
    assert table.search_optimization is False
    assert table.variant_schema is None


def test_table_metadata_serialization(sample_table_metadata):
    """Test TableMetadata serialization to dict."""
    result = asdict(sample_table_metadata)

    assert result["name"] == "users"
    assert result["schema"] == "PUBLIC"
    assert result["database"] == "ANALYTICS"
    assert len(result["columns"]) == 3
    assert result["row_count"] == 1000
    assert result["bytes"] == 50000


def test_view_metadata_instantiation(sample_column_metadata):
    """Test ViewMetadata instantiation."""
    view = ViewMetadata(
        name="active_users",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE VIEW active_users AS SELECT * FROM users WHERE active = TRUE;",
        columns=[sample_column_metadata],
        base_tables=["users"],
        last_ddl="2025-01-02",
        tags=[]
    )

    assert view.name == "active_users"
    assert view.object_type == "VIEW"
    assert len(view.base_tables) == 1
    assert view.base_tables[0] == "users"


def test_procedure_metadata_instantiation():
    """Test ProcedureMetadata instantiation."""
    procedure = ProcedureMetadata(
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
        last_ddl="2025-01-03"
    )

    assert procedure.name == "process_data"
    assert len(procedure.parameters) == 2
    assert procedure.parameters[0]["name"] == "input_data"
    assert procedure.return_type == "VARCHAR"
    assert procedure.language == "SQL"


def test_procedure_metadata_without_optional_fields():
    """Test ProcedureMetadata with optional fields as None."""
    procedure = ProcedureMetadata(
        name="simple_proc",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE PROCEDURE simple_proc() ...",
        parameters=[],
        return_type=None,
        language=None,
        last_ddl="2025-01-03"
    )

    assert procedure.return_type is None
    assert procedure.language is None
    assert len(procedure.parameters) == 0


def test_stream_metadata_instantiation():
    """Test StreamMetadata instantiation."""
    stream = StreamMetadata(
        name="users_stream",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE STREAM users_stream ON TABLE users;",
        source_object="ANALYTICS.PUBLIC.users",
        mode="INCREMENTAL",
        last_ddl="2025-01-04"
    )

    assert stream.name == "users_stream"
    assert stream.source_object == "ANALYTICS.PUBLIC.users"
    assert stream.mode == "INCREMENTAL"


def test_task_metadata_instantiation():
    """Test TaskMetadata instantiation."""
    task = TaskMetadata(
        name="daily_sync",
        schema="PUBLIC",
        database="ANALYTICS",
        ddl="CREATE TASK daily_sync ...",
        schedule="USING CRON 0 2 * * * America/New_York",
        state="STARTED",
        predecessors=["daily_data_load"],
        last_ddl="2025-01-05"
    )

    assert task.name == "daily_sync"
    assert task.schedule == "USING CRON 0 2 * * * America/New_York"
    assert task.state == "STARTED"
    assert len(task.predecessors) == 1
    assert task.predecessors[0] == "daily_data_load"


def test_discovery_manifest_instantiation():
    """Test DiscoveryManifest instantiation."""
    manifest = DiscoveryManifest(
        format_version="1.0.0",
        generated_at="2026-03-21T12:00:00Z",
        snowflake_account="xy12345.us-east-1",
        config_hash="abc123def456",
        object_count=10,
        errors=[]
    )

    assert manifest.format_version == "1.0.0"
    assert manifest.snowflake_account == "xy12345.us-east-1"
    assert manifest.object_count == 10
    assert len(manifest.errors) == 0


def test_discovery_manifest_with_errors():
    """Test DiscoveryManifest with errors."""
    error = DiscoveryError(
        object_name="problem_table",
        object_type="TABLE",
        error_message="Permission denied",
        retry_count=3
    )

    manifest = DiscoveryManifest(
        format_version="1.0.0",
        generated_at="2026-03-21T12:00:00Z",
        snowflake_account="xy12345.us-east-1",
        config_hash="abc123def456",
        object_count=9,
        errors=[error]
    )

    assert len(manifest.errors) == 1
    assert manifest.errors[0].object_name == "problem_table"
    assert manifest.errors[0].object_type == "TABLE"
    assert manifest.errors[0].error_message == "Permission denied"
    assert manifest.errors[0].retry_count == 3


def test_discovery_error_instantiation():
    """Test DiscoveryError instantiation."""
    error = DiscoveryError(
        object_name="failed_view",
        object_type="VIEW",
        error_message="Invalid syntax in view definition",
        retry_count=2
    )

    assert error.object_name == "failed_view"
    assert error.object_type == "VIEW"
    assert error.error_message == "Invalid syntax in view definition"
    assert error.retry_count == 2


def test_metadata_types_are_dataclasses():
    """Test that all metadata types are dataclasses."""
    from dataclasses import is_dataclass

    assert is_dataclass(ColumnMetadata)
    assert is_dataclass(ConstraintMetadata)
    assert is_dataclass(TagAssignment)
    assert is_dataclass(MaskingPolicy)
    assert is_dataclass(VariantSchema)
    assert is_dataclass(TableMetadata)
    assert is_dataclass(ViewMetadata)
    assert is_dataclass(ProcedureMetadata)
    assert is_dataclass(StreamMetadata)
    assert is_dataclass(TaskMetadata)
    assert is_dataclass(DiscoveryManifest)
    assert is_dataclass(DiscoveryError)


def test_metadata_types_json_serializable(sample_table_metadata):
    """Test that all metadata types are JSON serializable."""
    # This should not raise an exception
    result = asdict(sample_table_metadata)
    json_str = json.dumps(result)

    assert isinstance(json_str, str)
    assert "users" in json_str
    assert "ANALYTICS" in json_str


def test_nested_structure_serialization():
    """Test that nested structures serialize correctly."""
    table = TableMetadata(
        name="test",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE test (id INT);",
        columns=[
            ColumnMetadata(name="id", data_type="INT", nullable=False,
                          default_value=None, comment="PK"),
            ColumnMetadata(name="data", data_type="VARIANT", nullable=True,
                          default_value=None, comment="JSON data")
        ],
        row_count=100,
        bytes=5000,
        last_ddl="2025-01-01",
        clustering_key="id",
        constraints=[
            ConstraintMetadata(name="pk_test", type="PK", columns=["id"],
                              referenced_table=None, referenced_columns=None)
        ],
        tags=[
            TagAssignment(tag_name="PII", tag_value="HIGH", column_name=None)
        ],
        masking_policies=[
            MaskingPolicy(policy_name="data_mask", signature="VARIANT",
                         column_name="data")
        ],
        search_optimization=False,
        variant_schema=VariantSchema(
            column_name="data",
            inferred_structure={"field1": "string", "field2": "number"},
            sample_size=100,
            confidence=0.8
        )
    )

    result = asdict(table)
    json_str = json.dumps(result)

    # Verify all nested structures are in the JSON
    assert '"columns"' in json_str
    assert '"constraints"' in json_str
    assert '"tags"' in json_str
    assert '"masking_policies"' in json_str
    assert '"variant_schema"' in json_str
    assert '"inferred_structure"' in json_str


def test_metadata_field_types():
    """Test that metadata fields have correct types."""
    column = ColumnMetadata(
        name="test_col",
        data_type="VARCHAR(100)",
        nullable=True,
        default_value=None,
        comment=None
    )

    assert isinstance(column.name, str)
    assert isinstance(column.data_type, str)
    assert isinstance(column.nullable, bool)
    assert column.default_value is None
    assert column.comment is None

    constraint = ConstraintMetadata(
        name="fk_test",
        type="FK",
        columns=["id"],
        referenced_table="other_table",
        referenced_columns=["id"]
    )

    assert isinstance(constraint.name, str)
    assert isinstance(constraint.type, str)
    assert isinstance(constraint.columns, list)
    assert isinstance(constraint.columns[0], str)


def test_optional_fields_are_truly_optional():
    """Test that optional fields can be omitted."""
    table = TableMetadata(
        name="minimal_table",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE minimal_table (id INT);",
        columns=[
            ColumnMetadata(name="id", data_type="INT", nullable=False,
                          default_value=None, comment=None)
        ],
        row_count=0,
        bytes=0,
        last_ddl="",
        clustering_key=None,
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=False,
        variant_schema=None
    )

    # Should not raise any errors
    assert table.clustering_key is None
    assert table.variant_schema is None
    assert len(table.tags) == 0
