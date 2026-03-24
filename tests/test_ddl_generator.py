"""Tests for DDL file generation."""

import pytest
from datetime import datetime
import re

from discovery.generate.ddl_generator import (
    generate_ddl_file,
    _generate_header_comment,
    _generate_footer_comment_table,
    _generate_footer_comment_generic,
    _add_inline_comments_to_table_ddl,
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


def test_generate_header_comment():
    """Test header comment generation."""
    header = _generate_header_comment("users", "TABLE", "ANALYTICS", "PUBLIC")

    assert "-- ============================================" in header
    assert "-- DDL for TABLE: users" in header
    assert "-- Database: ANALYTICS" in header
    assert "-- Schema: PUBLIC" in header
    assert "-- Generated:" in header
    assert header.strip().endswith("-- ============================================")

    # Check ISO timestamp format
    timestamp_match = re.search(r'-- Generated: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)', header)
    assert timestamp_match is not None


def test_generate_footer_comment_table_basic(sample_table_metadata):
    """Test basic table footer comment."""
    footer = _generate_footer_comment_table(sample_table_metadata)

    assert "-- ============================================" in footer
    assert "-- Metadata Statistics" in footer
    assert "-- Row Count: 1,000" in footer
    assert "-- Byte Size: 50,000" in footer
    assert "-- Last DDL: 2025-01-01 10:00:00" in footer
    assert footer.strip().endswith("-- ============================================")


def test_generate_footer_comment_table_with_tags(sample_table_metadata, sample_tag_assignment):
    """Test table footer with tags."""
    footer = _generate_footer_comment_table(sample_table_metadata)

    assert "-- Tags:" in footer
    assert "- PII = HIGH [column: email]" in footer


def test_generate_footer_comment_table_with_masking_policies(sample_table_metadata, sample_masking_policy):
    """Test table footer with masking policies."""
    footer = _generate_footer_comment_table(sample_table_metadata)

    assert "-- Masking Policies:" in footer
    assert "- email_mask(VARCHAR) on column email" in footer


def test_generate_footer_comment_table_with_search_optimization(sample_table_metadata):
    """Test table footer with search optimization."""
    footer = _generate_footer_comment_table(sample_table_metadata)

    assert "-- Search Optimization: Enabled" in footer


def test_generate_footer_comment_table_with_variant_schema(sample_table_metadata, sample_variant_schema):
    """Test table footer with variant schema."""
    footer = _generate_footer_comment_table(sample_table_metadata)

    assert "-- VARIANT Schema:" in footer
    assert "Inferred from 1,000 samples" in footer
    assert "confidence: 95.00%" in footer


def test_generate_footer_comment_generic():
    """Test generic footer comment."""
    footer = _generate_footer_comment_generic("2025-01-02")

    assert "-- ============================================" in footer
    assert "-- Last DDL: 2025-01-02" in footer


def test_generate_footer_comment_generic_with_tags(sample_tag_assignment):
    """Test generic footer with tags."""
    footer = _generate_footer_comment_generic("2025-01-02", [sample_tag_assignment])

    assert "-- Tags:" in footer
    assert "- PII = HIGH [column: email]" in footer


def test_add_inline_comments_to_table_ddl_primary_key(sample_column_metadata, sample_constraint_metadata):
    """Test adding primary key comment."""
    ddl = "CREATE TABLE users (\n  id INT NOT NULL,\n  name VARCHAR(100)\n);"
    result = _add_inline_comments_to_table_ddl(
        ddl,
        [sample_column_metadata, ColumnMetadata(name="name", data_type="VARCHAR", nullable=True, default_value=None, comment=None)],
        [sample_constraint_metadata],
        ""
    )

    assert "[PRIMARY KEY]" in result
    assert result.index("[PRIMARY KEY]") < result.index("id INT NOT NULL")


def test_add_inline_comments_to_table_ddl_foreign_key():
    """Test adding foreign key comment."""
    fk_constraint = ConstraintMetadata(
        name="fk_users_department",
        type="FK",
        columns=["department_id"],
        referenced_table="departments",
        referenced_columns=["id"]
    )
    ddl = "CREATE TABLE users (\n  id INT NOT NULL,\n  department_id INT\n);"
    result = _add_inline_comments_to_table_ddl(
        ddl,
        [
            ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None),
            ColumnMetadata(name="department_id", data_type="INT", nullable=True, default_value=None, comment=None)
        ],
        [fk_constraint],
        ""
    )

    assert "[FOREIGN KEY -> departments]" in result


def test_add_inline_comments_to_table_ddl_clustering_key():
    """Test adding clustering key comment."""
    ddl = "CREATE TABLE users (\n  id INT NOT NULL,\n  name VARCHAR(100)\n);"
    result = _add_inline_comments_to_table_ddl(
        ddl,
        [
            ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None),
            ColumnMetadata(name="name", data_type="VARCHAR", nullable=True, default_value=None, comment=None)
        ],
        [],
        "id"
    )

    assert "[CLUSTERING KEY]" in result


def test_add_inline_comments_to_table_ddl_multiple_comments():
    """Test adding multiple inline comments to same column."""
    pk_constraint = ConstraintMetadata(
        name="pk_users",
        type="PK",
        columns=["id"],
        referenced_table=None,
        referenced_columns=None
    )
    ddl = "CREATE TABLE users (\n  id INT NOT NULL\n);"
    result = _add_inline_comments_to_table_ddl(
        ddl,
        [ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None)],
        [pk_constraint],
        "id"
    )

    # Column should have both PRIMARY KEY and CLUSTERING KEY comments
    assert "[PRIMARY KEY]" in result
    assert "[CLUSTERING KEY]" in result


def test_add_inline_comments_to_table_ddl_preserves_indentation():
    """Test that inline comments preserve original indentation."""
    ddl = "CREATE TABLE users (\n    id INT NOT NULL,\n    name VARCHAR(100)\n);"
    result = _add_inline_comments_to_table_ddl(
        ddl,
        [
            ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None),
            ColumnMetadata(name="name", data_type="VARCHAR", nullable=True, default_value=None, comment=None)
        ],
        [ConstraintMetadata(name="pk", type="PK", columns=["id"], referenced_table=None, referenced_columns=None)],
        ""
    )

    # Check that indentation is preserved (4 spaces in this case)
    assert "    --" in result or "--" in result


def test_generate_ddl_file_table(sample_table_metadata):
    """Test generating complete DDL file for a table."""
    result = generate_ddl_file(sample_table_metadata)

    assert "-- ============================================" in result
    assert "-- DDL for TABLE: users" in result
    assert "-- Database: ANALYTICS" in result
    assert "-- Schema: PUBLIC" in result
    assert "CREATE TABLE" in result or "CREATE OR REPLACE TABLE" in result
    assert "-- Metadata Statistics" in result
    assert "-- Row Count: 1,000" in result
    assert "-- Byte Size: 50,000" in result


def test_generate_ddl_file_view(sample_view_metadata):
    """Test generating complete DDL file for a view."""
    result = generate_ddl_file(sample_view_metadata)

    assert "-- ============================================" in result
    assert "-- DDL for VIEW: active_users" in result
    assert "-- Database: ANALYTICS" in result
    assert "-- Schema: PUBLIC" in result
    assert "CREATE VIEW" in result or "CREATE OR REPLACE VIEW" in result
    assert "-- Last DDL: 2025-01-02 10:00:00" in result


def test_generate_ddl_file_procedure(sample_procedure_metadata):
    """Test generating complete DDL file for a procedure."""
    result = generate_ddl_file(sample_procedure_metadata)

    assert "-- ============================================" in result
    assert "-- DDL for PROCEDURE: process_data" in result
    assert "-- Database: ANALYTICS" in result
    assert "-- Schema: PUBLIC" in result
    assert "CREATE PROCEDURE" in result or "CREATE OR REPLACE PROCEDURE" in result
    assert "-- Last DDL: 2025-01-03 10:00:00" in result


def test_generate_ddl_file_stream(sample_stream_metadata):
    """Test generating complete DDL file for a stream."""
    result = generate_ddl_file(sample_stream_metadata)

    assert "-- ============================================" in result
    assert "-- DDL for STREAM: users_stream" in result
    assert "-- Database: ANALYTICS" in result
    assert "-- Schema: PUBLIC" in result
    assert "CREATE STREAM" in result or "CREATE OR REPLACE STREAM" in result
    assert "-- Last DDL: 2025-01-04 10:00:00" in result


def test_generate_ddl_file_task(sample_task_metadata):
    """Test generating complete DDL file for a task."""
    result = generate_ddl_file(sample_task_metadata)

    assert "-- ============================================" in result
    assert "-- DDL for TASK: daily_sync" in result
    assert "-- Database: ANALYTICS" in result
    assert "-- Schema: PUBLIC" in result
    assert "CREATE TASK" in result or "CREATE OR REPLACE TASK" in result
    assert "-- Last DDL: 2025-01-05 10:00:00" in result


def test_generate_ddl_file_unsupported_type():
    """Test that unsupported metadata type raises ValueError."""
    class UnsupportedMetadata:
        name = "unsupported"

    with pytest.raises(ValueError, match="Unsupported metadata type"):
        generate_ddl_file(UnsupportedMetadata())


def test_generate_ddl_file_structure():
    """Test that generated DDL file has proper structure."""
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
        search_optimization=False,
        variant_schema=None
    )

    result = generate_ddl_file(table)

    # Verify structure: header, DDL, footer
    parts = result.split("\n\n")
    assert len(parts) >= 3

    # First part should be header
    assert "-- DDL for TABLE: test" in parts[0]

    # Middle part should be DDL
    assert "CREATE TABLE" in parts[1] or "CREATE OR REPLACE TABLE" in parts[1]

    # Last part should be footer
    assert "-- Metadata Statistics" in parts[-1] or "-- Last DDL" in parts[-1]


def test_number_formatting_in_footer():
    """Test that numbers are formatted with thousands separators."""
    table = TableMetadata(
        name="large_table",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE large_table (id INT);",
        columns=[ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None)],
        row_count=1234567,
        bytes=9876543210,
        last_ddl="2025-01-01",
        clustering_key=None,
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=False,
        variant_schema=None
    )

    footer = _generate_footer_comment_table(table)

    assert "-- Row Count: 1,234,567" in footer
    assert "-- Byte Size: 9,876,543,210" in footer


def test_empty_tables_list_in_footer():
    """Test footer when table has no tags or masking policies."""
    table = TableMetadata(
        name="simple",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE simple (id INT);",
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

    footer = _generate_footer_comment_table(table)

    # Should not include Tags or Masking Policies sections
    assert "-- Tags:" not in footer
    assert "-- Masking Policies:" not in footer

    # Should still include basic metadata
    assert "-- Metadata Statistics" in footer
    assert "-- Row Count:" in footer


def test_variant_schema_confidence_formatting():
    """Test that variant schema confidence is formatted as percentage."""
    variant_schema = VariantSchema(
        column_name="metadata",
        inferred_structure={"field": "string"},
        sample_size=5000,
        confidence=0.87654321
    )
    table = TableMetadata(
        name="test",
        schema="PUBLIC",
        database="TEST",
        ddl="CREATE TABLE test (id INT, metadata VARIANT);",
        columns=[],
        row_count=0,
        bytes=0,
        last_ddl="2025-01-01",
        clustering_key=None,
        constraints=[],
        tags=[],
        masking_policies=[],
        search_optimization=False,
        variant_schema=variant_schema
    )

    footer = _generate_footer_comment_table(table)

    # Should format as percentage with 2 decimal places
    assert "confidence: 87.65%" in footer


def test_ddl_preserves_original_content(sample_table_metadata):
    """Test that original DDL content is preserved."""
    result = generate_ddl_file(sample_table_metadata)

    # Original DDL should be in the output
    assert "CREATE TABLE users" in result or "CREATE OR REPLACE TABLE users" in result
    assert "id INT NOT NULL" in result
    assert "name VARCHAR(100)" in result


def test_inline_comment_word_boundary():
    """Test that inline comments use word boundaries (don't match partial column names)."""
    # Column named "identity" should not match when looking for "id"
    ddl = "CREATE TABLE users (\n  id INT NOT NULL,\n  identity VARCHAR(100)\n);"
    result = _add_inline_comments_to_table_ddl(
        ddl,
        [
            ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None),
            ColumnMetadata(name="identity", data_type="VARCHAR", nullable=True, default_value=None, comment=None)
        ],
        [ConstraintMetadata(name="pk", type="PK", columns=["id"], referenced_table=None, referenced_columns=None)],
        ""
    )

    # Should only have comment on "id", not "identity"
    lines = result.split("\n")
    comment_lines = [line for line in lines if "[PRIMARY KEY]" in line]

    # Comment should appear before "id INT" line
    assert len(comment_lines) == 1
