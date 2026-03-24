"""Tests for path construction and file writing."""

import pytest
import json
import os

from discovery.generate.assembler import (
    sanitize_filename,
    pluralize_object_type,
    build_output_path,
    write_discovery_files,
)


def test_sanitize_filename_basic():
    """Test basic filename sanitization."""
    assert sanitize_filename("my_table") == "my_table"
    assert sanitize_filename("my-table") == "my-table"
    assert sanitize_filename("my table") == "my table"


def test_sanitize_filename_special_chars():
    """Test sanitization of special characters."""
    assert sanitize_filename("my/table") == "my-table"
    assert sanitize_filename("my\\table") == "my-table"
    assert sanitize_filename("my:table") == "my-table"
    assert sanitize_filename("my*table") == "my-table"
    assert sanitize_filename("my?table") == "my-table"
    assert sanitize_filename('my"table') == "my-table"
    assert sanitize_filename("my<table") == "my-table"
    assert sanitize_filename("my>table") == "my-table"
    assert sanitize_filename("my|table") == "my-table"


def test_sanitize_filename_multiple_special_chars():
    """Test sanitization with multiple consecutive special characters."""
    assert sanitize_filename("my///table") == "my-table"
    assert sanitize_filename("my***table") == "my-table"
    assert sanitize_filename("my---table") == "my-table"


def test_sanitize_filename_leading_trailing_special_chars():
    """Test sanitization removes leading and trailing special characters."""
    assert sanitize_filename("---my-table---") == "my-table"
    assert sanitize_filename("   my table   ") == "my table"
    assert sanitize_filename("---   my table   ---") == "my table"


def test_sanitize_filename_empty_result():
    """Test that empty result becomes 'unnamed'."""
    assert sanitize_filename("---") == "unnamed"
    assert sanitize_filename("   ") == "unnamed"
    assert sanitize_filename("///") == "unnamed"


def test_sanitize_filename_preserves_spaces():
    """Test that spaces are preserved."""
    assert sanitize_filename("my table name") == "my table name"
    assert sanitize_filename("  my table  ") == "my table"


def test_pluralize_object_type_table():
    """Test pluralizing TABLE."""
    assert pluralize_object_type("TABLE") == "tables"
    assert pluralize_object_type("table") == "tables"


def test_pluralize_object_type_view():
    """Test pluralizing VIEW."""
    assert pluralize_object_type("VIEW") == "views"
    assert pluralize_object_type("view") == "views"


def test_pluralize_object_type_procedure():
    """Test pluralizing PROCEDURE."""
    assert pluralize_object_type("PROCEDURE") == "procedures"


def test_pluralize_object_type_function():
    """Test pluralizing FUNCTION."""
    assert pluralize_object_type("FUNCTION") == "functions"


def test_pluralize_object_type_stream():
    """Test pluralizing STREAM."""
    assert pluralize_object_type("STREAM") == "streams"


def test_pluralize_object_type_task():
    """Test pluralizing TASK."""
    assert pluralize_object_type("TASK") == "tasks"


def test_pluralize_object_type_dynamic_table():
    """Test pluralizing DYNAMIC_TABLE."""
    assert pluralize_object_type("DYNAMIC_TABLE") == "dynamic_tables"


def test_pluralize_object_type_stage():
    """Test pluralizing STAGE."""
    assert pluralize_object_type("STAGE") == "stages"


def test_pluralize_object_type_pipe():
    """Test pluralizing PIPE."""
    assert pluralize_object_type("PIPE") == "pipes"


def test_pluralize_object_type_sequence():
    """Test pluralizing SEQUENCE."""
    assert pluralize_object_type("SEQUENCE") == "sequences"


def test_pluralize_object_type_external_table():
    """Test pluralizing EXTERNAL_TABLE."""
    assert pluralize_object_type("EXTERNAL_TABLE") == "external_tables"


def test_pluralize_object_type_unknown():
    """Test that unknown object type raises ValueError."""
    with pytest.raises(ValueError, match="Unknown object type: UNKNOWN"):
        pluralize_object_type("UNKNOWN")


def test_build_output_path_table():
    """Test building output path for table."""
    path = build_output_path("ANALYTICS", "PUBLIC", "TABLE", "users", "sql")

    assert str(path) == "discovery/ANALYTICS/PUBLIC/tables/users.sql"


def test_build_output_path_view():
    """Test building output path for view."""
    path = build_output_path("ANALYTICS", "PUBLIC", "VIEW", "active_users", "json")

    assert str(path) == "discovery/ANALYTICS/PUBLIC/views/active_users.json"


def test_build_output_path_with_custom_base():
    """Test building output path with custom base path."""
    path = build_output_path("ANALYTICS", "PUBLIC", "TABLE", "users", "sql", "custom/path")

    assert str(path) == "custom/path/ANALYTICS/PUBLIC/tables/users.sql"


def test_build_output_path_with_nested_schema():
    """Test building output path with nested schema."""
    path = build_output_path("ANALYTICS", "STAGING.CLEANED", "TABLE", "orders", "sql")

    assert str(path) == "discovery/ANALYTICS/STAGING.CLEANED/tables/orders.sql"


def test_build_output_path_sanitizes_object_name():
    """Test that object name is sanitized in output path."""
    path = build_output_path("ANALYTICS", "PUBLIC", "TABLE", "my/table", "sql")

    assert str(path) == "discovery/ANALYTICS/PUBLIC/tables/my-table.sql"


def test_build_output_path_returns_path_object():
    """Test that build_output_path returns a Path object."""
    path = build_output_path("ANALYTICS", "PUBLIC", "TABLE", "users", "sql")

    assert isinstance(path, os.PathLike) or hasattr(path, '__fspath__')


def test_build_output_path_preserves_case():
    """Test that database and schema names preserve case."""
    path = build_output_path("AnalyticsDB", "MySchema", "TABLE", "users", "sql")

    assert "AnalyticsDB" in str(path)
    assert "MySchema" in str(path)


def test_write_discovery_files(tmp_discovery_dir):
    """Test writing both .sql and .json files."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "TABLE",
        "object_name": "users"
    }
    ddl_content = "CREATE TABLE users (id INT);"
    json_content = '{"name": "users"}'

    sql_path, json_path = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    # Check paths
    assert str(sql_path) == f"{tmp_discovery_dir}/ANALYTICS/PUBLIC/tables/users.sql"
    assert str(json_path) == f"{tmp_discovery_dir}/ANALYTICS/PUBLIC/tables/users.json"

    # Check files exist
    assert sql_path.exists()
    assert json_path.exists()

    # Check file contents
    with open(sql_path, 'r', encoding='utf-8') as f:
        assert f.read() == ddl_content

    with open(json_path, 'r', encoding='utf-8') as f:
        assert f.read() == json_content


def test_write_discovery_files_creates_directories(tmp_discovery_dir):
    """Test that write_discovery_files creates directories as needed."""
    metadata = {
        "database": "NEW_DB",
        "schema": "NEW_SCHEMA",
        "object_type": "TABLE",
        "object_name": "new_table"
    }
    ddl_content = "CREATE TABLE new_table (id INT);"
    json_content = '{"name": "new_table"}'

    sql_path, json_path = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    # Check that parent directory was created
    assert sql_path.parent.exists()
    assert json_path.parent.exists()


def test_write_discovery_files_with_special_characters(tmp_discovery_dir):
    """Test writing files with special characters in object name."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "TABLE",
        "object_name": "my/table:with*special?chars"
    }
    ddl_content = "CREATE TABLE my_table (id INT);"
    json_content = '{"name": "my_table"}'

    sql_path, json_path = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    # Check that files exist with sanitized names
    assert sql_path.exists()
    assert "my-table-with-special-chars" in str(sql_path)


def test_write_discovery_files_missing_metadata_key(tmp_discovery_dir):
    """Test that missing metadata key raises KeyError."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        # Missing object_type and object_name
    }
    ddl_content = "CREATE TABLE users (id INT);"
    json_content = '{"name": "users"}'

    with pytest.raises(KeyError, match="Missing required metadata keys"):
        write_discovery_files(metadata, ddl_content, json_content, str(tmp_discovery_dir))


def test_write_discovery_files_all_metadata_keys_present(tmp_discovery_dir):
    """Test that all metadata keys are validated."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "VIEW",
        "object_name": "my_view"
    }
    ddl_content = "CREATE VIEW my_view AS SELECT 1;"
    json_content = '{"name": "my_view"}'

    sql_path, json_path = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    assert sql_path.exists()
    assert json_path.exists()
    assert "views" in str(sql_path)


def test_write_discovery_files_json_serializable(tmp_discovery_dir):
    """Test that JSON content is properly written."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "TABLE",
        "object_name": "users"
    }
    ddl_content = "CREATE TABLE users (id INT);"
    json_dict = {
        "name": "users",
        "columns": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR"}
        ]
    }
    json_content = json.dumps(json_dict)

    sql_path, json_path = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    # Verify JSON can be parsed back
    with open(json_path, 'r', encoding='utf-8') as f:
        parsed = json.load(f)

    assert parsed["name"] == "users"
    assert len(parsed["columns"]) == 2


def test_write_discovery_files_utf8_encoding(tmp_discovery_dir):
    """Test that files are written with UTF-8 encoding."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "TABLE",
        "object_name": "users"
    }
    ddl_content = "CREATE TABLE users (id INT, name VARCHAR, comment VARCHAR);"
    json_content = '{"name": "用户", "comment": "测试"}'  # Chinese characters

    sql_path, json_path = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    # Read back and verify UTF-8
    with open(json_path, 'r', encoding='utf-8') as f:
        content = f.read()

    assert "用户" in content
    assert "测试" in content


def test_write_discovery_files_returns_tuple(tmp_discovery_dir):
    """Test that write_discovery_files returns a tuple of paths."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "TABLE",
        "object_name": "users"
    }
    ddl_content = "CREATE TABLE users (id INT);"
    json_content = '{"name": "users"}'

    result = write_discovery_files(
        metadata, ddl_content, json_content, str(tmp_discovery_dir)
    )

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(p, (str, os.PathLike)) for p in result)


def test_write_discovery_files_multiple_objects(tmp_discovery_dir):
    """Test writing multiple discovery files."""
    objects = [
        {
            "metadata": {
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "object_type": "TABLE",
                "object_name": "users"
            },
            "ddl": "CREATE TABLE users (id INT);",
            "json": '{"name": "users"}'
        },
        {
            "metadata": {
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "object_type": "TABLE",
                "object_name": "orders"
            },
            "ddl": "CREATE TABLE orders (id INT);",
            "json": '{"name": "orders"}'
        },
    ]

    paths = []
    for obj in objects:
        sql_path, json_path = write_discovery_files(
            obj["metadata"], obj["ddl"], obj["json"], str(tmp_discovery_dir)
        )
        paths.append((sql_path, json_path))

    # Check all files exist
    assert len(paths) == 2
    assert all(sql_path.exists() for sql_path, _ in paths)
    assert all(json_path.exists() for _, json_path in paths)


def test_sanitize_filename_does_not_modify_hyphens():
    """Test that hyphens are not modified unless they're problematic."""
    assert sanitize_filename("my-table-name") == "my-table-name"
    assert sanitize_filename("my_table_name") == "my_table_name"


def test_sanitize_filename_handles_unicode():
    """Test that unicode characters are preserved."""
    assert sanitize_filename("用户表") == "用户表"
    assert sanitize_filename("tâble") == "tâble"


def test_build_output_path_with_all_object_types():
    """Test building paths for all supported object types."""
    object_types = [
        ("TABLE", "tables"),
        ("VIEW", "views"),
        ("PROCEDURE", "procedures"),
        ("FUNCTION", "functions"),
        ("STREAM", "streams"),
        ("TASK", "tasks"),
        ("DYNAMIC_TABLE", "dynamic_tables"),
        ("STAGE", "stages"),
        ("PIPE", "pipes"),
        ("SEQUENCE", "sequences"),
        ("EXTERNAL_TABLE", "external_tables"),
    ]

    for obj_type, expected_plural in object_types:
        path = build_output_path("DB", "SCHEMA", obj_type, "obj", "sql")
        assert expected_plural in str(path), f"Failed for {obj_type}"


def test_write_discovery_files_overwrites_existing(tmp_discovery_dir):
    """Test that write_discovery_files overwrites existing files."""
    metadata = {
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "object_type": "TABLE",
        "object_name": "users"
    }

    # Write first time
    sql_path, json_path = write_discovery_files(
        metadata, "FIRST CONTENT", '{"version": 1}', str(tmp_discovery_dir)
    )

    # Write second time (should overwrite)
    write_discovery_files(
        metadata, "SECOND CONTENT", '{"version": 2}', str(tmp_discovery_dir)
    )

    # Check files contain new content
    with open(sql_path, 'r') as f:
        assert f.read() == "SECOND CONTENT"

    with open(json_path, 'r') as f:
        assert json.load(f) == {"version": 2}


def test_build_output_path_empty_object_name():
    """Test handling of empty object name."""
    path = build_output_path("DB", "SCHEMA", "TABLE", "", "sql")

    # Empty name becomes 'unnamed'
    assert "unnamed" in str(path)


def test_sanitize_filename_returns_string():
    """Test that sanitize_filename always returns a string."""
    result = sanitize_filename("any_name")
    assert isinstance(result, str)
    assert len(result) > 0
