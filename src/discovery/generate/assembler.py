"""Output assembler for constructing file paths and writing discovery files."""

import os
import re
from pathlib import Path
from typing import Dict, Any, Optional


# Object type to plural form mapping
OBJECT_TYPE_PLURALS = {
    "TABLE": "tables",
    "VIEW": "views",
    "PROCEDURE": "procedures",
    "FUNCTION": "functions",
    "STREAM": "streams",
    "TASK": "tasks",
    "DYNAMIC_TABLE": "dynamic_tables",
    "STAGE": "stages",
    "PIPE": "pipes",
    "SEQUENCE": "sequences",
    "EXTERNAL_TABLE": "external_tables",
}


def sanitize_filename(name: str) -> str:
    """
    Sanitize object name for filesystem use by replacing special characters.

    Args:
        name: Object name to sanitize

    Returns:
        Sanitized filename-safe string

    Examples:
        >>> sanitize_filename('my-table')
        'my-table'
        >>> sanitize_filename('my table')
        'my table'
        >>> sanitize_filename('my/table')
        'my-table'
        >>> sanitize_filename('my:table')
        'my-table'
    """
    # Replace characters that are invalid or problematic in filenames
    # Replace /, \\, :, *, ?, ", <, >, | with -
    sanitized = re.sub(r'[\\/:"*?<>|]', '-', name)
    # Also replace multiple consecutive - with single -
    sanitized = re.sub(r'-+', '-', sanitized)
    # Strip leading/trailing - and whitespace
    sanitized = sanitized.strip('- ')
    return sanitized if sanitized else 'unnamed'


def pluralize_object_type(object_type: str) -> str:
    """
    Convert object type to its plural form.

    Args:
        object_type: Snowflake object type (e.g., 'TABLE', 'VIEW')

    Returns:
        Plural form (e.g., 'tables', 'views')

    Raises:
        ValueError: If object type is not recognized

    Examples:
        >>> pluralize_object_type('TABLE')
        'tables'
        >>> pluralize_object_type('VIEW')
        'views'
    """
    object_type_upper = object_type.upper()
    if object_type_upper not in OBJECT_TYPE_PLURALS:
        raise ValueError(f"Unknown object type: {object_type}. "
                        f"Valid types: {', '.join(OBJECT_TYPE_PLURALS.keys())}")
    return OBJECT_TYPE_PLURALS[object_type_upper]


def build_output_path(
    db: str,
    schema: str,
    object_type: str,
    object_name: str,
    ext: str,
    base_path: str = "discovery"
) -> Path:
    """
    Construct output file path for discovery files.

    Path format: {base_path}/{db}/{schema}/{object_type_plural}/{object_name}.{ext}

    Args:
        db: Database name
        schema: Schema name
        object_type: Snowflake object type (e.g., 'TABLE', 'VIEW')
        object_name: Object name
        ext: File extension (without leading dot, e.g., 'sql', 'json')
        base_path: Base directory for discovery files (default: 'discovery')

    Returns:
        Path object representing the full file path

    Examples:
        >>> build_output_path('ANALYTICS', 'PUBLIC', 'TABLE', 'users', 'sql')
        Path('discovery/ANALYTICS/PUBLIC/tables/users.sql')
        >>> build_output_path('ANALYTICS', 'PUBLIC', 'VIEW', 'v_orders', 'json')
        Path('discovery/ANALYTICS/PUBLIC/views/v_orders.json')
    """
    # Get plural form of object type
    type_plural = pluralize_object_type(object_type)

    # Sanitize object name for filesystem
    sanitized_name = sanitize_filename(object_name)

    # Construct path components
    path_parts = [base_path, db, schema, type_plural, f"{sanitized_name}.{ext}"]

    return Path(*path_parts)


def write_discovery_files(
    metadata: Dict[str, Any],
    ddl_content: str,
    json_content: str,
    base_path: str = "discovery"
) -> tuple[Path, Path]:
    """
    Write both DDL (.sql) and metadata (.json) discovery files.

    Creates directories as needed using os.makedirs(exist_ok=True).

    Args:
        metadata: Object metadata dict containing database, schema, object_type,
                 and object_name keys
        ddl_content: DDL content to write to .sql file
        json_content: JSON metadata content to write to .json file
        base_path: Base directory for discovery files (default: 'discovery')

    Returns:
        Tuple of (sql_file_path, json_file_path) as Path objects

    Raises:
        KeyError: If required metadata keys are missing
        IOError: If file writing fails

    Examples:
        >>> metadata = {
        ...     'database': 'ANALYTICS',
        ...     'schema': 'PUBLIC',
        ...     'object_type': 'TABLE',
        ...     'object_name': 'users'
        ... }
        >>> sql_path, json_path = write_discovery_files(
        ...     metadata, 'CREATE TABLE...', '{"schema": "..."}'
        ... )
        >>> sql_path
        Path('discovery/ANALYTICS/PUBLIC/tables/users.sql')
    """
    # Extract required metadata
    db = metadata.get("database")
    schema = metadata.get("schema")
    object_type = metadata.get("object_type")
    object_name = metadata.get("object_name")

    if not all([db, schema, object_type, object_name]):
        missing = [k for k in ["database", "schema", "object_type", "object_name"]
                   if not metadata.get(k)]
        raise KeyError(f"Missing required metadata keys: {', '.join(missing)}")

    # Type assertion: after the check above, all values are non-None strings
    db_str: str = db  # type: ignore[assignment]
    schema_str: str = schema  # type: ignore[assignment]
    object_type_str: str = object_type  # type: ignore[assignment]
    object_name_str: str = object_name  # type: ignore[assignment]

    # Build file paths
    sql_path = build_output_path(db_str, schema_str, object_type_str, object_name_str, "sql", base_path)
    json_path = build_output_path(db_str, schema_str, object_type_str, object_name_str, "json", base_path)

    # Create directories as needed
    sql_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    # Write files
    try:
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(ddl_content)
    except IOError as e:
        raise IOError(f"Failed to write SQL file {sql_path}: {e}")

    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(json_content)
    except IOError as e:
        raise IOError(f"Failed to write JSON file {json_path}: {e}")

    return sql_path, json_path
