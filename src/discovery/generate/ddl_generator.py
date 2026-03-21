"""DDL file generator for Snowflake objects with metadata comments."""

from typing import Union, List, Optional
from datetime import datetime

from ..types import (
    TableMetadata,
    ViewMetadata,
    ProcedureMetadata,
    StreamMetadata,
    TaskMetadata,
)


def generate_ddl_file(metadata: Union[TableMetadata, ViewMetadata, ProcedureMetadata, StreamMetadata, TaskMetadata]) -> str:
    """Generate DDL file content with metadata comments.

    Args:
        metadata: Object metadata (Table, View, Procedure, Stream, or Task)

    Returns:
        DDL file content with header and footer comments
    """
    # Route to appropriate generator based on object type
    if isinstance(metadata, TableMetadata):
        return _generate_table_ddl(metadata)
    elif isinstance(metadata, ViewMetadata):
        return _generate_view_ddl(metadata)
    elif isinstance(metadata, ProcedureMetadata):
        return _generate_procedure_ddl(metadata)
    elif isinstance(metadata, StreamMetadata):
        return _generate_stream_ddl(metadata)
    elif isinstance(metadata, TaskMetadata):
        return _generate_task_ddl(metadata)
    else:
        raise ValueError(f"Unsupported metadata type: {type(metadata)}")


def _generate_header_comment(
    name: str,
    object_type: str,
    database: str,
    schema: str
) -> str:
    """Generate header comment block for DDL file."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    return f"""-- ============================================
-- DDL for {object_type}: {name}
-- Database: {database}
-- Schema: {schema}
-- Generated: {timestamp}
-- ============================================"""


def _generate_footer_comment_table(metadata: TableMetadata) -> str:
    """Generate footer comment with table metadata."""
    lines = [
        "-- ============================================",
        "-- Metadata Statistics",
        f"-- Row Count: {metadata.row_count:,}",
        f"-- Byte Size: {metadata.bytes:,}",
        f"-- Last DDL: {metadata.last_ddl}",
    ]

    if metadata.tags:
        lines.append("-- Tags:")
        for tag in metadata.tags:
            col_info = f" [column: {tag.column_name}]" if tag.column_name else ""
            lines.append(f"--   - {tag.tag_name} = {tag.tag_value}{col_info}")

    if metadata.masking_policies:
        lines.append("-- Masking Policies:")
        for policy in metadata.masking_policies:
            lines.append(f"--   - {policy.policy_name}({policy.signature}) on column {policy.column_name}")

    if metadata.search_optimization:
        lines.append("-- Search Optimization: Enabled")

    if metadata.variant_schema:
        lines.append(f"-- VARIANT Schema: Inferred from {metadata.variant_schema.sample_size:,} samples (confidence: {metadata.variant_schema.confidence:.2%})")

    lines.append("-- ============================================")
    return "\n".join(lines)


def _generate_footer_comment_generic(
    last_ddl: str,
    tags: Optional[List] = None
) -> str:
    """Generate generic footer comment for non-table objects."""
    if tags is None:
        tags = []
    lines = [
        "-- ============================================",
        f"-- Last DDL: {last_ddl}",
    ]

    if tags:
        lines.append("-- Tags:")
        for tag in tags:
            col_info = f" [column: {tag.column_name}]" if hasattr(tag, 'column_name') and tag.column_name else ""
            lines.append(f"--   - {tag.tag_name} = {tag.tag_value}{col_info}")

    lines.append("-- ============================================")
    return "\n".join(lines)


def _generate_table_ddl(metadata: TableMetadata) -> str:
    """Generate DDL file for a table with inline comments."""
    header = _generate_header_comment(metadata.name, "TABLE", metadata.database, metadata.schema)
    footer = _generate_footer_comment_table(metadata)

    # Parse DDL to add inline comments on key columns
    ddl_with_comments = _add_inline_comments_to_table_ddl(
        metadata.ddl,
        metadata.columns,
        metadata.constraints,
        metadata.clustering_key or ""
    )

    return f"{header}\n\n{ddl_with_comments}\n\n{footer}"


def _add_inline_comments_to_table_ddl(
    ddl: str,
    columns: List,
    constraints: List,
    clustering_key: str = ""
) -> str:
    """Add inline comments to table DDL for key columns.

    Comments are added on separate lines before the relevant column definition.
    """
    # Build mapping of column names to comment prefixes
    column_comments = {}

    # Primary key comments
    for constraint in constraints:
        if constraint.type == "PRIMARY KEY" or constraint.type == "PK":
            for col in constraint.columns:
                column_comments[col] = column_comments.get(col, "") + " [PRIMARY KEY]"

    # Foreign key comments
    for constraint in constraints:
        if constraint.type == "FOREIGN KEY" or constraint.type == "FK":
            for col in constraint.columns:
                ref_info = ""
                if constraint.referenced_table:
                    ref_info = f" -> {constraint.referenced_table}"
                column_comments[col] = column_comments.get(col, "") + f" [FOREIGN KEY{ref_info}]"

    # Clustering key comment
    if clustering_key:
        for col_name in clustering_key.split(","):
            col_name = col_name.strip().strip('"')
            column_comments[col_name] = column_comments.get(col_name, "") + " [CLUSTERING KEY]"

    # If no comments to add, return DDL as-is
    if not column_comments:
        return ddl

    # Add inline comments before column definitions
    # This is a simple approach that works for standard DDL format
    lines = ddl.split("\n")
    result = []

    for line in lines:
        # Check if this line defines a column
        for col_name, comment in column_comments.items():
            # Match column definition (e.g., "col_name TYPE" or "    col_name TYPE")
            import re
            if re.search(rf'\b{re.escape(col_name)}\s+[A-Z]', line, re.IGNORECASE):
                # Add comment on separate line before column definition
                indent = len(line) - len(line.lstrip())
                result.append(" " * indent + f"-- {comment}")
                column_comments.pop(col_name)  # Only comment once
                break
        result.append(line)

    return "\n".join(result)


def _generate_view_ddl(metadata: ViewMetadata) -> str:
    """Generate DDL file for a view."""
    header = _generate_header_comment(metadata.name, "VIEW", metadata.database, metadata.schema)
    footer = _generate_footer_comment_generic(metadata.last_ddl, metadata.tags)

    # Add base tables comment before DDL
    ddl = metadata.ddl
    if metadata.base_tables:
        base_tables_str = ", ".join(metadata.base_tables)
        ddl = f"-- Base Tables: {base_tables_str}\n{ddl}"

    return f"{header}\n\n{ddl}\n\n{footer}"


def _generate_procedure_ddl(metadata: ProcedureMetadata) -> str:
    """Generate DDL file for a stored procedure."""
    header = _generate_header_comment(metadata.name, "PROCEDURE", metadata.database, metadata.schema)
    footer = _generate_footer_comment_generic(metadata.last_ddl)

    # Add parameters comment
    ddl = metadata.ddl
    if metadata.parameters:
        params = []
        for param in metadata.parameters:
            param_str = f"{param.get('name', '')} {param.get('type', '')}"
            if param.get('default'):
                param_str += f" DEFAULT {param.get('default')}"
            params.append(param_str)
        if params:
            ddl = f"-- Parameters: {', '.join(params)}\n{ddl}"

    return f"{header}\n\n{ddl}\n\n{footer}"


def _generate_stream_ddl(metadata: StreamMetadata) -> str:
    """Generate DDL file for a stream."""
    header = _generate_header_comment(metadata.name, "STREAM", metadata.database, metadata.schema)
    footer = _generate_footer_comment_generic(metadata.last_ddl)

    # Add stream metadata comment
    ddl = metadata.ddl
    metadata_comment = f"-- Source Object: {metadata.source_object}\n-- Stream Mode: {metadata.mode}"
    ddl = f"{metadata_comment}\n{ddl}"

    return f"{header}\n\n{ddl}\n\n{footer}"


def _generate_task_ddl(metadata: TaskMetadata) -> str:
    """Generate DDL file for a task."""
    header = _generate_header_comment(metadata.name, "TASK", metadata.database, metadata.schema)
    footer = _generate_footer_comment_generic(metadata.last_ddl)

    # Add task metadata comment
    ddl = metadata.ddl
    metadata_lines = [
        f"-- Schedule: {metadata.schedule}",
        f"-- State: {metadata.state}",
    ]
    if metadata.predecessors:
        metadata_lines.append(f"-- Predecessors: {', '.join(metadata.predecessors)}")
    ddl = "\n".join(metadata_lines) + "\n" + ddl

    return f"{header}\n\n{ddl}\n\n{footer}"
