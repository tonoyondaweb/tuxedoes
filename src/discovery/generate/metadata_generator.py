"""JSON metadata generator for Snowflake objects."""

from typing import Any, Dict, Union
from dataclasses import asdict, is_dataclass
from pathlib import Path

from discovery.types import (
    TableMetadata,
    ViewMetadata,
    ProcedureMetadata,
    StreamMetadata,
    TaskMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    TagAssignment,
    MaskingPolicy,
    VariantSchema,
)


def _build_ddl_file_path(
    database: str,
    schema: str,
    object_name: str,
    object_type: str
) -> str:
    """Build the DDL file path for cross-reference.

    Path format: {base_path}/{db}/{schema}/{object_type_plural}/{object_name}.sql
    """
    type_plural = {
        "TABLE": "tables",
        "VIEW": "views",
        "PROCEDURE": "procedures",
        "FUNCTION": "functions",
        "STREAM": "streams",
        "TASK": "tasks",
    }.get(object_type, object_type.lower() + "s")

    return f"{database}/{schema}/{type_plural}/{object_name}.sql"


def _serialize_column_metadata(column: ColumnMetadata) -> Dict[str, Any]:
    """Serialize column metadata to dict."""
    return {
        "name": column.name,
        "data_type": column.data_type,
        "nullable": column.nullable,
        "default_value": column.default_value,
        "comment": column.comment,
    }


def _serialize_constraint_metadata(constraint: ConstraintMetadata) -> Dict[str, Any]:
    """Serialize constraint metadata to dict."""
    return {
        "name": constraint.name,
        "type": constraint.type,
        "columns": constraint.columns,
        "referenced_table": constraint.referenced_table,
        "referenced_columns": constraint.referenced_columns,
    }


def _serialize_tag_assignment(tag: TagAssignment) -> Dict[str, Any]:
    """Serialize tag assignment to dict."""
    return {
        "tag_name": tag.tag_name,
        "tag_value": tag.tag_value,
        "column_name": tag.column_name,
    }


def _serialize_masking_policy(policy: MaskingPolicy) -> Dict[str, Any]:
    """Serialize masking policy to dict."""
    return {
        "policy_name": policy.policy_name,
        "signature": policy.signature,
        "column_name": policy.column_name,
    }


def _serialize_variant_schema(variant: VariantSchema) -> Dict[str, Any]:
    """Serialize variant schema to dict."""
    return {
        "column_name": variant.column_name,
        "inferred_structure": variant.inferred_structure,
        "sample_size": variant.sample_size,
        "confidence": variant.confidence,
    }


def generate_metadata_json(
    metadata: Union[
        TableMetadata,
        ViewMetadata,
        ProcedureMetadata,
        StreamMetadata,
        TaskMetadata,
    ],
    base_path: str = "discovery"
) -> Dict[str, Any]:
    """Generate JSON-serializable dict from metadata object.

    Args:
        metadata: Metadata object (TableMetadata, ViewMetadata, etc.)
        base_path: Base output path for DDL file cross-reference

    Returns:
        JSON-serializable dict with all metadata fields and ddl_file reference
    """
    if isinstance(metadata, TableMetadata):
        result = {
            "object_type": "TABLE",
            "name": metadata.name,
            "schema": metadata.schema,
            "database": metadata.database,
            "ddl": metadata.ddl,
            "columns": [_serialize_column_metadata(col) for col in metadata.columns],
            "row_count": metadata.row_count,
            "bytes": metadata.bytes,
            "last_ddl": metadata.last_ddl,
            "clustering_key": metadata.clustering_key,
            "constraints": [_serialize_constraint_metadata(c) for c in metadata.constraints],
            "tags": [_serialize_tag_assignment(t) for t in metadata.tags],
            "masking_policies": [_serialize_masking_policy(p) for p in metadata.masking_policies],
            "search_optimization": metadata.search_optimization,
        }

        # Add variant schema if present
        if metadata.variant_schema is not None:
            result["variant_schema"] = _serialize_variant_schema(metadata.variant_schema)

        # Add DDL file cross-reference
        ddl_path = _build_ddl_file_path(
            metadata.database,
            metadata.schema,
            metadata.name,
            "TABLE"
        )
        result["ddl_file"] = f"{base_path}/{ddl_path}"

        return result

    elif isinstance(metadata, ViewMetadata):
        result = {
            "object_type": "VIEW",
            "name": metadata.name,
            "schema": metadata.schema,
            "database": metadata.database,
            "ddl": metadata.ddl,
            "columns": [_serialize_column_metadata(col) for col in metadata.columns],
            "base_tables": metadata.base_tables,
            "last_ddl": metadata.last_ddl,
            "tags": [_serialize_tag_assignment(t) for t in metadata.tags],
        }

        # Add DDL file cross-reference
        ddl_path = _build_ddl_file_path(
            metadata.database,
            metadata.schema,
            metadata.name,
            "VIEW"
        )
        result["ddl_file"] = f"{base_path}/{ddl_path}"

        return result

    elif isinstance(metadata, ProcedureMetadata):
        result = {
            "object_type": "PROCEDURE",
            "name": metadata.name,
            "schema": metadata.schema,
            "database": metadata.database,
            "ddl": metadata.ddl,
            "parameters": metadata.parameters,
            "return_type": metadata.return_type,
            "language": metadata.language,
            "last_ddl": metadata.last_ddl,
        }

        # Add DDL file cross-reference
        ddl_path = _build_ddl_file_path(
            metadata.database,
            metadata.schema,
            metadata.name,
            "PROCEDURE"
        )
        result["ddl_file"] = f"{base_path}/{ddl_path}"

        return result

    elif isinstance(metadata, StreamMetadata):
        result = {
            "object_type": "STREAM",
            "name": metadata.name,
            "schema": metadata.schema,
            "database": metadata.database,
            "ddl": metadata.ddl,
            "source_object": metadata.source_object,
            "mode": metadata.mode,
            "last_ddl": metadata.last_ddl,
        }

        # Add DDL file cross-reference
        ddl_path = _build_ddl_file_path(
            metadata.database,
            metadata.schema,
            metadata.name,
            "STREAM"
        )
        result["ddl_file"] = f"{base_path}/{ddl_path}"

        return result

    elif isinstance(metadata, TaskMetadata):
        result = {
            "object_type": "TASK",
            "name": metadata.name,
            "schema": metadata.schema,
            "database": metadata.database,
            "ddl": metadata.ddl,
            "schedule": metadata.schedule,
            "state": metadata.state,
            "predecessors": metadata.predecessors,
            "last_ddl": metadata.last_ddl,
        }

        # Add DDL file cross-reference
        ddl_path = _build_ddl_file_path(
            metadata.database,
            metadata.schema,
            metadata.name,
            "TASK"
        )
        result["ddl_file"] = f"{base_path}/{ddl_path}"

        return result

    else:
        raise TypeError(f"Unsupported metadata type: {type(metadata)}")
