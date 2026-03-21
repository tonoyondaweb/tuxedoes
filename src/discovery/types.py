"""Data type definitions for Snowflake metadata."""

from typing import TypedDict, Optional, Dict, List, Any
from dataclasses import dataclass, field


@dataclass
class TableMetadata:
    """Metadata for a Snowflake table."""
    name: str
    schema: str
    database: str
    ddl: str
    columns: List['ColumnMetadata']
    row_count: int
    bytes: int
    last_ddl: str
    clustering_key: Optional[str]
    constraints: List['ConstraintMetadata']
    tags: List['TagAssignment']
    masking_policies: List['MaskingPolicy']
    search_optimization: bool
    variant_schema: Optional['VariantSchema']


@dataclass
class ColumnMetadata:
    """Metadata for a Snowflake column."""
    name: str
    data_type: str
    nullable: bool
    default_value: Optional[str]
    comment: Optional[str]


@dataclass
class ConstraintMetadata:
    """Metadata for a table constraint."""
    name: str
    type: str  # PK/FK/UK
    columns: List[str]
    referenced_table: Optional[str]
    referenced_columns: Optional[List[str]]


@dataclass
class TagAssignment:
    """Tag assignment to an object."""
    tag_name: str
    tag_value: str
    column_name: Optional[str]


@dataclass
class MaskingPolicy:
    """Masking policy applied to a column."""
    policy_name: str
    signature: str
    column_name: str


@dataclass
class VariantSchema:
    """Inferred schema from a VARIANT column."""
    column_name: str
    inferred_structure: Dict[str, Any]
    sample_size: int
    confidence: float


@dataclass
class ViewMetadata:
    """Metadata for a Snowflake view."""
    name: str
    schema: str
    database: str
    ddl: str
    columns: List['ColumnMetadata']
    base_tables: List[str]
    last_ddl: str
    tags: List['TagAssignment']


@dataclass
class ProcedureMetadata:
    """Metadata for a Snowflake stored procedure."""
    name: str
    schema: str
    database: str
    ddl: str
    parameters: List[Dict[str, Any]]
    return_type: Optional[str]
    language: Optional[str]
    last_ddl: str


@dataclass
class StreamMetadata:
    """Metadata for a Snowflake stream."""
    name: str
    schema: str
    database: str
    ddl: str
    source_object: str
    mode: str
    last_ddl: str


@dataclass
class TaskMetadata:
    """Metadata for a Snowflake task."""
    name: str
    schema: str
    database: str
    ddl: str
    schedule: str
    state: str
    predecessors: List[str]
    last_ddl: str


@dataclass
class DiscoveryManifest:
    """Manifest for a discovery run."""
    format_version: str
    generated_at: str
    snowflake_account: str
    config_hash: str
    object_count: int
    errors: List['DiscoveryError']


@dataclass
class DiscoveryError:
    """Error encountered during discovery."""
    object_name: str
    object_type: str
    error_message: str
    retry_count: int
