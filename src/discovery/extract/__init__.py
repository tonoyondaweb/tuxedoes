"""Discovery extract module for Snowflake metadata extraction."""

# Import queries module (this task)
from .queries import (
    get_ddl_query,
    list_tables_query,
    list_columns_query,
    list_constraints_query,
    list_tags_query,
    list_masking_policies_query,
    get_variant_columns_query,
    get_table_storage_query,
)

# Import connection module if it exists (Task 4)
try:
    from .connection import SnowflakeConnection, connect
    _has_connection = True
except ImportError:
    # Connection module not yet implemented (parallel task)
    _has_connection = False

__all__ = [
    "get_ddl_query",
    "list_tables_query",
    "list_columns_query",
    "list_constraints_query",
    "list_tags_query",
    "list_masking_policies_query",
    "get_variant_columns_query",
    "get_table_storage_query",
]

if _has_connection:
    __all__.extend(["SnowflakeConnection", "connect"])
