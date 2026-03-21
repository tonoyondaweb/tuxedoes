"""SQL query builders for Snowflake metadata extraction.

This module provides functions that return SQL query strings for extracting
various metadata from Snowflake. Note that these functions return SQL strings,
not query results — execution is handled by the connection module.

ACCOUNT_USAGE views have ~45 minute latency. For near real-time metadata,
use INFORMATION_SCHEMA views where available.
"""

from typing import Literal


def get_ddl_query(object_type: str, object_name: str) -> str:
    """Generate GET_DDL query for a Snowflake object.

    Args:
        object_type: Type of object (e.g., 'TABLE', 'VIEW', 'PROCEDURE')
        object_name: Fully qualified object name (e.g., 'DB.SCHEMA.TABLE')

    Returns:
        SQL string using GET_DDL function

    Example:
        >>> get_ddl_query('TABLE', 'MY_DB.MY_SCHEMA.MY_TABLE')
        "SELECT GET_DDL('TABLE', 'MY_DB.MY_SCHEMA.MY_TABLE')"
    """
    return f"SELECT GET_DDL('{object_type}', '{object_name}')"


def list_tables_query(schema: str) -> str:
    """Generate query to list all tables in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.TABLES

    Example:
        >>> list_tables_query('PUBLIC')
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'PUBLIC' AND table_type = 'BASE TABLE'"
    """
    return f"""
        SELECT
            table_name,
            table_type,
            table_comment,
            is_typed
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema}'
        ORDER BY table_name
    """


def list_columns_query(schema: str, table: str) -> str:
    """Generate query to list all columns in a table.

    Args:
        schema: Schema name (not fully qualified)
        table: Table name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.COLUMNS

    Example:
        >>> list_columns_query('PUBLIC', 'USERS')
        "SELECT ... FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'PUBLIC' AND table_name = 'USERS'"
    """
    return f"""
        SELECT
            column_name,
            ordinal_position,
            column_default,
            is_nullable,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            comment
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
    """


def list_constraints_query(schema: str) -> str:
    """Generate query to list all constraints in a schema.

    Queries TABLE_CONSTRAINTS and KEY_COLUMN_USAGE to get both
    constraint definitions and column mappings.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.TABLE_CONSTRAINTS and KEY_COLUMN_USAGE

    Example:
        >>> list_constraints_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN ... WHERE tc.table_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            tc.constraint_name,
            tc.table_name,
            tc.constraint_type,
            kcu.column_name,
            kcu.ordinal_position,
            rc.unique_constraint_schema AS referenced_schema,
            rc.unique_constraint_name AS referenced_constraint,
            cc.table_name AS referenced_table,
            cc.column_name AS referenced_column
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.constraint_schema = kcu.constraint_schema
            AND tc.constraint_name = kcu.constraint_name
            AND tc.table_name = kcu.table_name
        LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            ON tc.constraint_schema = rc.constraint_schema
            AND tc.constraint_name = rc.constraint_name
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE cc
            ON rc.unique_constraint_schema = cc.constraint_schema
            AND rc.unique_constraint_name = cc.constraint_name
        WHERE tc.table_schema = '{schema}'
        ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
    """


def list_tags_query(schema: str) -> str:
    """Generate query to list all tag assignments in a schema.

    NOTE: ACCOUNT_USAGE.TAG_REFERENCES has ~45 minute latency.

    Args:
        schema: Schema name (fully qualified with database)

    Returns:
        SQL string querying SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES

    Example:
        >>> list_tags_query('MY_DB.MY_SCHEMA')
        "SELECT ... FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE ... LIKE '%MY_SCHEMA%'"
    """
    return f"""
        SELECT
            object_name,
            object_domain,
            tag_name,
            tag_value,
            level
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE object_domain IN ('TABLE', 'COLUMN', 'VIEW', 'PROCEDURE', 'FUNCTION', 'STREAM', 'TASK')
          AND object_name LIKE '%.{schema}.%'
        ORDER BY object_name, tag_name
    """


def list_masking_policies_query(schema: str) -> str:
    """Generate query to list all masking policies in a schema.

    NOTE: ACCOUNT_USAGE.MASKING_POLICIES has ~45 minute latency.

    Args:
        schema: Schema name (fully qualified with database)

    Returns:
        SQL string querying SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES

    Example:
        >>> list_masking_policies_query('MY_DB.MY_SCHEMA')
        "SELECT ... FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES WHERE ... LIKE '%MY_SCHEMA%'"
    """
    return f"""
        SELECT
            policy_name,
            policy_schema,
            policy_database,
            entry,
            argument_type
        FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES
        WHERE policy_schema = '{schema.split('.')[-1]}'
        ORDER BY policy_name
    """


def get_variant_columns_query(schema: str, table: str) -> str:
    """Generate query to identify VARIANT type columns in a table.

    Args:
        schema: Schema name (not fully qualified)
        table: Table name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.COLUMNS for VARIANT data type

    Example:
        >>> get_variant_columns_query('PUBLIC', 'USERS')
        "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE ... AND data_type = 'VARIANT'"
    """
    return f"""
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
          AND data_type = 'VARIANT'
        ORDER BY ordinal_position
    """


def get_table_storage_query(schema: str) -> str:
    """Generate query to get storage metrics for tables in a schema.

    NOTE: ACCOUNT_USAGE.TABLE_STORAGE_METRICS has ~45 minute latency.

    Args:
        schema: Schema name (fully qualified with database)

    Returns:
        SQL string querying SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS

    Example:
        >>> get_table_storage_query('MY_DB.MY_SCHEMA')
        "SELECT ... FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS WHERE ... LIKE '%MY_SCHEMA%'"
    """
    return f"""
        SELECT
            table_name,
            table_schema,
            database_name,
            active_bytes,
            time_travel_bytes,
            failsafe_bytes,
            retention_time,
            is_external
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
        WHERE table_schema = '{schema.split('.')[-1]}'
        ORDER BY table_name
    """


def list_views_query(schema: str) -> str:
    """Generate query to list all views in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.VIEWS

    Example:
        >>> list_views_query('PUBLIC')
        "SELECT table_name FROM INFORMATION_SCHEMA.VIEWS WHERE table_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            table_name,
            view_definition,
            check_option,
            is_updatable,
            is_insertable_into,
            is_trigger_updatable,
            is_trigger_deletable,
            is_trigger_insertable_into,
            table_comment
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE table_schema = '{schema}'
        ORDER BY table_name
    """


def list_procedures_query(schema: str) -> str:
    """Generate query to list all stored procedures in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.PROCEDURES

    Example:
        >>> list_procedures_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.PROCEDURES WHERE routine_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            specific_name,
            routine_name,
            routine_type,
            data_type,
            is_deterministic,
            external_language,
            parameter_style,
            is_null_call,
            sql_data_access,
            is_udf,
            routine_body,
            routine_definition,
            security_type,
            created,
            last_altered,
            comment
        FROM INFORMATION_SCHEMA.PROCEDURES
        WHERE routine_schema = '{schema}'
        ORDER BY routine_name
    """


def list_functions_query(schema: str) -> str:
    """Generate query to list all functions in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.FUNCTIONS

    Example:
        >>> list_functions_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.FUNCTIONS WHERE routine_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            specific_name,
            routine_name,
            routine_type,
            data_type,
            is_deterministic,
            external_language,
            parameter_style,
            is_null_call,
            sql_data_access,
            is_udf,
            routine_body,
            routine_definition,
            security_type,
            created,
            last_altered,
            comment
        FROM INFORMATION_SCHEMA.FUNCTIONS
        WHERE routine_schema = '{schema}'
        ORDER BY routine_name
    """


def list_streams_query(schema: str) -> str:
    """Generate query to list all streams in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.STREAMS

    Example:
        >>> list_streams_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.STREAMS WHERE table_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            table_name,
            table_schema,
            database_name,
            stale,
            mode,
            comment
        FROM INFORMATION_SCHEMA.STREAMS
        WHERE table_schema = '{schema}'
        ORDER BY table_name
    """


def list_tasks_query(schema: str) -> str:
    """Generate query to list all tasks in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.TASKS

    Example:
        >>> list_tasks_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.TASKS WHERE schema_name = 'PUBLIC'"
    """
    return f"""
        SELECT
            name,
            schema_name,
            database_name,
            owner,
            comment,
            warehouse,
            schedule,
            state,
            condition,
            definition,
            last_suspended_on,
            last_committed_on,
            last_suspended_reason
        FROM INFORMATION_SCHEMA.TASKS
        WHERE schema_name = '{schema}'
        ORDER BY name
    """


def list_stages_query(schema: str) -> str:
    """Generate query to list all stages in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.STAGES

    Example:
        >>> list_stages_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.STAGES WHERE stage_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            stage_name,
            stage_type,
            stage_catalog,
            stage_schema,
            stage_url,
            storage_provider,
            region,
            is_directory,
            encryption,
            comment
        FROM INFORMATION_SCHEMA.STAGES
        WHERE stage_schema = '{schema}'
        ORDER BY stage_name
    """


def list_pipes_query(schema: str) -> str:
    """Generate query to list all pipes in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.PIPES

    Example:
        >>> list_pipes_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.PIPES WHERE pipe_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            pipe_name,
            pipe_schema,
            pipe_catalog,
            definition,
            owner,
            created,
            last_altered,
            comment
        FROM INFORMATION_SCHEMA.PIPES
        WHERE pipe_schema = '{schema}'
        ORDER BY pipe_name
    """


def list_sequences_query(schema: str) -> str:
    """Generate query to list all sequences in a schema.

    Args:
        schema: Schema name (not fully qualified)

    Returns:
        SQL string querying INFORMATION_SCHEMA.SEQUENCES

    Example:
        >>> list_sequences_query('PUBLIC')
        "SELECT ... FROM INFORMATION_SCHEMA.SEQUENCES WHERE sequence_schema = 'PUBLIC'"
    """
    return f"""
        SELECT
            sequence_name,
            sequence_schema,
            sequence_catalog,
            start_value,
            minimum_value,
            maximum_value,
            increment,
            cycle_option,
            comment
        FROM INFORMATION_SCHEMA.SEQUENCES
        WHERE sequence_schema = '{schema}'
        ORDER BY sequence_name
    """
