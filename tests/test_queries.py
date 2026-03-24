"""Tests for SQL query generation."""

from discovery.extract.queries import (
    get_ddl_query,
    list_tables_query,
    list_columns_query,
    list_constraints_query,
    list_tags_query,
    list_masking_policies_query,
    get_variant_columns_query,
    get_table_storage_query,
    list_views_query,
    list_procedures_query,
    list_functions_query,
    list_streams_query,
    list_tasks_query,
    list_stages_query,
    list_pipes_query,
    list_sequences_query,
)


def test_get_ddl_query_table():
    """Test GET_DDL query for TABLE."""
    query = get_ddl_query("TABLE", "MY_DB.MY_SCHEMA.MY_TABLE")

    assert "SELECT GET_DDL('TABLE', 'MY_DB.MY_SCHEMA.MY_TABLE')" in query


def test_get_ddl_query_view():
    """Test GET_DDL query for VIEW."""
    query = get_ddl_query("VIEW", "MY_DB.MY_SCHEMA.MY_VIEW")

    assert "SELECT GET_DDL('VIEW', 'MY_DB.MY_SCHEMA.MY_VIEW')" in query


def test_get_ddl_query_procedure():
    """Test GET_DDL query for PROCEDURE."""
    query = get_ddl_query("PROCEDURE", "MY_DB.MY_SCHEMA.MY_PROCEDURE")

    assert "SELECT GET_DDL('PROCEDURE', 'MY_DB.MY_SCHEMA.MY_PROCEDURE')" in query


def test_list_tables_query():
    """Test list_tables_query generates correct SQL."""
    query = list_tables_query("PUBLIC")

    assert "SELECT" in query
    assert "table_name" in query
    assert "table_type" in query
    assert "table_comment" in query
    assert "is_typed" in query
    assert "FROM INFORMATION_SCHEMA.TABLES" in query
    assert "WHERE table_schema = 'PUBLIC'" in query
    assert "ORDER BY table_name" in query


def test_list_tables_query_with_staging_schema():
    """Test list_tables_query with STAGING schema."""
    query = list_tables_query("STAGING")

    assert "WHERE table_schema = 'STAGING'" in query


def test_list_columns_query():
    """Test list_columns_query generates correct SQL."""
    query = list_columns_query("PUBLIC", "USERS")

    assert "SELECT" in query
    assert "column_name" in query
    assert "ordinal_position" in query
    assert "column_default" in query
    assert "is_nullable" in query
    assert "data_type" in query
    assert "character_maximum_length" in query
    assert "numeric_precision" in query
    assert "numeric_scale" in query
    assert "datetime_precision" in query
    assert "comment" in query
    assert "FROM INFORMATION_SCHEMA.COLUMNS" in query
    assert "WHERE table_schema = 'PUBLIC'" in query
    assert "AND table_name = 'USERS'" in query
    assert "ORDER BY ordinal_position" in query


def test_list_columns_query_with_nested_schema():
    """Test list_columns_query with nested schema name."""
    query = list_columns_query("STAGING.CLEANED", "ORDERS")

    assert "WHERE table_schema = 'STAGING.CLEANED'" in query
    assert "AND table_name = 'ORDERS'" in query


def test_list_constraints_query():
    """Test list_constraints_query generates correct SQL."""
    query = list_constraints_query("PUBLIC")

    assert "SELECT" in query
    assert "tc.constraint_name" in query
    assert "tc.table_name" in query
    assert "tc.constraint_type" in query
    assert "kcu.column_name" in query
    assert "kcu.ordinal_position" in query
    assert "rc.unique_constraint_schema" in query
    assert "rc.unique_constraint_name" in query
    assert "cc.table_name" in query
    assert "cc.column_name" in query
    assert "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc" in query
    assert "LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu" in query
    assert "LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc" in query
    assert "WHERE tc.table_schema = 'PUBLIC'" in query
    assert "ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position" in query


def test_list_tags_query():
    """Test list_tags_query generates correct SQL."""
    query = list_tags_query("MY_DB.MY_SCHEMA")

    assert "SELECT" in query
    assert "object_name" in query
    assert "object_domain" in query
    assert "tag_name" in query
    assert "tag_value" in query
    assert "level" in query
    assert "FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES" in query
    assert "WHERE object_domain IN ('TABLE', 'COLUMN', 'VIEW', 'PROCEDURE', 'FUNCTION', 'STREAM', 'TASK')" in query
    assert "AND object_name LIKE '%.MY_DB.MY_SCHEMA.%'" in query
    assert "ORDER BY object_name, tag_name" in query


def test_list_tags_query_with_simple_schema():
    """Test list_tags_query with simple schema name."""
    query = list_tags_query("PUBLIC")

    assert "AND object_name LIKE '%.PUBLIC.%'" in query


def test_list_masking_policies_query():
    """Test list_masking_policies_query generates correct SQL."""
    query = list_masking_policies_query("MY_DB.MY_SCHEMA")

    assert "SELECT" in query
    assert "policy_name" in query
    assert "policy_schema" in query
    assert "policy_database" in query
    assert "entry" in query
    assert "argument_type" in query
    assert "FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES" in query
    assert "WHERE policy_schema = 'MY_SCHEMA'" in query
    assert "ORDER BY policy_name" in query


def test_list_masking_policies_query_simple_schema():
    """Test list_masking_policies_query extracts schema from qualified name."""
    query = list_masking_policies_query("MY_DB.PUBLIC")

    assert "WHERE policy_schema = 'PUBLIC'" in query


def test_get_variant_columns_query():
    """Test get_variant_columns_query generates correct SQL."""
    query = get_variant_columns_query("PUBLIC", "USERS")

    assert "SELECT" in query
    assert "column_name" in query
    assert "data_type" in query
    assert "is_nullable" in query
    assert "FROM INFORMATION_SCHEMA.COLUMNS" in query
    assert "WHERE table_schema = 'PUBLIC'" in query
    assert "AND table_name = 'USERS'" in query
    assert "AND data_type = 'VARIANT'" in query
    assert "ORDER BY ordinal_position" in query


def test_get_table_storage_query():
    """Test get_table_storage_query generates correct SQL."""
    query = get_table_storage_query("MY_DB.MY_SCHEMA")

    assert "SELECT" in query
    assert "table_name" in query
    assert "table_schema" in query
    assert "database_name" in query
    assert "active_bytes" in query
    assert "time_travel_bytes" in query
    assert "failsafe_bytes" in query
    assert "retention_time" in query
    assert "is_external" in query
    assert "FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS" in query
    assert "WHERE table_schema = 'MY_SCHEMA'" in query
    assert "ORDER BY table_name" in query


def test_get_table_storage_query_simple_schema():
    """Test get_table_storage_query extracts schema from qualified name."""
    query = get_table_storage_query("PUBLIC")

    assert "WHERE table_schema = 'PUBLIC'" in query


def test_list_views_query():
    """Test list_views_query generates correct SQL."""
    query = list_views_query("PUBLIC")

    assert "SELECT" in query
    assert "table_name" in query
    assert "view_definition" in query
    assert "FROM INFORMATION_SCHEMA.VIEWS" in query
    assert "WHERE table_schema = 'PUBLIC'" in query
    assert "ORDER BY table_name" in query


def test_list_procedures_query():
    """Test list_procedures_query generates correct SQL."""
    query = list_procedures_query("PUBLIC")

    assert "SELECT" in query
    assert "procedure_name" in query
    assert "procedure_schema" in query
    assert "procedure_catalog" in query
    assert "argument_signature" in query
    assert "is_udf" in query
    assert "FROM INFORMATION_SCHEMA.PROCEDURES" in query
    assert "WHERE procedure_schema = 'PUBLIC'" in query
    assert "ORDER BY procedure_name" in query


def test_list_functions_query():
    """Test list_functions_query generates correct SQL."""
    query = list_functions_query("PUBLIC")

    assert "SELECT" in query
    assert "function_name" in query
    assert "function_schema" in query
    assert "argument_signature" in query
    assert "data_type" in query
    assert "FROM INFORMATION_SCHEMA.FUNCTIONS" in query
    assert "WHERE function_schema = 'PUBLIC'" in query
    assert "ORDER BY function_name" in query


def test_list_streams_query():
    """Test list_streams_query generates correct SQL."""
    query = list_streams_query("PUBLIC")

    assert "SELECT" in query
    assert "table_name" in query
    assert "schema_name" in query
    assert "database_name" in query
    assert "owner" in query
    assert "comment" in query
    assert "FROM INFORMATION_SCHEMA.STREAMS" in query
    assert "WHERE schema_name = 'PUBLIC'" in query
    assert "ORDER BY table_name" in query


def test_list_tasks_query():
    """Test list_tasks_query generates correct SQL."""
    query = list_tasks_query("PUBLIC")

    assert "SELECT" in query
    assert "name" in query
    assert "schema_name" in query
    assert "database_name" in query
    assert "owner" in query
    assert "schedule" in query
    assert "state" in query
    assert "definition" in query
    assert "FROM INFORMATION_SCHEMA.TASKS" in query
    assert "WHERE schema_name = 'PUBLIC'" in query
    assert "ORDER BY name" in query


def test_list_stages_query():
    """Test list_stages_query generates correct SQL."""
    query = list_stages_query("PUBLIC")

    assert "SELECT" in query
    assert "stage_name" in query
    assert "stage_schema" in query
    assert "stage_type" in query
    assert "owner" in query
    assert "comment" in query
    assert "FROM INFORMATION_SCHEMA.STAGES" in query
    assert "WHERE stage_schema = 'PUBLIC'" in query
    assert "ORDER BY stage_name" in query


def test_list_pipes_query():
    """Test list_pipes_query generates correct SQL."""
    query = list_pipes_query("PUBLIC")

    assert "SELECT" in query
    assert "pipe_name" in query
    assert "pipe_schema" in query
    assert "owner" in query
    assert "definition" in query
    assert "FROM INFORMATION_SCHEMA.PIPES" in query
    assert "WHERE pipe_schema = 'PUBLIC'" in query
    assert "ORDER BY pipe_name" in query


def test_list_sequences_query():
    """Test list_sequences_query generates correct SQL."""
    query = list_sequences_query("PUBLIC")

    assert "SELECT" in query
    assert "sequence_name" in query
    assert "sequence_schema" in query
    assert "owner" in query
    assert "comment" in query
    assert "FROM INFORMATION_SCHEMA.SEQUENCES" in query
    assert "WHERE sequence_schema = 'PUBLIC'" in query
    assert "ORDER BY sequence_name" in query


def test_query_functions_return_strings():
    """Test that all query functions return strings."""
    queries = [
        get_ddl_query("TABLE", "DB.SCHEMA.TABLE"),
        list_tables_query("PUBLIC"),
        list_columns_query("PUBLIC", "USERS"),
        list_constraints_query("PUBLIC"),
        list_tags_query("DB.PUBLIC"),
        list_masking_policies_query("DB.PUBLIC"),
        get_variant_columns_query("PUBLIC", "USERS"),
        get_table_storage_query("DB.PUBLIC"),
        list_views_query("PUBLIC"),
        list_procedures_query("PUBLIC"),
        list_functions_query("PUBLIC"),
        list_streams_query("PUBLIC"),
        list_tasks_query("PUBLIC"),
        list_stages_query("PUBLIC"),
        list_pipes_query("PUBLIC"),
        list_sequences_query("PUBLIC"),
    ]

    for query in queries:
        assert isinstance(query, str)
        assert len(query) > 0


def test_queries_do_not_execute():
    """Test that query functions only return SQL strings, not execute."""
    # This is a design verification test
    # The functions should not execute queries, only generate SQL strings
    query = get_ddl_query("TABLE", "DB.SCHEMA.TABLE")
    assert query.startswith("SELECT")
    assert "GET_DDL" in query
    # If this function executed, it would raise an exception or return a result set
    # But it just returns a string


def test_queries_use_proper_sql_syntax():
    """Test that all generated queries use proper SQL syntax."""
    queries = [
        get_ddl_query("TABLE", "DB.SCHEMA.TABLE"),
        list_tables_query("PUBLIC"),
        list_columns_query("PUBLIC", "USERS"),
    ]

    for query in queries:
        # Check that queries start with SELECT or contain valid SQL keywords
        assert query.strip().startswith("SELECT") or "CREATE" in query.upper()
        # Check for proper statement termination (if present)
        if ";" in query:
            assert query.strip().endswith(";") or ";" in query.split("\n")[-1]


def test_queries_handle_special_characters_in_names():
    """Test that queries properly handle schema/table names with special characters."""
    # Test with schema name that has underscores
    query = list_tables_query("STAGING_AREA")
    assert "WHERE table_schema = 'STAGING_AREA'" in query

    # Test with table name that has underscores
    query = list_columns_query("PUBLIC", "USER_PROFILE")
    assert "AND table_name = 'USER_PROFILE'" in query


def test_queries_are_case_sensitive():
    """Test that queries preserve case in schema/table names."""
    query1 = list_tables_query("PUBLIC")
    query2 = list_tables_query("public")

    assert "table_schema = 'PUBLIC'" in query1
    assert "table_schema = 'public'" in query2
    assert query1 != query2


def test_all_object_types_supported_in_get_ddl():
    """Test that get_ddl_query supports all Snowflake object types."""
    object_types = [
        "TABLE", "VIEW", "PROCEDURE", "FUNCTION", "STREAM", "TASK",
        "DYNAMIC_TABLE", "STAGE", "PIPE", "SEQUENCE", "EXTERNAL_TABLE"
    ]

    for obj_type in object_types:
        query = get_ddl_query(obj_type, "DB.SCHEMA.OBJ")
        assert f"'{obj_type}'" in query
        assert "GET_DDL" in query
