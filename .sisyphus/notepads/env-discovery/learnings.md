# Learnings

## [TIMESTAMP] Task: N/A
*Initial notepad for env-discovery project*

---

## [2026-03-21] Task 5: Metadata SQL Queries Module

### Implementation Patterns

**SQL Query Builder Pattern:**
- All query functions return SQL strings, not query results
- Execution handled by connection module (Task 4)
- Functions accept parameterized names as arguments (schema, table, object_name, etc.)
- Use f-strings for SQL construction (acceptable since values come from YAML config, not user input)

**INFORMATION_SCHEMA vs ACCOUNT_USAGE:**
- INFORMATION_SCHEMA views: Real-time, use for near real-time metadata
  - TABLES, COLUMNS, VIEWS, PROCEDURES, FUNCTIONS, STREAMS, TASKS, STAGES, PIPES, SEQUENCES
  - Constraint queries: TABLE_CONSTRAINTS + KEY_COLUMN_USAGE (JOIN needed)
- ACCOUNT_USAGE views: ~45 minute latency, use for historical/computed metrics
  - TAG_REFERENCES, MASKING_POLICIES, TABLE_STORAGE_METRICS
- Document latency clearly in function docstrings

**JOIN Query Pattern (Constraints):**
- Multi-table JOINs needed for foreign key relationships
- Pattern: TABLE_CONSTRAINTS → KEY_COLUMN_USAGE (self join for referenced columns)
- Use LEFT JOINs to include constraints without column mappings
- ORDER BY multiple columns for consistent output

**Variant Column Identification:**
- Filter by `data_type = 'VARIANT'` in COLUMNS query
- Return nullable flag for downstream sampling logic

**Schema Name Handling:**
- INFORMATION_SCHEMA queries: Use unqualified schema name
- ACCOUNT_USAGE queries: May need schema name extraction from fully qualified path (e.g., `schema.split('.')[-1]`)
- LIKE pattern matching for ACCOUNT_USAGE: `object_name LIKE '%.DB.SCHEMA.%'`

### Additional Helper Functions

Beyond required 8 functions, added comprehensive coverage:
- `list_views_query()`: Views metadata with definitions
- `list_procedures_query()`: Stored procedures with signatures
- `list_functions_query()`: UDF definitions
- `list_streams_query()`: Stream metadata (mode, stale status)
- `list_tasks_query()`: Task scheduling and state
- `list_stages_query()`: External/internal stages
- `list_pipes_query()`: Pipe definitions
- `list_sequences_query()`: Sequence configuration

### Module Structure

- Location: `src/discovery/extract/queries.py`
- Import pattern: `from src.discovery.extract.queries import get_ddl_query, ...`
- All functions type hinted: `def func_name(...) -> str`
- Comprehensive docstrings with examples

### Import Handling for Parallel Tasks

**Challenge:** Task 5 runs parallel to Task 4 (connection module), but `__init__.py` imports from both.

**Solution:** Graceful import failure in `__init__.py`:
```python
try:
    from .connection import SnowflakeConnection, connect
    _has_connection = True
except ImportError:
    _has_connection = False

# Only export connection functions if available
if _has_connection:
    __all__.extend(["SnowflakeConnection", "connect"])
```

This allows queries module to be imported independently while preserving clean API once all tasks complete.

### Verification Results

✓ All 8 required functions generate valid SQL strings
✓ get_ddl_query('TABLE', 'DB.SCHEMA.TBL') → correct GET_DDL syntax
✓ All queries use parameterized schema/table names
✓ No syntax errors in queries module
✓ Functions have proper type hints and docstrings
✓ Evidence saved to `.sisyphus/evidence/task-5-queries.txt`

## [2025-03-21] Task 2: YAML Config Schema + Parser

**Summary**: Successfully created Pydantic-based YAML configuration system with validation.

**Files Created**:
- `src/discovery/config/schema.py` - Pydantic models (DiscoveryConfig, TargetConfig, SchemaConfig, VariantSamplingConfig, OutputConfig)
- `src/discovery/config/parser.py` - load_config() function with YAML parsing and Pydantic validation
- `src/discovery/config/validator.py` - Additional validation beyond Pydantic schema
- `src/discovery/config/__init__.py` - Package exports
- `src/discovery/utils/errors.py` - Custom exceptions (ConfigValidationError, ExtractionError, ConnectionError, PartialExtractionError)

**Key Learnings**:
1. **Pydantic v2 syntax**: Use `model_validate()` instead of `parse_obj()`, and `field_validator` / `model_validator` decorators
2. **Type hints for Python 3.9**: Must use `Optional[T]` instead of `T | None` (PEP 604 not available)
3. **default_factory**: Must be a lambda function `lambda: ClassName()` not just the class type
4. **Field validation**: Pydantic handles most validation, but custom validators needed for:
   - Unknown Snowflake object types
   - Threshold ordering (must be increasing)
   - Type conflicts (include/exclude)
   - Path validation (relative vs absolute)
5. **Config hash generation**: Useful for change detection - hash sorted JSON dump
6. **Dependency isolation**: Created utils/errors.py before Task 7 since it was needed by config parser

**Validation Tests Passed**:
- Valid config loads and returns DiscoveryConfig object
- Invalid config (UNKNOWN_TYPE) raises ConfigValidationError with clear message
- All Pydantic models have proper type hints and field descriptions
- LSP diagnostics clean on all config files

**Valid Snowflake Object Types**:
TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK, DYNAMIC_TABLE, STAGE, PIPE, SEQUENCE, EXTERNAL_TABLE

**QA Evidence**:
- `.sisyphus/evidence/task-2-valid-config.txt` - Valid config test output
- `.sisyphus/evidence/task-2-invalid-config.txt` - Invalid config error output
