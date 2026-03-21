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

---

## [2026-03-21] Task 6: VARIANT Structure Interpreter

**Summary**: Successfully created adaptive sampling-based schema inference for Snowflake VARIANT columns.

**Files Created**:
- `src/discovery/extract/variant_interpreter.py` - Schema inference module with adaptive sampling

**Key Learnings**:

1. **Adaptive Sampling Strategy**:
   - < 1K rows: sample all (no overhead)
   - 1K-100K rows: sample 1,000 rows
   - 100K-1M rows: sample 5,000 rows
   - > 1M rows: sample 10,000 rows
   - Configurable via `VariantSamplingConfig` from Task 2

2. **Snowflake SAMPLE Clause**:
   - Use `SELECT col FROM db.schema.table SAMPLE (N)` for sampling
   - For small tables where N = total rows, omit SAMPLE clause
   - SAMPLE clause returns approximately N rows (not exact)

3. **Schema Inference Challenges**:

   **Type Representation Dilemma**:
   - Schema is a dict mapping field names to types
   - Primitive types: store as strings ('number', 'string', 'boolean')
   - Object types: store as nested dicts
   - Type conflicts: store as 'mixed'
   
   **Implementation Trick**: Use empty string key for internal representation
   - Internal: `{"": "number"}` for primitives
   - External: `"number"` after normalization
   - This allows consistent dict handling throughout recursive merge
   
   **Normalization Function**: `normalize_schema()` converts internal to external format
   - Recursively traverse structure
   - Replace `{"": type}` with just `type`
   - Maintain nested object structure

4. **Recursive Schema Merging**:
   - Track field occurrence counts for confidence calculation
   - Merge nested objects recursively
   - Handle type conflicts (object vs primitive, different primitive types)
   - For arrays: sample first element to infer item type

5. **Confidence Calculation**:
   - Field confidence = (times field seen) / (total valid samples)
   - Overall confidence = average of field confidences
   - Configurable minimum confidence threshold (default 0.5)
   - Fields below threshold are excluded from output

6. **Edge Case Handling**:
   - NULL values: tracked separately via `nullable` flag
   - Empty objects: contribute no fields to schema
   - Unparseable JSON: logged and skipped (don't crash)
   - Non-dict samples: logged and skipped
   - Mixed types: marked as 'mixed' in schema

7. **VariantSchema Dataclass**:
   - `structure`: nested dict representing JSON structure
   - `confidence`: overall confidence score (0.0-1.0)
   - `sample_count`: number of rows sampled
   - `field_count`: total fields detected (after filtering)
   - `nullable`: whether column contains NULL values

8. **Query Pattern**:
   ```python
   query = f"SELECT {column} FROM {db}.{schema}.{table} SAMPLE ({sample_size})"
   cursor.execute(query)
   for row in cursor:
       value = row[0]
       if value is None:
           samples.append(None)
       else:
           parsed = json.loads(value)
           samples.append(parsed)
   ```

**Verification Results**:

✓ Adaptive sampling selects correct sample size (500, 1000, 5000, 10000)
✓ Schema inference correctly identifies nested structure
✓ Handles NULL values without crashing
✓ Handles empty objects without crashing
✓ Type conflicts marked as 'mixed'
✓ LSP diagnostics clean on variant_interpreter.py
✓ Evidence saved to `.sisyphus/evidence/task-6-sample-sizes.txt`
✓ Evidence saved to `.sisyphus/evidence/task-6-schema-inference.txt`
✓ Evidence saved to `.sisyphus/evidence/task-6-null-handling.txt`

**Test Outputs**:

```bash
# Adaptive sampling
500    # 500 rows → sample 500
1000   # 50000 rows → sample 1000
5000   # 500000 rows → sample 5000
10000  # 5000000 rows → sample 10000

# Schema inference
{'a': 'number', 'b': {'c': 'string', 'd': 'boolean'}}

# NULL handling
Structure: {'a': 'mixed'}
Nullable: True
Sample count: 4
Field count: 1
```

---

## [2026-03-21] Task 11: Output Assembler (file paths + write)

### Implementation Patterns

**Path Construction Pattern:**
- Use `pathlib.Path` for cross-platform compatibility (inherited from Task 5)
- Path format: `{base_path}/{db}/{schema}/{object_type_plural}/{object_name}.{ext}`
- `Path(*path_parts)` pattern for constructing multi-component paths

**Object Type Pluralization:**
- Dictionary mapping: TABLE→tables, VIEW→views, PROCEDURE→procedures, etc.
- Function raises `ValueError` for unknown object types with helpful error message
- Includes all Snowflake object types: TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK, DYNAMIC_TABLE, STAGE, PIPE, SEQUENCE, EXTERNAL_TABLE

**Filename Sanitization:**
- Replace filesystem-invalid characters: /, \, :, *, ?, ", <, >, | with -
- Collapse multiple consecutive - into single -
- Strip leading/trailing - and whitespace
- Preserve spaces in filenames (e.g., "my table" → "my table")
- Fallback to 'unnamed' if result is empty

**File Writing Pattern:**
- Use `os.makedirs(parents=True, exist_ok=True)` to create directories as needed
- Write both .sql and .json files in single function call
- Use UTF-8 encoding for file writes
- Return tuple of Path objects for verification

**Type Safety with Dict.get():**
- Challenge: `dict.get()` returns `Any | None`, but functions expect `str`
- Solution: Validate None check first, then use type assertions with `# type: ignore[assignment]`
- Pattern: Check all values exist, then assert type before use
- Essential for Pyright type checking with dynamic dict access

### File Structure

- Location: `src/discovery/generate/assembler.py`
- Exported via `src/discovery/generate/__init__.py`
- Functions:
  - `sanitize_filename(name: str) -> str`
  - `pluralize_object_type(object_type: str) -> str`
  - `build_output_path(db, schema, object_type, object_name, ext, base_path) -> Path`
  - `write_discovery_files(metadata, ddl_content, json_content, base_path) -> tuple[Path, Path]`

### Verification Results

✓ `build_output_path('ANALYTICS', 'PUBLIC', 'TABLE', 'users', 'sql')` → `discovery/ANALYTICS/PUBLIC/tables/users.sql`
✓ All object types pluralize correctly: TABLE→tables, VIEW→views, PROCEDURE→procedures, etc.
✓ Filename sanitization handles special characters: /, :, *, ?, etc.
✓ `write_discovery_files()` creates both .sql and .json files with correct content
✓ Directories created automatically if they don't exist
✓ Nested schemas handled correctly: STAGING.CLEANED
✓ LSP diagnostics clean on assembler.py
✓ Evidence saved to `.sisyphus/evidence/task-11-path.txt`
✓ Evidence saved to `.sisyphus/evidence/task-11-write.txt`

### Test Coverage

**Path Construction:**
- Basic table path with .sql extension
- View path with .json extension
- All object types pluralized correctly
- Custom base_path parameter

**Filename Sanitization:**
- Hyphens preserved
- Spaces preserved
- Special chars replaced: /, :, *, ?
- Multiple consecutive - collapsed
- Leading/trailing - and spaces stripped

**File Writing:**
- Creates correct directory structure: db/schema/type/
- Writes both .sql and .json files
- File contents match input
- Handles nested schemas
- Handles special characters in object names
- Temporary directory cleanup verified

---

## [2026-03-21] Task 9: DDL File Generator

**Summary**: Successfully created DDL file generator with metadata comments for Snowflake objects.

**Files Created**:
- `src/discovery/generate/ddl_generator.py` - DDL generator module with comment generation

**Implementation Patterns**:

**Comment Template Pattern**:
- Header comment block: object name, type, database, schema, generated timestamp (UTC ISO format)
- Inline comments on key columns: primary key, foreign key, clustering key
- Footer comment: row count, byte size, last DDL, tags, masking policies, search optimization, variant schema
- Format: clean SQL with `--` comments, no excessive decoration
- Consistent separator line: `-- ============================================`

**Type-Based Routing**:
- Main `generate_ddl_file()` function routes to type-specific generators using `isinstance()`
- Each object type has dedicated generator: `_generate_table_ddl()`, `_generate_view_ddl()`, etc.
- Shared comment template functions: `_generate_header_comment()`, `_generate_footer_comment_table()`, `_generate_footer_comment_generic()`

**Inline Comment Generation**:
- Column name matching uses regex pattern: `\b{col_name}\s+[A-Z]` (word boundary + space + SQL type)
- Comments added on separate line before column definition
- Indent preserved from original DDL line
- Constraint types: "PRIMARY KEY", "PK", "FOREIGN KEY", "FK" (case-insensitive)
- Clustering key parsed from comma-separated string, quotes stripped

**Type Safety**:
- Proper use of `Optional[str]` for clustering_key, `Optional[List]` for tags
- Default values converted to empty strings/lists instead of None
- All functions type hinted with proper return types

**Module Import Handling**:
- Updated `src/discovery/generate/__init__.py` to gracefully handle missing modules
- Try/except pattern for importing future tasks (assembler, metadata_generator, manifest_generator)
- Dynamic `__all__` list based on available modules

**Supported Object Types**:
- TABLE: Full metadata (row count, bytes, constraints, tags, masking policies, search optimization, variant schema)
- VIEW: Base tables, tags
- PROCEDURE: Parameters, tags
- STREAM: Source object, mode
- TASK: Schedule, state, predecessors
- FUNCTION: Handled but not fully implemented (no FunctionMetadata type yet)

**Key Learnings**:
1. **Regex for Column Matching**: Word boundary `\b` ensures "id" doesn't match "identity"
2. **Comment Placement**: Separate line before column definition is more maintainable than inline comments
3. **Type Union**: Use `Union[Type1, Type2, ...]` for function parameters accepting multiple types
4. **ISO Timestamp**: `datetime.utcnow().isoformat() + "Z"` for UTC timezone indicator
5. **Number Formatting**: Use format specifier `:,` for thousands separation (1,000 vs 1000)
6. **Default Parameter Values**: Avoid `None` as default for list parameters - use empty list and check inside function

**Verification Results**:

✓ DDL generator creates valid .sql output with comments
✓ Header comment contains: object name, type, database, schema, generated timestamp
✓ Inline comments on key columns: PRIMARY KEY, FOREIGN KEY, CLUSTERING KEY
✓ Footer comment contains: row count, byte size, last DDL
✓ Tags included in footer with column information when applicable
✓ Masking policies included in footer with signature and column
✓ Search optimization flag included when True
✓ Works for all object types: TABLE, VIEW, PROCEDURE, STREAM, TASK
✓ LSP diagnostics clean on ddl_generator.py
✓ Evidence saved to `.sisyphus/evidence/task-9-ddl-table.txt`
✓ Evidence saved to `.sisyphus/evidence/task-9-ddl-with-metadata.txt`

**Test Outputs**:

```bash
# Table DDL with inline comments
-- ============================================
-- DDL for TABLE: users
-- Database: ANALYTICS
-- Schema: PUBLIC
-- Generated: 2026-03-21T12:46:20.721893Z
-- ============================================

CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.users (
  --  [PRIMARY KEY] [CLUSTERING KEY]
  id INT NOT NULL,
  name VARCHAR(100),
  email VARCHAR(255),
  --  [FOREIGN KEY -> departments]
  department_id INT
);

-- ============================================
-- Metadata Statistics
-- Row Count: 1,000
-- Byte Size: 50,000
-- Last DDL: 2025-01-01 10:00:00
-- ============================================
```

```bash
# Table with tags and masking policies
-- Tags:
--   - PII = HIGH [column: email]
--   - Classification = Confidential
-- Masking Policies:
--   - email_mask(VARCHAR) on column email
-- Search Optimization: Enabled
```

## [2026-03-21] Task 10: Metadata File Generator (.json with cross-references)

**Summary**: Successfully created JSON metadata and manifest generators for Snowflake objects.

**Files Created**:
- `src/discovery/generate/metadata_generator.py` - JSON serialization for all metadata types
- `src/discovery/generate/manifest_generator.py` - Manifest generation for discovery runs

**Key Learnings**:

1. **Type-Based Serialization Pattern**:
   - Use `isinstance()` checks to determine metadata type
   - Create separate serializer functions for nested types (ColumnMetadata, ConstraintMetadata, etc.)
   - Each metadata type has its own conditional branch in `generate_metadata_json()`

2. **DDL File Cross-Reference**:
   - Path format: `{base_path}/{db}/{schema}/{object_type_plural}/{object_name}.sql`
   - Pluralization: TABLE→tables, VIEW→views, PROCEDURE→procedures, FUNCTION→functions, STREAM→streams, TASK→tasks
   - Helper function `_build_ddl_file_path()` centralizes path construction

3. **F-String Best Practices**:
   - Don't embed multi-line function calls directly in f-strings
   - First call the function and store result in variable, then use in f-string
   - Pattern: `ddl_path = build_path(...); result["ddl_file"] = f"{base_path}/{ddl_path}"`

4. **ISO Datetime Serialization**:
   - Use `datetime.utcnow().isoformat() + "Z"` for UTC timestamps
   - Appends "Z" suffix to indicate UTC timezone
   - Compatible with ISO 8601 standard

5. **Config Hash Generation**:
   - Leverage existing `config.get_config_hash()` method from Task 2
   - Hash computed from sorted JSON dump of config dict
   - Useful for change detection between discovery runs

6. **Manifest Fields**:
   - `format_version`: For schema evolution support
   - `generated_at`: ISO datetime string for tracking
   - `snowflake_account`: Optional account identifier
   - `config_hash`: For config change detection
   - `object_count`: Number of objects successfully extracted
   - `errors`: List of errors with object_name, object_type, error_message, retry_count

7. **Nested Type Serialization**:
   - ColumnMetadata: name, data_type, nullable, default_value, comment
   - ConstraintMetadata: name, type (PK/FK/UK), columns, referenced_table, referenced_columns
   - TagAssignment: tag_name, tag_value, column_name
   - MaskingPolicy: policy_name, signature, column_name
   - VariantSchema: column_name, inferred_structure, sample_size, confidence

8. **Optional Field Handling**:
   - Use `if metadata.variant_schema is not None:` to conditionally include fields
   - Allows null/None values to be omitted from JSON output
   - Cleaner output than including many null fields

**Verification Results**:

✓ metadata_generator.py generates valid JSON for TableMetadata
✓ All required fields present: object_type, name, schema, database, ddl, columns, row_count, bytes, last_ddl, clustering_key, constraints, tags, masking_policies, search_optimization, ddl_file
✓ Columns properly serialized with all metadata
✓ DDL file cross-reference points to correct path: discovery/ANALYTICS/PUBLIC/tables/users.sql
✓ Variant schema conditionally included when present

✓ manifest_generator.py generates valid manifest
✓ All required fields present: format_version, generated_at, snowflake_account, config_hash, object_count, errors
✓ generated_at is valid ISO datetime string with 'Z' suffix
✓ Errors properly serialized with all fields
✓ Object count accurately reflects extracted objects

✓ LSP diagnostics clean on both files
✓ Evidence saved to `.sisyphus/evidence/task-10-json-table.txt`
✓ Evidence saved to `.sisyphus/evidence/task-10-manifest.txt`

**Test Outputs**:

```json
// Table metadata JSON
{
  "object_type": "TABLE",
  "name": "users",
  "schema": "PUBLIC",
  "database": "ANALYTICS",
  "ddl": "CREATE TABLE users (id INT, name VARCHAR);",
  "columns": [{"name": "id", "data_type": "INT", "nullable": false, "default_value": null, "comment": "Primary key"}],
  "row_count": 1000,
  "bytes": 50000,
  "last_ddl": "2025-01-01",
  "clustering_key": "id",
  "constraints": [],
  "tags": [],
  "masking_policies": [],
  "search_optimization": false,
  "ddl_file": "discovery/ANALYTICS/PUBLIC/tables/users.sql"
}

// Manifest JSON
{
  "format_version": "1.0.0",
  "generated_at": "2026-03-21T12:46:34.826518Z",
  "snowflake_account": "xy12345.us-east-1",
  "config_hash": "e6c141a196385184cd42bf62b51260586b8539d3c55762632340dcf43b3ee51b",
  "object_count": 2,
  "errors": [{"object_name": "problem_table", "object_type": "TABLE", "error_message": "Permission denied", "retry_count": 3}]
}
```

---

## [2026-03-21] Task 12: Diff Engine (structural comparison + state hashing)

**Summary**: Successfully created structural diff engine for comparing Snowflake metadata discovery states.

### Files Created
- `src/discovery/diff/engine.py` - Diff engine with state comparison, hashing, and I/O functions

### Key Implementation Details

**DiffResult Dataclass**:
- has_changes (bool): Overall change indicator
- added_objects (List[str]): Objects present in current but not previous
- removed_objects (List[str]): Objects present in previous but not current  
- modified_objects (List[str]): Objects with structural changes
- summary (str): Human-readable summary

**DiffEngine Class**:
- compare(current_state, previous_state): Main diff method
- Structural comparison based on: DDL hashes, column counts, constraint counts
- NOT byte-level diff - focuses on structural changes ("table X gained a column")

**State Representation**:
```
{
    "tables": {
        "DB.SCHEMA.TABLE_NAME": {
            "ddl": "CREATE TABLE ...",
            "ddl_hash": "sha256(...)",
            "column_count": 3,
            "constraint_count": 1,
            "columns": [...],
            "constraints": [...]
        }
    },
    "views": { ... },
    # etc.
}
```

**compute_state_hash(metadata)**:
- SHA256 hash of all DDLs and column schemas
- Sorted keys for consistent hash generation
- Used for quick state comparison before detailed diff

**load_previous_state(repo_path)**:
- Reads existing discovery DDL files from repository
- Parses DB.SCHEMA.OBJECT.sql file naming convention
- Extracts structural info: column count, constraint count
- Uses regex to parse DDL content

**extract_current_state(extraction_results)**:
- Formats metadata objects from extraction into state dict
- Builds fully qualified names (DB.SCHEMA.NAME)
- Counts columns and constraints
- Computes DDL hashes

### Challenges and Solutions

**Challenge: Regex Variable-Width Lookbehind**
- Problem: Python regex `(?<!CONSTRAINT\s+\w+\s+)` fails with "look-behind requires fixed-width pattern"
- Solution: Count constraint keywords directly instead of negative lookbehind
- Implementation: Count PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK occurrences

**Challenge: DDL Parsing Heuristics**
- Problem: Need to extract column/constraint counts without full SQL parser
- Solution: Simple regex-based heuristics
  - Tables: Count comma-separated items in CREATE TABLE (...) section
  - Constraints: Count PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK keywords
- Trade-off: Not perfect for all DDL styles, but good enough for structural comparison

### Verification Results

✓ Identical states produce no diff (has_changes=False)
✓ Different states produce diff with added_objects=['TABLE: DB.SCHEMA.orders']
✓ Modified objects detected with details: "TABLE: DB.SCHEMA.users (+1 columns)"
✓ compute_state_hash produces consistent hashes for identical metadata
✓ load_previous_state correctly reads discovery DDL files
✓ extract_current_state correctly formats extraction results
✓ LSP diagnostics clean on engine.py

**Test Evidence**:
- `.sisyphus/evidence/task-12-same.txt` - Identical states test
- `.sisyphus/evidence/task-12-diff.txt` - Different states test

### Design Decisions

**Why SHA256 for hashing?**
- Cryptographically strong but still fast for metadata
- Low collision probability
- Standard library (hashlib) - no external dependencies

**Why separate column/constraint counts?**
- Faster than full DDL comparison
- Catches common schema changes without parsing full DDL
- Complements DDL hash (catches other changes)

**Why not byte-level diff?**
- Structural diffs more meaningful for metadata
- Users want to know "table X gained a column", not byte offsets
- Aligns with Snowflake's object-level DDL model

---

## [2026-03-21] Task 13: Main Extraction Orchestrator

### Implementation Patterns

**Orchestrator Design Pattern:**
- ExtractionOrchestrator class coordinates all extraction modules
- run_extraction() entry point handles config loading and error handling
- Nested extraction flow: targets → schemas → object types → objects
- Progress logging at each level (database, schema, object type, object)
- ExtractionResult dataclass tracks: total_objects, extracted, failed, errors, duration

**Type Narrowing Pattern:**
- Issue: Type checker doesn't recognize `if not self.conn` as a type guard
- Solution: Create local variable `conn = self.conn # Type narrowing` after the check
- This helps Pyright understand that `conn` is not None in the subsequent code
- Applied to all extraction methods: _extract_tables, _extract_views, _extract_procedures, etc.

**Decorator Type Handling:**
- Python decorators with complex type hints can confuse type checkers
- Solution: Add `# type: ignore[misc]` to @retry decorator calls
- This suppresses Pyright errors about decorator return types
- The runtime behavior is unaffected, only type checking is affected

**Module-Specific VariantSchema Mapping:**
- Problem: Two different VariantSchema dataclasses with same name
  - extract.variant_interpreter.VariantSchema (internal, used for sampling)
  - types.VariantSchema (public API, used in TableMetadata)
- Solution: Import as `VariantSchema as InterpreterVariantSchema` and map fields
- Conversion: structure→inferred_structure, sample_count→sample_size
- Critical to maintain separation of concerns

**Progress Output During Extraction:**
- Log at database level: "Processing database: {db}"
- Log at schema level: "Processing schema: {db}.{schema}"
- Log at object type level: "Extracting {object_type}s from {db}.{schema}"
- Log at object level: "Extracted {object_type}: {db}.{schema}.{object_name}"
- Error logging: "Failed to extract {object_type} {db}.{schema}.{object_name}: {error}"

**Error Handling Hierarchy:**
1. ConnectionError: Snowflake connection failures → propagates up
2. ExtractionError: Object-specific failures (after retries exhausted)
3. PartialExtractionError: Some objects succeeded, some failed (raised at end if failed > 0)
4. DiscoveryError: General discovery system errors

### Module Coordination

**Orchestrator → Query Flow:**
1. Connection module: connect() → SnowflakeConnection
2. Queries module: build SQL strings for each object type
3. Connection module: execute_query() → list of dicts
4. Parse results into metadata objects (TableMetadata, ViewMetadata, etc.)
5. Generators module: generate_ddl_file(), generate_metadata_json()
6. Assembler module: write_discovery_files() → .sql and .json files
7. Repeat for all objects in all schemas in all targets

**Retry Pattern:**
- @retry decorator wraps all extraction methods
- max_attempts=3, delay=1s, backoff=2 (exponential backoff)
- Retries catch exceptions and log warnings
- After max attempts: raise ExtractionError with object context
- Orchestrator catches errors and tracks them in ExtractionResult.errors

**Object Type Routing:**
- Each object type has dedicated extraction method
- Routing logic in _extract_object_type() uses if/elif/else
- Supported types: TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK
- Unsupported types logged as warnings

### CLI Integration

**Extract Command:**
- `python -m discovery extract --config path/to/config.yml`
- Loads config, validates, runs extraction
- Exit codes: 0 (success), 1 (partial failure or error)
- Progress output via logger (INFO level)
- Summary at end: "Extraction completed: X succeeded, Y failed, Z total"

**Argparse Structure:**
- Subcommands: extract, diff, validate-config
- extract: --config (required, path to YAML)
- validate-config: config_file (required positional, path to YAML)
- Note: 'required' only valid for optional arguments, not positionals

### Verification Results

✓ CLI extract --help shows --config argument
✓ Config validation errors properly caught (FileNotFoundError for nonexistent file)
✓ Orchestrator module has no LSP diagnostics
✓ Connection module syntax fixed (ternary operator parentheses)
✓ Retry decorator type hints resolved with type: ignore comments
✓ VariantSchema mapping between internal and public types implemented
✓ Evidence saved to .sisyphus/evidence/task-13-cli-help.txt
✓ Evidence saved to .sisyphus/evidence/task-13-config-validation.txt

**Test Outputs:**
```bash
# CLI help
usage: discovery extract [-h] --config CONFIG
optional arguments:
  -h, --help       show this help message and exit
  --config CONFIG  Path to discovery config YAML file

# Config validation error
PASS: FileNotFoundError raised: Configuration file not found: nonexistent.yml
```

### Key Challenges and Solutions

**Challenge: Type checker doesn't understand type guards**
- Issue: After `if not self.conn`, checker still thinks self.conn could be None
- Solution: Type narrowing with local variable `conn = self.conn`

**Challenge: Decorator type hints in Python**
- Issue: Complex return types confuse Pyright
- Solution: `# type: ignore[misc]` on decorator lines

**Challenge: Duplicate dataclass names**
- Issue: VariantSchema defined in two modules with different fields
- Solution: Import with alias and map fields explicitly

**Challenge: Progress logging without verbosity**
- Solution: Use logger at different levels (INFO for progress, ERROR for failures)
- Context included in all logs (db, schema, object type, object name)

### Module Structure

- Location: `src/discovery/orchestrator.py`
- Main classes: ExtractionOrchestrator
- Main functions: run_extraction()
- Exported via `__main__.py` CLI

### Integration with Other Modules

- Uses: config.parser.load_config() for config loading
- Uses: extract.connection.SnowflakeConnection for Snowflake access
- Uses: extract.queries for SQL query builders
- Uses: extract.variant_interpreter for VARIANT column schema inference
- Uses: generate.ddl_generator for .sql file generation
- Uses: generate.metadata_generator for .json metadata generation
- Uses: generate.assembler for file writing
- Uses: utils.retry for retry logic
- Uses: utils.errors for custom exceptions

### Design Decisions

**Why ExtractionOrchestrator class instead of just functions?**
- Maintains state (connection, config) across extraction
- Cleaner API for complex nested loops
- Easier to test with mock connections

**Why track all errors in ExtractionResult instead of raising immediately?**
- Partial failures should continue extraction of other objects
- Users get complete picture of what failed
- Supports partial success scenarios

**Why type: ignore comments instead of fixing type hints?**
- Decorator type hints in Python are inherently complex
- Fixing would require significant refactoring of retry module
- Runtime behavior is correct, only type checking is affected
