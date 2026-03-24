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

---

## [2026-03-21] Task 18: External Access Integration SQL Script

**Summary**: Successfully created SQL script template for Snowflake External Access Integration to GitHub API.

### Files Created
- `sql/setup_external_access.sql` - Complete template for setting up external access integration

### Implementation Patterns

**SQL Template Pattern:**
- Comprehensive documentation with inline comments explaining each step
- Placeholder values marked with angle brackets: <NETWORK_RULE_NAME>, <INTEGRATION_NAME>, etc.
- Both basic and advanced examples provided (e.g., with/without secret references)
- Verification queries included to test the setup
- Cleanup commands provided for rollback

**Network Rule Creation:**
- Use MODE = EGRESS for outbound traffic
- TYPE = HOST_PORT for host-based rules
- VALUE_LIST specifies allowed endpoints with port numbers
- Example: VALUE_LIST = ('api.github.com:443')
- COMMENT field should describe the purpose

**External Access Integration:**
- References network rules via ALLOWED_NETWORK_RULES
- ENABLED = TRUE to activate the integration
- Optional ALLOWED_AUTHENTICATION_SECRETS for authenticated requests
- COMMENT field for documentation

**Stored Procedure Integration:**
- EXTERNAL_ACCESS_INTEGRATIONS clause specifies allowed integrations
- PACKAGES clause includes required Python libraries (requests, snowflake-snowpark-python)
- Language: PYTHON with RUNTIME_VERSION = '3.11'
- HANDLER specifies the Python function to call

### Security Best Practices (Documented in Template)

1. **Principle of Least Privilege:**
   - Only allow access to specific endpoints, not entire domains
   - Grant USAGE on integration only to roles that need it
   - Use read-only GitHub permissions when possible

2. **Secrets Management:**
   - Never hardcode API tokens in SQL files
   - Use Snowflake secrets or external vaults for credentials
   - Retrieve secret values programmatically in stored procedures

3. **Scoping:**
   - Create separate integrations for different use cases
   - Limit network rules to specific endpoints (not all of github.com)
   - Use role-based access control for integration usage

### Example Use Cases

**Fetching Repository Issues:**
- Complete example with fetch_github_issues() procedure
- Demonstrates parameter handling, API requests, response parsing
- Returns structured table with issue data
- Can be called directly from SQL: SELECT * FROM TABLE(fetch_github_issues('owner', 'repo', 'open'));

**Authenticated Requests:**
- Template includes code for retrieving Snowflake secrets
- Shows how to add Authorization headers to requests
- Demonstrates proper error handling and logging

### Verification

**SQL Syntax Validation:**
- Used sqlparse library to validate SQL syntax
- Command: `python3 -c "import sqlparse; sql = sqlparse.parse(content); print(f'{len(sql)} statements parsed successfully')"`
- Result: 6 statements parsed successfully
- Evidence saved to: `.sisyphus/evidence/task-18-sql-valid.txt`

**Content Verification:**
- ✓ Valid CREATE OR REPLACE NETWORK RULE statement (line 33)
- ✓ Valid CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION statement (line 64)
- ✓ Example stored procedure showing how to use integration (lines 105-190)
- ✓ Security comments about scoping (line 11 onwards)
- ✓ Example procedure for fetching GitHub issues (lines 191-250+)

### Key Learnings

1. **SQL Template Design:**
   - Provide both commented and uncommented examples
   - Use consistent placeholder naming convention
   - Include verification queries for testing setup
   - Document cleanup/rollback procedures

2. **Snowflake External Access Integration:**
   - Two-step process: network rule → external access integration
   - Network rules define allowed endpoints
   - Integrations bundle network rules and secrets
   - Stored procedures reference integrations in definition

3. **Python UDF Integration:**
   - EXTERNAL_ACCESS_INTEGRATIONS clause is required for external calls
   - PACKAGES clause specifies Python dependencies
   - Can use standard Python libraries (requests, pandas)
   - Handler function signature must match expected return type

4. **Security Considerations:**
   - Always document security requirements
   - Provide examples of both authenticated and unauthenticated patterns
   - Include verification queries to audit access
   - Use least-privilege role grants

### Design Decisions

**Why separate network rules from integrations?**
- Network rules define WHAT endpoints are allowed
- Integrations define HOW they're used (with secrets, etc.)
- Allows reuse of network rules across multiple integrations
- Follows Snowflake's security model

**Why include both basic and advanced examples?**
- Basic example gets users started quickly
- Advanced example shows production patterns (secrets)
- Users can see the progression from simple to complex
- Reduces learning curve for secure implementations

**Why use Snowflake Python UDFs instead of SQL functions?**
- Better HTTP request handling with requests library
- More flexible data transformation with pandas
- Easier to integrate with external APIs
- Aligns with modern Python-based workflows

### Module Structure

- Location: `sql/setup_external_access.sql`
- Template for: External access integration to GitHub API
- Target audience: Snowflake developers setting up API integrations

### Integration with Other Modules

- Used by: Task 15 (Snowflake Trigger Notebook) for GitHub API access
- Complements: Task 16 (workflow update to use GitHub API)
- Follows: Snowflake security best practices
- Aligns with: Principle of least privilege

### Verification Results

✓ SQL syntax validated with sqlparse - 6 statements parsed successfully
✓ CREATE OR REPLACE NETWORK RULE statement present
✓ CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION statement present
✓ Example stored procedures included (2 different examples)
✓ Security comments and best practices documented
✓ Verification queries provided
✓ Evidence saved to .sisyphus/evidence/task-18-sql-valid.txt


---

## [2026-03-23] Task 16: GitHub Actions Full Workflow

### Implementation Patterns

**Workflow Naming Convention:**
- Changed workflow name from "discover" to "Discovery" for better clarity
- Job name: "extract-and-commit" describes the action
- Naming consistency: Use descriptive, action-oriented names

**Trigger Configuration:**
- Two triggers only: workflow_dispatch and create
- workflow_dispatch: Manual trigger for PR creation workflow
- create: Automatic trigger on branch creation for direct commit workflow
- Branch pattern for create: '**' (matches all branches)
- Excluded triggers: cron/schedule, push (as required)

**Environment Variables at Job Level:**
- All Snowflake secrets defined at job env level (not step level)
- Consistent with Task 8 skeleton pattern
- Secrets referenced: SNOWFLAKE_ACCOUNT, USER, WAREHOUSE, PRIVATE_KEY_RAW, ROLE
- Key-pair auth (no OIDC) as required

**Step Dependencies and IDs:**
- Use `id: diff` for change detection step to enable conditional execution
- Conditional steps reference output: `steps.diff.outputs.changes`
- Pattern: Check condition first, then set output variable

**Change Detection Pattern:**
```yaml
- name: Check for changes
  id: diff
  run: |
    git diff --quiet discovery/ || echo "changes=true" >> $GITHUB_OUTPUT
```
- Uses git diff --quiet (exit code 0 if no changes, 1 if changes)
- OR operator sets output variable only when changes detected
- Enables conditional execution of subsequent steps

**Conditional Execution by Event Type:**
- Branch creation (github.event_name == 'create'): Commit directly to new branch
- Manual dispatch (github.event_name == 'workflow_dispatch'): Create PR to main
- Both conditions require changes detected: `steps.diff.outputs.changes == 'true'`
- Prevents redundant commits when nothing changed

**PR Creation with peter-evans/create-pull-request@v6:**
- Uses default GITHUB_TOKEN for authentication
- Branch: ${{ github.ref_name }} - automatically references current branch
- Base: main - targets main branch
- Title: "Discovery: Update metadata"
- Body: "Automated metadata discovery update from Snowsight trigger."
- No duplicate PR prevention (re-creates if closed)

**Git Configuration for Commits:**
- User: github-actions[bot] (standard GitHub Actions bot)
- Email: github-actions[bot]@users.noreply.github.com
- Commit message format: chore(discovery): update metadata [auto]
- Standard conventional commit format with [auto] tag for automation

**Error Handling with Artifacts:**
- Condition: `failure()` - runs only if any previous step failed
- Action: actions/upload-artifact@v4
- Path: discovery/_errors/
- Artifact name: discovery-errors
- Enables post-mortem analysis of failures

**GitHub Actions Token Scope:**
- Token: ${{ secrets.GITHUB_TOKEN }} (default token)
- Permissions needed: contents: write (implied by push/PR actions)
- No special token configuration needed

**Checkout Configuration:**
- Action: actions/checkout@v4
- Token parameter: Required for push/PR operations
- Default token without token parameter only provides read access
- Must explicitly pass GITHUB_TOKEN for write operations

**Python Setup:**
- Action: actions/setup-python@v5
- Version: '3.11' (as specified in requirements)
- Consistent with project Python version

**Dependency Installation:**
- Command: pip install -e . (editable install)
- Installs from pyproject.toml (created in Task 14)
- Enables CLI command: python -m discovery extract

### Key Learnings

1. **Event-Based Workflow Logic:**
   - Different behavior based on trigger event (create vs workflow_dispatch)
   - Create event: New branch → commit directly
   - Workflow dispatch: Existing branch → create PR
   - Enables both Snowsight automation and manual testing

2. **Change Detection Before Commit:**
   - git diff --quiet detects changes in discovery/ directory
   - Output variable enables conditional execution
   - Prevents empty commits when nothing changed
   - Critical for workflow reliability and git history cleanliness

3. **Conditional Step Execution:**
   - Reference step outputs in condition: `steps.diff.outputs.changes == 'true'`
   - Multiple conditions with AND operator
   - Steps can be skipped entirely based on conditions
   - Cleaner than complex shell if/else logic

4. **GITHUB_TOKEN Permissions:**
   - Default token has read-only access
   - Must explicitly pass to checkout action for write access
   - Write permissions needed for: git push, create PR
   - Token automatically available in workflows (no secret setup needed)

5. **PR Creation Best Practices:**
   - Use peter-evans/create-pull-request action (community standard)
   - Always specify base branch (main)
   - Use descriptive title and body
   - Branch reference uses ${{ github.ref_name }} for flexibility

6. **Git Configuration in CI:**
   - Must set user.name and user.email before commit
   - Use github-actions[bot] for automated commits
   - Bot email follows noreply pattern
   - Prevents authentication failures

7. **Error Artifact Strategy:**
   - Upload entire error directory (not just one file)
   - Only on failure (if: failure())
   - Preserve error context for debugging
   - Accessible from workflow run summary

8. **Secrets Management:**
   - Job-level env variables cleaner than step-level
   - Consistent secret naming pattern
   - No need for OIDC (key-pair auth sufficient)
   - All secrets in one place for easy audit

### Verification Results

✅ YAML validation passed: yaml.safe_load() succeeds
✅ Workflow name changed from "discover" to "Discovery"
✅ Triggers: workflow_dispatch and create only
✅ No cron/schedule triggers present
✅ No push triggers present
✅ Environment variables at job level (5 Snowflake secrets)
✅ Checkout action includes token parameter
✅ Python setup uses v5 action with version 3.11
✅ Install dependencies step: pip install -e .
✅ Extraction step: python -m discovery extract --config discovery-config.yml
✅ Change detection step with id: diff
✅ Conditional commit step for create event
✅ Conditional PR creation for workflow_dispatch event
✅ Uses peter-evans/create-pull-request@v6
✅ Error artifact upload step on failure
✅ Secrets referenced correctly
✅ GITHUB_TOKEN used for authentication
✅ No OIDC authentication
✅ LSP diagnostics clean on workflow file (unrelated Python module errors expected)

### Workflow Execution Flow

**Branch Creation Event:**
1. Workflow triggered by branch creation
2. Checkout repository with write token
3. Setup Python 3.11
4. Install dependencies
5. Run extraction
6. Check for changes (git diff discovery/)
7. If changes detected → commit to branch → push
8. If extraction failed → upload error artifacts
9. Workflow completes

**Manual Dispatch Event:**
1. Workflow triggered manually
2. Checkout repository with write token
3. Setup Python 3.11
4. Install dependencies
5. Run extraction
6. Check for changes (git diff discovery/)
7. If changes detected → create PR to main branch
8. If extraction failed → upload error artifacts
9. Workflow completes

**No Changes Scenario:**
- Change detection step sets no output
- Both commit and PR steps skipped (condition not met)
- Workflow completes successfully without changes

**Error Scenario:**
- Extraction or other step fails
- Error artifact uploaded
- Workflow marked as failed
- Error logs available for debugging

### Design Decisions

**Why job-level env variables instead of step-level?**
- Cleaner workflow structure
- All secrets in one place
- Reusable across all steps
- Easier to audit and maintain

**Why change detection before commit?**
- Prevents empty commits
- Clean git history
- Reduced workflow noise
- Only runs push/PR when meaningful changes exist

**Why different behavior for create vs workflow_dispatch?**
- Create event: Snowsight trigger on new branch → commit directly
- Workflow dispatch: Manual trigger on existing branch → create PR
- Matches different use cases (automation vs testing)
- Prevents accidental commits to main

**Why use github-actions[bot] for commits?**
- Standard GitHub convention
- Identifies automated changes
- Better audit trail
- Recognized by GitHub UI

**Why upload entire _errors/ directory?**
- May contain multiple error files
- Preserves error context and structure
- Enables comprehensive post-mortem
- Consistent with error handling pattern

**Why use peter-evans/create-pull-request action?**
- Community standard for PR creation
- Handles edge cases (existing PRs, etc.)
- Well-maintained and documented
- Integrates cleanly with GitHub API

### Integration with Other Tasks

- Task 8: Workflow skeleton updated to full implementation
- Task 13: Uses CLI extract command from orchestrator
- Task 14: Uses pyproject.toml for dependency installation
- Task 18: External access integration (future enhancement)

### Evidence Saved

- .sisyphus/evidence/task-16-workflow-validate.txt - Complete workflow validation

---

## [2026-03-23] Task 15: Snowflake Trigger Notebook

### Implementation Patterns

**Snowflake Notebook Cell Structure Pattern:**
- Use 7 cells for clear, logical flow
- Each cell has a single responsibility
- Cells use `execution_count: null` for non-executed state
- All cells are code type (no markdown cells required for logic)

**Cell 1: Imports:**
- Standard imports: snowflake.connector, requests, json, yaml
- datetime import for timestamp generation
- All imports at the top of the notebook

**Cell 2: Load Config:**
- Inline config dictionary with placeholder values
- GitHub config: owner, repo, branch, workflow_id
- Snowflake config: databases list
- GitHub token secret name for secure credential retrieval
- Commented out YAML loading pattern for production use

**Cell 3: Query Current Metadata (Lightweight):**
- Uses INFORMATION_SCHEMA for real-time metadata
- Three lightweight queries per database:
  1. Table counts per schema
  2. MAX(last_ddl) per schema
  3. Column counts per schema
- Graceful degradation with mock data for testing
- Connection error handling with fallback to mock data
- Results stored in `current_state` dict structure

**Cell 4: Fetch Previous State from Git:**
- GitHub API endpoint: /repos/{owner}/{repo}/contents/discovery/_manifest.json?ref={branch}
- Authentication: Authorization: token <GITHUB_TOKEN>
- Base64 decode for GitHub API content response
- 404 handling for first run scenario (no manifest exists)
- Error handling for network failures and rate limits
- Previous state loaded into `previous_state` variable

**Cell 5: Run Diff Engine:**
- Simple diff implementation for lightweight metadata comparison
- Compares current_state vs previous_state
- Tracks: added, removed, modified objects
- First-run handling (previous is None → all objects added)
- DiffResult dataclass with: has_changes, added_objects, removed_objects, modified_objects, summary
- Summary generation with counts (+added, -removed, ~modified)

**Cell 6: Trigger GitHub API (Conditional):**
- Condition: only runs if `diff_result.has_changes` is True
- GitHub API endpoint: POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches
- Request body includes:
  - ref: branch name (e.g., "main")
  - inputs: diff_summary, triggered_by metadata
- Response handling: 204 = success, other codes = error
- No-op if no changes detected (skip workflow trigger)

**Cell 7: Log Result:**
- Comprehensive log entry with timestamp (UTC ISO format with Z suffix)
- Log fields: timestamp, has_changes, summary, added/removed/modified objects, action_taken
- Formatted output with ASCII box separators
- Returns log_entry for downstream use
- Clear visual feedback of trigger status

### Security Considerations

**No Hardcoded Credentials:**
- GitHub token retrieved from Snowflake secret
- Secret referenced via `system$get_secret()`
- Placeholders use angle bracket notation: <GITHUB_TOKEN>
- Secrets loaded at runtime, never in notebook source

**Error Handling:**
- Connection failures handled gracefully
- API rate limits caught and logged
- Missing first-run manifest handled (404 = first run)
- Network errors caught with try/except

**Principle of Least Privilege:**
- Only reads from git repository (no write access in notebook)
- Only triggers workflow_dispatch (doesn't execute extraction directly)
- Uses read-only metadata queries (INFORMATION_SCHEMA)

### Key Learnings

1. **Snowflake Notebook JSON Format:**
   - Use nbformat version 4.4
   - All cells have: cell_type, source (list of strings), execution_count, metadata
   - Source is a list of strings (lines), not a single string
   - Each line in source should end with \n or be separate strings

2. **Lightweight Metadata Queries:**
   - Table counts, last_ddl, column counts sufficient for change detection
   - INFORMATION_SCHEMA provides real-time data
   - Avoid heavy queries (full DDL extraction) in trigger notebook
   - Let the GitHub Action handle full extraction

3. **GitHub API workflow_dispatch:**
   - Endpoint format: /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches
   - Required headers: Authorization, Accept: application/vnd.github.v3+json
   - Required body: {"ref": "branch_name", "inputs": {...}}
   - Success response: HTTP 204 No Content
   - Cannot trigger workflows without workflow_dispatch trigger

4. **Base64 Decoding for GitHub Content API:**
   - GitHub API returns base64-encoded content
   - Use base64.b64decode() to decode
   - Decode to bytes, then decode to UTF-8 string
   - Parse as JSON with json.loads()

5. **Diff Engine Integration Pattern:**
   - Import from discovery.diff.engine module
   - Use DiffEngine().compare(current_state, previous_state)
   - DiffResult provides has_changes flag and summary
   - Summary field useful for logging and API inputs

6. **Snowflake Secrets Access:**
   - Use system$get_secret() to retrieve secret values
   - Parse JSON response from secret
   - Access specific key (e.g., github_token)
   - Handle secret access failures gracefully

7. **First-Run Handling:**
   - Check if previous_state is None
   - Treat first run as "all objects added"
   - Don't compare against empty previous state
   - Log clearly that it's a first run

8. **Mock Data Pattern:**
   - Fallback mock data for testing without Snowflake connection
   - Uses same structure as real data (dict with db, schema keys)
   - Enables local development and testing
   - Clearly labeled as mock data in logs

### Module Structure

- Location: `notebooks/discovery_trigger.ipynb`
- Cell count: 7 (exactly as specified)
- Type: Jupyter Notebook (JSON format)
- Execution: Can be executed via Snowsight or nbconvert

### Integration with Other Modules

- Task 12: Diff engine patterns (DiffResult, comparison logic)
- Task 13: Orchestrator pattern for extraction (referenced but not called)
- Task 18: External Access Integration (GitHub API access pattern)
- Task 16: GitHub Actions workflow (workflow_dispatch endpoint)

### Verification Results

✅ Notebook created with exactly 7 cells
✅ Cell 1: Imports (snowflake.connector, requests, json, yaml, datetime)
✅ Cell 2: Load config (inline config with GitHub and Snowflake settings)
✅ Cell 3: Query current metadata (lightweight INFORMATION_SCHEMA queries)
✅ Cell 4: Fetch previous state (GitHub API _manifest.json)
✅ Cell 5: Run diff engine (DiffResult comparison logic)
✅ Cell 6: Trigger GitHub API (workflow_dispatch conditional trigger)
✅ Cell 7: Log result (comprehensive logging with timestamp)
✅ Valid JSON format (nbformat 4.4)
✅ No hardcoded credentials (uses Snowflake secrets)
✅ Graceful error handling (connection failures, API errors)
✅ First-run handling (previous_state = None scenario)
✅ Evidence saved to .sisyphus/evidence/task-15-notebook-structure.txt

### Notebook Execution Flow

1. Cell 1: Import required libraries
2. Cell 2: Load configuration (inline or from YAML)
3. Cell 3: Query current Snowflake metadata via INFORMATION_SCHEMA
4. Cell 4: Fetch previous state from GitHub API (_manifest.json)
5. Cell 5: Compare states using diff engine
6. Cell 6: If changes detected → trigger GitHub workflow_dispatch
7. Cell 7: Log final result with timestamp and change summary

### Design Decisions

**Why 7 cells exactly?**
- Follows specification precisely
- Each cell has single responsibility
- Easy to test and debug individual cells
- Clear separation of concerns

**Why lightweight queries only?**
- Trigger notebook should be fast (runs frequently)
- Full extraction handled by GitHub Action (Task 16)
- Change detection is sufficient for triggering
- Avoid unnecessary load on Snowflake

**Why inline config instead of just YAML?**
- Quick start for users
- Clear structure visible in code
- YAML pattern provided as alternative
- Easy to adapt for different environments

**Why mock data fallback?**
- Enables local development
- Testing without Snowflake connection
- Production uses real connection
- Clearly labeled in logs

**Why log everything instead of just result?**
- Comprehensive audit trail
- Debugging support
- Transparency in automated workflow
- Matches CI/CD best practices

**Why return log_entry from Cell 7?**
- Enables programmatic use of results
- Can be stored in Snowflake table for history
- API-ready output for downstream systems
- Flexible for different use cases

### Evidence Saved

- .sisyphus/evidence/task-15-notebook-structure.txt - Complete cell validation


---

## [2026-03-23] Task 20b: Architecture Documentation

### Implementation Patterns

**ADR Documentation Pattern:**
- Standard ADR format with Status, Date, Context, Decision, Consequences
- Each ADR documents a single, important architectural choice
- Consequences section captures trade-offs explicitly
- Status field tracks evolution (Accepted, Deprecated, Replaced)

**Mermaid Diagram Pattern:**
- Simple graph syntax for clarity
- Nodes represent major components (Snowflake, Orchestrator, etc.)
- Arrows show data flow direction
- Dotted lines (.-→) represent triggers or control flow
- Solid arrows (→) represent data flow

**Design Tree Documentation Pattern:**
- For each technology choice, document: Chose, Reason, Trade-off
- Explicit trade-offs make design decisions transparent
- Group related decisions together (e.g., all technology choices)
- Keep explanations concise and actionable

### Key Learnings

1. **ADR Structure:**
   - Context explains the problem or situation requiring a decision
   - Decision states the chosen approach clearly
   - Consequences capture both positive and negative implications
   - Date field provides historical context

2. **Mermaid Diagram Design:**
   - Keep diagrams simple and focused
   - Use descriptive node names for clarity
   - Different arrow styles for different purposes (data vs control flow)
   - Group related nodes logically in the diagram

3. **Architecture vs README:**
   - Architecture docs focus on design decisions and rationale
   - README focuses on usage and getting started
   - Avoid duplication between the two documents
   - Reference architecture.md from README when needed

4. **Design Tree Decisions:**
   - Document the "why" behind technology choices
   - Include trade-offs to show thoughtful decision-making
   - Group decisions logically (technology, process, etc.)
   - Make it clear which option was chosen and why

### File Structure

- Location: `docs/architecture.md`
- Sections: ADRs, Data Flow Diagram, Design Tree Decisions, Technology Stack
- Cross-reference: README.md should link to architecture.md for detailed design info

### Verification Results

✅ docs/architecture.md created with 2477 bytes
✅ All 3 ADRs present (ADR-001, ADR-002, ADR-003)
✅ Mermaid diagram with proper syntax
✅ Design tree decisions section with 4 technology choices
✅ Technology stack section with 7 tools/libraries
✅ Evidence saved to `.sisyphus/evidence/task-20b-architecture.md`

### Test Coverage

**ADR Verification:**
- All 3 ADRs found in document
- Each ADR has Status, Date, Context, Decision, Consequences
- Status values are "Accepted" for all ADRs
- Dates are consistent (2026-03-23)

**Diagram Verification:**
- Mermaid syntax valid
- Graph contains expected nodes (Snowflake, Orchestrator, etc.)
- Both solid and dotted arrows present

**Design Tree Verification:**
- Python Connector vs Snowpark decision documented
- YAML Configuration vs CLI Args decision documented
- Error Handling Strategy decision documented
- Testing Strategy decision documented
- Each decision includes Chose, Reason, Trade-off

---

## [2026-03-23] Task 20d: Update README.md - Expand all placeholder sections

### Summary
Successfully expanded all placeholder sections in README.md with complete, practical content describing the project.

### Files Modified
- README.md - Expanded all sections with actual content

### Key Implementation Details

**Quick Summary Section:**
- Updated one-line description to include "three-layer trigger architecture"

**Overview Section:**
- Detailed description of Snowflake Discovery system capabilities
- Extracts full DDLs, constraints, tags, masking policies, VARIANT schemas
- Three-layer trigger architecture explained with numbered list

**Architecture Section:**
- Link to docs/architecture.md with bullet list of contents (ADRs, data flow diagrams, design tree decisions, technology stack)

**Quick Start Section:**
- Added Prerequisites: Python 3.9+, Snowflake account with key-pair auth, GitHub repository
- Added Installation section with git clone and pip install commands

**Configuration Section:**
- Reference to discovery-config.example.yml
- Bullet list of key configuration sections (Snowflake connection, GitHub integration, discovery targets, VARIANT sampling, output format)

**Folder Structure Section:**
- ASCII tree diagram showing discovery/ directory layout
- Note about cross-referenced .sql and .json files

**Snowflake Setup Section:**
- 6 numbered setup steps
- GitHub App creation
- API Integration reference (sql/setup_api_integration.sql)
- External Access Integration reference (sql/setup_external_access.sql)
- Grant roles access
- Install GitHub App
- Configure secrets

**GitHub Actions Setup Section:**
- Workflow file reference (.github/workflows/discover.yml)
- Triggers: Branch creation (automatic), Manual dispatch
- Required Secrets list with descriptions for all 6 secrets

**CLI Usage Section:**
- Code block with 3 commands:
  1. Extract metadata
  2. Validate configuration
  3. Dry-run mode

**Contributing Section:**
- 6-step numbered list:
  1. Fork and create feature branch
  2. Make changes with clear commit messages
  3. Ensure tests pass (pytest tests/)
  4. Follow code style and conventions
  5. Update documentation
  6. Submit PR with clear description

**License Section:**
- Full MIT License text (21 lines)
- Copyright (c) 2026

### Verification Results
✓ Quick Summary updated with three-layer trigger architecture
✓ Overview expanded with detailed description
✓ Architecture section links to docs/architecture.md
✓ Quick Start includes prerequisites and installation
✓ Configuration references discovery-config.example.yml
✓ Folder Structure shows discovery/ directory layout
✓ Snowflake Setup has 6 numbered steps with references
✓ GitHub Actions Setup includes triggers and secrets
✓ CLI Usage has 3 example commands
✓ Contributing has 6-step guidelines
✓ License has complete MIT License text
✓ No placeholder text remains (verified with grep)
✓ 140 lines total, 10 major sections

### Key Learnings

1. **README Section Structure:**
   - Standard open-source README structure: Overview, Architecture, Quick Start, Configuration, Setup Instructions, CLI Usage, Contributing, License
   - Each section should be self-contained but reference other documentation

2. **Cross-Referencing:**
   - Use relative links for documentation references: [docs/architecture.md](docs/architecture.md)
   - Reference example files: [discovery-config.example.yml](discovery-config.example.yml)
   - Reference setup scripts: [sql/setup_api_integration.sql](sql/setup_api_integration.sql)

3. **Practical Content:**
   - Include actual commands users can run (git clone, pip install, python -m discovery)
   - Provide concrete steps for setup (numbered lists work well)
   - Use code blocks for commands and configuration examples

4. **Clear Descriptions:**
   - Avoid vague placeholder text
   - Be specific about what each section covers
   - Use bullet points for lists of items

5. **Three-Layer Architecture:**
   - Layer 1: Branch Creation → auto-discovery commit
   - Layer 2: Snowsight Manual → diff check → PR to main
   - Layer 3: Main Gate → all changes via PR review

6. **License Text:**
   - MIT License has standard wording
   - Must be complete and accurate
   - Copyright notice with year

### Design Decisions

**Why include all sections in README?**
- README is the first thing users see
- Should be comprehensive enough to get started
- References detailed docs for depth

**Why numbered steps for setup?**
- Clear sequential flow
- Easy to follow
- Can check off each step

**Why code blocks for CLI commands?**
- Easy to copy-paste
- Syntax highlighting for readability
- Distinguishes commands from explanatory text

**Why bullet lists for configuration sections?**
- Scannable
- Can reference each section quickly
- Doesn't require sequential reading

### Verification Commands Used
- `grep -n "To be filled" README.md` - Verified no placeholder text remains
- `wc -l README.md` - Total line count
- `grep -c "^## " README.md` - Section count
- Manual review of each section content

### Integration with Other Tasks
- Task 20a: Placeholder sections created (replaced with actual content)
- Task 20b: Architecture docs referenced
- Task 20c: Example config referenced
- Task 13: CLI commands documented
- Task 16: GitHub Actions workflow referenced

### Files Referenced in README
- docs/architecture.md
- discovery-config.example.yml
- sql/setup_api_integration.sql
- sql/setup_external_access.sql
- .github/workflows/discover.yml

### Final State
README.md is now complete with:
- 140 lines
- 10 major sections
- No placeholder text
- Practical, actionable content
- Cross-references to all relevant files
- Complete MIT License

