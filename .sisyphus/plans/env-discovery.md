# Environment Discovery — Snowflake Cortex Code Agentic Workflow

## TL;DR

> **Quick Summary**: Build a GitHub Actions-powered environment discovery system that extracts full Snowflake metadata catalog (DDLs, constraints, tags, masking policies, VARIANT schemas) into a version-controlled workspace structure, with Snowsight-triggered diff detection and three-layer trigger architecture.
>
> **Deliverables**:
> - Python extraction engine (Snowflake connector + SQL)
> - Hybrid output generator (.sql with comments + .json with rich metadata)
> - YAML-configurable discovery targets
> - Snowflake Notebook for diff detection + trigger
> - GitHub Actions workflow for automated extraction + commit
> - Adaptive VARIANT column structure interpreter
>
> **Estimated Effort**: Large
> **Parallel Execution**: YES - 4 waves + final verification
> **Critical Path**: T1 → T5 → T6 → T13 → T16 → T20 → F1-F4

---

## Context

### Original Request
Build an agentic development workflow on top of Cortex Code for Snowflake. First feature: environment discovery — deterministically load all DDLs and table-related metadata into a workspace file structure via GitHub Actions. Must be invokable from Snowsight workspace.

### Interview Summary
**Key Discussions**:
- **Metadata scope**: Full catalog — DDLs, column types/names/nullable/defaults, row counts, byte sizes, last DDL, clustering keys, constraints (PK/FK/UK), tags, masking policies, search optimization, stream/task dependencies
- **VARIANT interpretation**: Adaptive sampling based on table size with probability-based field coverage
- **Trigger mechanism**: Snowflake Notebook + GitHub API via External Access Integration
- **Diff strategy**: Full structural comparison in Python (notebook), git repo as state store
- **Three trigger layers**: (1) Branch creation → auto-discovery commit, (2) Snowsight manual → diff check → PR to main, (3) Main always PR-reviewed
- **Extraction tool**: Python connector + SQL (GET_DDL, INFORMATION_SCHEMA, ACCOUNT_USAGE) — NOT Snowpark
- **Auth**: Key-pair auth (private key in GitHub Secrets)
- **Output format**: Hybrid — .sql with comments + .json with rich metadata, cross-referenced
- **Folder hierarchy**: discovery/{db}/{schema}/{object_type}/{object_name}.{ext}
- **Error handling**: Retry N times then skip with failure log
- **Testing**: TDD with pytest
- **Notebook location**: In repo under notebooks/, synced to Snowflake via Git API Integration

**Research Findings**:
- Snowflake Cortex Code: SNOWFLAKE.CORTEX.COMPLETE() + REST API for AI code generation
- Snowsight Workspace: File-based editor GA Sept 2025, Git integration via API INTEGRATION
- GitHub Actions: Official snowflakedb/snowflake-cli-action@v2.0, supports OIDC and key-pair
- Metadata APIs: GET_DDL() for DDLs, INFORMATION_SCHEMA.TABLES/COLUMNS for metadata, ACCOUNT_USAGE for tags/policies
- VARIANT analysis: FLATTEN() + TYPEOF() with adaptive sampling

### Gap Analysis (Self-Conducted — Metis timed out)
**Gaps Identified & Addressed**:
- YAML config validation: Added as task — schema validation before extraction
- INFORMATION_SCHEMA limitations: ACCOUNT_USAGE has 45-min latency, noted in extraction strategy
- Path length issues on Windows: Mitigated by reasonable schema/type names
- VARIANT sampling edge cases: Empty VARIANTs, nested VARIANTs, mixed types — addressed in interpreter task
- Config hash for manifest: Ensures re-extraction when config changes, not just metadata changes

---

## Work Objectives

### Core Objective
Build a deterministic, version-controlled Snowflake environment discovery system that extracts full metadata catalog into a workspace file structure, triggered from Snowsight or GitHub branch creation.

### Concrete Deliverables
- Python package: `discovery/` with extraction, generation, diff, and orchestration modules
- YAML config: `discovery-config.yml` with targets and type filters
- GitHub Actions workflow: `.github/workflows/discover.yml`
- Snowflake Notebook: `notebooks/discovery_trigger.ipynb`
- SQL scripts: `sql/setup_api_integration.sql`, `sql/setup_external_access.sql`
- Output: `discovery/{db}/{schema}/{type}/{object}.sql|.json` + `_manifest.json`

### Definition of Done
- [ ] `python -m discovery extract --config discovery-config.yml` produces correct folder structure
- [ ] GitHub Actions workflow runs on `workflow_dispatch` and branch creation
- [ ] Snowflake Notebook triggers workflow after detecting changes
- [ ] All tests pass: `pytest tests/`
- [ ] Discovery output matches expected .sql + .json format per object type

### Must Have
- Full metadata catalog extraction (DDL + constraints + tags + policies + VARIANT schema)
- YAML-configurable discovery targets with type filters
- Hybrid .sql + .json output format with cross-references
- Three-layer trigger architecture (branch auto, Snowsight manual, main PR gate)
- Key-pair auth for GitHub Actions → Snowflake
- Adaptive VARIANT column structure interpretation
- Retry-then-skip error handling with failure log
- TDD with pytest

### Must NOT Have (Guardrails)
- No Snowpark sessions (use Python connector + SQL only)
- No cron/schedule triggers (manual only)
- No direct commits to main (always via PR)
- No OIDC auth (user chose key-pair)
- No storing credentials in repo (use GitHub Secrets)
- No AI-slop: no excessive comments, no over-abstraction, no generic variable names
- No human-in-the-loop acceptance criteria (all agent-executable verification)

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed.

### Test Decision
- **Infrastructure exists**: NO (fresh repo)
- **Automated tests**: TDD
- **Framework**: pytest
- **Setup**: Include pytest + pytest fixtures in pyproject.toml

### QA Policy
Every task MUST include agent-executed QA scenarios. Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Python modules**: Bash (`python -m pytest`, `python -c "import ..."`)
- **GitHub Actions**: Bash (validate YAML syntax, dry-run)
- **SQL scripts**: Bash (syntax validation via snowflake CLI or manual review)
- **Notebook**: Bash (nbconvert validation, cell execution check)

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately — foundation + independent modules):
├── Task 1: Project scaffolding + config [quick]
├── Task 2: YAML config schema + parser [unspecified-high]
├── Task 3: Data type definitions [quick]
├── Task 4: Snowflake connection module [quick]
├── Task 5: Metadata SQL queries module [unspecified-high]
├── Task 6: VARIANT structure interpreter [deep]
├── Task 7: Error handling + retry module [quick]
└── Task 8: GitHub Actions workflow skeleton [quick]

Wave 2 (After Wave 1 — core generators + orchestrator):
├── Task 9: DDL file generator (.sql with comments) [unspecified-high]
├── Task 10: Metadata file generator (.json) [unspecified-high]
├── Task 11: Output assembler (file paths + write) [unspecified-high]
├── Task 12: Diff engine [deep]
├── Task 13: Main extraction orchestrator [deep]
├── Task 14: Config files (pyproject.toml, pytest setup) [quick]
└── Task 15: Snowflake trigger notebook [unspecified-high]

Wave 3 (After Wave 2 — integration + deployment):
├── Task 16: GitHub Actions full workflow [unspecified-high]
├── Task 17: Git API integration SQL script [quick]
├── Task 18: External access integration SQL script [quick]
├── Task 19: Integration tests [unspecified-high]
└── Task 20: README + documentation [writing]

Wave FINAL (After ALL tasks — 4 parallel reviews, then user okay):
├── Task F1: Plan compliance audit (oracle)
├── Task F2: Code quality review (unspecified-high)
├── Task F3: Real manual QA (unspecified-high)
└── Task F4: Scope fidelity check (deep)
→ Present results → Get explicit user okay

Critical Path: T1 → T5 → T6 → T13 → T16 → T20 → F1-F4 → user okay
Parallel Speedup: ~65% faster than sequential
Max Concurrent: 8 (Wave 1)
```

### Dependency Matrix

- **T1-T4, T7-T8**: None — T9-T15
- **T5**: T4 — T9, T10, T13
- **T6**: T4 — T10, T13
- **T9**: T5 — T11, T13
- **T10**: T5, T6 — T11, T13
- **T11**: T9, T10 — T12, T13
- **T12**: T3, T11 — T15
- **T13**: T5, T6, T7, T8, T11 — T16, T19
- **T14**: T1 — T19
- **T15**: T12, T13 — T17, T18
- **T16**: T13 — F1-F4
- **T17**: T15 — F1-F4
- **T18**: T15 — F1-F4
- **T19**: T13, T14 — F1-F4
- **T20**: All — F1-F4
- **F1-F4**: All tasks — user okay

### Agent Dispatch Summary

- **Wave 1**: **8** — T1 → `quick`, T2 → `unspecified-high`, T3 → `quick`, T4 → `quick`, T5 → `unspecified-high`, T6 → `deep`, T7 → `quick`, T8 → `quick`
- **Wave 2**: **7** — T9 → `unspecified-high`, T10 → `unspecified-high`, T11 → `unspecified-high`, T12 → `deep`, T13 → `deep`, T14 → `quick`, T15 → `unspecified-high`
- **Wave 3**: **5** — T16 → `unspecified-high`, T17 → `quick`, T18 → `quick`, T19 → `unspecified-high`, T20 → `writing`
- **Wave FINAL**: **4** — F1 → `oracle`, F2 → `unspecified-high`, F3 → `unspecified-high`, F4 → `deep`

---

## TODOs

- [x] 1. Project Scaffolding

  **What to do**:
  - Create Python project structure: `src/discovery/` package with `__init__.py`
  - Create subpackages: `src/discovery/extract/`, `src/discovery/generate/`, `src/discovery/diff/`, `src/discovery/config/`, `src/discovery/utils/`
  - Create `src/discovery/__main__.py` with CLI entry point (argparse: `extract`, `diff`, `validate-config` subcommands)
  - Create `tests/` directory with `conftest.py` and `__init__.py`
  - Create `notebooks/` directory
  - Create `sql/` directory
  - Create `discovery/` output directory (gitignored except for committed discovery files)

  **Must NOT do**:
  - Do not add dependencies yet (Task 14)
  - Do not implement any logic — just directory structure and empty `__init__.py` files

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Simple directory creation and boilerplate files
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2-8)
  - **Blocks**: Tasks 9-14
  - **Blocked By**: None

  **References**:
  - Standard Python project layout: `src/` layout pattern for packages
  - CLI pattern: `python -m discovery <subcommand>` via `__main__.py`

  **Acceptance Criteria**:
  - [ ] `python -m discovery --help` runs without error (shows subcommands)
  - [ ] All `__init__.py` files exist and are importable
  - [ ] `tests/conftest.py` exists with basic fixture setup

  **QA Scenarios**:

  ```
  Scenario: CLI entry point works
    Tool: Bash
    Preconditions: Project scaffolding complete
    Steps:
      1. Run `python -m discovery --help`
      2. Assert output contains "extract", "diff", "validate-config" subcommands
    Expected Result: Help text displays all three subcommands
    Failure Indicators: ImportError, ModuleNotFoundError, or missing subcommands
    Evidence: .sisyphus/evidence/task-1-cli-help.txt

  Scenario: Package structure is correct
    Tool: Bash
    Preconditions: Project scaffolding complete
    Steps:
      1. Run `python -c "import discovery; import discovery.extract; import discovery.generate; import discovery.diff; import discovery.config; import discovery.utils"`
      2. Assert no ImportError
    Expected Result: All subpackages importable
    Failure Indicators: ImportError on any subpackage
    Evidence: .sisyphus/evidence/task-1-imports.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(discovery): scaffold project structure`
  - Files: `src/discovery/**`, `tests/**`, `notebooks/`, `sql/`


- [x] 2. YAML Config Schema + Parser

  **What to do**:
  - Create `src/discovery/config/schema.py` — define Pydantic models for config:
    - `DiscoveryConfig` (top-level)
    - `TargetConfig` (database + schemas)
    - `SchemaConfig` (name, include_types, exclude_types)
    - `VariantSamplingConfig` (thresholds and sample sizes)
    - `OutputConfig` (base_path, sql_comments, json_metadata)
  - Create `src/discovery/config/parser.py` — `load_config(path: str) -> DiscoveryConfig`
  - Create `src/discovery/config/validator.py` — validate config (check types exist, paths valid)
  - Include validation: reject unknown object types, require at least one target

  **Must NOT do**:
  - Do not implement Snowflake connection logic (Task 4)
  - Do not hardcode Snowflake-specific object types beyond the standard set

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Pydantic modeling requires careful schema design with validation rules
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 3-8)
  - **Blocks**: Task 15 (notebook needs config)
  - **Blocked By**: None

  **References**:
  - Pydantic v2 docs: `https://docs.pydantic.dev/latest/` — BaseModel, Field, validators
  - Valid Snowflake object types: TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK, DYNAMIC_TABLE, STAGE, PIPE, SEQUENCE, EXTERNAL_TABLE

  **Acceptance Criteria**:
  - [ ] `python -c "from discovery.config import load_config; print(load_config('test-config.yml'))"` returns valid config object
  - [ ] Invalid config (unknown type, missing database) raises `ConfigValidationError`
  - [ ] All Pydantic models have proper type hints and field descriptions

  **QA Scenarios**:

  ```
  Scenario: Valid config loads successfully
    Tool: Bash
    Preconditions: test-config.yml exists with valid targets
    Steps:
      1. Create test-config.yml with one database, one schema, TABLE type
      2. Run `python -c "from discovery.config import load_config; c = load_config('test-config.yml'); print(c.targets[0].database)"`
    Expected Result: Prints database name
    Failure Indicators: ConfigValidationError or parse error
    Evidence: .sisyphus/evidence/task-2-valid-config.txt

  Scenario: Invalid config raises error
    Tool: Bash
    Preconditions: invalid-config.yml with UNKNOWN_TYPE in include_types
    Steps:
      1. Create invalid-config.yml with include_types: ["UNKNOWN_TYPE"]
      2. Run `python -c "from discovery.config import load_config; load_config('invalid-config.yml')"`
    Expected Result: Raises ConfigValidationError with message about invalid type
    Failure Indicators: No error raised or generic Python error
    Evidence: .sisyphus/evidence/task-2-invalid-config.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(config): add YAML config schema and parser`
  - Files: `src/discovery/config/schema.py`, `src/discovery/config/parser.py`, `src/discovery/config/validator.py`


- [x] 3. Data Type Definitions

  **What to do**:
  - Create `src/discovery/types.py` — TypedDict/dataclass definitions:
    - `TableMetadata`: name, schema, database, ddl, columns, row_count, bytes, last_ddl, clustering_key, constraints, tags, masking_policies, search_optimization, variant_schema
    - `ColumnMetadata`: name, data_type, nullable, default_value, comment
    - `ConstraintMetadata`: name, type (PK/FK/UK), columns, referenced_table, referenced_columns
    - `TagAssignment`: tag_name, tag_value, column_name (nullable)
    - `MaskingPolicy`: policy_name, signature, column_name
    - `VariantSchema`: column_name, inferred_structure (nested dict), sample_size, confidence
    - `ViewMetadata`: name, schema, database, ddl, columns, base_tables, last_ddl, tags
    - `ProcedureMetadata`: name, schema, database, ddl, parameters, return_type, language, last_ddl
    - `StreamMetadata`: name, schema, database, ddl, source_object, mode, last_ddl
    - `TaskMetadata`: name, schema, database, ddl, schedule, state, predecessors, last_ddl
    - `DiscoveryManifest`: format_version, generated_at, snowflake_account, config_hash, object_count, errors
    - `DiscoveryError`: object_name, object_type, error_message, retry_count

  **Must NOT do**:
  - Do not implement serialization/deserialization (will be in generators)
  - Do not add Snowflake connection logic

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Pure type definitions, no logic
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 4-8)
  - **Blocks**: Tasks 5, 6, 9, 10, 11, 12
  - **Blocked By**: None

  **References**:
  - Python TypedDict: `from typing import TypedDict` for structured dicts
  - Python dataclasses: `from dataclasses import dataclass` for mutable objects

  **Acceptance Criteria**:
  - [ ] All types importable: `from discovery.types import TableMetadata, ColumnMetadata, ...`
  - [ ] Each type has complete field definitions with type hints
  - [ ] `TableMetadata` includes all fields from the finalized architecture

  **QA Scenarios**:

  ```
  Scenario: Types are importable and complete
    Tool: Bash
    Preconditions: types.py written
    Steps:
      1. Run `python -c "from discovery.types import TableMetadata, ColumnMetadata, ConstraintMetadata, TagAssignment, MaskingPolicy, VariantSchema, ViewMetadata, ProcedureMetadata, StreamMetadata, TaskMetadata, DiscoveryManifest, DiscoveryError"`
      2. Assert no ImportError
    Expected Result: All types import successfully
    Failure Indicators: ImportError or missing types
    Evidence: .sisyphus/evidence/task-3-types-import.txt

  Scenario: Type instantiation works
    Tool: Bash
    Preconditions: types.py written
    Steps:
      1. Run `python -c "from discovery.types import TableMetadata; tm = TableMetadata(name='test', schema='PUBLIC', database='DB', ddl='CREATE TABLE test (id INT);', columns=[], row_count=0, bytes=0, last_ddl='2025-01-01', clustering_key=None, constraints=[], tags=[], masking_policies=[], search_optimization=False, variant_schema=None); print(tm.name)"`
    Expected Result: Prints "test"
    Failure Indicators: TypeError (missing required fields) or AttributeError
    Evidence: .sisyphus/evidence/task-3-instantiate.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(types): add metadata type definitions`
  - Files: `src/discovery/types.py`


- [x] 4. Snowflake Connection Module

  **What to do**:
  - Create `src/discovery/extract/connection.py` — Snowflake connection management:
    - `SnowflakeConnection` class wrapping `snowflake.connector`
    - `connect(config: dict) -> SnowflakeConnection` factory
    - Key-pair auth: load private key from env var `SNOWFLAKE_PRIVATE_KEY_RAW` (PEM format), convert to DER for connector
    - Connection parameters from env vars: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_PRIVATE_KEY_RAW`, `SNOWFLAKE_ROLE` (optional)
    - `execute_query(sql: str) -> list[dict]` method returning rows as dicts
    - `close()` method for cleanup
    - Context manager support (`__enter__`/`__exit__`)

  **Must NOT do**:
  - Do not hardcode credentials
  - Do not implement retry logic (Task 7 handles that)
  - Do not implement metadata queries (Task 5)

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Standard connector wrapper, well-documented API
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1-3, 5-8)
  - **Blocks**: Tasks 5, 6, 13
  - **Blocked By**: None

  **References**:
  - Snowflake Connector Python: `https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example`
  - Key-pair auth: Load PEM, convert with `serialization.load_pem_private_key()`, serialize to DER with `private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8, encryption_algorithm=serialization.NoEncryption())`
  - cryptography library: `from cryptography.hazmat.primitives import serialization`

  **Acceptance Criteria**:
  - [ ] `SnowflakeConnection` class can be instantiated with env vars
  - [ ] Key-pair auth loads from `SNOWFLAKE_PRIVATE_KEY_RAW` env var
  - [ ] Context manager works: `with SnowflakeConnection(cfg) as conn: conn.execute_query(...)`
  - [ ] `execute_query` returns list of dicts with column names as keys

  **QA Scenarios**:

  ```
  Scenario: Connection module imports and instantiates
    Tool: Bash
    Preconditions: Module written, no Snowflake connection needed
    Steps:
      1. Run `python -c "from discovery.extract.connection import SnowflakeConnection; print('OK')"`
    Expected Result: Prints "OK"
    Failure Indicators: ImportError or syntax error
    Evidence: .sisyphus/evidence/task-4-import.txt

  Scenario: Key-pair loading works
    Tool: Bash
    Preconditions: Generate a test RSA key pair
    Steps:
      1. Generate test key: `openssl genrsa -out /tmp/test.pem 2048`
      2. Run `python -c "from discovery.extract.connection import load_private_key; pk = load_private_key(open('/tmp/test.pem').read()); print(type(pk))"`
    Expected Result: Prints key type (e.g., RSAPrivateKey)
    Failure Indicators: ValueError, TypeError, or format error
    Evidence: .sisyphus/evidence/task-4-keypair.txt

  Scenario: Invalid key raises clear error
    Tool: Bash
    Preconditions: Module written
    Steps:
      1. Run `python -c "from discovery.extract.connection import load_private_key; load_private_key('not-a-valid-key')"`
    Expected Result: Raises ValueError with clear message about invalid PEM format
    Failure Indicators: Generic Python traceback or unclear error message
    Evidence: .sisyphus/evidence/task-4-invalid-key.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(extract): add Snowflake connection module with key-pair auth`
  - Files: `src/discovery/extract/connection.py`


- [x] 5. Metadata SQL Queries Module

  **What to do**:
  - Create `src/discovery/extract/queries.py` — SQL query builders:
    - `get_ddl_query(object_type: str, object_name: str) -> str` — wraps `GET_DDL()`
    - `list_tables_query(schema: str) -> str` — queries `INFORMATION_SCHEMA.TABLES`
    - `list_columns_query(schema: str, table: str) -> str` — queries `INFORMATION_SCHEMA.COLUMNS`
    - `list_constraints_query(schema: str) -> str` — queries `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE`
    - `list_tags_query(schema: str) -> str` — queries `SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`
    - `list_masking_policies_query(schema: str) -> str` — queries `SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES`
    - `get_variant_columns_query(schema: str, table: str) -> str` — identifies VARIANT columns
    - `get_table_storage_query(schema: str) -> str` — queries `SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS`
  - Each function returns a SQL string (no execution — that's the connection module's job)
  - Handle INFORMATION_SCHEMA limitations: note that ACCOUNT_USAGE has ~45 min latency

  **Must NOT do**:
  - Do not execute queries (connection module does that)
  - Do not parse results (orchestrator does that)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Requires understanding of Snowflake INFORMATION_SCHEMA structure and edge cases
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1-4, 6-8)
  - **Blocks**: Tasks 9, 10, 13
  - **Blocked By**: None (uses types from Task 3 but loosely coupled)

  **References**:
  - GET_DDL: `https://docs.snowflake.com/sql-reference/functions/get_ddl`
  - INFORMATION_SCHEMA.TABLES: `https://docs.snowflake.com/sql-reference/info-schema/tables`
  - INFORMATION_SCHEMA.COLUMNS: `https://docs.snowflake.com/sql-reference/info-schema/columns`
  - ACCOUNT_USAGE.TAG_REFERENCES: `https://docs.snowflake.com/sql-reference/account-usage/tag_references`
  - ACCOUNT_USAGE.MASKING_POLICIES: `https://docs.snowflake.com/sql-reference/account-usage/masking_policies`

  **Acceptance Criteria**:
  - [ ] All query functions return valid SQL strings
  - [ ] `get_ddl_query('TABLE', 'MY_DB.MY_SCHEMA.MY_TABLE')` returns correct GET_DDL syntax
  - [ ] All queries use parameterized schema/table names (no SQL injection)

  **QA Scenarios**:

  ```
  Scenario: Query functions generate valid SQL
    Tool: Bash
    Preconditions: queries.py written
    Steps:
      1. Run `python -c "from discovery.extract.queries import get_ddl_query, list_tables_query; print(get_ddl_query('TABLE', 'DB.SCHEMA.TBL')); print(list_tables_query('SCHEMA'))"`
    Expected Result: Prints valid SQL strings with GET_DDL and INFORMATION_SCHEMA.TABLES
    Failure Indicators: ImportError or malformed SQL
    Evidence: .sisyphus/evidence/task-5-queries.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(extract): add metadata SQL query builders`
  - Files: `src/discovery/extract/queries.py`


- [x] 6. VARIANT Structure Interpreter

  **What to do**:
  - Create `src/discovery/extract/variant_interpreter.py`:
    - `interpret_variant_column(conn, db, schema, table, column, row_count, config) -> VariantSchema`
    - Adaptive sampling logic:
      - <1K rows: sample all
      - 1K-100K: sample 1000
      - 100K-1M: sample 5000
      - >1M: sample 10000
    - Use `SELECT {column} FROM {db}.{schema}.{table} SAMPLE ({sample_size})` or `TABLESAMPLE`
    - Parse sampled JSON: `json.loads()` each row
    - Merge schemas: recursively union all keys found across samples
    - Track field occurrence frequency (confidence = fields_seen / total_sampled)
    - Handle edge cases: NULL values, empty objects, nested arrays, mixed types
    - Return `VariantSchema` with inferred structure as nested dict

  **Must NOT do**:
  - Do not modify the source data
  - Do not sample more than the configured maximum
  - Do not fail on unparseable values — log and skip

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Complex adaptive sampling + schema inference logic with edge cases
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1-5, 7-8)
  - **Blocks**: Tasks 10, 13
  - **Blocked By**: None (uses connection module T4 and types T3 but loosely coupled)

  **References**:
  - Snowflake SAMPLE: `https://docs.snowflake.com/sql-reference/constructs/sample`
  - Python json module: `json.loads()` for parsing VARIANT values
  - Schema merging pattern: recursive dict union with type tracking

  **Acceptance Criteria**:
  - [ ] Function correctly determines sample size based on row count thresholds
  - [ ] Handles NULL VARIANT values without crashing
  - [ ] Returns nested dict representing JSON structure
  - [ ] Confidence score reflects field occurrence frequency

  **QA Scenarios**:

  ```
  Scenario: Adaptive sampling selects correct sample size
    Tool: Bash
    Preconditions: variant_interpreter.py written
    Steps:
      1. Run `python -c "from discovery.extract.variant_interpreter import get_sample_size; print(get_sample_size(500)); print(get_sample_size(50000)); print(get_sample_size(500000)); print(get_sample_size(5000000))"`
    Expected Result: Prints 500, 1000, 5000, 10000
    Failure Indicators: Wrong sample sizes or errors
    Evidence: .sisyphus/evidence/task-6-sample-sizes.txt

  Scenario: Schema inference from sample JSON
    Tool: Bash
    Preconditions: variant_interpreter.py written
    Steps:
      1. Run `python -c "from discovery.extract.variant_interpreter import infer_schema; sample = [{'a': 1, 'b': {'c': 'x'}}, {'a': 2, 'b': {'c': 'y', 'd': True}}]; schema = infer_schema(sample); print(schema)"`
    Expected Result: Prints nested dict: {'a': 'number', 'b': {'c': 'string', 'd': 'boolean'}}
    Failure Indicators: Incorrect schema or crash on nested objects
    Evidence: .sisyphus/evidence/task-6-schema-inference.txt

  Scenario: Handles NULL and empty values
    Tool: Bash
    Preconditions: variant_interpreter.py written
    Steps:
      1. Run `python -c "from discovery.extract.variant_interpreter import infer_schema; sample = [{'a': 1}, None, {}, {'a': None}]; schema = infer_schema(sample); print(schema)"`
    Expected Result: Prints schema with 'a' as nullable, handles None and empty dicts
    Failure Indicators: Crash on None or empty dict
    Evidence: .sisyphus/evidence/task-6-null-handling.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(extract): add VARIANT column structure interpreter with adaptive sampling`
  - Files: `src/discovery/extract/variant_interpreter.py`


- [x] 7. Error Handling + Retry Module

  **What to do**:
  - Create `src/discovery/utils/retry.py`:
    - `@retry(max_attempts=3, delay=1, backoff=2)` decorator
    - Catches `snowflake.connector.errors.DatabaseError` and `ProgrammingError`
    - Logs each retry attempt with error details
    - After max retries: raises `ExtractionError` with context
  - Create `src/discovery/utils/errors.py`:
    - `ExtractionError` — base exception with object context
    - `ConfigValidationError` — for invalid config
    - `ConnectionError` — for Snowflake connection failures
    - `PartialExtractionError` — for when some objects succeed and some fail
  - Create `src/discovery/utils/logging.py`:
    - Structured logging setup (Python logging module)
    - Format: timestamp, level, module, message
    - Log to both console and optional file

  **Must NOT do**:
  - Do not implement Snowflake-specific error handling beyond connector exceptions
  - Do not suppress errors silently

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Standard retry decorator and exception classes
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1-6, 8)
  - **Blocks**: Task 13
  - **Blocked By**: None

  **References**:
  - Python decorator pattern: `functools.wraps` for preserving function metadata
  - Snowflake connector exceptions: `snowflake.connector.errors.DatabaseError`, `ProgrammingError`

  **Acceptance Criteria**:
  - [ ] Retry decorator retries on exception and eventually raises after max attempts
  - [ ] Custom exceptions have meaningful string representations
  - [ ] Logging outputs structured format

  **QA Scenarios**:

  ```
  Scenario: Retry decorator works
    Tool: Bash
    Preconditions: retry.py written
    Steps:
      1. Run `python -c "from discovery.utils.retry import retry; call_count = 0; @retry(max_attempts=3, delay=0.1); def flaky(): global call_count; call_count += 1; raise ValueError('fail'); try: flaky(); except: pass; print(f'Called {call_count} times')"`
    Expected Result: Prints "Called 3 times"
    Failure Indicators: Called once (no retry) or more than 3 times
    Evidence: .sisyphus/evidence/task-7-retry.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `feat(utils): add error handling, retry decorator, and logging`
  - Files: `src/discovery/utils/retry.py`, `src/discovery/utils/errors.py`, `src/discovery/utils/logging.py`


- [x] 8. GitHub Actions Workflow Skeleton

  **What to do**:
  - Create `.github/workflows/discover.yml` — skeleton workflow:
    - Trigger: `workflow_dispatch` (manual) + `create` (branch creation)
    - Jobs: `extract-and-commit`
    - Steps: checkout, setup Python, install deps, run extraction, commit
    - Environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_PRIVATE_KEY_RAW` (from secrets)
    - Skip commit if no changes (git diff --quiet check)
  - Use `actions/checkout@v4`, `actions/setup-python@v5`
  - Placeholder for extraction command: `python -m discovery extract --config discovery-config.yml`

  **Must NOT do**:
  - Do not implement the extraction logic (that's the Python code)
  - Do not add OIDC auth (user chose key-pair)
  - Do not add cron schedule (manual only)

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: YAML workflow definition, well-documented GitHub Actions syntax
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1-7)
  - **Blocks**: Task 16
  - **Blocked By**: None

  **References**:
  - GitHub Actions workflow_dispatch: `https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#onworkflow_dispatch`
  - GitHub Actions create event: `https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows#create`
  - actions/checkout@v4: `https://github.com/actions/checkout`
  - actions/setup-python@v5: `https://github.com/actions/setup-python`

  **Acceptance Criteria**:
  - [ ] Workflow YAML is valid (no syntax errors)
  - [ ] Triggers on `workflow_dispatch` and `create` events
  - [ ] Uses `actions/checkout@v4` and `actions/setup-python@v5`
  - [ ] Secrets are referenced correctly: `${{ secrets.SNOWFLAKE_* }}`

  **QA Scenarios**:

  ```
  Scenario: Workflow YAML is syntactically valid
    Tool: Bash
    Preconditions: discover.yml written
    Steps:
      1. Run `python -c "import yaml; yaml.safe_load(open('.github/workflows/discover.yml'))"`
    Expected Result: No YAML parse error
    Failure Indicators: yaml.YAMLError
    Evidence: .sisyphus/evidence/task-8-yaml-valid.txt
  ```

  **Commit**: YES (grouped with Wave 1)
  - Message: `ci: add discover workflow skeleton`
  - Files: `.github/workflows/discover.yml`


- [x] 9. DDL File Generator (.sql with comments)

  **What to do**:
  - Create `src/discovery/generate/ddl_generator.py`:
    - `generate_ddl_file(metadata: TableMetadata) -> str` — produces .sql content
    - Header comment block: object name, type, database, schema, generated timestamp
    - Inline comments on key columns: primary key, foreign key, clustering key
    - Footer comment: row count, byte size, last DDL, tags, masking policies
    - Format: clean SQL with `--` comments, no excessive decoration
    - Handle all object types: TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK
  - Each object type has its own generator function but shares common comment template

  **Must NOT do**:
  - Do not implement .json generation (Task 10)
  - Do not write files to disk (Task 11)
  - Do not modify the DDL text itself — only add comments

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Template generation for multiple object types requires careful formatting
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 10-15)
  - **Blocks**: Tasks 11, 13
  - **Blocked By**: Tasks 5 (query results), 3 (types)

  **References**:
  - SQL comment syntax: `--` for single-line, `/* */` for multi-line
  - GET_DDL output format: See Snowflake docs for typical DDL structure

  **Acceptance Criteria**:
  - [ ] `generate_ddl_file(table_metadata)` returns valid .sql string with DDL + comments
  - [ ] Comments include: row count, byte size, last DDL, tags
  - [ ] Works for TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK

  **QA Scenarios**:

  ```
  Scenario: DDL file generation for table
    Tool: Bash
    Preconditions: ddl_generator.py written, sample TableMetadata
    Steps:
      1. Run `python -c "from discovery.generate.ddl_generator import generate_ddl_file; from discovery.types import TableMetadata; tm = TableMetadata(name='users', schema='PUBLIC', database='ANALYTICS', ddl='CREATE TABLE users (id INT, name VARCHAR);', columns=[], row_count=1000, bytes=50000, last_ddl='2025-01-01', clustering_key=None, constraints=[], tags=[], masking_policies=[], search_optimization=False, variant_schema=None); print(generate_ddl_file(tm))"`
    Expected Result: Output contains DDL and comments with row count, bytes, last DDL
    Failure Indicators: Missing comments, malformed SQL, crash
    Evidence: .sisyphus/evidence/task-9-ddl-table.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `feat(generate): add DDL file generator with metadata comments`
  - Files: `src/discovery/generate/ddl_generator.py`


- [ ] 10. Metadata File Generator (.json)

  **What to do**:
  - Create `src/discovery/generate/metadata_generator.py`:
    - `generate_metadata_json(metadata) -> dict` — produces JSON-serializable dict
    - Tables: all fields from TableMetadata + variant_schema if present
    - Views: DDL, columns, base_tables (dependencies), last_ddl, tags
    - Procedures/Functions: DDL, parameters (name/type/default), return_type, language, last_ddl
    - Streams/Tasks: DDL, source_object, schedule, state, last_ddl
    - Cross-reference: include `ddl_file` field pointing to corresponding .sql file path
    - Use `dataclasses.asdict()` or manual serialization for clean output
  - Create `src/discovery/generate/manifest_generator.py`:
    - `generate_manifest(config, results, errors) -> dict` — produces _manifest.json content
    - Fields: format_version, generated_at, snowflake_account, config_hash, object_count, errors

  **Must NOT do**:
  - Do not implement .sql generation (Task 9)
  - Do not write files to disk (Task 11)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: JSON schema design for multiple object types, cross-referencing logic
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 9, 11-15)
  - **Blocks**: Tasks 11, 13
  - **Blocked By**: Tasks 5, 6 (query results + variant schema), 3 (types)

  **References**:
  - Python dataclasses.asdict: `dataclasses.asdict(instance)` for clean serialization
  - JSON serialization: `json.dumps(obj, indent=2, default=str)` for datetime handling

  **Acceptance Criteria**:
  - [ ] `generate_metadata_json(table_metadata)` returns dict with all fields populated
  - [ ] Includes `ddl_file` cross-reference pointing to .sql path
  - [ ] `generate_manifest()` returns valid manifest with format_version, counts, errors
  - [ ] All datetime values serialized as ISO strings

  **QA Scenarios**:

  ```
  Scenario: Metadata JSON generation for table
    Tool: Bash
    Preconditions: metadata_generator.py written
    Steps:
      1. Run `python -c "from discovery.generate.metadata_generator import generate_metadata_json; import json; from discovery.types import TableMetadata; tm = TableMetadata(name='users', schema='PUBLIC', database='ANALYTICS', ddl='CREATE TABLE users (id INT);', columns=[{'name': 'id', 'data_type': 'INT', 'nullable': True, 'default_value': None, 'comment': None}], row_count=1000, bytes=50000, last_ddl='2025-01-01', clustering_key=None, constraints=[], tags=[], masking_policies=[], search_optimization=False, variant_schema=None); print(json.dumps(generate_metadata_json(tm), indent=2))"`
    Expected Result: Valid JSON with columns, row_count, bytes, ddl_file reference
    Failure Indicators: JSON serialization error, missing fields
    Evidence: .sisyphus/evidence/task-10-json-table.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `feat(generate): add metadata JSON and manifest generators`
  - Files: `src/discovery/generate/metadata_generator.py`, `src/discovery/generate/manifest_generator.py`


- [x] 11. Output Assembler (file paths + write)

  **What to do**:
  - Create `src/discovery/generate/assembler.py`:
    - `build_output_path(db, schema, object_type, object_name, ext) -> Path` — constructs file path
    - Path format: `{base_path}/{db}/{schema}/{object_type_plural}/{object_name}.{ext}`
    - Object type pluralization: TABLE→tables, VIEW→views, PROCEDURE→procedures, FUNCTION→functions, STREAM→streams, TASK→tasks
    - `write_discovery_files(metadata, ddl_content, json_content, base_path)` — writes both .sql and .json
    - Creates directories as needed (`os.makedirs(exist_ok=True)`)
    - Sanitize object names for filesystem (replace special chars)
  - Create `src/discovery/generate/__init__.py` — export main functions

  **Must NOT do**:
  - Do not implement content generation (Tasks 9, 10)
  - Do not implement diff logic (Task 12)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: File path construction with edge cases (special chars, long names)
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 9, 10, 12-15)
  - **Blocks**: Tasks 12, 13
  - **Blocked By**: Tasks 9, 10 (needs content to write)

  **References**:
  - Python pathlib: `Path` for cross-platform path construction
  - Filename sanitization: replace `/\:*?"<>|` with `_`

  **Acceptance Criteria**:
  - [ ] `build_output_path('ANALYTICS', 'PUBLIC', 'TABLE', 'users', 'sql')` returns `discovery/ANALYTICS/PUBLIC/tables/users.sql`
  - [ ] `write_discovery_files()` creates both .sql and .json files
  - [ ] Directories created automatically if they don't exist

  **QA Scenarios**:

  ```
  Scenario: Output path construction
    Tool: Bash
    Preconditions: assembler.py written
    Steps:
      1. Run `python -c "from discovery.generate.assembler import build_output_path; print(build_output_path('ANALYTICS', 'PUBLIC', 'TABLE', 'users', 'sql'))"`
    Expected Result: Prints "discovery/ANALYTICS/PUBLIC/tables/users.sql"
    Failure Indicators: Wrong path format
    Evidence: .sisyphus/evidence/task-11-path.txt

  Scenario: File writing creates correct structure
    Tool: Bash
    Preconditions: assembler.py written, temp dir
    Steps:
      1. Run script that calls write_discovery_files with test data to /tmp/test_output
      2. Assert /tmp/test_output/ANALYTICS/PUBLIC/tables/users.sql exists
      3. Assert /tmp/test_output/ANALYTICS/PUBLIC/tables/users.json exists
    Expected Result: Both files exist with correct content
    Failure Indicators: Missing files or wrong directory structure
    Evidence: .sisyphus/evidence/task-11-write.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `feat(generate): add output assembler with path construction and file writing`
  - Files: `src/discovery/generate/assembler.py`, `src/discovery/generate/__init__.py`


- [ ] 12. Diff Engine

  **What to do**:
  - Create `src/discovery/diff/engine.py`:
    - `DiffEngine` class with `compare(current_state, previous_state) -> DiffResult`
    - `DiffResult`: has_changes (bool), added_objects, removed_objects, modified_objects, summary (str)
    - Structural comparison: compare object lists, DDL hashes, column counts, constraint counts
    - NOT byte-level diff — structural: "table X gained a column", "view Y was dropped"
    - `compute_state_hash(metadata) -> str` — hash of all DDLs + schemas for quick comparison
    - `load_previous_state(repo_path) -> dict` — read existing discovery files as previous state
    - `extract_current_state(extraction_results) -> dict` — format extraction results for comparison
  - Create `src/discovery/diff/__init__.py`

  **Must NOT do**:
  - Do not implement git operations (that's the notebook/GitHub Action's job)
  - Do not trigger workflows (Task 15)

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Structural diff logic with multiple comparison strategies
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 9-11, 13-15)
  - **Blocks**: Task 15 (notebook uses diff engine)
  - **Blocked By**: Tasks 3 (types), 11 (needs file structure)

  **References**:
  - Python hashlib: `hashlib.sha256()` for state hashing
  - Structural diff pattern: compare sorted lists of objects, detect additions/removals/modifications

  **Acceptance Criteria**:
  - [ ] `DiffEngine().compare(same_state, same_state)` returns `has_changes=False`
  - [ ] `DiffEngine().compare(state_with_new_table, old_state)` returns `has_changes=True` with new table in added_objects
  - [ ] `compute_state_hash()` produces consistent hash for identical metadata

  **QA Scenarios**:

  ```
  Scenario: Identical states produce no diff
    Tool: Bash
    Preconditions: engine.py written
    Steps:
      1. Run `python -c "from discovery.diff.engine import DiffEngine; d = DiffEngine(); result = d.compare({'tables': {'users': 'CREATE TABLE users (id INT);'}}, {'tables': {'users': 'CREATE TABLE users (id INT);'}}); print(result.has_changes)"`
    Expected Result: Prints "False"
    Failure Indicators: True or error
    Evidence: .sisyphus/evidence/task-12-same.txt

  Scenario: Different states produce diff
    Tool: Bash
    Preconditions: engine.py written
    Steps:
      1. Run `python -c "from discovery.diff.engine import DiffEngine; d = DiffEngine(); result = d.compare({'tables': {'users': 'CREATE TABLE users (id INT);', 'orders': 'CREATE TABLE orders (id INT);'}}, {'tables': {'users': 'CREATE TABLE users (id INT);'}}); print(result.has_changes, result.added_objects)"`
    Expected Result: Prints "True ['orders']"
    Failure Indicators: False or missing 'orders' in added_objects
    Evidence: .sisyphus/evidence/task-12-diff.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `feat(diff): add structural diff engine for metadata comparison`
  - Files: `src/discovery/diff/engine.py`, `src/discovery/diff/__init__.py`


- [x] 13. Main Extraction Orchestrator

  **What to do**:
  - Create `src/discovery/orchestrator.py`:
    - `run_extraction(config: DiscoveryConfig) -> ExtractionResult` — main entry point
    - Flow: connect → for each target DB/schema → for each object type → query metadata → generate files
    - Use connection module (Task 4) for Snowflake connection
    - Use queries module (Task 5) for SQL queries
    - Use variant interpreter (Task 6) for VARIANT columns
    - Use generators (Tasks 9-11) for output
    - Use retry module (Task 7) for error handling
    - Track: success count, failure count, errors list
    - `ExtractionResult`: total_objects, extracted, failed, errors, duration
    - Implement `python -m discovery extract --config path` CLI command in `__main__.py`
  - Create `src/discovery/__main__.py` — CLI arg parsing + orchestrator invocation

  **Must NOT do**:
  - Do not implement diff logic (separate concern)
  - Do not implement git operations
  - Do not hardcode config values

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Complex orchestration coordinating multiple modules with error handling
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (depends on Tasks 4-11)
  - **Blocks**: Tasks 15, 16, 19
  - **Blocked By**: Tasks 4, 5, 6, 7, 8, 9, 10, 11

  **References**:
  - Python argparse: subcommands for CLI
  - Extraction pattern: nested loops (databases → schemas → object types → objects)

  **Acceptance Criteria**:
  - [ ] `python -m discovery extract --config test-config.yml` runs without import errors (even without Snowflake connection)
  - [ ] Orchestrator coordinates all modules correctly
  - [ ] ExtractionResult contains accurate counts and error list
  - [ ] CLI shows progress output during extraction

  **QA Scenarios**:

  ```
  Scenario: CLI extract command loads
    Tool: Bash
    Preconditions: orchestrator.py and __main__.py written
    Steps:
      1. Run `python -m discovery extract --help`
    Expected Result: Shows --config argument help
    Failure Indicators: ImportError, argparse error
    Evidence: .sisyphus/evidence/task-13-cli-help.txt

  Scenario: Orchestrator handles config validation
    Tool: Bash
    Preconditions: orchestrator.py written
    Steps:
      1. Run `python -m discovery extract --config nonexistent.yml`
    Expected Result: Shows clear error about missing config file
    Failure Indicators: Generic Python traceback
    Evidence: .sisyphus/evidence/task-13-config-error.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `feat(orchestrator): add main extraction orchestrator and CLI`
  - Files: `src/discovery/orchestrator.py`, `src/discovery/__main__.py`


- [x] 14. Config Files (pyproject.toml, pytest setup, conftest.py fixtures, .gitignore) (pyproject.toml, pytest setup)

  **What to do**:
  - Create `pyproject.toml`:
    - Project metadata: name="snowflake-discovery", version="0.1.0"
    - Dependencies: snowflake-connector-python, pyyaml, pydantic, cryptography
    - Dev dependencies: pytest, pytest-cov, ruff, sqlparse
    - Build system: setuptools or hatchling
    - Entry point: `discovery = "discovery.__main__:main"`
  - Create `pytest.ini` or pyproject.toml pytest section:
    - Test discovery: `tests/`
    - Verbose output by default
  - Create `tests/conftest.py` with fixtures:
    - `sample_table_metadata` fixture
    - `sample_config` fixture
    - `tmp_discovery_dir` fixture (temporary output directory)
  - Create `.gitignore`:
    - `__pycache__/`, `.pytest_cache/`, `*.pyc`
    - `discovery/` output directory (except committed files)
    - `.env`, `*.pem` (credentials)
    - `.sisyphus/evidence/`

  **Must NOT do**:
  - Do not add unnecessary dependencies
  - Do not configure CI (GitHub Actions handles that)

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Standard Python project configuration
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 9-13, 15)
  - **Blocks**: Task 19
  - **Blocked By**: Task 1 (project structure)

  **References**:
  - pyproject.toml spec: `https://packaging.python.org/en/latest/guides/writing-pyproject-toml/`
  - pytest fixtures: `https://docs.pytest.org/en/stable/explanation/fixtures.html`

  **Acceptance Criteria**:
  - [ ] `pip install -e .` installs the package
  - [ ] `pytest tests/` discovers and runs tests
  - [ ] All dependencies install without conflicts

  **QA Scenarios**:

  ```
  Scenario: Package installs correctly
    Tool: Bash
    Preconditions: pyproject.toml written
    Steps:
      1. Run `pip install -e .`
      2. Run `python -c "import discovery; print(discovery.__version__)"`
    Expected Result: Prints version "0.1.0"
    Failure Indicators: Install error or ImportError
    Evidence: .sisyphus/evidence/task-14-install.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `build: add pyproject.toml and pytest configuration`
  - Files: `pyproject.toml`, `tests/conftest.py`, `.gitignore`


- [ ] 15. Snowflake Trigger Notebook

  **What to do**:
  - Create `notebooks/discovery_trigger.ipynb` — Snowflake Notebook for diff detection + trigger:
    - Cell 1: Imports (snowflake.connector, requests, json, yaml)
    - Cell 2: Load config from git repo (via Snowflake Git API Integration)
    - Cell 3: Query current Snowflake metadata (lightweight: table counts, MAX(last_ddl), column counts)
    - Cell 4: Fetch previous state from git repo (via GitHub API — read discovery/_manifest.json)
    - Cell 5: Run diff engine (import from discovery.diff)
    - Cell 6: If changes detected → call GitHub API to trigger workflow_dispatch
    - Cell 7: Log result (triggered or no changes)
  - Use External Access Integration for both GitHub API and Snowflake connection
  - Handle errors gracefully: connection failure, API rate limit, missing manifest

  **Must NOT do**:
  - Do not implement full extraction (that's the GitHub Action's job)
  - Do not store credentials in notebook (use Snowflake's connection)
  - Do not auto-commit results

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Multi-cell notebook with API integration and diff logic
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (depends on Tasks 12, 13)
  - **Blocks**: Tasks 17, 18
  - **Blocked By**: Tasks 12 (diff engine), 13 (orchestrator pattern)

  **References**:
  - Snowflake Notebooks: `https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks`
  - GitHub REST API workflow_dispatch: `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`
  - EXECUTE NOTEBOOK: `EXECUTE NOTEBOOK DB.SCHEMA.NOTEBOOK_NAME('args')`

  **Acceptance Criteria**:
  - [ ] Notebook has all 7 cells with correct logic flow
  - [ ] Cell 6 correctly formats GitHub API POST request
  - [ ] Notebook can be validated via `jupyter nbconvert --execute`

  **QA Scenarios**:

  ```
  Scenario: Notebook structure is valid
    Tool: Bash
    Preconditions: notebook written
    Steps:
      1. Run `python -c "import json; nb = json.load(open('notebooks/discovery_trigger.ipynb')); print(len(nb['cells']), 'cells')"`
    Expected Result: Prints "7 cells" (or similar)
    Failure Indicators: JSON parse error or wrong cell count
    Evidence: .sisyphus/evidence/task-15-notebook-structure.txt
  ```

  **Commit**: YES (grouped with Wave 2)
  - Message: `feat(notebook): add Snowflake trigger notebook with diff detection`
  - Files: `notebooks/discovery_trigger.ipynb`


- [ ] 16. GitHub Actions Full Workflow

  **What to do**:
  - Update `.github/workflows/discover.yml` from skeleton (Task 8) to full workflow:
    - Step: Install dependencies (`pip install -e .`)
    - Step: Run extraction (`python -m discovery extract --config discovery-config.yml`)
    - Step: Check for changes (`git diff --quiet discovery/`)
    - Step: If changes → commit with message `chore(discovery): update metadata [auto]`
    - Step: Push to current branch
    - Conditional: On `create` event (branch creation) → commit directly to new branch
    - Conditional: On `workflow_dispatch` (Snowsight trigger) → create PR to main
    - Error handling: Upload failure log as artifact on failure
  - Add `discovery/_errors/` to commit if failures occurred
  - Skip push if no changes detected

  **Must NOT do**:
  - Do not add cron triggers (manual only)
  - Do not use OIDC auth (key-pair from secrets)
  - Do not modify Python code

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Complex workflow with conditionals, PR creation, error handling
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (depends on Task 13)
  - **Blocks**: Task 19
  - **Blocked By**: Task 13 (orchestrator must work)

  **References**:
  - GitHub Actions create PR: `gh pr create` or `peter-evans/create-pull-request@v6`
  - GitHub Actions conditional: `if: github.event_name == 'create'`
  - GitHub Actions artifacts: `actions/upload-artifact@v4`

  **Acceptance Criteria**:
  - [ ] Workflow YAML validates with no syntax errors
  - [ ] Branch creation triggers commit to branch
  - [ ] workflow_dispatch triggers PR creation to main
  - [ ] Secrets are referenced correctly

  **QA Scenarios**:

  ```
  Scenario: Full workflow YAML validates
    Tool: Bash
    Preconditions: discover.yml updated
    Steps:
      1. Run `python -c "import yaml; wf = yaml.safe_load(open('.github/workflows/discover.yml')); print('Triggers:', list(wf['on'].keys())); print('Jobs:', list(wf['jobs'].keys()))"`
    Expected Result: Prints triggers (create, workflow_dispatch) and jobs (extract-and-commit)
    Failure Indicators: YAML error or missing triggers/jobs
    Evidence: .sisyphus/evidence/task-16-workflow-validate.txt
  ```

  **Commit**: YES (grouped with Wave 3)
  - Message: `ci: complete discover workflow with conditional triggers and PR creation`
  - Files: `.github/workflows/discover.yml`


- [ ] 17. Git API Integration SQL Script

  **What to do**:
  - Create `sql/setup_api_integration.sql`:
    - `CREATE OR REPLACE API INTEGRATION` for GitHub Git API
    - `API_PROVIDER = git_https_api`
    - `API_ALLOWED_PREFIXES = ('https://github.com/')`
    - `API_USER_AUTHENTICATION = (TYPE = snowflake_github_app)`
    - Include comments explaining each parameter
    - Include `CREATE GIT REPOSITORY` to sync notebooks from repo
  - Document required GitHub App installation steps

  **Must NOT do**:
  - Do not hardcode account-specific values (use placeholders)
  - Do not create the actual GitHub App (documentation only)

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: SQL script with standard Snowflake DDL
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 16, 18-20)
  - **Blocks**: None
  - **Blocked By**: Task 15 (notebook design informs git repo structure)

  **References**:
  - Snowflake Git API Integration: `https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces-git`
  - CREATE API INTEGRATION: `https://docs.snowflake.com/en/sql-reference/sql/create-api-integration`

  **Acceptance Criteria**:
  - [ ] SQL script contains valid CREATE API INTEGRATION statement
  - [ ] Includes placeholder comments for account-specific values
  - [ ] Includes CREATE GIT REPOSITORY statement

  **QA Scenarios**:

  ```
  Scenario: SQL syntax is valid
    Tool: Bash
    Preconditions: setup_api_integration.sql written
    Steps:
      1. Run `python -c "import sqlparse; sql = open('sql/setup_api_integration.sql').read(); parsed = sqlparse.parse(sql); print(f'{len(parsed)} statements parsed')"`
    Expected Result: Statements parsed without error
    Failure Indicators: SQL syntax error
    Evidence: .sisyphus/evidence/task-17-sql-valid.txt
  ```

  **Commit**: YES (grouped with Wave 3)
  - Message: `feat(sql): add Git API integration setup script`
  - Files: `sql/setup_api_integration.sql`


- [ ] 18. External Access Integration SQL Script

  **What to do**:
  - Create `sql/setup_external_access.sql`:
    - `CREATE OR REPLACE NETWORK RULE` for `api.github.com:443`
    - `CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION` bound to the network rule
    - Comments explaining security implications
    - Include example stored procedure showing how to use the integration
  - Document how to grant access to specific roles

  **Must NOT do**:
  - Do not hardcode credentials
  - Do not create overly permissive network rules

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: SQL script with standard Snowflake DDL
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 16, 17, 19, 20)
  - **Blocks**: None
  - **Blocked By**: Task 15 (notebook needs external access)

  **References**:
  - External Access Integration: `https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access`
  - NETWORK RULE: `https://docs.snowflake.com/en/sql-reference/sql/create-network-rule`

  **Acceptance Criteria**:
  - [ ] SQL script contains valid CREATE NETWORK RULE and CREATE EXTERNAL ACCESS INTEGRATION
  - [ ] Network rule is scoped to api.github.com only
  - [ ] Includes role grant example

  **QA Scenarios**:

  ```
  Scenario: SQL syntax is valid
    Tool: Bash
    Preconditions: setup_external_access.sql written
    Steps:
      1. Run `python -c "import sqlparse; sql = open('sql/setup_external_access.sql').read(); parsed = sqlparse.parse(sql); print(f'{len(parsed)} statements parsed')"`
    Expected Result: Statements parsed without error
    Failure Indicators: SQL syntax error
    Evidence: .sisyphus/evidence/task-18-sql-valid.txt
  ```

  **Commit**: YES (grouped with Wave 3)
  - Message: `feat(sql): add External Access integration setup script`
  - Files: `sql/setup_external_access.sql`


- [ ] 19. Integration Tests

  **What to do**:
  - Create `tests/` test files:
    - `tests/test_config.py` — test YAML config parsing, validation, edge cases
    - `tests/test_types.py` — test type instantiation and serialization
    - `tests/test_queries.py` — test SQL query generation (mock, no Snowflake)
    - `tests/test_ddl_generator.py` — test DDL file generation with various object types
    - `tests/test_metadata_generator.py` — test JSON metadata generation
    - `tests/test_assembler.py` — test path construction and file writing
    - `tests/test_diff_engine.py` — test structural comparison
    - `tests/test_variant_interpreter.py` — test sampling logic and schema inference
    - `tests/test_orchestrator.py` — test orchestrator with mocked Snowflake
  - Use pytest fixtures from conftest.py
  - Mock Snowflake connection (no real connection needed)
  - Aim for >80% code coverage

  **Must NOT do**:
  - Do not require real Snowflake connection (mock everything)
  - Do not test infrastructure code (GitHub Actions, SQL scripts)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Comprehensive test suite with mocking and fixtures
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (depends on all implementation tasks)
  - **Blocks**: Final verification
  - **Blocked By**: Tasks 2, 3, 5, 6, 9, 10, 11, 12, 13, 14

  **References**:
  - pytest mocking: `unittest.mock.patch` for Snowflake connection
  - pytest fixtures: `@pytest.fixture` for sample data

  **Acceptance Criteria**:
  - [ ] `pytest tests/ -v` passes all tests
  - [ ] All modules have corresponding test files
  - [ ] Mock Snowflake connection works correctly
  - [ ] Coverage >80%

  **QA Scenarios**:

  ```
  Scenario: All tests pass
    Tool: Bash
    Preconditions: All test files written
    Steps:
      1. Run `pytest tests/ -v --tb=short`
    Expected Result: All tests pass, 0 failures
    Failure Indicators: Any test failure
    Evidence: .sisyphus/evidence/task-19-test-results.txt
  ```

  **Commit**: YES (grouped with Wave 3)
  - Message: `test: add comprehensive test suite for all modules`
  - Files: `tests/test_*.py`


- [ ] 20. README + Documentation

  **What to do**:
  - Update `README.md`:
    - Project description and purpose
    - Architecture overview (trigger flow diagram)
    - Quick start guide
    - Configuration reference (discovery-config.yml schema)
    - Folder structure explanation
    - Snowflake setup instructions (API Integration, External Access)
    - GitHub Actions setup (secrets, OIDC/key-pair)
    - CLI usage examples
  - Create `docs/architecture.md`:
    - Detailed architecture decision records (ADRs)
    - Design tree decisions
    - Data flow diagrams
  - Create `discovery-config.example.yml` — example config file

  **Must NOT do**:
  - Do not include actual credentials or account-specific values
  - Do not create excessive documentation (keep it practical)

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Documentation and technical writing
  - **Skills**: []
    - No specialized skills needed

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (depends on all implementation tasks)
  - **Blocks**: Final verification
  - **Blocked By**: All tasks

  **References**:
  - README structure: Standard open-source README template
  - Mermaid diagrams: For architecture flow diagrams

  **Acceptance Criteria**:
  - [ ] README.md contains all required sections
  - [ ] Example config is valid YAML
  - [ ] Architecture doc explains the three trigger layers

  **QA Scenarios**:

  ```
  Scenario: Example config is valid
    Tool: Bash
    Preconditions: discovery-config.example.yml written
    Steps:
      1. Run `python -c "import yaml; yaml.safe_load(open('discovery-config.example.yml')); print('Valid YAML')"`
    Expected Result: Prints "Valid YAML"
    Failure Indicators: YAML parse error
    Evidence: .sisyphus/evidence/task-20-config-valid.txt
  ```

  **Commit**: YES (grouped with Wave 3)
  - Message: `docs: add README, architecture docs, and example config`
  - Files: `README.md`, `docs/architecture.md`, `discovery-config.example.yml`

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.

- [ ] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists (read file, run command). For each "Must NOT Have": search codebase for forbidden patterns — reject with file:line if found. Check evidence files exist in .sisyphus/evidence/. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **Code Quality Review** — `unspecified-high`
  Run `pytest tests/` + linter + `python -m py_compile`. Review all changed files for: bare excepts, unused imports, hardcoded credentials, console.log/print in prod code, commented-out code. Check AI slop: excessive comments, over-abstraction, generic names (data/result/item/temp).
  Output: `Tests [PASS/FAIL] | Lint [PASS/FAIL] | Files [N clean/N issues] | VERDICT`

- [ ] F3. **Real Manual QA** — `unspecified-high`
  Start from clean state. Run `python -m discovery extract --config discovery-config.yml` with a mock/test config. Verify output folder structure matches spec. Verify .sql files contain DDL + comments. Verify .json files contain full metadata schema. Test error handling: invalid config, connection failure, partial permissions. Save to `.sisyphus/evidence/final-qa/`.
  Output: `Scenarios [N/N pass] | Edge Cases [N tested] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual diff (git log/diff). Verify 1:1 — everything in spec was built (no missing), nothing beyond spec was built (no creep). Check "Must NOT do" compliance. Detect cross-task contamination: Task N touching Task M's files. Flag unaccounted changes.
  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | Unaccounted [CLEAN/N files] | VERDICT`

---

## Commit Strategy

- **Wave 1**: `feat(discovery): scaffold project + core modules` — pyproject.toml, src/discovery/**
- **Wave 2**: `feat(discovery): add generators + orchestrator` — src/discovery/generators/**, src/discovery/orchestrator.py
- **Wave 3**: `feat(discovery): add CI/CD + notebook + docs` — .github/workflows/**, notebooks/**, sql/**, README.md

---

## Success Criteria

### Verification Commands
```bash
# Project builds
python -m py_compile discovery/

# Tests pass
pytest tests/ -v

# Extraction works (with test config)
python -m discovery extract --config test-config.yml

# Output structure correct
ls -la discovery/ANALYTICS/PUBLIC/tables/

# YAML config validates
python -c "from discovery.config import load_config; load_config('discovery-config.yml')"

# GitHub Actions syntax valid
act --list  # or yamllint .github/workflows/discover.yml
```

### Final Checklist
- [ ] All "Must Have" present
- [ ] All "Must NOT Have" absent
- [ ] All tests pass
- [ ] Discovery output matches expected structure
- [ ] Three trigger layers functional
