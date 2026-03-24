# Technical Documentation: Snowflake Discovery Environment

This document provides a comprehensive technical and operational overview of how the Snowflake Discovery system works at the implementation level.

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Data Flow](#data-flow)
3. [Core Components](#core-components)
4. [Configuration System](#configuration-system)
5. [Snowflake Connection](#snowflake-connection)
6. [Metadata Extraction](#metadata-extraction)
7. [VARIANT Column Interpretation](#variant-column-interpretation)
8. [Output Generation](#output-generation)
9. [Diff Engine](#diff-engine)
10. [Error Handling](#error-handling)
11. [Three-Layer Trigger Architecture](#three-layer-trigger-architecture)
12. [GitHub Actions Integration](#github-actions-integration)

---

## System Architecture

The Snowflake Discovery system is a Python-based metadata extraction tool that connects to Snowflake, extracts DDL and metadata for database objects, and generates version-controlled artifacts.

### Technology Stack

- **Python 3.9+**: Core runtime
- **snowflake-connector-python**: Snowflake database connector
- **Pydantic v2**: Configuration validation and schema definition
- **PyYAML**: Configuration file parsing
- **pytest**: Testing framework with mocking support
- **cryptography**: RSA private key handling for authentication

### Module Structure

```
src/discovery/
├── __main__.py              # CLI entry point
├── orchestrator.py          # Main extraction coordinator
├── config/
│   ├── __init__.py         # Config loading utilities
│   ├── schema.py           # Pydantic models
│   ├── parser.py           # YAML parser
│   └── validator.py       # Config validation logic
├── extract/
│   ├── __init__.py
│   ├── connection.py       # Snowflake connection wrapper
│   ├── queries.py          # SQL query builders
│   └── variant_interpreter.py  # VARIANT schema inference
├── generate/
│   ├── __init__.py
│   ├── ddl_generator.py    # .sql file generation
│   ├── metadata_generator.py  # .json file generation
│   └── assembler.py        # Output file writer
├── diff/
│   ├── __init__.py
│   └── engine.py          # Structural diff engine
├── utils/
│   ├── __init__.py
│   ├── retry.py           # Retry decorator
│   ├── errors.py          # Custom exceptions
│   └── logging.py         # Logging configuration
└── types/
    └── __init__.py        # TypedDict/dataclass definitions
```

---

## Data Flow

### High-Level Extraction Flow

```
┌─────────────┐
│   CLI       │  python -m discovery extract --config config.yml
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ ExtractionOrchestrator                                    │
│  1. Load config from YAML                                 │
│  2. Establish Snowflake connection                        │
│  3. Iterate through targets (database → schema → objects)   │
│  4. For each object: extract → generate → write          │
└────────────────────┬──────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Metadata Extraction Pipeline                              │
│  1. List objects (INFORMATION_SCHEMA queries)            │
│  2. Get DDL (GET_DDL function)                          │
│  3. Get columns, constraints, tags, masking policies      │
│  4. Interpret VARIANT columns (adaptive sampling)          │
│  5. Build metadata objects (TypedDict/dataclass)          │
└────────────────────┬──────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Output Generation                                         │
│  1. Generate .sql files with inline comments            │
│  2. Generate .json files with rich metadata             │
│  3. Write to discovery/{database}/{schema}/{type}/       │
└────────────────────┬──────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Git Repository                                           │
│  1. Commit changes to branch                            │
│  2. Push to remote                                      │
│  3. Trigger GitHub Actions workflow                      │
└─────────────────────────────────────────────────────────────┘
```

### Detailed Object Extraction Flow

For each Snowflake object (TABLE, VIEW, PROCEDURE, FUNCTION, STREAM, TASK):

```
1. List Objects Query
   └─> INFORMATION_SCHEMA.TABLES / VIEWS / ROUTINES / STREAMS / TASKS
   └─> Returns: list of object names

2. For Each Object:
   ├─> GET_DDL Query
   │   └─> SELECT GET_DDL('TABLE', 'DB.SCHEMA.OBJECT')
   │   └─> Returns: complete CREATE statement
   │
   ├─> Columns Query (TABLE/VIEW only)
   │   └─> INFORMATION_SCHEMA.COLUMNS
   │   └─> Returns: column names, types, defaults, nullable, comments
   │
   ├─> Constraints Query (TABLE only)
   │   └─> INFORMATION_SCHEMA.TABLE_CONSTRAINTS + KEY_COLUMN_USAGE
   │   └─> Returns: PK, FK, UNIQUE, CHECK constraints with column mappings
   │
   ├─> Tags Query
   │   └─> ACCOUNT_USAGE.TAG_REFERENCES (45-min latency)
   │   └─> Returns: tag name, tag value, object domain
   │
   ├─> Masking Policies Query
   │   └─> INFORMATION_SCHEMA.POLICY_REFERENCES
   │   └─> Returns: policy name, signature, applied to column
   │
   └─> VARIANT Schema Query (TABLE with VARIANT columns only)
       └─> Get row count from STORAGE_METRICS
       └─> Calculate adaptive sample size
       └─> SAMPLE (N) rows from table
       └─> Parse JSON, infer schema recursively
       └─> Returns: inferred structure with confidence score

3. Build Metadata Object
   └─> TableMetadata / ViewMetadata / ProcedureMetadata / etc.
   └─> Contains: DDL, columns, constraints, tags, policies, variant_schema

4. Generate Output Files
   ├─> .sql file: DDL with inline comments (metadata header + footer)
   └─> .json file: Structured metadata for programmatic access

5. Write to Repository
   └─> discovery/{database}/{schema}/{type}/{database}.{schema}.{object}.{ext}
```

---

## Core Components

### 1. CLI Entry Point (`__main__.py`)

The CLI uses Python's `argparse` to provide three subcommands:

- **extract**: Run full metadata extraction
  ```bash
  python -m discovery extract --config discovery-config.yml
  ```
- **diff**: Compare discovery outputs (placeholder, not yet implemented)
  ```bash
  python -m discovery diff --config discovery-config.yml
  ```
- **validate-config**: Validate configuration file
  ```bash
  python -m discovery validate-config discovery-config.yml
  ```

**Exit Codes:**
- `0`: Success
- `1`: Partial extraction (some objects failed but overall completed)
- `1`: Configuration error, file not found, or unexpected error

### 2. Extraction Orchestrator (`orchestrator.py`)

The `ExtractionOrchestrator` class coordinates the entire extraction process.

#### Initialization

```python
orchestrator = ExtractionOrchestrator(config)
```

#### Main Execution Flow

1. **Connection Setup** (`_connect()`)
   - Reads environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_PRIVATE_KEY_RAW`, `SNOWFLAKE_ROLE`
   - Creates `SnowflakeConnection` instance
   - Establishes connection with key-pair authentication

2. **Target Processing** (`_process_target()`)
   - Iterates through each database in `config.targets`
   - For each schema in the database:
     - Determines object types to extract (`include_types` or all minus `exclude_types`)
     - Calls appropriate extraction method per object type

3. **Object Extraction** (`_extract_object_type()`)
   - Routes to specific extractor: `_extract_tables()`, `_extract_views()`, `_extract_procedures()`, etc.
   - Each extractor is decorated with `@retry(max_attempts=3, delay=1, backoff=2)`

4. **Metadata Extraction** (`_extract_table_metadata()`, etc.)
   - Executes multiple queries to gather complete metadata
   - Builds typed metadata objects (dataclass/TypedDict)
   - Generates .sql and .json content
   - Writes files via assembler

5. **Error Tracking**
   - Individual object failures don't stop extraction
   - Errors captured in `ExtractionResult.errors` list
   - Partial failures raise `PartialExtractionError` with statistics

#### Retry Decorator

All extraction methods are decorated with:

```python
@retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
```

This provides:
- Up to 3 retry attempts
- Exponential backoff: 1s, 2s, 4s delays
- Error logging on each retry attempt
- Skip to next object after final retry failure

---

## Configuration System

### Schema Definition (`config/schema.py`)

Configuration uses Pydantic v2 for validation and type safety.

#### Top-Level Model: `DiscoveryConfig`

```python
class DiscoveryConfig(BaseModel):
    targets: list[TargetConfig]  # List of database targets
    variant_sampling: VariantSamplingConfig  # VARIANT sampling settings
    output: OutputConfig  # Output file settings
```

**Validation Rules:**
- Must have at least 1 target
- Database names must be unique across targets
- Schema names must be unique within a database

#### Target Configuration: `TargetConfig`

```python
class TargetConfig(BaseModel):
    database: str  # Database name to discover
    schemas: list[SchemaConfig]  # List of schemas to discover (min 1)
```

#### Schema Configuration: `SchemaConfig`

```python
class SchemaConfig(BaseModel):
    name: str  # Schema name (e.g., 'PUBLIC', 'STAGING')
    include_types: list[str]  # Object types to include (empty = all)
    exclude_types: list[str]  # Object types to exclude
```

**Valid Object Types:**
- `TABLE`, `VIEW`, `PROCEDURE`, `FUNCTION`, `STREAM`, `TASK`, `DYNAMIC_TABLE`, `STAGE`, `PIPE`, `SEQUENCE`, `EXTERNAL_TABLE`

**Validation Rules:**
- Cannot specify both `include_types` and `exclude_types`
- All types must be in `SNOWFLAKE_OBJECT_TYPES` set

#### VARIANT Sampling Configuration: `VariantSamplingConfig`

```python
class VariantSamplingConfig(BaseModel):
    small_table_threshold: int = 1000  # Sample all rows below this
    medium_table_threshold: int = 100000  # Medium: 1K-100K rows
    large_table_threshold: int = 1000000  # Large: 100K-1M rows
    medium_table_sample_size: int = 1000  # Sample size for medium tables
    large_table_sample_size: int = 5000  # Sample size for large tables
    extra_large_sample_size: int = 10000  # Sample size for very large tables
    min_confidence: float = 0.5  # Minimum confidence for inferred fields
```

**Adaptive Sampling Logic:**

| Row Count | Sample Size |
|-----------|--------------|
| < 1,000 | All rows |
| 1,000 - 100,000 | 1,000 rows |
| 100,000 - 1,000,000 | 5,000 rows |
| > 1,000,000 | 10,000 rows |

#### Output Configuration: `OutputConfig`

```python
class OutputConfig(BaseModel):
    base_path: str = "discovery"  # Base directory for output
    sql_comments: bool = True  # Include metadata comments in .sql files
    json_metadata: bool = True  # Generate .json metadata files
```

### Configuration Loading (`config/parser.py`)

```python
from discovery.config import load_config

config = load_config("discovery-config.yml")
```

**Loading Process:**
1. Read YAML file
2. Parse into Pydantic models
3. Validate all constraints
4. Raise `ConfigValidationError` on invalid configuration

### Configuration Validation (`config/validator.py`)

```python
from discovery.config.validator import validate_config

validate_config(config)
```

**Validation Checks:**
- Pydantic model validation (types, required fields)
- Business logic validation (threshold ordering, no type conflicts)
- Raises `ConfigValidationError` with descriptive messages

---

## Snowflake Connection

### Connection Wrapper (`extract/connection.py`)

The `SnowflakeConnection` class provides a simplified interface to Snowflake's connector.

#### Key-Pair Authentication

**Why Key-Pair Authentication:**
- More secure than password-based auth
- No browser/OIDC flow required for automation
- Private keys can be stored in GitHub Secrets and rotated independently
- Supports service account authentication

**Authentication Flow:**

```python
# 1. Load private key from PEM format
private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')

# 2. Parse PEM and convert to DER format
der_key = self._load_private_key(private_key_raw)

# 3. Establish connection
conn = snowflake.connector.connect(
    account=account,
    user=user,
    private_key=der_key,  # DER format required
    warehouse=warehouse,
    database=database,
    role=role,
    application='snowflake-discovery',
)
```

**Private Key Conversion:**

```python
def _load_private_key(self, private_key_pem: str) -> bytes:
    # Load PEM private key
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode('utf-8'),
        password=None,
        backend=default_backend()
    )

    # Convert to DER format required by Snowflake
    der_key = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return der_key
```

**PEM Format:**
```
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAz8x8H3x8H3x8H3x8H3x8H3x8H3x8H3x8H3x8H3x8H3x8...
-----END RSA PRIVATE KEY-----
```

**DER Format:** Binary encoding (used by Snowflake connector)

#### Connection Context Manager

```python
with SnowflakeConnection(config) as conn:
    results = conn.execute_query("SELECT * FROM table")
# Connection automatically closed on exit
```

#### Query Execution

```python
results = conn.execute_query(sql_query)

# Returns: List[Dict[str, Any]]
# Example:
# [
#   {"column1": "value1", "column2": 123},
#   {"column1": "value2", "column2": 456},
# ]
```

**Error Handling:**
- `sf_errors.DatabaseError`: SQL execution errors
- `sf_errors.OperationalError`: Connection/operational errors
- All errors wrapped in `ConnectionError` with logging

---

## Metadata Extraction

### SQL Query Builders (`extract/queries.py`)

The queries module provides functions that return SQL strings (not execute queries).

#### INFORMATION_SCHEMA vs ACCOUNT_USAGE

- **INFORMATION_SCHEMA**: Near real-time, schema-level metadata
  - Tables, columns, constraints, views, procedures, functions, streams, tasks
  - Low latency (seconds)
  - Limited to current schema

- **ACCOUNT_USAGE**: Account-wide usage metrics and metadata
  - Tag references, storage metrics, query history
  - ~45 minute latency
  - Account-level access required

#### Key Queries

**1. List Tables**

```sql
SELECT
    table_name,
    table_type,
    table_comment,
    is_typed
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = '{schema}'
  AND table_type = 'BASE TABLE'
ORDER BY table_name
```

**2. List Columns**

```sql
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
```

**3. List Constraints (PK, FK, UNIQUE, CHECK)**

```sql
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
```

**4. GET_DDL (Single Object)**

```sql
SELECT GET_DDL('TABLE', 'DB.SCHEMA.TABLE')
```

Returns complete `CREATE TABLE` statement with all clauses.

---

## VARIANT Column Interpretation

### Adaptive Sampling Strategy

The `variant_interpreter.py` module provides schema inference for VARIANT columns containing semi-structured JSON data.

#### Why Adaptive Sampling?

VARIANT columns can contain arbitrarily complex nested JSON structures. Full table scans are expensive for large tables, but small samples may miss rare fields.

**Adaptive Approach:**
- Small tables (< 1K rows): Sample all rows (accurate schema)
- Medium tables (1K-100K): Sample 1K rows (balanced)
- Large tables (100K-1M): Sample 5K rows (deeper coverage)
- Very large tables (> 1M): Sample 10K rows (reasonable coverage)

#### Sample Size Calculation

```python
def get_sample_size(row_count: int, config: VariantSamplingConfig) -> int:
    if row_count < config.small_table_threshold:
        return row_count  # Sample all
    elif row_count < config.medium_table_threshold:
        return config.medium_table_sample_size  # 1000
    elif row_count < config.large_table_threshold:
        return config.large_table_sample_size  # 5000
    else:
        return config.extra_large_sample_size  # 10000
```

#### Schema Inference Process

**1. Sampling Query**

```sql
-- Small tables: no sampling
SELECT variant_col FROM DB.SCHEMA.TABLE

-- Larger tables: use SAMPLE clause
SELECT variant_col FROM DB.SCHEMA.TABLE SAMPLE ({sample_size})
```

**2. Parse JSON Values**

```python
samples = []
for row in cursor:
    value = row[0]
    if value is None:
        samples.append(None)  # Track nullable
    elif isinstance(value, str):
        parsed = json.loads(value)  # Parse JSON
        samples.append(parsed)
```

**3. Type Inference**

```python
def infer_type(value: Any) -> str:
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int) or isinstance(value, float):
        return "number"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return "unknown"
```

**4. Recursive Schema Merging**

```python
def merge_schemas(existing_schema, new_value, field_counts):
    inferred_type = infer_type(new_value)

    if inferred_type == "object":
        # Merge each key recursively
        for key, value in new_value.items():
            field_counts[key] = field_counts.get(key, 0) + 1
            if key not in existing_schema:
                existing_schema[key] = {}
            merge_schemas(existing_schema[key], value, field_counts)

    elif inferred_type == "array":
        # Sample first element to infer item type
        if len(new_value) > 0:
            merge_schemas(existing_schema, new_value[0], field_counts)

    else:
        # Handle primitive types
        # Check for type conflicts (e.g., string vs number in same field)
        if existing_schema and existing_schema != inferred_type:
            existing_schema[""] = "mixed"
        else:
            existing_schema[""] = inferred_type
```

**5. Confidence-Based Filtering**

After processing all samples, filter fields by occurrence confidence:

```python
for key, value in structure.items():
    field_count = field_counts.get(key, 0)
    field_confidence = field_count / valid_samples

    # Only include fields that appear in >= 50% of samples
    if field_confidence >= config.min_confidence:  # default 0.5
        filtered_structure[key] = value
```

**6. Output Schema**

```python
@dataclass
class VariantSchema:
    structure: Dict[str, Any]  # Inferred JSON structure
    confidence: float  # Overall confidence (0.0-1.0)
    sample_count: int  # Number of rows sampled
    field_count: int  # Number of fields detected
    nullable: bool  # Whether column contains NULL values
```

**Example Output:**

```json
{
  "structure": {
    "user_id": "number",
    "profile": {
      "name": "string",
      "age": "number",
      "preferences": {
        "theme": "string",
        "notifications": "boolean"
      }
    },
    "orders": "array"
  },
  "confidence": 0.875,
  "sample_count": 5000,
  "field_count": 5,
  "nullable": true
}
```

---

## Output Generation

### DDL Generator (`generate/ddl_generator.py`)

The DDL generator creates human-readable .sql files with metadata comments.

#### File Structure

```
-- ============================================
-- DDL for TABLE: CUSTOMERS
-- Database: ANALYTICS
-- Schema: PUBLIC
-- Generated: 2026-03-24T10:30:45.123Z
-- ============================================

CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.CUSTOMERS (
    -- [PRIMARY KEY]
    customer_id NUMBER(38,0) NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    -- [FOREIGN KEY -> ANALYTICS.PUBLIC.REGION]
    region_id NUMBER(38,0),
    created_at TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT fk_customers_region FOREIGN KEY (region_id)
        REFERENCES ANALYTICS.PUBLIC.REGION(region_id)
);

-- ============================================
-- Metadata Statistics
-- Row Count: 1,234,567
-- Byte Size: 987,654,321
-- Last DDL: 2026-03-20 15:30:00.000
-- Tags:
--   - PII = HIGH [column: email]
--   - RETENTION = 7_YEARS
-- Masking Policies:
--   - mask_email(VARCHAR) on column email
-- VARIANT Schema: Inferred from 10,000 samples (confidence: 92.50%)
-- ============================================
```

#### Generator Functions

**Table DDL:**

```python
def _generate_table_ddl(metadata: TableMetadata) -> str:
    header = _generate_header_comment(metadata.name, "TABLE", metadata.database, metadata.schema)
    footer = _generate_footer_comment_table(metadata)
    ddl_with_comments = _add_inline_comments_to_table_ddl(
        metadata.ddl,
        metadata.columns,
        metadata.constraints,
        metadata.clustering_key or ""
    )
    return f"{header}\n\n{ddl_with_comments}\n\n{footer}"
```

**Inline Comments:**
- Primary key columns: `-- [PRIMARY KEY]`
- Foreign key columns: `-- [FOREIGN KEY -> REFERENCED_TABLE]`
- Clustering key columns: `-- [CLUSTERING KEY]`

### Metadata Generator (`generate/metadata_generator.py`)

The metadata generator creates machine-readable .json files with rich metadata.

**Example JSON Output:**

```json
{
  "object_name": "customers",
  "object_type": "TABLE",
  "database": "ANALYTICS",
  "schema": "PUBLIC",
  "fully_qualified_name": "ANALYTICS.PUBLIC.CUSTOMERS",
  "ddl": "CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.CUSTOMERS ...",
  "last_ddl": "2026-03-20T15:30:00.000Z",
  "row_count": 1234567,
  "bytes": 987654321,
  "columns": [
    {
      "name": "customer_id",
      "data_type": "NUMBER(38,0)",
      "nullable": false,
      "default_value": null,
      "comment": null
    },
    {
      "name": "email",
      "data_type": "VARCHAR(255)",
      "nullable": false,
      "default_value": null,
      "comment": null
    }
  ],
  "constraints": [
    {
      "name": "pk_customers",
      "type": "PRIMARY KEY",
      "columns": ["customer_id"]
    }
  ],
  "tags": [
    {
      "tag_name": "PII",
      "tag_value": "HIGH",
      "column_name": "email"
    }
  ],
  "masking_policies": [
    {
      "policy_name": "mask_email",
      "signature": "VARCHAR",
      "column_name": "email"
    }
  ],
  "variant_schema": {
    "column_name": "preferences",
    "inferred_structure": {
      "theme": "string",
      "notifications": "boolean"
    },
    "sample_size": 10000,
    "confidence": 0.925
  }
}
```

### Output Assembler (`generate/assembler.py`)

The assembler writes files to the correct directory structure.

**Directory Structure:**

```
discovery/
├── ANALYTICS/
│   ├── PUBLIC/
│   │   ├── tables/
│   │   │   ├── ANALYTICS.PUBLIC.CUSTOMERS.sql
│   │   │   └── ANALYTICS.PUBLIC.CUSTOMERS.json
│   │   ├── views/
│   │   ├── procedures/
│   │   ├── functions/
│   │   ├── streams/
│   │   └── tasks/
│   └── STAGING/
└── _manifest.json
```

**File Naming Convention:**

```
{database}.{schema}.{object_name}.{ext}
```

**Write Process:**

```python
def write_discovery_files(
    metadata_dict: Dict[str, Any],
    ddl_content: str,
    json_content: str,
    base_path: str
) -> None:
    # Build path: discovery/{database}/{schema}/{type}/
    object_type = metadata_dict["object_type"].lower()
    db = metadata_dict["database"]
    schema = metadata_dict["schema"]
    object_name = metadata_dict["object_name"]

    dir_path = Path(base_path) / db / schema / f"{object_type}s"
    dir_path.mkdir(parents=True, exist_ok=True)

    # Write .sql file
    filename = f"{db}.{schema}.{object_name}"
    sql_path = dir_path / f"{filename}.sql"
    sql_path.write_text(ddl_content, encoding="utf-8")

    # Write .json file
    json_path = dir_path / f"{filename}.json"
    json_path.write_text(json_content, encoding="utf-8")
```

---

## Diff Engine

### Structural Comparison (`diff/engine.py`)

The diff engine compares discovery states to detect structural changes in Snowflake objects.

#### State Representation

**Current State:** Extracted from Snowflake during discovery

**Previous State:** Loaded from existing discovery files in repository

#### State Hash Computation

```python
def compute_state_hash(metadata: Dict[str, Any]) -> str:
    # Sort all objects and compute SHA256 hash
    sorted_items = []
    for object_type in sorted(metadata.keys()):
        for object_key in sorted(objects.keys()):
            obj = objects[object_key]
            sorted_items.append(f"{object_type}:{object_key}:{obj['ddl']}")
            sorted_items.append(f"{object_type}:{object_key}:columns:{json.dumps(obj['columns'])}")

    content = "|".join(sorted_items)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
```

#### Diff Comparison

```python
def compare(current_state, previous_state) -> DiffResult:
    added_objects = []
    removed_objects = []
    modified_objects = []

    for object_type in all_object_types:
        current = current_state.get(object_type, {})
        previous = previous_state.get(object_type, {})

        for object_key in set(current.keys()) | set(previous.keys()):
            if object_key not in previous:
                added_objects.append(f"{object_type}: {object_key}")
            elif object_key not in current:
                removed_objects.append(f"{object_type}: {object_key}")
            elif _has_object_changes(current[object_key], previous[object_key]):
                change_details = _get_change_details(current[object_key], previous[object_key])
                modified_objects.append(f"{object_type}: {object_key} ({change_details})")

    return DiffResult(
        has_changes=bool(added_objects or removed_objects or modified_objects),
        added_objects=added_objects,
        removed_objects=removed_objects,
        modified_objects=modified_objects,
        summary=str(...)
    )
```

#### Change Detection

**Structural Changes:**

1. **DDL Hash Changed:** Schema structure modified
2. **Column Count Changed:** Columns added or removed
3. **Constraint Count Changed:** Constraints added or removed

**Example Output:**

```
Added: 3 objects
Removed: 1 objects
Modified: 2 objects

Modified objects:
- TABLE: ANALYTICS.PUBLIC.CUSTOMERS (+2 columns, +1 constraints)
- VIEW: ANALYTICS.PUBLIC.CUSTOMER_SUMMARY (DDL changed)
```

---

## Error Handling

### Custom Exceptions (`utils/errors.py`)

**1. ConfigValidationError**

Raised when configuration is invalid.

```python
class ConfigValidationError(Exception):
    """Configuration validation error."""
    pass
```

**2. PartialExtractionError**

Raised when some objects fail to extract but overall process completes.

```python
class PartialExtractionError(Exception):
    """Partial extraction error with statistics."""
    def __init__(self, message: str, extracted_count: int, failed_count: int):
        self.extracted_count = extracted_count
        self.failed_count = failed_count
        super().__init__(message)
```

**3. ExtractionError**

Raised when extraction completely fails (e.g., connection error).

```python
class ExtractionError(Exception):
    """Extraction error."""
    pass
```

**4. ConnectionError**

Raised when Snowflake connection fails.

```python
class ConnectionError(Exception):
    """Snowflake connection error."""
    pass
```

### Retry Decorator (`utils/retry.py`)

The `@retry` decorator provides resilient retry logic.

```python
def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to retry function calls with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay in seconds
        backoff: Multiplier for delay between retries

    Returns:
        Decorated function that retries on exception
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {e}"
                    )
                    if attempt < max_attempts - 1:
                        sleep_delay = delay * (backoff ** attempt)
                        logger.info(f"Retrying in {sleep_delay}s...")
                        time.sleep(sleep_delay)

            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception  # type: ignore

        return wrapper
    return decorator
```

**Retry Behavior:**
- Attempt 1: Execute function
- Attempt 2: Wait 1s, retry
- Attempt 3: Wait 2s, retry
- Final: Raise exception after all attempts fail

**Applied To:**
- Table extraction (`_extract_tables`)
- View extraction (`_extract_views`)
- Procedure extraction (`_extract_procedures`)
- Function extraction (`_extract_functions`)
- Stream extraction (`_extract_streams`)
- Task extraction (`_extract_tasks`)

**Not Applied To:**
- Connection setup (fail fast)
- Configuration loading (fail fast)
- File writing (fail fast)

---

## Three-Layer Trigger Architecture

The system uses a three-layer trigger architecture to ensure changes are properly reviewed.

### Layer 1: Branch Creation (Automatic)

**Trigger:**
- GitHub Actions workflow triggered on `push` to non-main branches
- Workflow runs: `.github/workflows/discover.yml`

**Behavior:**
```yaml
on:
  push:
    branches:
      - '**'
      - '!main'  # Exclude main branch
```

**Outcome:**
- Discovery runs automatically
- Changes committed to branch
- No pull request created (branch workflow)

**Use Case:**
- Development branches
- Feature branches
- Automated discovery without human intervention

### Layer 2: Snowsight Manual (Triggered by User)

**Trigger:**
- Snowsight Notebook detects changes via diff
- User manually triggers workflow via GitHub API

**Snowsight Notebook Cells:**

```python
# Cell 1: Import libraries
import requests
import json

# Cell 2: Configuration
github_repo = "org/repo"
workflow_id = "discover.yml"
github_token = "SELECT SYSTEM$GET_TAG('GITHUB_TOKEN')"

# Cell 3: Load previous state
SELECT * FROM DISCOVERY_PREVIOUS_STATE

# Cell 4: Extract current state
-- Call discovery CLI or extract metadata

# Cell 5: Compare states
-- Use diff engine to detect changes

# Cell 6: Trigger workflow if changes detected
if has_changes:
    url = f"https://api.github.com/repos/{github_repo}/actions/workflows/{workflow_id}/dispatches"
    payload = {"ref": "main"}
    headers = {"Authorization": f"token {github_token}"}
    requests.post(url, json=payload, headers=headers)

# Cell 7: Create PR
-- Use GitHub API to create pull request
```

**Behavior:**
- User runs notebook in Snowsight
- Diff engine compares current vs previous state
- If changes detected, triggers GitHub Actions workflow
- Creates pull request to main branch

**Outcome:**
- PR created for review
- Changes visible in GitHub

**Use Case:**
- Production updates
- Manual discovery triggers
- Controlled deployment process

### Layer 3: Main Gate (Pull Request Required)

**Trigger:**
- No direct commits to main allowed
- All changes must go through pull request

**Branch Protection Rules:**
```yaml
# .github/branch-protection-rules.yml
required_pull_request_reviews:
  required_approving_review_count: 1
  dismiss_stale_reviews: false
  require_code_owner_reviews: true
  require_last_push_approval: true
```

**Behavior:**
- Pull request must be created
- Code review required
- Approval from reviewer(s)
- All CI checks must pass
- Only then can merge to main

**Outcome:**
- All changes reviewed before reaching main
- Audit trail maintained
- Code quality enforced

**Use Case:**
- Production changes
- Security/Compliance requirements
- Collaborative development

---

## GitHub Actions Integration

### Workflow Configuration (`.github/workflows/discover.yml`)

```yaml
name: Snowflake Discovery

on:
  push:
    branches:
      - '**'
      - '!main'  # Exclude main from auto-discovery
  workflow_dispatch:  # Manual trigger
    inputs:
      create_pr:
        description: 'Create PR to main'
        required: false
        default: 'false'
        type: boolean

jobs:
  discover:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -e .
          pip install snowflake-connector-python cryptography pydantic pyyaml

      - name: Run discovery
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
          SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
        run: |
          python -m discovery extract --config discovery-config.yml

      - name: Commit changes
        run: |
          git config --local user.email "action@github.com"
          git config --local user.name "GitHub Action"
          git add discovery/
          git diff --quiet && git diff --staged --quiet || git commit -m "Auto-discovery: Update metadata"

      - name: Push changes
        if: github.ref != 'refs/heads/main'
        run: |
          git push origin HEAD:${{ github.ref_name }}

      - name: Create PR
        if: github.event.inputs.create_pr == 'true'
        run: |
          gh pr create --title "Discovery Update" --body "Automated metadata discovery"
```

### Required Secrets

Configure these secrets in GitHub repository settings:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | `service_account@company.com` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | `DISCOVERY_WH` |
| `SNOWFLAKE_PRIVATE_KEY_RAW` | RSA private key (PEM format) | `-----BEGIN RSA PRIVATE KEY-----...` |
| `SNOWFLAKE_ROLE` | Snowflake role name | `DISCOVERY_ROLE` |
| `GITHUB_TOKEN` | GitHub API token (auto-provided) | `ghp_xxxxxxxxxxxxxx` |

### Conditional Triggers

**Branch Creation (Layer 1):**
```yaml
on:
  push:
    branches:
      - '**'
      - '!main'
```
- Triggers on any branch except `main`
- Auto-commits to branch
- No PR created

**Manual Dispatch (Layer 2):**
```yaml
on:
  workflow_dispatch:
    inputs:
      create_pr:
        type: boolean
        default: false
```
- Triggered via GitHub UI or API
- Can optionally create PR to main
- Used by Snowsight Notebook

**Main Branch (Layer 3):**
- No direct push triggers on main
- All changes must come from PRs
- Enforced via branch protection rules

---

## Operational Considerations

### Performance Optimization

**1. Parallel Extraction:**
Currently sequential, but could be parallelized by database/schema.

**2. Query Optimization:**
- Use `INFORMATION_SCHEMA` for low-latency metadata
- Use `ACCOUNT_USAGE` only for tags (45-min latency acceptable)
- Limit columns in queries (select only needed fields)

**3. Sampling Strategy:**
- Adaptive VARIANT sampling reduces full table scans
- Confidence threshold filters noisy fields
- Sample sizes configurable per table size category

### Security Considerations

**1. Key-Pair Authentication:**
- Private keys stored in GitHub Secrets (encrypted)
- No passwords in configuration files
- Keys can be rotated independently

**2. Branch Protection:**
- Main branch requires PR review
- Code owner approval required
- All CI checks must pass

**3. Tag-Based Access Control:**
- Snowflake tags for PII, retention, classification
- Masking policies on sensitive columns
- Access control via roles

### Scalability Considerations

**1. Large Databases:**
- Processing per-schema (can parallelize)
- Retry logic handles transient failures
- Partial extraction prevents complete failure

**2. Many Objects:**
- Extractor processes one object at a time
- Could add concurrency with thread pool
- Memory usage: O(1) per object (streaming)

**3. VARIANT Columns:**
- Adaptive sampling reduces load
- Sample sizes configurable
- Confidence threshold limits output size

### Monitoring and Logging

**Logging Levels:**
- `DEBUG`: Query execution details
- `INFO`: Progress updates (processing X objects)
- `WARNING`: Partial failures, retry attempts
- `ERROR`: Complete failures, connection errors

**Log Output:**
```python
logger.info("Connected to Snowflake")
logger.info("Processing database: ANALYTICS")
logger.info("Found 123 tables in ANALYTICS.PUBLIC")
logger.warning("Failed to get tags for ANALYTICS.PUBLIC.TABLE1: API limit")
logger.error("Connection error: Authentication failed")
```

### Troubleshooting

**Common Issues:**

1. **Connection Fails:**
   - Check environment variables set correctly
   - Verify private key format (PEM)
   - Confirm account, user, warehouse, database names

2. **Partial Extraction:**
   - Review error logs for specific failures
   - Check Snowflake permissions
   - Verify INFORMATION_SCHEMA access

3. **VARIANT Schema Inference Fails:**
   - Check if column contains valid JSON
   - Increase sample size for complex structures
   - Lower confidence threshold for noisy data

4. **GitHub Actions Fails:**
   - Verify secrets configured
   - Check workflow syntax
   - Review runner logs for errors

---

## Future Enhancements

### Potential Improvements

**1. Parallel Extraction:**
- Extract multiple objects concurrently
- Use thread pool for database/schema-level parallelism
- Add configuration for max concurrency

**2. Incremental Extraction:**
- Only extract changed objects (based on last DDL timestamp)
- Use ACCOUNT_USAGE.OBJECT_CHANGE_HISTORY
- Faster discovery for large environments

**3. Enhanced Diff Engine:**
- Column-level diff (added, removed, modified columns)
- Constraint-level diff
- Tag changes detection

**4. Web UI:**
- Browse discovery output
- Visual diff between states
- Search across objects

**5. Integration with Data Catalog:**
- Export to data catalog platforms (Collibra, Alation, DataHub)
- Maintain lineage information
- Business metadata sync

---

## Conclusion

The Snowflake Discovery system provides a robust, automated solution for metadata extraction and version control. Key strengths include:

- **Resilient Design:** Retry logic, partial failure handling
- **Flexible Configuration:** YAML-based, multi-target support
- **Rich Metadata:** DDL, constraints, tags, masking policies, VARIANT schemas
- **Adaptive Sampling:** Intelligent VARIANT column inference
- **Three-Layer Architecture:** Automated, manual, and review-gated workflows
- **Version Controlled:** All changes tracked in Git
- **CI/CD Integrated:** GitHub Actions automation

The system is designed to scale from small development environments to large production deployments with hundreds of databases and thousands of objects.
