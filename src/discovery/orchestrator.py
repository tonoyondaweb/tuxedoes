"""Main extraction orchestrator for Snowflake metadata discovery.

This module coordinates all extraction modules to extract Snowflake metadata
and generate discovery files (.sql and .json).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional
import time

from .config.schema import DiscoveryConfig
from .config.parser import load_config
from .types import (
    TableMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    TagAssignment,
    MaskingPolicy,
    ViewMetadata,
    ProcedureMetadata,
    StreamMetadata,
    TaskMetadata,
    DiscoveryError,
    VariantSchema,
)
from .extract.connection import SnowflakeConnection, connect, ConnectionError
from .extract.queries import (
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
)
from .extract.variant_interpreter import interpret_variant_column
from .generate.ddl_generator import generate_ddl_file
from .generate.metadata_generator import generate_metadata_json
from .generate.assembler import write_discovery_files
from .utils.retry import retry, ExtractionError
from .utils.errors import PartialExtractionError

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result of metadata extraction.

    Attributes:
        total_objects: Total number of objects attempted
        extracted: Number of successfully extracted objects
        failed: Number of objects that failed extraction
        errors: List of errors encountered during extraction
        duration: Extraction duration in seconds
    """
    total_objects: int = 0
    extracted: int = 0
    failed: int = 0
    errors: List[DiscoveryError] = field(default_factory=list)
    duration: float = 0.0


class ExtractionOrchestrator:
    """Orchestrates metadata extraction from Snowflake."""

    def __init__(self, config: DiscoveryConfig):
        """Initialize orchestrator with configuration.

        Args:
            config: Discovery configuration
        """
        self.config = config
        self.conn: Optional[SnowflakeConnection] = None

    def run(self) -> ExtractionResult:
        """Run full extraction pipeline.

        Returns:
            ExtractionResult with statistics and errors
        """
        start_time = time.time()
        result = ExtractionResult()

        try:
            # Connect to Snowflake
            self.conn = self._connect()
            logger.info("Connected to Snowflake")

            # Process each target database/schema
            for target in self.config.targets:
                self._process_target(target, result)

            # Log summary
            logger.info(
                f"Extraction completed: {result.extracted} succeeded, "
                f"{result.failed} failed, {result.total_objects} total"
            )

        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise ExtractionError(f"Failed to connect to Snowflake: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            raise ExtractionError(f"Extraction failed: {e}") from e
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Closed Snowflake connection")

        result.duration = time.time() - start_time
        return result

    def _connect(self) -> SnowflakeConnection:
        """Establish Snowflake connection.

        Returns:
            SnowflakeConnection instance
        """
        # Build connection config from environment variables
        # The connection module reads SNOWFLAKE_* env vars
        conn_config = {}  # Empty dict - connection module uses env vars
        return connect(conn_config).connect()

    def _process_target(self, target: Any, result: ExtractionResult) -> None:
        """Process a target database/schema.

        Args:
            target: TargetConfig with database and schemas
            result: ExtractionResult to update with statistics
        """
        db = target.database
        logger.info(f"Processing database: {db}")

        for schema_config in target.schemas:
            schema = schema_config.name
            logger.info(f"Processing schema: {db}.{schema}")

            # Determine object types to extract
            object_types = self._get_object_types(schema_config)

            # Extract each object type
            for object_type in object_types:
                try:
                    self._extract_object_type(db, schema, object_type, result)
                except Exception as e:
                    logger.error(
                        f"Failed to extract {object_type} from {db}.{schema}: {e}"
                    )
                    error = DiscoveryError(
                        object_name=schema,
                        object_type=object_type,
                        error_message=str(e),
                        retry_count=0,
                    )
                    result.errors.append(error)

    def _get_object_types(self, schema_config: Any) -> List[str]:
        """Get list of object types to extract for a schema.

        Args:
            schema_config: SchemaConfig with include/exclude types

        Returns:
            List of object type strings
        """
        # If include_types specified, use only those
        if schema_config.include_types:
            return schema_config.include_types

        # Otherwise, use all supported types minus excluded ones
        all_types = ["TABLE", "VIEW", "PROCEDURE", "FUNCTION", "STREAM", "TASK"]
        if schema_config.exclude_types:
            return [t for t in all_types if t not in schema_config.exclude_types]
        return all_types

    def _extract_object_type(
        self,
        db: str,
        schema: str,
        object_type: str,
        result: ExtractionResult,
    ) -> None:
        """Extract all objects of a given type from a schema.

        Args:
            db: Database name
            schema: Schema name
            object_type: Type of object (TABLE, VIEW, etc.)
            result: ExtractionResult to update
        """
        logger.info(f"Extracting {object_type}s from {db}.{schema}")

        # Route to appropriate extraction method
        if object_type == "TABLE":
            self._extract_tables(db, schema, result)
        elif object_type == "VIEW":
            self._extract_views(db, schema, result)
        elif object_type == "PROCEDURE":
            self._extract_procedures(db, schema, result)
        elif object_type == "FUNCTION":
            self._extract_functions(db, schema, result)
        elif object_type == "STREAM":
            self._extract_streams(db, schema, result)
        elif object_type == "TASK":
            self._extract_tasks(db, schema, result)
        else:
            logger.warning(f"Unsupported object type: {object_type}")

    @retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
    def _extract_tables(self, db: str, schema: str, result: ExtractionResult) -> None:
        """Extract tables from schema.

        Args:
            db: Database name
            schema: Schema name
            result: ExtractionResult to update
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        conn = self.conn  # Type narrowing
        # List tables
        query = list_tables_query(schema)
        tables = conn.execute_query(query)

        logger.info(f"Found {len(tables)} tables in {db}.{schema}")

        for table_info in tables:
            table_name = table_info["table_name"]
            result.total_objects += 1

            try:
                # Extract table metadata
                metadata = self._extract_table_metadata(db, schema, table_name)

                # Generate DDL and JSON
                ddl_content = generate_ddl_file(metadata)
                json_content = str(generate_metadata_json(metadata, self.config.output.base_path))

                # Build metadata dict for assembler
                metadata_dict = {
                    "database": db,
                    "schema": schema,
                    "object_type": "TABLE",
                    "object_name": table_name,
                }

                # Write files
                base_path = self.config.output.base_path
                write_discovery_files(metadata_dict, ddl_content, json_content, base_path)

                result.extracted += 1
                logger.info(f"Extracted table: {db}.{schema}.{table_name}")

            except Exception as e:
                logger.error(f"Failed to extract table {db}.{schema}.{table_name}: {e}")
                result.failed += 1
                error = DiscoveryError(
                    object_name=table_name,
                    object_type="TABLE",
                    error_message=str(e),
                    retry_count=0,
                )
                result.errors.append(error)

    def _extract_table_metadata(
        self,
        db: str,
        schema: str,
        table_name: str,
    ) -> TableMetadata:
        """Extract full metadata for a table.

        Args:
            db: Database name
            schema: Schema name
            table_name: Table name

        Returns:
            TableMetadata object
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        # Get DDL
        qualified_name = f"{db}.{schema}.{table_name}"
        ddl_query = get_ddl_query("TABLE", qualified_name)
        ddl_result = self.conn.execute_query(ddl_query)
        ddl = ddl_result[0]["GET_DDL('TABLE', '" + qualified_name + "')"] if ddl_result else ""

        # Get columns
        columns_query = list_columns_query(schema, table_name)
        columns_result = self.conn.execute_query(columns_query)
        columns = [
            ColumnMetadata(
                name=row["column_name"],
                data_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                default_value=row["column_default"],
                comment=row["comment"],
            )
            for row in columns_result
        ]

        # Get constraints
        constraints_query = list_constraints_query(schema)
        constraints_result = self.conn.execute_query(constraints_query)
        # Build constraint metadata
        constraints = self._build_constraints(constraints_result, table_name)

        # Get tags
        tags = self._get_tags(db, schema, table_name, "TABLE")

        # Get masking policies
        masking_policies = self._get_masking_policies(db, schema, table_name)

        # Get storage metrics (row count, bytes)
        storage_query = get_table_storage_query(f"{db}.{schema}")
        storage_result = self.conn.execute_query(storage_query)
        row_count = 0
        bytes_size = 0
        for row in storage_result:
            if row["table_name"] == table_name:
                row_count = row.get("active_bytes", 0) // 1000  # Approximate
                bytes_size = row.get("active_bytes", 0)
                break

        # Get variant schema if applicable
        variant_schema = self._extract_variant_schema(db, schema, table_name, row_count)

        return TableMetadata(
            name=table_name,
            schema=schema,
            database=db,
            ddl=ddl,
            columns=columns,
            row_count=row_count,
            bytes=bytes_size,
            last_ddl="",
            clustering_key=None,
            constraints=constraints,
            tags=tags,
            masking_policies=masking_policies,
            search_optimization=False,
            variant_schema=variant_schema,
        )

    def _build_constraints(
        self,
        constraints_result: List[Dict[str, Any]],
        table_name: str,
    ) -> List[ConstraintMetadata]:
        """Build constraint metadata from query results.

        Args:
            constraints_result: Query results from constraints query
            table_name: Table name to filter for

        Returns:
            List of ConstraintMetadata objects
        """
        constraints_dict = {}

        for row in constraints_result:
            if row["table_name"] != table_name:
                continue

            constraint_name = row["constraint_name"]
            if constraint_name not in constraints_dict:
                constraints_dict[constraint_name] = ConstraintMetadata(
                    name=constraint_name,
                    type=row["constraint_type"],
                    columns=[],
                    referenced_table=row.get("referenced_table"),
                    referenced_columns=[],
                )

            # Add column to constraint
            if row["column_name"]:
                constraints_dict[constraint_name].columns.append(row["column_name"])

        return list(constraints_dict.values())

    def _get_tags(
        self,
        db: str,
        schema: str,
        object_name: str,
        object_type: str,
    ) -> List[TagAssignment]:
        """Get tags for an object.

        Args:
            db: Database name
            schema: Schema name
            object_name: Object name
            object_type: Object type (TABLE, COLUMN, VIEW, etc.)

        Returns:
            List of TagAssignment objects
        """
        if not self.conn:
            return []

        try:
            qualified_schema = f"{db}.{schema}"
            query = list_tags_query(qualified_schema)
            results = self.conn.execute_query(query)

            tags = []
            for row in results:
                # Filter by object name
                if object_name not in row["object_name"]:
                    continue

                tags.append(
                    TagAssignment(
                        tag_name=row["tag_name"],
                        tag_value=row["tag_value"],
                        column_name=None,  # Simplified - could parse column from object_name
                    )
                )

            return tags
        except Exception as e:
            logger.warning(f"Failed to get tags for {db}.{schema}.{object_name}: {e}")
            return []

    def _get_masking_policies(
        self,
        db: str,
        schema: str,
        table_name: str,
    ) -> List[MaskingPolicy]:
        """Get masking policies for a table.

        Args:
            db: Database name
            schema: Schema name
            table_name: Table name

        Returns:
            List of MaskingPolicy objects
        """
        if not self.conn:
            return []

        try:
            query = list_masking_policies_query(schema)
            results = self.conn.execute_query(query)

            policies = []
            for row in results:
                # Simplified - full implementation would parse column mappings
                policies.append(
                    MaskingPolicy(
                        policy_name=row["policy_name"],
                        signature=row["entry"],
                        column_name="",  # Would need additional query
                    )
                )

            return policies
        except Exception as e:
            logger.warning(
                f"Failed to get masking policies for {db}.{schema}.{table_name}: {e}"
            )
            return []

    def _extract_variant_schema(
        self,
        db: str,
        schema: str,
        table_name: str,
        row_count: int,
    ) -> Optional[Any]:
        """Extract variant schema for VARIANT columns in table.

        Args:
            db: Database name
            schema: Schema name
            table_name: Table name
            row_count: Row count of table

        Returns:
            VariantSchema object or None
        """
        if not self.conn:
            return None

        try:
            # Get variant columns
            query = get_variant_columns_query(schema, table_name)
            variant_columns = self.conn.execute_query(query)

            if not variant_columns:
                return None

            # Interpret first variant column (simplified - could handle multiple)
            column_name = variant_columns[0]["column_name"]

            # Interpret variant schema using adaptive sampling
            from .extract.variant_interpreter import interpret_variant_column, VariantSchema as InterpreterVariantSchema

            internal_schema = interpret_variant_column(
                self.conn._conn,  # Use raw snowflake connection
                db,
                schema,
                table_name,
                column_name,
                row_count,
                self.config.variant_sampling,
            )

            # Convert internal schema to public API VariantSchema
            variant_schema = VariantSchema(
                column_name=column_name,
                inferred_structure=internal_schema.structure,
                sample_size=internal_schema.sample_count,
                confidence=internal_schema.confidence,
            )

            return variant_schema
        except Exception as e:
            logger.warning(
                f"Failed to extract variant schema for {db}.{schema}.{table_name}: {e}"
            )
            return None

    @retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
    def _extract_views(self, db: str, schema: str, result: ExtractionResult) -> None:
        """Extract views from schema.

        Args:
            db: Database name
            schema: Schema name
            result: ExtractionResult to update
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        conn = self.conn  # Type narrowing
        query = list_views_query(schema)
        views = conn.execute_query(query)

        logger.info(f"Found {len(views)} views in {db}.{schema}")

        for view_info in views:
            view_name = view_info["table_name"]
            result.total_objects += 1

            try:
                # Extract view metadata
                qualified_name = f"{db}.{schema}.{view_name}"
                ddl_query = get_ddl_query("VIEW", qualified_name)
                ddl_result = self.conn.execute_query(ddl_query)
                ddl = (
                    ddl_result[0][f"GET_DDL('VIEW', '{qualified_name}')"]
                    if ddl_result
                    else ""
                )

                # Get columns (re-use columns query)
                columns_query = list_columns_query(schema, view_name)
                columns_result = self.conn.execute_query(columns_query)
                columns = [
                    ColumnMetadata(
                        name=row["column_name"],
                        data_type=row["data_type"],
                        nullable=row["is_nullable"] == "YES",
                        default_value=row["column_default"],
                        comment=row["comment"],
                    )
                    for row in columns_result
                ]

                # Get tags
                tags = self._get_tags(db, schema, view_name, "VIEW")

                metadata = ViewMetadata(
                    name=view_name,
                    schema=schema,
                    database=db,
                    ddl=ddl,
                    columns=columns,
                    base_tables=[],  # Simplified - would parse from DDL
                    last_ddl="",
                    tags=tags,
                )

                # Generate and write files
                ddl_content = generate_ddl_file(metadata)
                json_content = str(
                    generate_metadata_json(metadata, self.config.output.base_path)
                )
                metadata_dict = {
                    "database": db,
                    "schema": schema,
                    "object_type": "VIEW",
                    "object_name": view_name,
                }
                write_discovery_files(
                    metadata_dict, ddl_content, json_content, self.config.output.base_path
                )

                result.extracted += 1
                logger.info(f"Extracted view: {db}.{schema}.{view_name}")

            except Exception as e:
                logger.error(f"Failed to extract view {db}.{schema}.{view_name}: {e}")
                result.failed += 1
                error = DiscoveryError(
                    object_name=view_name,
                    object_type="VIEW",
                    error_message=str(e),
                    retry_count=0,
                )
                result.errors.append(error)

    @retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
    def _extract_procedures(
        self,
        db: str,
        schema: str,
        result: ExtractionResult,
    ) -> None:
        """Extract stored procedures from schema.

        Args:
            db: Database name
            schema: Schema name
            result: ExtractionResult to update
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        conn = self.conn  # Type narrowing
        query = list_procedures_query(schema)
        procedures = conn.execute_query(query)

        logger.info(f"Found {len(procedures)} procedures in {db}.{schema}")

        for proc_info in procedures:
            proc_name = proc_info["routine_name"]
            result.total_objects += 1

            try:
                qualified_name = f"{db}.{schema}.{proc_name}"
                ddl_query = get_ddl_query("PROCEDURE", qualified_name)
                ddl_result = self.conn.execute_query(ddl_query)
                ddl = (
                    ddl_result[0][f"GET_DDL('PROCEDURE', '{qualified_name}')"]
                    if ddl_result
                    else ""
                )

                metadata = ProcedureMetadata(
                    name=proc_name,
                    schema=schema,
                    database=db,
                    ddl=ddl,
                    parameters=[],  # Simplified - would parse from DDL
                    return_type=proc_info.get("data_type"),
                    language=proc_info.get("external_language"),
                    last_ddl="",
                )

                ddl_content = generate_ddl_file(metadata)
                json_content = str(
                    generate_metadata_json(metadata, self.config.output.base_path)
                )
                metadata_dict = {
                    "database": db,
                    "schema": schema,
                    "object_type": "PROCEDURE",
                    "object_name": proc_name,
                }
                write_discovery_files(
                    metadata_dict, ddl_content, json_content, self.config.output.base_path
                )

                result.extracted += 1
                logger.info(f"Extracted procedure: {db}.{schema}.{proc_name}")

            except Exception as e:
                logger.error(
                    f"Failed to extract procedure {db}.{schema}.{proc_name}: {e}"
                )
                result.failed += 1
                error = DiscoveryError(
                    object_name=proc_name,
                    object_type="PROCEDURE",
                    error_message=str(e),
                    retry_count=0,
                )
                result.errors.append(error)

    @retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
    def _extract_functions(
        self,
        db: str,
        schema: str,
        result: ExtractionResult,
    ) -> None:
        """Extract functions from schema.

        Args:
            db: Database name
            schema: Schema name
            result: ExtractionResult to update
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        conn = self.conn  # Type narrowing
        query = list_functions_query(schema)
        functions = conn.execute_query(query)

        logger.info(f"Found {len(functions)} functions in {db}.{schema}")

        for func_info in functions:
            func_name = func_info["routine_name"]
            result.total_objects += 1

            try:
                qualified_name = f"{db}.{schema}.{func_name}"
                ddl_query = get_ddl_query("FUNCTION", qualified_name)
                ddl_result = self.conn.execute_query(ddl_query)
                ddl = (
                    ddl_result[0][f"GET_DDL('FUNCTION', '{qualified_name}')"]
                    if ddl_result
                    else ""
                )

                # Functions don't have dedicated metadata type - use ProcedureMetadata
                metadata = ProcedureMetadata(
                    name=func_name,
                    schema=schema,
                    database=db,
                    ddl=ddl,
                    parameters=[],
                    return_type=func_info.get("data_type"),
                    language=func_info.get("external_language"),
                    last_ddl="",
                )

                ddl_content = generate_ddl_file(metadata)
                json_content = str(
                    generate_metadata_json(metadata, self.config.output.base_path)
                )
                metadata_dict = {
                    "database": db,
                    "schema": schema,
                    "object_type": "FUNCTION",
                    "object_name": func_name,
                }
                write_discovery_files(
                    metadata_dict, ddl_content, json_content, self.config.output.base_path
                )

                result.extracted += 1
                logger.info(f"Extracted function: {db}.{schema}.{func_name}")

            except Exception as e:
                logger.error(f"Failed to extract function {db}.{schema}.{func_name}: {e}")
                result.failed += 1
                error = DiscoveryError(
                    object_name=func_name,
                    object_type="FUNCTION",
                    error_message=str(e),
                    retry_count=0,
                )
                result.errors.append(error)

    @retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
    def _extract_streams(
        self,
        db: str,
        schema: str,
        result: ExtractionResult,
    ) -> None:
        """Extract streams from schema.

        Args:
            db: Database name
            schema: Schema name
            result: ExtractionResult to update
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        conn = self.conn  # Type narrowing
        query = list_streams_query(schema)
        streams = conn.execute_query(query)

        logger.info(f"Found {len(streams)} streams in {db}.{schema}")

        for stream_info in streams:
            stream_name = stream_info["table_name"]
            result.total_objects += 1

            try:
                qualified_name = f"{db}.{schema}.{stream_name}"
                ddl_query = get_ddl_query("STREAM", qualified_name)
                ddl_result = self.conn.execute_query(ddl_query)
                ddl = (
                    ddl_result[0][f"GET_DDL('STREAM', '{qualified_name}')"]
                    if ddl_result
                    else ""
                )

                metadata = StreamMetadata(
                    name=stream_name,
                    schema=schema,
                    database=db,
                    ddl=ddl,
                    source_object="",  # Simplified - would parse from DDL
                    mode=stream_info.get("mode", ""),
                    last_ddl="",
                )

                ddl_content = generate_ddl_file(metadata)
                json_content = str(
                    generate_metadata_json(metadata, self.config.output.base_path)
                )
                metadata_dict = {
                    "database": db,
                    "schema": schema,
                    "object_type": "STREAM",
                    "object_name": stream_name,
                }
                write_discovery_files(
                    metadata_dict, ddl_content, json_content, self.config.output.base_path
                )

                result.extracted += 1
                logger.info(f"Extracted stream: {db}.{schema}.{stream_name}")

            except Exception as e:
                logger.error(f"Failed to extract stream {db}.{schema}.{stream_name}: {e}")
                result.failed += 1
                error = DiscoveryError(
                    object_name=stream_name,
                    object_type="STREAM",
                    error_message=str(e),
                    retry_count=0,
                )
                result.errors.append(error)

    @retry(max_attempts=3, delay=1, backoff=2)  # type: ignore[misc]
    def _extract_tasks(self, db: str, schema: str, result: ExtractionResult) -> None:
        """Extract tasks from schema.

        Args:
            db: Database name
            schema: Schema name
            result: ExtractionResult to update
        """
        if not self.conn:
            raise ExtractionError("No active connection")

        conn = self.conn  # Type narrowing
        query = list_tasks_query(schema)
        tasks = conn.execute_query(query)

        logger.info(f"Found {len(tasks)} tasks in {db}.{schema}")

        for task_info in tasks:
            task_name = task_info["name"]
            result.total_objects += 1

            try:
                qualified_name = f"{db}.{schema}.{task_name}"
                ddl_query = get_ddl_query("TASK", qualified_name)
                ddl_result = self.conn.execute_query(ddl_query)
                ddl = (
                    ddl_result[0][f"GET_DDL('TASK', '{qualified_name}')"]
                    if ddl_result
                    else ""
                )

                metadata = TaskMetadata(
                    name=task_name,
                    schema=schema,
                    database=db,
                    ddl=ddl,
                    schedule=task_info.get("schedule", ""),
                    state=task_info.get("state", ""),
                    predecessors=[],  # Simplified - would parse from DDL
                    last_ddl="",
                )

                ddl_content = generate_ddl_file(metadata)
                json_content = str(
                    generate_metadata_json(metadata, self.config.output.base_path)
                )
                metadata_dict = {
                    "database": db,
                    "schema": schema,
                    "object_type": "TASK",
                    "object_name": task_name,
                }
                write_discovery_files(
                    metadata_dict, ddl_content, json_content, self.config.output.base_path
                )

                result.extracted += 1
                logger.info(f"Extracted task: {db}.{schema}.{task_name}")

            except Exception as e:
                logger.error(f"Failed to extract task {db}.{schema}.{task_name}: {e}")
                result.failed += 1
                error = DiscoveryError(
                    object_name=task_name,
                    object_type="TASK",
                    error_message=str(e),
                    retry_count=0,
                )
                result.errors.append(error)


def run_extraction(config_path: str) -> ExtractionResult:
    """Main entry point for metadata extraction.

    Args:
        config_path: Path to configuration YAML file

    Returns:
        ExtractionResult with extraction statistics

    Raises:
        ExtractionError: If extraction fails
        ConfigValidationError: If config is invalid
        FileNotFoundError: If config file doesn't exist
    """
    # Load configuration
    config = load_config(config_path)

    # Create orchestrator and run extraction
    orchestrator = ExtractionOrchestrator(config)
    result = orchestrator.run()

    # Check for partial failures
    if result.failed > 0:
        raise PartialExtractionError(
            f"Extraction completed with {result.failed} failures",
            extracted_count=result.extracted,
            failed_count=result.failed,
        )

    return result
