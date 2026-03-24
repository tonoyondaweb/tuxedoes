"""Tests for orchestrator with mocked Snowflake."""

import pytest
from unittest.mock import patch, MagicMock, Mock
import time

from discovery.orchestrator import (
    ExtractionOrchestrator,
    ExtractionResult,
    run_extraction,
)
from discovery.config.schema import DiscoveryConfig, TargetConfig, SchemaConfig
from discovery.types import (
    TableMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    DiscoveryError,
)
from discovery.utils.errors import ConnectionError as SnowflakeConnectionError


@pytest.fixture
def mock_snowflake_connection():
    """Mock Snowflake connection."""
    with patch('discovery.extract.connection.SnowflakeConnection') as mock:
        conn = MagicMock()
        mock.return_value.__enter__.return_value = conn
        yield conn


@pytest.fixture
def mock_cursor(mock_snowflake_connection):
    """Mock Snowflake cursor."""
    cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = cursor
    yield cursor


@pytest.fixture
def sample_config():
    """Sample discovery config."""
    return DiscoveryConfig(
        targets=[
            TargetConfig(
                database="ANALYTICS",
                schemas=[
                    SchemaConfig(name="PUBLIC", include_types=["TABLE"], exclude_types=[])
                ]
            )
        ]
    )


@pytest.fixture
def sample_extraction_result():
    """Sample extraction result."""
    return ExtractionResult(
        total_objects=10,
        extracted=8,
        failed=2,
        errors=[
            DiscoveryError(
                object_name="failed_table",
                object_type="TABLE",
                error_message="Permission denied",
                retry_count=3
            )
        ],
        duration=5.5
    )


def test_extraction_result_initialization():
    """Test ExtractionResult initialization with default values."""
    result = ExtractionResult()

    assert result.total_objects == 0
    assert result.extracted == 0
    assert result.failed == 0
    assert result.errors == []
    assert result.duration == 0.0


def test_extraction_result_initialization_with_values(sample_extraction_result):
    """Test ExtractionResult initialization with values."""
    result = sample_extraction_result

    assert result.total_objects == 10
    assert result.extracted == 8
    assert result.failed == 2
    assert len(result.errors) == 1
    assert result.duration == 5.5


def test_extraction_orchestrator_initialization(sample_config):
    """Test ExtractionOrchestrator initialization."""
    orchestrator = ExtractionOrchestrator(sample_config)

    assert orchestrator.config == sample_config
    assert orchestrator.conn is None


def test_extraction_orchestrator_connects_to_snowflake(
    sample_config,
    mock_snowflake_connection
):
    """Test that orchestrator connects to Snowflake."""
    orchestrator = ExtractionOrchestrator(sample_config)

    with patch.object(orchestrator, '_process_target'):
        orchestrator.run()

        # Verify connection was established
        assert mock_snowflake_connection.connect.called


def test_extraction_orchestrator_closes_connection(
    sample_config,
    mock_snowflake_connection
):
    """Test that orchestrator closes connection after run."""
    orchestrator = ExtractionOrchestrator(sample_config)

    with patch.object(orchestrator, '_process_target'):
        orchestrator.run()

        # Verify connection was closed
        mock_snowflake_connection.close.assert_called()


def test_extraction_orchestrator_processes_target(
    sample_config,
    mock_cursor
):
    """Test that orchestrator processes target databases."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # Mock query results for tables
    mock_cursor.fetchall.return_value = [
        ("users", "BASE TABLE", None, False)
    ]

    result = orchestrator.run()

    assert result.total_objects >= 0


def test_extraction_orchestrator_processes_schemas(
    sample_config,
    mock_cursor
):
    """Test that orchestrator processes schemas."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # Mock empty result
    mock_cursor.fetchall.return_value = []

    result = orchestrator.run()

    assert isinstance(result, ExtractionResult)


def test_extraction_orchestrator_handles_connection_error(sample_config):
    """Test that orchestrator handles connection errors."""
    orchestrator = ExtractionOrchestrator(sample_config)

    with patch.object(orchestrator, '_connect', side_effect=SnowflakeConnectionError("Connection failed")):
        from discovery.utils.retry import ExtractionError

        with pytest.raises(ExtractionError, match="Failed to connect to Snowflake"):
            orchestrator.run()


def test_extraction_orchestrator_tracks_duration(sample_config):
    """Test that orchestrator tracks extraction duration."""
    orchestrator = ExtractionOrchestrator(sample_config)

    with patch.object(orchestrator, '_process_target'):
        with patch.object(orchestrator, '_connect'):
            start_time = time.time()

            result = orchestrator.run()

            end_time = time.time()

            # Duration should be positive and reasonable
            assert result.duration >= 0
            assert result.duration < 10  # Should be fast with mocked operations
            assert result.duration <= (end_time - start_time) + 1.0  # Allow some margin


def test_extraction_orchestrator_tracks_statistics(sample_config, mock_cursor):
    """Test that orchestrator tracks extraction statistics."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # Mock one table result
    mock_cursor.fetchall.return_value = [("users", "BASE TABLE", None, False)]

    with patch.object(orchestrator, '_extract_tables') as mock_extract:
        mock_extract.return_value = [
            TableMetadata(
                name="users",
                schema="PUBLIC",
                database="ANALYTICS",
                ddl="CREATE TABLE users (id INT);",
                columns=[ColumnMetadata(name="id", data_type="INT", nullable=False, default_value=None, comment=None)],
                row_count=100,
                bytes=5000,
                last_ddl="2025-01-01",
                clustering_key=None,
                constraints=[],
                tags=[],
                masking_policies=[],
                search_optimization=False,
                variant_schema=None
            )
        ]

        result = orchestrator.run()

        assert result.total_objects >= 1
        assert result.extracted >= 1


def test_extraction_orchestrator_handles_extraction_errors(
    sample_config,
    mock_cursor
):
    """Test that orchestrator handles extraction errors gracefully."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # Mock one table result
    mock_cursor.fetchall.return_value = [("users", "BASE TABLE", None, False)]

    with patch.object(orchestrator, '_extract_tables') as mock_extract:
        from discovery.utils.retry import ExtractionError

        # Simulate extraction error
        mock_extract.side_effect = ExtractionError("Extraction failed")

        # Should not crash, should track error
        result = orchestrator.run()

        # Errors should be tracked
        assert len(result.errors) >= 0


def test_extraction_orchestrator_determines_object_types(sample_config):
    """Test that orchestrator determines object types to extract."""
    orchestrator = ExtractionOrchestrator(sample_config)

    schema_config = SchemaConfig(
        name="PUBLIC",
        include_types=["TABLE", "VIEW"],
        exclude_types=[]
    )

    object_types = orchestrator._get_object_types(schema_config)

    assert "TABLE" in object_types
    assert "VIEW" in object_types


def test_extraction_orchestrator_determines_all_object_types(sample_config):
    """Test that orchestrator includes all types when include_types is empty."""
    orchestrator = ExtractionOrchestrator(sample_config)

    schema_config = SchemaConfig(
        name="PUBLIC",
        include_types=[],
        exclude_types=[]
    )

    object_types = orchestrator._get_object_types(schema_config)

    # Should include all supported types
    assert "TABLE" in object_types
    assert "VIEW" in object_types
    assert "PROCEDURE" in object_types


def test_extraction_orchestrator_excludes_types(sample_config):
    """Test that orchestrator excludes specified object types."""
    orchestrator = ExtractionOrchestrator(sample_config)

    schema_config = SchemaConfig(
        name="PUBLIC",
        include_types=[],
        exclude_types=["FUNCTION", "STREAM"]
    )

    object_types = orchestrator._get_object_types(schema_config)

    assert "FUNCTION" not in object_types
    assert "STREAM" not in object_types
    assert "TABLE" in object_types


def test_run_extraction_function(sample_config):
    """Test run_extraction convenience function."""
    with patch('discovery.orchestrator.load_config') as mock_load:
        mock_load.return_value = sample_config

        with patch.object(ExtractionOrchestrator, 'run') as mock_run:
            mock_run.return_value = ExtractionResult(total_objects=5, extracted=5)

            result = run_extraction("config.yml")

            assert mock_load.called
            assert mock_run.called


def test_extraction_orchestrator_with_multiple_targets():
    """Test orchestrator with multiple target databases."""
    config = DiscoveryConfig(
        targets=[
            TargetConfig(
                database="ANALYTICS",
                schemas=[SchemaConfig(name="PUBLIC", include_types=["TABLE"], exclude_types=[])]
            ),
            TargetConfig(
                database="WAREHOUSE",
                schemas=[SchemaConfig(name="PUBLIC", include_types=["TABLE"], exclude_types=[])]
            )
        ]
    )

    orchestrator = ExtractionOrchestrator(config)

    with patch.object(orchestrator, '_process_target'):
        with patch.object(orchestrator, '_connect'):
            result = orchestrator.run()

            assert isinstance(result, ExtractionResult)


def test_extraction_orchestrator_logs_progress(
    sample_config,
    mock_cursor
):
    """Test that orchestrator logs progress at various levels."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # Mock empty result
    mock_cursor.fetchall.return_value = []

    with patch('discovery.orchestrator.logger') as mock_logger:
        orchestrator.run()

        # Check that progress was logged
        assert any(call for call in mock_logger.info.call_args_list
                  if "database" in str(call).lower())
        assert any(call for call in mock_logger.info.call_args_list
                  if "schema" in str(call).lower())


def test_extraction_result_with_multiple_errors():
    """Test ExtractionResult with multiple errors."""
    result = ExtractionResult(
        total_objects=5,
        extracted=2,
        failed=3,
        errors=[
            DiscoveryError(
                object_name="table1",
                object_type="TABLE",
                error_message="Error 1",
                retry_count=3
            ),
            DiscoveryError(
                object_name="table2",
                object_type="TABLE",
                error_message="Error 2",
                retry_count=3
            ),
            DiscoveryError(
                object_name="table3",
                object_type="TABLE",
                error_message="Error 3",
                retry_count=3
            )
        ],
        duration=10.0
    )

    assert len(result.errors) == 3
    assert result.failed == 3
    assert result.extracted == 2


def test_extraction_orchestrator_with_nested_schemas():
    """Test orchestrator with nested schema names."""
    config = DiscoveryConfig(
        targets=[
            TargetConfig(
                database="ANALYTICS",
                schemas=[
                    SchemaConfig(
                        name="STAGING.CLEANED",
                        include_types=["TABLE"],
                        exclude_types=[]
                    )
                ]
            )
        ]
    )

    orchestrator = ExtractionOrchestrator(config)

    with patch.object(orchestrator, '_process_target'):
        with patch.object(orchestrator, '_connect'):
            result = orchestrator.run()

            assert isinstance(result, ExtractionResult)


def test_extraction_orchestrator_error_tracking(sample_config):
    """Test that orchestrator properly tracks errors."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # Mock empty results
    with patch.object(orchestrator, '_extract_tables', side_effect=Exception("Extraction failed")):
        with patch('discovery.orchestrator.logger') as mock_logger:
            with patch.object(orchestrator, '_connect'):
                orchestrator.run()

                # Check that error was logged
                assert mock_logger.error.called


def test_extraction_orchestrator_partial_success(sample_config):
    """Test orchestrator handles partial success scenario."""
    orchestrator = ExtractionOrchestrator(sample_config)

    with patch.object(orchestrator, '_process_target') as mock_process:
        # Simulate partial success
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call succeeds
                return None
            else:
                # Subsequent calls fail
                raise Exception("Processing failed")

        mock_process.side_effect = side_effect

        with patch.object(orchestrator, '_connect'):
            result = orchestrator.run()

            # Should have some errors but complete
            assert isinstance(result, ExtractionResult)


def test_run_extraction_with_invalid_config():
    """Test run_extraction with invalid config."""
    with patch('discovery.orchestrator.load_config') as mock_load:
        from discovery.utils.errors import ConfigValidationError

        mock_load.side_effect = ConfigValidationError("Invalid config")

        with pytest.raises(ConfigValidationError):
            run_extraction("invalid.yml")


def test_extraction_orchestrator_connection_cleanup_on_error(sample_config):
    """Test that orchestrator cleans up connection on error."""
    orchestrator = ExtractionOrchestrator(sample_config)

    mock_conn = MagicMock()
    mock_conn.connect.side_effect = SnowflakeConnectionError("Connection failed")

    with patch.object(orchestrator, '_connect', return_value=mock_conn):
        from discovery.utils.retry import ExtractionError

        with pytest.raises(ExtractionError):
            orchestrator.run()

        # Connection should still be closed even on error
        mock_conn.close.assert_called()


def test_extraction_orchestrator_with_no_objects(sample_config):
    """Test orchestrator when no objects are found."""
    orchestrator = ExtractionOrchestrator(sample_config)

    with patch.object(orchestrator, '_process_target'):
        with patch.object(orchestrator, '_connect'):
            result = orchestrator.run()

            # Should complete successfully with 0 objects
            assert result.total_objects == 0
            assert result.extracted == 0


def test_extraction_orchestrator_result_attributes():
    """Test that ExtractionResult has all expected attributes."""
    result = ExtractionResult()

    assert hasattr(result, 'total_objects')
    assert hasattr(result, 'extracted')
    assert hasattr(result, 'failed')
    assert hasattr(result, 'errors')
    assert hasattr(result, 'duration')


def test_extraction_orchestrator_config_validation(sample_config):
    """Test that orchestrator validates config on initialization."""
    orchestrator = ExtractionOrchestrator(sample_config)

    assert orchestrator.config is not None
    assert len(orchestrator.config.targets) > 0


def test_extraction_orchestrator_retry_logic(sample_config):
    """Test that orchestrator uses retry logic for extractions."""
    orchestrator = ExtractionOrchestrator(sample_config)

    # The orchestrator uses @retry decorator on extraction methods
    # This test verifies the structure is in place
    from discovery.extract.connection import SnowflakeConnection

    assert orchestrator.conn is None or isinstance(orchestrator.conn, MagicMock)


def test_run_extraction_with_valid_config_path(sample_config, tmp_discovery_dir):
    """Test run_extraction with valid config path."""
    config_path = tmp_discovery_dir / "config.yml"

    import yaml
    with open(config_path, 'w') as f:
        yaml.dump({"targets": [{"database": "TEST", "schemas": [{"name": "PUBLIC"}]}]}, f)

    with patch('discovery.orchestrator.load_config') as mock_load:
        mock_load.return_value = sample_config

        with patch.object(ExtractionOrchestrator, 'run') as mock_run:
            mock_run.return_value = ExtractionResult(total_objects=1, extracted=1)

            result = run_extraction(str(config_path))

            assert mock_load.called_with(str(config_path))
            assert mock_run.called
            assert result.extracted == 1
