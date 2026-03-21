"""Manifest generator for discovery runs."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from discovery.types import DiscoveryError
from discovery.config.schema import DiscoveryConfig


def _serialize_error(error: DiscoveryError) -> Dict[str, Any]:
    """Serialize discovery error to dict."""
    return {
        "object_name": error.object_name,
        "object_type": error.object_type,
        "error_message": error.error_message,
        "retry_count": error.retry_count,
    }


def generate_manifest(
    config: DiscoveryConfig,
    results: List[Any],
    errors: List[DiscoveryError],
    snowflake_account: Optional[str] = None,
    format_version: str = "1.0.0"
) -> Dict[str, Any]:
    """Generate manifest dict for a discovery run.

    Args:
        config: Discovery configuration
        results: List of extracted metadata objects
        errors: List of errors encountered during discovery
        snowflake_account: Snowflake account identifier (if available)
        format_version: Manifest format version

    Returns:
        JSON-serializable manifest dict with format_version, generated_at,
        snowflake_account, config_hash, object_count, and errors
    """
    # Get config hash (cached in config object)
    config_hash = config.get_config_hash()

    # Count extracted objects
    object_count = len(results)

    # Serialize errors
    serialized_errors = [_serialize_error(error) for error in errors]

    # Build manifest
    manifest = {
        "format_version": format_version,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "snowflake_account": snowflake_account,
        "config_hash": config_hash,
        "object_count": object_count,
        "errors": serialized_errors,
    }

    return manifest
