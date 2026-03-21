"""Snowflake connection module with key-pair authentication."""

import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from typing import Optional, List, Dict, Any
import snowflake.connector
from snowflake.connector import errors as sf_errors
import logging

logger = logging.getLogger(__name__)


class ConnectionError(Exception):
    """Snowflake connection error."""
    pass


class SnowflakeConnection:
    """Wrapper for Snowflake connector with key-pair authentication."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Snowflake connection from config dict.

        Args:
            config: Connection parameters from environment or provided dict
        """
        self._config = config
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    def _load_private_key(self, private_key_pem: str) -> bytes:
        """Load PEM private key and convert to DER bytes.

        Args:
            private_key_pem: PEM formatted private key string

        Returns:
            DER encoded private key bytes

        Raises:
            ValueError: If key is invalid or cannot be parsed
        """
        try:
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
            logger.info("Successfully loaded and converted private key to DER format")
            return der_key
        except ValueError as e:
            logger.error(f"Invalid PEM private key: {e}")
            raise ValueError(f"Invalid PEM format: {e}") from e
        except Exception as e:
            logger.error(f"Error converting private key to DER: {e}")
            raise ValueError(f"Failed to process private key: {e}") from e

    def connect(self) -> 'SnowflakeConnection':
        """Establish connection to Snowflake using key-pair authentication.

        Returns:
            Snowflake connection object

        Raises:
            ConnectionError: If connection fails
        """
        if self._conn:
            logger.warning("Connection already established")
            return self._conn

        # Get connection parameters from config dict
        account = self._config.get('account', os.getenv('SNOWFLAKE_ACCOUNT'))
        user = self._config.get('user', os.getenv('SNOWFLAKE_USER'))
        warehouse = self._config.get('warehouse', os.getenv('SNOWFLAKE_WAREHOUSE'))
        database = self._config.get('database', os.getenv('SNOWFLAKE_DATABASE'))
        role = self._config.get('role', os.getenv('SNOWFLAKE_ROLE'))
        private_key_raw = self._config.get('private_key', os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW'))

        if not all([account, user, warehouse, database, private_key_raw]):
            missing = [k for k in ['account', 'user', 'warehouse', 'database', 'private_key']
                      if not self._config.get(k) and not os.getenv(k)]
            raise ConnectionError(f"Missing required connection parameters: {', '.join(missing)}")

        try:
            der_key = self._load_private_key(private_key_raw)
        except ValueError as e:
            raise ConnectionError(f"Failed to load private key: {e}") from e

        try:
            logger.info(f"Connecting to Snowflake account: {account}, user: {user}, warehouse: {warehouse}")
            conn = snowflake.connector.connect(
                account=account,
                user=user,
                private_key=der_key,
                warehouse=warehouse,
                database=database,
                role=role,
                application='snowflake-discovery',
            )
            self._conn = conn
            logger.info("Successfully connected to Snowflake")
            return self._conn
        except sf_errors.DatabaseError as e:
            logger.error(f"Snowflake database error: {e}")
            raise ConnectionError(f"Database error: {e}") from e
        except sf_errors.OperationalError as e:
            logger.error(f"Snowflake operational error: {e}")
            raise ConnectionError(f"Operational error: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error connecting to Snowflake: {e}")
            raise ConnectionError(f"Connection error: {e}") from e

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results as list of dicts.

        Args:
            sql: SQL query string to execute

        Returns:
            List of dicts where each dict represents a row with column names as keys

        Raises:
            Exception: If query execution fails
        """
        if not self._conn:
            raise ConnectionError("No active connection")

        try:
            cursor = self._conn.cursor()
            logger.debug(f"Executing query: {sql[:100]}...")
            cursor.execute(sql)
            results = cursor.fetchall()
            # Get column names from cursor description
            column_names = ([desc[0] for desc in cursor.description] if cursor.description else [])
            # Convert to list of dicts
            rows = [dict(zip(column_names, row)) for row in results]
            logger.debug(f"Query returned {len(rows)} rows")
            return rows
        except sf_errors.DatabaseError as e:
            logger.error(f"Query execution error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            raise

    def close(self) -> None:
        """Close the Snowflake connection."""
        if self._conn:
            logger.info("Closing Snowflake connection")
            self._conn.close()
            self._conn = None

    def __enter__(self) -> 'SnowflakeConnection':
        """Context manager entry point."""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit point."""
        self.close()
        if exc_type:
            logger.error(f"Connection closed with exception: {exc_val}")
            raise exc_val


def load_private_key(pem_content: str) -> bytes:
    """Load PEM private key and convert to DER format.

    Args:
        pem_content: PEM formatted private key string

    Returns:
        DER encoded private key bytes

    Raises:
        ValueError: If key is invalid or cannot be parsed
    """
    try:
        private_key = serialization.load_pem_private_key(
                pem_content.encode('utf-8'),
                password=None,
                backend=default_backend()
            )
        # Convert to DER format required by Snowflake
        der_key = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        logger.info("Successfully loaded and converted private key to DER format")
        return der_key
    except ValueError as e:
        logger.error(f"Invalid PEM private key: {e}")
        raise ValueError(f"Invalid PEM format: {e}") from e
    except Exception as e:
        logger.error(f"Error converting private key to DER: {e}")
        raise ValueError(f"Failed to process private key: {e}") from e


def connect(config: Dict[str, Any]) -> SnowflakeConnection:
    """Factory function to create SnowflakeConnection from config dict.

    Args:
        config: Connection parameters

    Returns:
        SnowflakeConnection instance
    """
    return SnowflakeConnection(config)


def execute_query(conn: SnowflakeConnection, sql: str) -> List[Dict[str, Any]]:
    """Execute query using a Snowflake connection.

    Args:
        conn: SnowflakeConnection instance
        sql: SQL query string

    Returns:
        List of dicts representing query results
    """
    return conn.execute_query(sql)
