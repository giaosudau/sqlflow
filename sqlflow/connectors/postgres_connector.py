"""PostgreSQL connector for SQLFlow."""

import dataclasses
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import psycopg2.pool
import pyarrow as pa

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    HealthCheckError,
    IncrementalError,
    ParameterError,
    ParameterValidator,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector
from sqlflow.core.errors import ConnectorError
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class PostgresParameterValidator(ParameterValidator):
    """Parameter validator for PostgreSQL connector."""

    def __init__(self):
        super().__init__("POSTGRES")

    def _get_required_params(self) -> List[str]:
        """Get required parameters for PostgreSQL connector."""
        return ["host"]  # Only host is truly required, database/dbname can be derived

    def _get_optional_params(self) -> Dict[str, Any]:
        """Get optional parameters with defaults."""
        return {
            **super()._get_optional_params(),
            "port": 5432,
            "schema": "public",
            "connect_timeout": 10,
            "application_name": "sqlflow",
            "min_connections": 1,
            "max_connections": 5,
            "sync_mode": "full_refresh",
            "sslmode": "prefer",
            # Support both old and new parameter names
            "database": None,  # New industry-standard name
            "dbname": None,  # Old parameter name (backward compatibility)
            "username": None,  # New industry-standard name
            "user": None,  # Old parameter name (backward compatibility)
            "password": "",  # Required but can be empty for some auth methods
        }

    def validate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters with backward compatibility support."""
        validated = super().validate(params)

        # Handle database name - new 'database' takes precedence over old 'dbname'
        database = validated.get("database") or validated.get("dbname")
        if not database:
            raise ParameterError("Either 'database' or 'dbname' parameter is required")
        validated["database"] = database

        # Handle username - new 'username' takes precedence over old 'user'
        username = validated.get("username") or validated.get("user")
        if not username:
            raise ParameterError("Either 'username' or 'user' parameter is required")
        validated["username"] = username

        # Ensure integer parameters are converted properly
        for int_param in [
            "port",
            "connect_timeout",
            "min_connections",
            "max_connections",
        ]:
            if int_param in validated and validated[int_param] is not None:
                try:
                    validated[int_param] = int(validated[int_param])
                except (ValueError, TypeError):
                    raise ParameterError(f"Parameter '{int_param}' must be an integer")

        return validated


@dataclasses.dataclass
class PostgresConnectionParams:
    """PostgreSQL connection parameters with industry-standard naming."""

    host: str
    port: int = 5432
    database: str = ""  # Changed from dbname to database for industry standard
    username: str = ""  # Changed from user to username for industry standard
    password: str = ""
    schema: str = "public"  # Added schema parameter
    connect_timeout: int = 10
    application_name: str = "sqlflow"
    min_connections: int = 1
    max_connections: int = 5
    sslmode: str = "prefer"

    # Incremental loading parameters
    sync_mode: str = "full_refresh"
    cursor_field: Optional[str] = None
    primary_key: Optional[List[str]] = None
    table: Optional[str] = None  # Target table for incremental loading
    query: Optional[str] = None  # Custom query override


@register_connector("POSTGRES")
class PostgresConnector(Connector):
    """Enhanced PostgreSQL connector with industry-standard parameters and incremental loading."""

    def __init__(self):
        """Initialize a PostgresConnector."""
        super().__init__()
        self.params: Optional[PostgresConnectionParams] = None
        self.connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._parameter_validator = PostgresParameterValidator()

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with industry-standard parameters.

        Args:
        ----
            params: Configuration parameters including:
                   - host, port, database, username, password (connection)
                   - sync_mode, cursor_field, primary_key (incremental)
                   - schema, table, query (data selection)

        Raises:
        ------
            ParameterError: If parameters are invalid
            ConnectorError: If configuration fails

        """
        try:
            # Validate parameters using standardized framework
            validated_params = self.validate_params(params)

            self.params = PostgresConnectionParams(
                host=validated_params["host"],
                port=validated_params["port"],
                database=validated_params["database"],
                username=validated_params["username"],
                password=validated_params["password"],
                schema=validated_params.get("schema", "public"),
                connect_timeout=validated_params.get("connect_timeout", 10),
                application_name=validated_params.get("application_name", "sqlflow"),
                min_connections=validated_params.get("min_connections", 1),
                max_connections=validated_params.get("max_connections", 5),
                sslmode=validated_params.get("sslmode", "prefer"),
                sync_mode=validated_params.get("sync_mode", "full_refresh"),
                cursor_field=validated_params.get("cursor_field"),
                primary_key=validated_params.get("primary_key"),
                table=validated_params.get("table"),
                query=validated_params.get("query"),
            )

            # Validate incremental parameters if specified
            if self.params.sync_mode == "incremental":
                self.validate_incremental_params(validated_params)

            self.state = ConnectorState.CONFIGURED
            logger.info(
                f"PostgreSQL connector configured for {self.params.host}:{self.params.port}/{self.params.database}"
            )

        except (ParameterError, IncrementalError):
            self.state = ConnectorState.ERROR
            raise
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "POSTGRES", f"Configuration failed: {str(e)}"
            )

    def validate_incremental_params(self, params: Dict[str, Any]) -> None:
        """Validate incremental loading parameters."""
        sync_mode = params.get("sync_mode")

        if sync_mode == "incremental":
            cursor_field = params.get("cursor_field")
            if not cursor_field:
                raise IncrementalError(
                    "cursor_field is required for incremental sync_mode",
                    self.name or "POSTGRES",
                )

            # Validate that either table or query is specified
            table = params.get("table")
            query = params.get("query")
            if not table and not query:
                raise IncrementalError(
                    "Either 'table' or 'query' parameter must be specified",
                    self.name or "POSTGRES",
                )

    def _create_connection_pool(self) -> None:
        """Create a connection pool with enhanced error handling.

        Raises
        ------
            ConnectorError: If connection pool creation fails

        """
        if self.params is None:
            raise ConnectorError(
                self.name or "POSTGRES", "Cannot create connection pool: not configured"
            )

        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.params.min_connections,
                maxconn=self.params.max_connections,
                host=self.params.host,
                port=self.params.port,
                dbname=self.params.database,  # psycopg2 uses dbname
                user=self.params.username,  # psycopg2 uses user
                password=self.params.password,
                connect_timeout=self.params.connect_timeout,
                application_name=self.params.application_name,
                sslmode=self.params.sslmode,
            )
            logger.info(
                f"Created PostgreSQL connection pool with {self.params.min_connections}-{self.params.max_connections} connections"
            )

        except Exception as e:
            raise ConnectorError(
                self.name or "POSTGRES",
                f"Failed to create connection pool: {str(e)}",
            )

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the PostgreSQL database with enhanced monitoring.

        Returns
        -------
            Result of the connection test

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.params is None:
                return ConnectionTestResult(False, "Not configured")

            start_time = time.time()
            conn = psycopg2.connect(
                host=self.params.host,
                port=self.params.port,
                dbname=self.params.database,
                user=self.params.username,
                password=self.params.password,
                connect_timeout=self.params.connect_timeout,
                application_name=self.params.application_name,
                sslmode=self.params.sslmode,
            )

            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version_result = cursor.fetchone()
            if version_result is None:
                raise ConnectorError(
                    self.name or "POSTGRES", "Failed to get PostgreSQL version"
                )
            version_info = version_result[0]
            cursor.execute(
                "SELECT current_database(), current_user, inet_server_addr(), inet_server_port()"
            )
            db_result = cursor.fetchone()
            if db_result is None:
                raise ConnectorError(
                    self.name or "POSTGRES", "Failed to get database information"
                )
            db_info = db_result
            cursor.close()
            conn.close()

            connection_time = time.time() - start_time

            self._create_connection_pool()
            self.state = ConnectorState.READY

            message = f"Connected to PostgreSQL {version_info} as {db_info[1]}@{db_info[0]} in {connection_time:.2f}s"
            logger.info(message)

            return ConnectionTestResult(True, message)

        except Exception as e:
            self.state = ConnectorState.ERROR
            error_msg = str(e)
            logger.error(f"PostgreSQL connection test failed: {error_msg}")
            return ConnectionTestResult(False, error_msg)

    def check_health(self) -> Dict[str, Any]:
        """Comprehensive health check with PostgreSQL-specific metrics."""
        start_time = time.time()

        try:
            if self.state != ConnectorState.READY:
                test_result = self.test_connection()
                if not test_result.success:
                    return {
                        "status": "unhealthy",
                        "connected": False,
                        "error": test_result.message,
                        "response_time_ms": (time.time() - start_time) * 1000,
                        "last_check": datetime.utcnow().isoformat(),
                        "capabilities": {
                            "incremental": False,
                            "schema_discovery": False,
                        },
                    }

            # Ensure params and connection pool are available
            if self.params is None:
                raise HealthCheckError(
                    "Connector not configured", self.name or "POSTGRES"
                )

            # Test actual query execution
            if self.connection_pool is None:
                self._create_connection_pool()

            if self.connection_pool is None:
                raise HealthCheckError(
                    "Failed to create connection pool", self.name or "POSTGRES"
                )

            conn = self.connection_pool.getconn()
            try:
                cursor = conn.cursor()

                # Get database statistics
                cursor.execute(
                    """
                    SELECT 
                        pg_database_size(current_database()) as db_size_bytes,
                        (SELECT count(*) FROM information_schema.tables WHERE table_schema = %s) as table_count,
                        version() as version
                """,
                    (self.params.schema,),
                )

                db_stats = cursor.fetchone()
                cursor.close()

                if not db_stats:
                    raise HealthCheckError(
                        "Failed to get database statistics", self.name or "POSTGRES"
                    )

                response_time = (time.time() - start_time) * 1000

                return {
                    "status": "healthy",
                    "connected": True,
                    "response_time_ms": response_time,
                    "last_check": datetime.utcnow().isoformat(),
                    "database_size_bytes": db_stats[0],
                    "table_count": db_stats[1],
                    "postgresql_version": db_stats[2],
                    "schema": self.params.schema,
                    "capabilities": {
                        "incremental": True,
                        "schema_discovery": True,
                        "batch_reading": True,
                        "custom_queries": True,
                    },
                }

            finally:
                self.connection_pool.putconn(conn)

        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e),
                "response_time_ms": (time.time() - start_time) * 1000,
                "last_check": datetime.utcnow().isoformat(),
                "capabilities": {
                    "incremental": False,
                    "schema_discovery": False,
                },
            }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get PostgreSQL-specific performance metrics."""
        if self.connection_pool is None or self.params is None:
            return {
                "connection_time_ms": 0,
                "query_time_ms": 0,
                "rows_per_second": 0,
                "bytes_transferred": 0,
                "active_connections": 0,
                "max_connections": 0,
            }

        try:
            conn = self.connection_pool.getconn()
            start_time = time.time()

            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s",
                    (self.params.schema,),
                )
                table_count = cursor.fetchone()[0]
                cursor.close()

                query_time = (time.time() - start_time) * 1000

                return {
                    "connection_time_ms": query_time,
                    "query_time_ms": query_time,
                    "rows_per_second": 1000.0 / max(query_time, 1),  # Rough estimate
                    "bytes_transferred": 64,  # Approximate bytes for table count query
                    "active_connections": self.connection_pool.minconn,
                    "max_connections": self.connection_pool.maxconn,
                    "schema_table_count": table_count,
                }

            finally:
                self.connection_pool.putconn(conn)

        except Exception as e:
            logger.warning(f"Failed to get PostgreSQL performance metrics: {e}")
            return {
                "connection_time_ms": 0,
                "query_time_ms": 0,
                "rows_per_second": 0,
                "bytes_transferred": 0,
                "active_connections": 0,
                "max_connections": 0,
                "error": str(e),
            }

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental loading."""
        return True

    def discover(self) -> List[str]:
        """Discover available tables in the specified schema.

        Returns
        -------
            List of table names

        Raises
        ------
            ConnectorError: If discovery fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        if self.params is None:
            raise ConnectorError(self.name or "POSTGRES", "Connector not configured")

        try:
            if self.connection_pool is None:
                self._create_connection_pool()

            if self.connection_pool is None:
                raise ConnectorError(
                    self.name or "POSTGRES", "Connection pool initialization failed"
                )

            conn = self.connection_pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                    """,
                    (self.params.schema,),
                )
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()

                self.state = ConnectorState.READY
                logger.info(
                    f"Discovered {len(tables)} tables in schema '{self.params.schema}'"
                )
                return tables

            finally:
                self.connection_pool.putconn(conn)

        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "POSTGRES", f"Discovery failed: {str(e)}")

    def _pg_type_to_arrow(self, pg_type: str) -> pa.DataType:
        """Convert PostgreSQL type to Arrow type.

        Args:
        ----
            pg_type: PostgreSQL data type

        Returns:
        -------
            Arrow data type

        """
        if pg_type in ("integer", "bigint", "smallint"):
            return pa.int64()
        elif pg_type in ("real", "double precision", "numeric"):
            return pa.float64()
        elif pg_type == "boolean":
            return pa.bool_()
        elif pg_type in ("date",):
            return pa.date32()
        elif pg_type in ("timestamp", "timestamp without time zone"):
            return pa.timestamp("ns")
        else:
            return pa.string()

    def _fetch_table_columns(self, conn, object_name: str) -> List[tuple]:
        """Fetch column information for a table.

        Args:
        ----
            conn: Database connection
            object_name: Table name

        Returns:
        -------
            List of (column_name, data_type) tuples

        Raises:
        ------
            ValueError: If table not found

        """
        if self.params is None:
            raise ValueError("Connector not configured")

        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    column_name, 
                    data_type
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = %s AND 
                    table_name = %s
                ORDER BY 
                    ordinal_position
                """,
                (self.params.schema, object_name),
            )
            columns = cursor.fetchall()

            if not columns:
                raise ValueError(
                    f"Table not found: {object_name} in schema {self.params.schema}"
                )

            return columns
        finally:
            cursor.close()

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for a table.

        Args:
        ----
            object_name: Table name

        Returns:
        -------
            Schema for the table

        Raises:
        ------
            ConnectorError: If schema retrieval fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.connection_pool is None:
                self._create_connection_pool()

            if self.connection_pool is None:
                raise ConnectorError(
                    self.name or "POSTGRES", "Connection pool initialization failed"
                )

            conn = self.connection_pool.getconn()
            try:
                columns = self._fetch_table_columns(conn, object_name)

                fields = [
                    pa.field(name, self._pg_type_to_arrow(pg_type))
                    for name, pg_type in columns
                ]

                self.state = ConnectorState.READY
                return Schema(pa.schema(fields))
            finally:
                self.connection_pool.putconn(conn)
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "POSTGRES", f"Schema retrieval failed: {str(e)}"
            )

    def _build_query(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> tuple:
        """Build a SQL query with filters.

        Args:
        ----
            object_name: Table name
            columns: Optional list of columns to read
            filters: Optional filters to apply

        Returns:
        -------
            Tuple of (query, params)

        """
        column_str = "*"
        if columns:
            escaped_columns = [f'"{col}"' for col in columns]
            column_str = ", ".join(escaped_columns)

        query = f'SELECT {column_str} FROM "{object_name}"'
        params = []

        if filters:
            where_clauses = []
            for key, value in filters.items():
                if isinstance(value, (list, tuple)):
                    placeholders = ", ".join(["%s"] * len(value))
                    where_clauses.append(f'"{key}" IN ({placeholders})')
                    params.extend(value)
                else:
                    where_clauses.append(f'"{key}" = %s')
                    params.append(value)

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        return query, params

    def _fetch_data_in_batches(self, cursor, batch_size: int) -> Iterator[DataChunk]:
        """Fetch data from cursor in batches.

        Args:
        ----
            cursor: Database cursor
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects

        """
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break

            df = pd.DataFrame(batch)
            yield DataChunk(df)

    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from a PostgreSQL table in chunks.

        Args:
        ----
            object_name: Table name
            columns: Optional list of columns to read
            filters: Optional filters to apply
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects

        Raises:
        ------
            ConnectorError: If reading fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.connection_pool is None:
                self._create_connection_pool()

            if self.connection_pool is None:
                raise ConnectorError(
                    self.name or "POSTGRES", "Connection pool initialization failed"
                )

            conn = self.connection_pool.getconn()
            try:
                query, params = self._build_query(object_name, columns, filters)

                cursor = conn.cursor(
                    name="sqlflow_cursor", cursor_factory=psycopg2.extras.DictCursor
                )
                cursor.itersize = batch_size
                cursor.execute(query, params)

                yield from self._fetch_data_in_batches(cursor, batch_size)

                cursor.close()
                self.state = ConnectorState.READY
            finally:
                self.connection_pool.putconn(conn)
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "POSTGRES", f"Reading failed: {str(e)}")

    def close(self) -> None:
        """Close the connection pool."""
        if self.connection_pool is not None:
            self.connection_pool.closeall()
            self.connection_pool = None

    def _build_incremental_query(
        self, object_name: str, cursor_field: str, columns: Optional[List[str]] = None
    ) -> str:
        """Build incremental query string.

        Args:
        ----
            object_name: Table name to read from
            cursor_field: Field to use for incremental loading
            columns: Optional list of columns to read

        Returns:
        -------
            SQL query string
        """
        # Build column string
        column_str = "*"
        if columns:
            escaped_columns = [f'"{col}"' for col in columns]
            column_str = ", ".join(escaped_columns)

        # Use custom query if provided, otherwise build table query
        if self.params and self.params.query:
            base_query = f"SELECT {column_str} FROM ({self.params.query}) as subquery"
        else:
            table_name = (self.params and self.params.table) or object_name
            if self.params and self.params.schema and self.params.schema != "public":
                table_name = f'"{self.params.schema}"."{table_name}"'
            else:
                table_name = f'"{table_name}"'
            base_query = f"SELECT {column_str} FROM {table_name}"

        return base_query

    def _execute_incremental_query(
        self,
        base_query: str,
        cursor_field: str,
        cursor_value: Optional[Any],
        columns: Optional[List[str]],
        batch_size: int,
    ) -> Iterator[DataChunk]:
        """Execute incremental query and yield data chunks.

        Args:
        ----
            base_query: Base SQL query
            cursor_field: Field to use for incremental loading
            cursor_value: Last processed value for incremental loading
            columns: Optional list of columns to read
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects with incremental data
        """
        # Add incremental filter
        params = []
        if cursor_value is not None:
            base_query += f' WHERE "{cursor_field}" > %s'
            params.append(cursor_value)

        # Add ordering for consistent incremental loading
        base_query += f' ORDER BY "{cursor_field}"'

        logger.info(
            f"Executing incremental query: {base_query} with cursor_value: {cursor_value}"
        )

        if self.connection_pool is None:
            raise ConnectorError(
                self.name or "POSTGRES", "Connection pool not initialized"
            )

        conn = self.connection_pool.getconn()
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(base_query, params)

            # Get column names from cursor description
            if not columns:
                column_names = [desc.name for desc in cursor.description]
            else:
                column_names = columns

            total_rows = 0
            for chunk in self._fetch_data_in_batches(cursor, batch_size):
                if hasattr(chunk.pandas_df, "columns"):
                    chunk.pandas_df.columns = column_names[
                        : len(chunk.pandas_df.columns)
                    ]
                total_rows += len(chunk.pandas_df)
                yield chunk

            cursor.close()
            logger.info(
                f"Read {total_rows} incremental rows where {cursor_field} > {cursor_value}"
            )

        finally:
            self.connection_pool.putconn(conn)

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data incrementally from a PostgreSQL table.

        Args:
        ----
            object_name: Table name to read from
            cursor_field: Field to use for incremental loading
            cursor_value: Last processed value for incremental loading
            columns: Optional list of columns to read
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects with incremental data

        Raises:
        ------
            ConnectorError: If incremental read fails

        """
        self.validate_state(ConnectorState.CONFIGURED)

        if self.params is None:
            raise ConnectorError(self.name or "POSTGRES", "Connector not configured")

        try:
            if self.connection_pool is None:
                self._create_connection_pool()

            # Build and execute incremental query
            base_query = self._build_incremental_query(
                object_name, cursor_field, columns
            )
            yield from self._execute_incremental_query(
                base_query, cursor_field, cursor_value, columns, batch_size
            )

            self.state = ConnectorState.READY

        except Exception as e:
            self.state = ConnectorState.ERROR
            logger.error(f"Incremental read failed for {object_name}: {e}")
            raise ConnectorError(
                self.name or "POSTGRES", f"Incremental read failed: {str(e)}"
            )

    def get_cursor_value(
        self, data_chunk: DataChunk, cursor_field: str
    ) -> Optional[Any]:
        """Extract the maximum cursor value from a data chunk.

        Args:
        ----
            data_chunk: DataChunk to extract cursor value from
            cursor_field: Name of the cursor field

        Returns:
        -------
            Maximum cursor value from the chunk, or None if not found

        """
        try:
            df = data_chunk.pandas_df
            if df.empty:
                return None

            if cursor_field not in df.columns:
                logger.warning(
                    f"Cursor field '{cursor_field}' not found in data chunk columns: {list(df.columns)}"
                )
                return None

            max_value = df[cursor_field].max()

            # Handle pandas NaT or NaN values - use simple None check
            try:
                # Convert to Python object if it's a pandas scalar
                if hasattr(max_value, "item"):
                    max_value = max_value.item()

                # Simple None check to avoid pandas Series boolean issues
                if max_value is None:
                    return None

                # Check for pandas NaN/NaT using string representation (safer approach)
                if str(max_value) in ("NaT", "nan", "NaN"):
                    return None

            except (TypeError, ValueError, AttributeError):
                # If conversion fails, return the original value
                pass

            return max_value

        except Exception as e:
            logger.error(
                f"Failed to extract cursor value for field '{cursor_field}': {e}"
            )
            return None
