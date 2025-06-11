from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.resilience import resilient_operation

logger = logging.getLogger(__name__)


class PostgresSource(Connector):
    """
    Connector for reading data from PostgreSQL with resilience patterns.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        self._engine = None
        self.conn_params = {}

        if config is not None:
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters."""
        self.conn_params = params

        # Apply parameter translation for industry-standard to psycopg2 compatibility
        self.conn_params = self._translate_parameters(params)

        # Validate that we have either operational parameters or connection parameters
        has_connection_params = all(
            key in self.conn_params for key in ["user", "host", "port", "dbname"]
        )
        has_operational_params = any(
            key in self.conn_params for key in ["query", "table_name", "table"]
        )

        if not has_connection_params and not has_operational_params:
            raise ValueError(
                "PostgresSource: must provide either connection parameters (host, port, database/dbname, username/user, password) "
                "or operational parameters (query, table_name, table)"
            )

        # Configure resilience patterns
        self._configure_resilience(params)

        self.state = ConnectorState.CONFIGURED

    def _translate_parameters(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Translate industry-standard parameters to psycopg2-compatible names.

        Provides backward compatibility by supporting both naming conventions:
        - Industry standard: database, username (Airbyte/Fivetran compatible)
        - psycopg2 legacy: dbname, user

        Args:
            config: Original configuration parameters

        Returns:
            Translated configuration with psycopg2-compatible parameter names
        """
        translated = config.copy()

        # Parameter mapping: industry_standard -> psycopg2_name
        param_mapping = {
            "database": "dbname",
            "username": "user",
        }

        for industry_std, psycopg2_name in param_mapping.items():
            if industry_std in config and psycopg2_name not in config:
                translated[psycopg2_name] = config[industry_std]
                logger.debug(
                    f"PostgresSource: Translated parameter '{industry_std}' -> '{psycopg2_name}' "
                    f"for psycopg2 compatibility"
                )

        # Also handle table parameter mapping for consistency
        if "table" in config and "table_name" not in config:
            translated["table_name"] = config["table"]
            logger.debug("PostgresSource: Mapped 'table' parameter to 'table_name'")

        return translated

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            # Build connection URI with fallback defaults for testing
            user = self.conn_params.get("user", "testuser")
            password = self.conn_params.get("password", "testpass")
            host = self.conn_params.get("host", "localhost")
            port = self.conn_params.get("port", 5432)
            dbname = self.conn_params.get("dbname", "testdb")

            conn_uri = (
                f"postgresql+psycopg2://{user}:{password}" f"@{host}:{port}/{dbname}"
            )

            connect_args = {}
            if "connect_timeout" in self.conn_params:
                connect_args["connect_timeout"] = self.conn_params["connect_timeout"]

            self._engine = create_engine(conn_uri, connect_args=connect_args)
            logger.debug(
                f"PostgresSource: Created engine with URI postgresql://{user}:***@{host}:{port}/{dbname}"
            )
        return self._engine

    @resilient_operation()
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to PostgreSQL with resilience."""
        try:
            with self.engine.connect() as connection:
                # Simple test query - wrap in text() for SQLAlchemy 2.x compatibility
                result = connection.execute(text("SELECT 1")).fetchone()
                if result and result[0] == 1:
                    return ConnectionTestResult(
                        success=True,
                        message=f"Successfully connected to PostgreSQL database '{self.conn_params.get('dbname')}'",
                    )
                else:
                    return ConnectionTestResult(
                        success=False,
                        message="Connection test query returned unexpected result",
                    )
        except Exception as e:
            logger.error(f"PostgresSource: Connection test failed: {str(e)}")
            return ConnectionTestResult(
                success=False, message=f"Connection failed: {str(e)}"
            )

    @resilient_operation()
    def discover(self) -> List[str]:
        """Discover available tables in the database with resilience."""
        try:
            return self.get_tables()
        except Exception as e:
            logger.error(f"PostgresSource: Failed to discover tables: {str(e)}")
            raise

    @resilient_operation()
    def get_schema(self, object_name: str) -> Schema:
        """Get schema for a PostgreSQL table with resilience."""
        try:
            with self.engine.connect() as connection:
                # Get column information
                schema_name = self.conn_params.get("schema", "public")
                query = text(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = :schema_name AND table_name = :table_name
                    ORDER BY ordinal_position
                """
                )
                result = connection.execute(
                    query, {"schema_name": schema_name, "table_name": object_name}
                )
                columns = []
                for row in result:
                    columns.append(
                        {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
                    )

                return Schema(name=object_name, columns=columns)
        except Exception as e:
            logger.error(
                f"PostgresSource: Failed to get schema for '{object_name}': {str(e)}"
            )
            raise

    def _build_column_list(self, columns: Optional[List[str]] = None) -> str:
        """Build the column list part of the SQL query."""
        if columns:
            return ", ".join(f'"{col}"' for col in columns)
        return "*"

    def _build_filter_conditions(self, filters: Optional[Dict[str, Any]] = None) -> str:
        """Build the WHERE clause of the SQL query."""
        if not filters:
            return ""

        conditions = []
        for key, value in filters.items():
            if isinstance(value, str):
                conditions.append(f"\"{key}\" = '{value}'")
            else:
                conditions.append(f'"{key}" = {value}')

        return " WHERE " + " AND ".join(conditions) if conditions else ""

    def _build_sql_query(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Build the complete SQL query."""
        schema = self.conn_params.get("schema", "public")
        column_list = self._build_column_list(columns)
        filter_conditions = self._build_filter_conditions(filters)

        return (
            f"SELECT {column_list} "
            f'FROM {schema}."{object_name}"'
            f"{filter_conditions}"
        )

    def _execute_query_in_chunks(
        self, sql: str, batch_size: int
    ) -> Iterator[DataChunk]:
        """Execute SQL query and yield results in chunks."""
        try:
            with self.engine.connect() as connection:
                chunk_iter = pd.read_sql_query(
                    text(sql), connection, chunksize=batch_size
                )

                for chunk_df in chunk_iter:
                    yield DataChunk(pa.Table.from_pandas(chunk_df))
        except Exception as e:
            logger.error(f"PostgresSource: Failed to execute query '{sql}': {str(e)}")
            raise

    @resilient_operation()
    def read(
        self,
        object_name: str = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterator[DataChunk]:
        """Read data from PostgreSQL in chunks with resilience.

        Supports both new interface and legacy options-based interface.

        Args:
            object_name: Name of the table or view to read from
            columns: Optional list of columns to select
            filters: Optional dictionary of column filters
            batch_size: Number of rows to read per chunk
            options: Legacy options dictionary for backward compatibility

        Returns:
            Iterator yielding DataChunk objects

        Raises:
            ValueError: If object_name is not provided when not using options
        """
        # Handle legacy options parameter for backward compatibility
        if options:
            yield from self._read_with_options(options, batch_size)
            return

        if not object_name:
            raise ValueError("object_name is required when not using options")

        # Build and execute query
        sql = self._build_sql_query(object_name, columns, filters)
        logger.info(f"Executing query: {sql}")
        yield from self._execute_query_in_chunks(sql, batch_size)

    def _read_with_options(
        self, options: Dict[str, Any], batch_size: int = 10000
    ) -> Iterator[DataChunk]:
        """Read data using legacy options format (for backward compatibility)."""
        # Use the legacy read method but return DataChunk instead of DataFrame
        df = self.read_legacy(options)

        # Convert DataFrame to DataChunk iterator with batching
        for i in range(0, len(df), batch_size):
            chunk_df = df.iloc[i : i + batch_size]
            # Convert to PyArrow table for standardized format
            table = pa.Table.from_pandas(chunk_df)
            yield DataChunk(table)

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        batch_size: int = 10000,
        **kwargs,
    ) -> Iterator[DataChunk]:
        """Read data incrementally using a cursor field."""
        # Build incremental query
        filters = kwargs.get("filters", {})
        if cursor_value is not None:
            filters[cursor_field] = f"> '{cursor_value}'"

        # Use the resilient read method
        return self.read(
            object_name=object_name,
            columns=kwargs.get("columns"),
            filters=filters,
            batch_size=batch_size,
        )

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental reading."""
        return True

    def get_cursor_value(self, chunk: DataChunk, cursor_field: str) -> Optional[Any]:
        """Get the maximum cursor value from a data chunk."""
        df = chunk.pandas_df
        if cursor_field in df.columns and not df.empty:
            return df[cursor_field].max()
        return None

    @resilient_operation()
    def get_tables(self, schema: str = "public") -> List[str]:
        """Get list of tables in the specified schema with resilience."""
        try:
            with self.engine.connect() as connection:
                result = pd.read_sql_query(
                    text(
                        """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                    """
                    ),
                    connection,
                    params={"schema": schema},
                )
                return result["table_name"].tolist()
        except Exception as e:
            logger.error(
                f"PostgresSource: Failed to get tables for schema '{schema}': {str(e)}"
            )
            raise

    # Legacy method for backward compatibility
    def read_legacy(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Legacy read method for backward compatibility.
        """
        read_options = options or {}
        # Try to get query/table_name from options first, then from constructor config
        query = read_options.get("query")
        table_name = read_options.get("table_name") or read_options.get("table")
        schema = read_options.get("schema", "public")

        if not query and not table_name:
            raise ValueError(
                "PostgresSource: either 'query' or 'table_name' (or 'table') must be specified in options or config"
            )

        sql = ""
        if query:
            sql = query
        else:
            sql = f'SELECT * FROM {schema}."{table_name}"'

        logger.info(f"Executing query: {sql}")
        try:
            with self.engine.connect() as connection:
                return pd.read_sql_query(text(sql), connection)
        except Exception as e:
            logger.error(f"PostgresSource: Failed to execute query '{sql}': {str(e)}")
            raise
