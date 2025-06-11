from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool, StaticPool

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.postgres.utils import translate_postgres_parameters
from sqlflow.connectors.resilience import resilient_operation
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class PostgresSource(Connector):
    """
    Connector for reading data from PostgreSQL with optimized resilience patterns and performance.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        self._engine = None
        self.conn_params = {}
        self._query_strategy_cache = {}

        if config is not None:
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters and performance optimizations."""
        self.conn_params = translate_postgres_parameters(params)

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

    def _get_optimized_pool_config(self) -> Dict[str, Any]:
        """Get optimized connection pool configuration based on usage patterns."""
        # Default pool configuration for PostgreSQL
        pool_config = {
            "poolclass": QueuePool,
            "pool_size": 5,  # Base connections
            "max_overflow": 10,  # Additional connections under load
            "pool_recycle": 3600,  # Recycle connections every hour
            "pool_pre_ping": True,  # Validate connections before use
            "pool_timeout": 30,  # Timeout for getting connection from pool
        }

        # Override with user-provided values
        if "pool_size" in self.conn_params:
            pool_config["pool_size"] = self.conn_params["pool_size"]
        if "max_overflow" in self.conn_params:
            pool_config["max_overflow"] = self.conn_params["max_overflow"]
        if "pool_timeout" in self.conn_params:
            pool_config["pool_timeout"] = self.conn_params["pool_timeout"]

        # For testing environments, use smaller pools
        if self.conn_params.get("dbname") in ["testdb", "test"]:
            pool_config.update(
                {"poolclass": StaticPool, "pool_size": 1, "max_overflow": 2}
            )

        return pool_config

    def _get_optimized_connect_args(self) -> Dict[str, Any]:
        """Get optimized connection arguments for PostgreSQL performance."""
        connect_args = {
            "application_name": "sqlflow",
            "connect_timeout": self.conn_params.get("connect_timeout", 30),
        }

        # Add SSL configuration if provided
        if "sslmode" in self.conn_params:
            connect_args["sslmode"] = self.conn_params["sslmode"]

        return connect_args

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

            # Get optimized configurations
            pool_config = self._get_optimized_pool_config()
            connect_args = self._get_optimized_connect_args()

            self._engine = create_engine(
                conn_uri,
                connect_args=connect_args,
                **pool_config,
                echo=False,  # Disable SQL logging for performance
            )
            logger.debug(
                f"PostgresSource: Created optimized engine with pool_size={pool_config['pool_size']}, "
                f"max_overflow={pool_config['max_overflow']} for postgresql://{user}:***@{host}:{port}/{dbname}"
            )
        return self._engine

    def _estimate_query_complexity(self, sql: str) -> str:
        """Estimate query complexity to determine optimal execution strategy."""
        sql_lower = sql.lower()

        # Cache result if we've seen this query before
        cache_key = sql_lower[:100]  # Use first 100 chars as cache key
        if cache_key in self._query_strategy_cache:
            return self._query_strategy_cache[cache_key]

        strategy = "medium"  # Default

        # Simple heuristics for complexity
        complexity_indicators = [
            ("join", 1),
            ("group by", 1),
            ("order by", 1),
            ("having", 1),
            ("window", 2),
            ("cte", 2),
            ("union", 1),
            ("subquery", 1),
            ("recursive", 3),
        ]

        complexity_score = 0
        for indicator, weight in complexity_indicators:
            if indicator in sql_lower:
                complexity_score += weight

        # Determine strategy based on complexity
        if complexity_score == 0:
            strategy = "simple"
        elif complexity_score <= 2:
            strategy = "medium"
        else:
            strategy = "complex"

        # Cache the result
        self._query_strategy_cache[cache_key] = strategy
        return strategy

    def _get_optimal_batch_size(self, sql: str, base_batch_size: int = 10000) -> int:
        """Calculate optimal batch size based on query complexity and system resources."""
        complexity = self._estimate_query_complexity(sql)

        # Adjust batch size based on complexity
        if complexity == "simple":
            # Simple selects can handle larger batches
            return min(50000, base_batch_size * 5)
        elif complexity == "medium":
            # Standard batch size for moderate complexity
            return base_batch_size
        else:
            # Complex queries need smaller batches to avoid memory issues
            return max(1000, base_batch_size // 5)

    def _use_server_side_cursor(
        self, sql: str, estimated_rows: Optional[int] = None
    ) -> bool:
        """Determine if server-side cursor should be used for this query."""
        complexity = self._estimate_query_complexity(sql)

        # Use server-side cursor for:
        # 1. Complex queries (always)
        # 2. When estimated rows > 100,000 (explicit threshold)
        # 3. Queries with explicit large result indicators (but not simple SELECT *)

        if complexity == "complex":
            return True

        if estimated_rows and estimated_rows > 100000:
            return True

        # For test environments, be conservative with server-side cursors
        # since test tables are usually small
        if self.conn_params.get("dbname") in ["testdb", "test", "postgres"]:
            # Only use server-side cursors for explicitly large operations in test environments
            sql_lower = sql.lower()
            explicit_large_indicators = [
                "count(*)",
                "sum(",
                "avg(",
                "full outer join",
                "cross join",
                "group by",  # Aggregations often mean larger processing
            ]
            return any(
                indicator in sql_lower for indicator in explicit_large_indicators
            )

        # For production environments, be more aggressive but still intelligent
        sql_lower = sql.lower()

        # Check for indicators that suggest large result sets
        # But avoid simple SELECT * from small tables
        large_result_indicators = [
            "full outer join",
            "cross join",
            "union all",  # More specific than just "union"
        ]

        # Check for aggregation functions that might process lots of data
        aggregation_indicators = [
            "count(*)",
            "sum(",
            "avg(",
            "max(",
            "min(",
            "group by",
            "having",
        ]

        return any(
            indicator in sql_lower for indicator in large_result_indicators
        ) or any(indicator in sql_lower for indicator in aggregation_indicators)

    @resilient_operation()
    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to PostgreSQL with resilience."""
        try:
            with self.engine.connect() as connection:
                # Simple test query - wrap in text() for SQLAlchemy 2.x compatibility
                result = connection.execute(text("SELECT 1")).fetchone()
                if result and result[0] == 1:
                    # Also test pool status
                    pool = self.engine.pool
                    return ConnectionTestResult(
                        success=True,
                        message=f"Successfully connected to PostgreSQL database '{self.conn_params.get('dbname')}' "
                        f"(pool: {pool.checkedin()}/{pool.size()} connections available)",
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

    def _fetch_cursor_data(self, cursor, batch_size: int) -> Iterator[DataChunk]:
        """Fetch data from cursor and yield as DataChunks."""
        # For server-side cursors, description may not be available until after first fetch
        # Try to get the first batch to initialize description
        first_batch = cursor.fetchmany(batch_size)
        if not first_batch:
            # Empty result set
            return

        # Now we should have description available
        if cursor.description is None:
            raise ValueError("Cursor description not available after first fetch")

        columns = [desc[0] for desc in cursor.description]

        # Yield the first batch
        df = pd.DataFrame(first_batch, columns=columns)
        yield DataChunk(pa.Table.from_pandas(df))

        # Continue with remaining batches
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            # Convert to DataFrame and then PyArrow
            df = pd.DataFrame(rows, columns=columns)
            yield DataChunk(pa.Table.from_pandas(df))

    def _execute_query_with_cursor(
        self, sql: str, batch_size: int
    ) -> Iterator[DataChunk]:
        """Execute SQL query using server-side cursor for large result sets."""
        try:
            # Use raw psycopg2 connection for server-side cursor
            raw_connection = self.engine.raw_connection()
            try:
                with raw_connection.cursor(name="sqlflow_cursor") as cursor:
                    logger.debug(
                        f"Executing query with server-side cursor, batch_size={batch_size}"
                    )
                    cursor.execute(sql)
                    yield from self._fetch_cursor_data(cursor, batch_size)

            finally:
                raw_connection.close()

        except Exception as e:
            logger.error(
                f"PostgresSource: Failed to execute query with cursor '{sql}': {str(e)}"
            )
            # Fallback to pandas reading when server-side cursor fails
            logger.info("Falling back to pandas chunked reading")
            try:
                with self.engine.connect() as connection:
                    # Convert text() object to string for pandas compatibility
                    sql_text = str(sql) if hasattr(sql, "compile") else sql
                    chunk_iter = pd.read_sql_query(
                        sql_text, connection, chunksize=batch_size
                    )

                    for chunk_df in chunk_iter:
                        yield DataChunk(pa.Table.from_pandas(chunk_df))
            except Exception as fallback_error:
                logger.error(
                    f"PostgresSource: Fallback pandas reading also failed for '{sql}': {str(fallback_error)}"
                )
                raise fallback_error

    def _execute_query_in_chunks(
        self, sql: str, batch_size: int
    ) -> Iterator[DataChunk]:
        """Execute SQL query and yield results in chunks with optimization."""
        # Determine if we should use server-side cursor
        use_cursor = self._use_server_side_cursor(sql)

        if use_cursor:
            logger.debug("Using server-side cursor for large/complex query")
            yield from self._execute_query_with_cursor(sql, batch_size)
        else:
            # Use pandas chunked reading for simpler queries
            try:
                with self.engine.connect() as connection:
                    chunk_iter = pd.read_sql_query(
                        text(sql), connection, chunksize=batch_size
                    )

                    for chunk_df in chunk_iter:
                        yield DataChunk(pa.Table.from_pandas(chunk_df))
            except Exception as e:
                logger.error(
                    f"PostgresSource: Failed to execute query '{sql}': {str(e)}"
                )
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
        """Read data from PostgreSQL in chunks with optimization and resilience.

        Supports both new interface and legacy options-based interface.

        Args:
            object_name: Name of the table or view to read from
            columns: Optional list of columns to select
            filters: Optional dictionary of column filters
            batch_size: Number of rows to read per chunk (will be optimized automatically)
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

        # Build and execute query with optimization
        sql = self._build_sql_query(object_name, columns, filters)

        # Optimize batch size based on query complexity
        optimized_batch_size = self._get_optimal_batch_size(sql, batch_size)

        logger.info(
            f"Executing optimized query: {sql} (batch_size: {optimized_batch_size})"
        )
        yield from self._execute_query_in_chunks(sql, optimized_batch_size)

    def _read_with_options(
        self, options: Dict[str, Any], batch_size: int = 10000
    ) -> Iterator[DataChunk]:
        """Read data using legacy options format with optimization (for backward compatibility)."""
        # Extract query or table information
        query = options.get("query")
        table_name = options.get("table_name") or options.get("table")

        if query:
            # Direct query execution with optimization
            optimized_batch_size = self._get_optimal_batch_size(query, batch_size)
            logger.info(
                f"Executing optimized query: {query} (batch_size: {optimized_batch_size})"
            )
            yield from self._execute_query_in_chunks(query, optimized_batch_size)
        elif table_name:
            # Table-based read with optimization
            schema = options.get("schema", "public")
            sql = f'SELECT * FROM {schema}."{table_name}"'
            optimized_batch_size = self._get_optimal_batch_size(sql, batch_size)
            logger.info(
                f"Executing optimized table read: {sql} (batch_size: {optimized_batch_size})"
            )
            yield from self._execute_query_in_chunks(sql, optimized_batch_size)
        else:
            # Fallback to legacy method for full backward compatibility
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
        """Read data incrementally using a cursor field with optimization."""
        # Build incremental query
        filters = kwargs.get("filters", {})
        if cursor_value is not None:
            filters[cursor_field] = f"> '{cursor_value}'"

        # Use the resilient read method with optimization
        yield from self.read(
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

        logger.info(f"Executing legacy query: {sql}")
        try:
            with self.engine.connect() as connection:
                return pd.read_sql_query(text(sql), connection)
        except Exception as e:
            logger.error(f"PostgresSource: Failed to execute query '{sql}': {str(e)}")
            raise
