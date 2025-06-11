from __future__ import annotations

import io
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool, StaticPool

from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.connectors.postgres.utils import translate_postgres_parameters
from sqlflow.connectors.resilience import resilient_operation
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class PostgresDestination(DestinationConnector):
    """
    Connector for writing data to PostgreSQL with optimized performance.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Apply parameter translation for industry-standard to psycopg2 compatibility
        self.conn_params = translate_postgres_parameters(self.config)
        self._engine = None
        self._bulk_write_threshold = 10000  # Use COPY for datasets larger than this

        # Validate that we have either operational parameters or connection parameters
        has_connection_params = all(
            key in self.conn_params for key in ["user", "host", "port", "dbname"]
        )
        has_operational_params = any(
            key in self.conn_params for key in ["table_name", "table"]
        )

        if not has_connection_params and not has_operational_params:
            raise ValueError(
                "PostgresDestination: must provide either connection parameters (host, port, database/dbname, username/user, password) "
                "or operational parameters (table_name, table)"
            )

        self._configure_resilience(self.conn_params)

    def _get_optimized_pool_config(self) -> Dict[str, Any]:
        """Get optimized connection pool configuration for write operations."""
        pool_config = {
            "poolclass": QueuePool,
            "pool_size": 3,  # Smaller pool for write operations
            "max_overflow": 7,  # Additional connections under load
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
        """Get optimized connection arguments for PostgreSQL write performance."""
        connect_args = {
            "application_name": "sqlflow_destination",
            "connect_timeout": self.conn_params.get("connect_timeout", 30),
            # Optimize for write operations
            "options": "-c synchronous_commit=off -c checkpoint_completion_target=0.9",
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
                f"PostgresDestination: Created optimized engine with pool_size={pool_config['pool_size']}, "
                f"max_overflow={pool_config['max_overflow']} for postgresql://{user}:***@{host}:{port}/{dbname}"
            )
        return self._engine

    def _estimate_table_complexity(self, df: pd.DataFrame) -> str:
        """Estimate table complexity for write optimization."""
        num_cols = len(df.columns)
        num_rows = len(df)

        # Check for complex data types
        has_text_cols = any(df[col].dtype == "object" for col in df.columns)
        has_large_text = False

        if has_text_cols:
            # Sample some text columns to check for large text
            for col in df.columns:
                if df[col].dtype == "object" and not df[col].empty:
                    sample_val = (
                        str(df[col].iloc[0]) if pd.notna(df[col].iloc[0]) else ""
                    )
                    if len(sample_val) > 1000:  # Large text content
                        has_large_text = True
                        break

        # Determine complexity
        if num_cols > 50 or has_large_text:
            return "complex"
        elif num_cols > 20 or num_rows > 100000:
            return "medium"
        else:
            return "simple"

    def _get_optimal_batch_size(
        self, df: pd.DataFrame, default_batch: int = 5000
    ) -> int:
        """Calculate optimal batch size based on data characteristics."""
        complexity = self._estimate_table_complexity(df)
        num_rows = len(df)

        if complexity == "simple":
            # Simple tables can handle larger batches
            return min(20000, max(default_batch, num_rows // 4))
        elif complexity == "medium":
            # Medium complexity tables use default batch size
            return min(default_batch, max(1000, num_rows // 8))
        else:
            # Complex tables need smaller batches
            return min(2000, max(500, num_rows // 16))

    def _should_use_copy(self, df: pd.DataFrame, mode: str) -> bool:
        """Determine if COPY should be used for bulk loading."""
        # Use COPY for large datasets and replace mode
        if len(df) >= self._bulk_write_threshold and mode == "replace":
            return True

        # Use COPY for medium datasets with simple schema
        if len(df) >= 5000 and self._estimate_table_complexity(df) == "simple":
            return True

        return False

    def _create_temp_table(self, table_name: str, schema: str, df: pd.DataFrame) -> str:
        """Create optimized temporary table for bulk operations."""
        temp_table_name = f"temp_{table_name}_{id(df)}"

        with self.engine.connect() as connection:
            # Get column definitions from a sample of the DataFrame
            sample_df = df.head(1)

            # Create temporary table with same structure
            sample_df.to_sql(
                temp_table_name,
                connection,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",  # Use multi-row inserts for efficiency
            )

            # Drop the sample data
            connection.execute(text(f'DELETE FROM {schema}."{temp_table_name}"'))
            connection.commit()

        logger.debug(f"Created temporary table: {schema}.{temp_table_name}")
        return temp_table_name

    def _bulk_copy_to_table(
        self, df: pd.DataFrame, table_name: str, schema: str
    ) -> None:
        """Use PostgreSQL COPY for high-performance bulk loading."""
        try:
            # Get raw psycopg2 connection for COPY
            raw_connection = self.engine.raw_connection()
            try:
                with raw_connection.cursor() as cursor:
                    # Create a StringIO buffer with CSV data
                    buffer = io.StringIO()
                    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
                    buffer.seek(0)

                    # Use COPY to bulk load data
                    copy_sql = f"COPY {schema}.\"{table_name}\" FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                    cursor.copy_expert(copy_sql, buffer)

                raw_connection.commit()
                logger.info(
                    f"Successfully bulk loaded {len(df)} rows using COPY to {schema}.{table_name}"
                )

            finally:
                raw_connection.close()

        except Exception as e:
            logger.error(f"PostgresDestination: COPY operation failed: {str(e)}")
            raise

    def _write_with_upsert(
        self, df: pd.DataFrame, table_name: str, schema: str, keys: List[str]
    ) -> None:
        """Implement optimized upsert using temporary table strategy."""
        temp_table = self._create_temp_table(table_name, schema, df)

        try:
            # Bulk load to temporary table
            self._bulk_copy_to_table(df, temp_table, schema)

            # Perform upsert using temporary table
            key_conditions = " AND ".join([f't."{key}" = s."{key}"' for key in keys])
            update_columns = [col for col in df.columns if col not in keys]
            update_sets = ", ".join([f'"{col}" = s."{col}"' for col in update_columns])

            upsert_sql = f"""
            INSERT INTO {schema}."{table_name}" 
            SELECT * FROM {schema}."{temp_table}" s
            ON CONFLICT ({", ".join([f'"{key}"' for key in keys])}) 
            DO UPDATE SET {update_sets}
            """

            with self.engine.connect() as connection:
                connection.execute(text(upsert_sql))
                connection.commit()

            logger.info(
                f"Successfully upserted {len(df)} rows to {schema}.{table_name}"
            )

        finally:
            # Clean up temporary table
            with self.engine.connect() as connection:
                connection.execute(
                    text(f'DROP TABLE IF EXISTS {schema}."{temp_table}"')
                )
                connection.commit()

    def _write_chunked(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: str,
        batch_size: int,
    ) -> None:
        """Write data in optimized chunks."""
        total_rows = len(df)

        for i in range(0, total_rows, batch_size):
            chunk_df = df.iloc[i : i + batch_size]
            chunk_if_exists = "append" if i > 0 else if_exists

            logger.debug(
                f"Writing chunk {i//batch_size + 1}: rows {i+1}-{min(i+batch_size, total_rows)} of {total_rows}"
            )

            with self.engine.connect() as connection:
                chunk_df.to_sql(
                    table_name,
                    connection,
                    schema=schema,
                    if_exists=chunk_if_exists,
                    index=False,
                    method="multi",  # Use multi-row inserts for efficiency
                )

    @resilient_operation()
    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "replace",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the PostgreSQL database with optimized performance.
        """
        write_options = options or {}
        # Try to get table_name from options first, then from constructor config
        table_name = (
            write_options.get("table_name")
            or self.config.get("table_name")
            or self.config.get("table")
        )
        schema = write_options.get("schema", "public")

        if not table_name:
            raise ValueError(
                "PostgresDestination: 'table_name' (or 'table') not specified in options or config"
            )

        # Determine optimal write strategy
        total_rows = len(df)
        optimal_batch_size = self._get_optimal_batch_size(df)

        logger.info(
            f"Writing {total_rows} rows to {schema}.{table_name} (mode: {mode}, "
            f"optimal_batch_size: {optimal_batch_size})"
        )

        try:
            if mode == "upsert" and keys:
                # Use optimized upsert strategy
                self._write_with_upsert(df, table_name, schema, keys)
            elif self._should_use_copy(df, mode):
                # Use COPY for bulk loading
                if mode == "replace":
                    # First truncate the table
                    with self.engine.connect() as connection:
                        connection.execute(
                            text(f'TRUNCATE TABLE {schema}."{table_name}"')
                        )
                        connection.commit()

                self._bulk_copy_to_table(df, table_name, schema)
            else:
                # Use chunked writing for smaller datasets or complex schemas
                if total_rows > optimal_batch_size:
                    self._write_chunked(
                        df, table_name, schema, mode, optimal_batch_size
                    )
                else:
                    # Direct write for small datasets
                    with self.engine.connect() as connection:
                        df.to_sql(
                            table_name,
                            connection,
                            schema=schema,
                            if_exists=mode,
                            index=False,
                            method="multi",  # Use multi-row inserts for efficiency
                        )

        except Exception as e:
            logger.error(
                f"PostgresDestination: Failed to write data to {schema}.{table_name}: {str(e)}"
            )
            raise
