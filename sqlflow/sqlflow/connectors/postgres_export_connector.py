"""PostgreSQL export connector for SQLFlow."""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import psycopg2
import psycopg2.extras
import psycopg2.pool
import pyarrow as pa

from sqlflow.sqlflow.connectors.base import (
    ConnectionTestResult,
    ConnectorState,
    ExportConnector,
)
from sqlflow.sqlflow.connectors.data_chunk import DataChunk
from sqlflow.sqlflow.connectors.postgres_connector import PostgresConnectionParams
from sqlflow.sqlflow.connectors.registry import register_export_connector
from sqlflow.sqlflow.core.errors import ConnectorError


@register_export_connector("POSTGRES")
class PostgresExportConnector(ExportConnector):
    """Export connector for PostgreSQL databases."""

    def __init__(self):
        """Initialize a PostgresExportConnector."""
        super().__init__()
        self.params: Optional[PostgresConnectionParams] = None
        self.connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self.batch_size: int = 1000
        self.write_mode: str = "append"  # append, overwrite, upsert
        self.upsert_keys: Optional[List[str]] = None
        self.target_table: Optional[str] = None

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters including host, port, dbname, user, password,
                   batch_size, write_mode, upsert_keys, target_table

        Raises:
            ConnectorError: If configuration fails
        """
        try:
            self.params = PostgresConnectionParams(
                host=params.get("host", ""),
                port=int(params.get("port", 5432)),
                dbname=params.get("dbname", ""),
                user=params.get("user", ""),
                password=params.get("password", ""),
                connect_timeout=int(params.get("connect_timeout", 10)),
                application_name=params.get("application_name", "sqlflow"),
                min_connections=int(params.get("min_connections", 1)),
                max_connections=int(params.get("max_connections", 5)),
            )

            if self.params is None or not self.params.host:
                raise ValueError("Host is required")

            if self.params is None or not self.params.dbname:
                raise ValueError("Database name is required")

            self.batch_size = int(params.get("batch_size", 1000))
            self.write_mode = params.get("write_mode", "append").lower()

            if self.write_mode not in ["append", "overwrite", "upsert"]:
                raise ValueError(
                    f"Invalid write mode: {self.write_mode}. "
                    "Must be one of: append, overwrite, upsert"
                )

            if self.write_mode == "upsert":
                self.upsert_keys = params.get("upsert_keys")
                if not self.upsert_keys:
                    raise ValueError("upsert_keys is required for upsert mode")

            self.target_table = params.get("target_table")
            if not self.target_table:
                raise ValueError("target_table is required")

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", f"Configuration failed: {str(e)}"
            )

    def _create_connection_pool(self) -> None:
        """Create a connection pool.

        Raises:
            ConnectorError: If connection pool creation fails
        """
        if self.params is None:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT",
                "Cannot create connection pool: not configured",
            )

        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.params.min_connections,
                maxconn=self.params.max_connections,
                host=self.params.host,
                port=self.params.port,
                dbname=self.params.dbname,
                user=self.params.user,
                password=self.params.password,
                connect_timeout=self.params.connect_timeout,
                application_name=self.params.application_name,
            )
        except Exception as e:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT",
                f"Failed to create connection pool: {str(e)}",
            )

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the PostgreSQL database.

        Returns:
            Result of the connection test
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if self.params is None:
                return ConnectionTestResult(False, "Not configured")

            conn = psycopg2.connect(
                host=self.params.host,
                port=self.params.port,
                dbname=self.params.dbname,
                user=self.params.user,
                password=self.params.password,
                connect_timeout=self.params.connect_timeout,
                application_name=self.params.application_name,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()

            self._create_connection_pool()

            self.state = ConnectorState.READY
            return ConnectionTestResult(True)
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, str(e))

    def _arrow_to_postgres_type(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to PostgreSQL type.

        Args:
            arrow_type: Arrow data type

        Returns:
            PostgreSQL data type
        """
        if pa.types.is_integer(arrow_type):
            return "BIGINT"
        elif pa.types.is_floating(arrow_type):
            return "DOUBLE PRECISION"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_date(arrow_type):
            return "DATE"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        elif pa.types.is_binary(arrow_type):
            return "BYTEA"
        elif pa.types.is_decimal(arrow_type):
            return "NUMERIC"
        else:
            return "TEXT"

    def _create_table_if_not_exists(self, conn, schema: pa.Schema) -> None:
        """Create table if it doesn't exist.

        Args:
            conn: Database connection
            schema: Arrow schema

        Raises:
            ConnectorError: If table creation fails
        """
        if self.target_table is None:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", "Target table not configured"
            )

        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
                """,
                (self.target_table,),
            )
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                columns = []
                for field in schema:
                    pg_type = self._arrow_to_postgres_type(field.type)
                    columns.append(f'"{field.name}" {pg_type}')

                if self.write_mode == "upsert" and self.upsert_keys:
                    quoted_keys = [f'"{k}"' for k in self.upsert_keys]
                    pk_constraint = f', PRIMARY KEY ({", ".join(quoted_keys)})'
                else:
                    pk_constraint = ""

                create_table_sql = f"""
                CREATE TABLE "{self.target_table}" (
                    {", ".join(columns)}{pk_constraint}
                )
                """
                cursor.execute(create_table_sql)
                conn.commit()

            cursor.close()
        except Exception as e:
            conn.rollback()
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", f"Failed to create table: {str(e)}"
            )

    def _truncate_table(self, conn) -> None:
        """Truncate the target table.

        Args:
            conn: Database connection

        Raises:
            ConnectorError: If truncation fails
        """
        if self.target_table is None:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", "Target table not configured"
            )

        try:
            cursor = conn.cursor()
            cursor.execute(f'TRUNCATE TABLE "{self.target_table}"')
            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", f"Failed to truncate table: {str(e)}"
            )

    def _write_dataframe_to_table(
        self, conn, df: pd.DataFrame, temp_table: bool = False
    ) -> None:
        """Write DataFrame to table using COPY.

        Args:
            conn: Database connection
            df: DataFrame to write
            temp_table: Whether to write to a temporary table

        Raises:
            ConnectorError: If write fails
        """
        if self.target_table is None:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", "Target table not configured"
            )

        table_name = f"temp_{self.target_table}" if temp_table else self.target_table

        try:
            columns = list(df.columns)
            column_str = ", ".join([f'"{col}"' for col in columns])

            buffer = df.to_csv(
                index=False, header=False, sep="\t", na_rep="\\N", quoting=0
            )

            cursor = conn.cursor()

            cursor.copy_expert(
                f"COPY \"{table_name}\" ({column_str}) FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '\\N'",
                buffer,
            )

            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", f"Failed to write data: {str(e)}"
            )

    def _handle_upsert(self, conn, df: pd.DataFrame) -> None:
        """Handle upsert operation using a temporary table.

        Args:
            conn: Database connection
            df: DataFrame to upsert

        Raises:
            ConnectorError: If upsert fails
        """
        if self.target_table is None or self.upsert_keys is None:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT",
                "Target table or upsert keys not configured",
            )

        try:
            cursor = conn.cursor()

            temp_table_name = f"temp_{self.target_table}"
            cursor.execute(
                f'CREATE TEMP TABLE "{temp_table_name}" (LIKE "{self.target_table}")'
            )

            self._write_dataframe_to_table(conn, df, temp_table=True)

            all_columns = list(df.columns)
            update_columns = [col for col in all_columns if col not in self.upsert_keys]
            set_clause = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_columns]
            )

            conflict_target = ", ".join([f'"{key}"' for key in self.upsert_keys])

            upsert_sql = f"""
            INSERT INTO "{self.target_table}" 
            SELECT * FROM "{temp_table_name}"
            ON CONFLICT ({conflict_target}) 
            DO UPDATE SET {set_clause}
            """
            cursor.execute(upsert_sql)

            cursor.execute(f'DROP TABLE "{temp_table_name}"')

            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", f"Failed to upsert data: {str(e)}"
            )

    def _prepare_connection(self) -> psycopg2.extensions.connection:
        """Prepare database connection.

        Returns:
            Database connection

        Raises:
            ConnectorError: If connection preparation fails
        """
        if self.connection_pool is None:
            self._create_connection_pool()

        if self.connection_pool is None:
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT",
                "Connection pool initialization failed",
            )

        return self.connection_pool.getconn()

    def _process_data_chunks(
        self,
        conn: psycopg2.extensions.connection,
        data_chunks: List[DataChunk],
        schema: pa.Schema,
    ) -> None:
        """Process data chunks according to write mode.

        Args:
            conn: Database connection
            data_chunks: List of DataChunks to process
            schema: Arrow schema

        Raises:
            ConnectorError: If processing fails
        """
        self._create_table_if_not_exists(conn, schema)

        if self.write_mode == "overwrite":
            self._truncate_table(conn)

        for chunk in data_chunks:
            df = chunk.pandas_df

            if self.write_mode == "upsert":
                self._handle_upsert(conn, df)
            else:  # append or overwrite
                self._write_dataframe_to_table(conn, df)

    def write(self, data: Union[DataChunk, List[DataChunk]]) -> None:
        """Write data to PostgreSQL table.

        Args:
            data: DataChunk or list of DataChunks to write

        Raises:
            ConnectorError: If write fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if isinstance(data, DataChunk):
                data_chunks = [data]
            else:
                data_chunks = data

            if not data_chunks:
                return

            schema = data_chunks[0].arrow_table.schema

            conn = self._prepare_connection()
            try:
                self._process_data_chunks(conn, data_chunks, schema)
                self.state = ConnectorState.READY
            finally:
                if self.connection_pool is not None:
                    self.connection_pool.putconn(conn)
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "POSTGRES_EXPORT", f"Write operation failed: {str(e)}"
            )

    def close(self) -> None:
        """Close the connection pool."""
        if self.connection_pool is not None:
            self.connection_pool.closeall()
            self.connection_pool = None
