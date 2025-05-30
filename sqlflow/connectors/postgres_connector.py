"""PostgreSQL connector for SQLFlow."""

import dataclasses
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
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector
from sqlflow.core.errors import ConnectorError


@dataclasses.dataclass
class PostgresConnectionParams:
    """PostgreSQL connection parameters."""

    host: str
    port: int = 5432
    dbname: str = ""
    user: str = ""
    password: str = ""
    connect_timeout: int = 10
    application_name: str = "sqlflow"
    min_connections: int = 1
    max_connections: int = 5


@register_connector("POSTGRES")
class PostgresConnector(Connector):
    """Connector for PostgreSQL databases."""

    def __init__(self):
        """Initialize a PostgresConnector."""
        super().__init__()
        self.params: Optional[PostgresConnectionParams] = None
        self.connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
        ----
            params: Configuration parameters including host, port, dbname, user, password

        Raises:
        ------
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

            if not self.params.host:
                raise ValueError("Host is required")

            if not self.params.dbname:
                raise ValueError("Database name is required")

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "POSTGRES", f"Configuration failed: {str(e)}"
            )

    def _create_connection_pool(self) -> None:
        """Create a connection pool.

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
                dbname=self.params.dbname,
                user=self.params.user,
                password=self.params.password,
                connect_timeout=self.params.connect_timeout,
                application_name=self.params.application_name,
            )
        except Exception as e:
            raise ConnectorError(
                self.name or "POSTGRES",
                f"Failed to create connection pool: {str(e)}",
            )

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the PostgreSQL database.

        Returns
        -------
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

    def discover(self) -> List[str]:
        """Discover available tables in the database.

        Returns
        -------
            List of table names

        Raises
        ------
            ConnectorError: If discovery fails

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
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                    """
                )
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()

                self.state = ConnectorState.READY
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
                    table_schema = 'public' AND 
                    table_name = %s
                ORDER BY 
                    ordinal_position
                """,
                (object_name,),
            )
            columns = cursor.fetchall()

            if not columns:
                raise ValueError(f"Table not found: {object_name}")

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
