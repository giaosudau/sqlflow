from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from sqlflow.connectors.base.source_connector import SourceConnector

logger = logging.getLogger(__name__)


class PostgresSource(SourceConnector):
    """
    Connector for reading data from PostgreSQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn_params = self.config
        self._engine = None

        # Validate that we have either operational parameters or connection parameters
        has_connection_params = all(
            key in config for key in ["user", "host", "port", "dbname"]
        )
        has_operational_params = any(key in config for key in ["query", "table_name"])

        if not has_connection_params and not has_operational_params:
            raise ValueError(
                "PostgresSource: must provide either connection parameters or operational parameters"
            )

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
            self._engine = create_engine(conn_uri)
        return self._engine

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the PostgreSQL database.
        """
        read_options = options or {}
        # Try to get query/table_name from options first, then from constructor config
        query = read_options.get("query") or self.config.get("query")
        table_name = read_options.get("table_name") or self.config.get("table_name")
        schema = read_options.get("schema", "public")

        if not query and not table_name:
            raise ValueError(
                "PostgresSource: either 'query' or 'table_name' must be specified in options or config"
            )

        if query:
            sql = query
        else:
            sql = f'SELECT * FROM {schema}."{table_name}"'

        logger.info(f"Executing query: {sql}")
        with self.engine.connect() as connection:
            return pd.read_sql_query(sql, connection)

    def get_tables(self, schema: str = "public") -> List[str]:
        with self.engine.connect() as connection:
            return pd.read_sql_query(
                f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                """,
                connection,
            )["table_name"].tolist()
