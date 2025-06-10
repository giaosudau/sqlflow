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
        # Apply parameter translation for industry-standard to psycopg2 compatibility
        self.conn_params = self._translate_parameters(self.config)
        self._engine = None

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
        table_name = (
            read_options.get("table_name")
            or self.config.get("table_name")
            or self.config.get("table")
        )
        schema = read_options.get("schema", "public")

        if not query and not table_name:
            raise ValueError(
                "PostgresSource: either 'query' or 'table_name' (or 'table') must be specified in options or config"
            )

        if query:
            sql = query
        else:
            sql = f'SELECT * FROM {schema}."{table_name}"'

        logger.info(f"Executing query: {sql}")
        try:
            with self.engine.connect() as connection:
                return pd.read_sql_query(sql, connection)
        except Exception as e:
            logger.error(f"PostgresSource: Failed to execute query '{sql}': {str(e)}")
            raise

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
