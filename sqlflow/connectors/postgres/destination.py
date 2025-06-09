from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from sqlflow.connectors.base.destination_connector import DestinationConnector

logger = logging.getLogger(__name__)


class PostgresDestination(DestinationConnector):
    """
    Connector for writing data to PostgreSQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn_params = self.config
        self._engine = None

        # Validate that we have either operational parameters or connection parameters
        has_connection_params = all(
            key in config for key in ["user", "host", "port", "dbname"]
        )
        has_operational_params = any(key in config for key in ["table_name"])

        if not has_connection_params and not has_operational_params:
            raise ValueError(
                "PostgresDestination: must provide either connection parameters or operational parameters"
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

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to the PostgreSQL database.
        """
        write_options = options or {}
        # Try to get table_name from options first, then from constructor config
        table_name = write_options.get("table_name") or self.config.get("table_name")
        schema = write_options.get("schema", "public")
        if_exists = write_options.get("if_exists", "replace")

        if not table_name:
            raise ValueError(
                "PostgresDestination: 'table_name' not specified in options or config"
            )

        with self.engine.connect() as connection:
            df.to_sql(
                table_name,
                connection,
                schema=schema,
                if_exists=if_exists,
                index=False,
            )
