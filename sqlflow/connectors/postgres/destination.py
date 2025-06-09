import logging
from typing import Any, Dict, Optional

import pandas as pd
import psycopg2

from sqlflow.connectors.base.destination_connector import DestinationConnector

logger = logging.getLogger(__name__)


class PostgresDestination(DestinationConnector):
    """
    Connector for writing data to PostgreSQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn_params = self.config
        self.table_name = self.config.get("table_name")
        if not self.table_name:
            raise ValueError(
                "PostgresDestination: 'table_name' not specified in config"
            )

    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to the PostgreSQL database.
        """
        with psycopg2.connect(**self.conn_params) as conn:
            # A simplistic approach to writing data.
            # In a real-world scenario, you'd want to handle this more robustly.
            df.to_sql(
                self.table_name,
                conn,
                if_exists="replace",
                index=False,
            )
