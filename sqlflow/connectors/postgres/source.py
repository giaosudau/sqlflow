import logging
from typing import Any, Dict, Optional

import pandas as pd
import psycopg2

from sqlflow.connectors.base.source_connector import SourceConnector

logger = logging.getLogger(__name__)


class PostgresSource(SourceConnector):
    """
    Connector for reading data from PostgreSQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn_params = self.config
        self.query = self.config.get("query")
        if not self.query:
            raise ValueError("PostgresSource: 'query' not specified in config")

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the PostgreSQL database.
        """
        with psycopg2.connect(**self.conn_params) as conn:
            return pd.read_sql_query(self.query, conn)
