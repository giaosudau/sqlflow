from typing import Any, Dict, Optional

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from sqlflow.connectors.new_base.source_connector import SourceConnector


class RestApiConnector(SourceConnector):
    """
    Connector for reading data from REST APIs.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.url = self.config.get("url")
        if not self.url:
            raise ValueError("RestApiConnector: 'url' not specified in config")

        self.auth_config = self.config.get("auth")

    def _get_auth(self):
        if not self.auth_config:
            return None

        auth_type = self.auth_config.get("type")
        if auth_type == "basic":
            return HTTPBasicAuth(
                self.auth_config.get("username"), self.auth_config.get("password")
            )
        elif auth_type == "bearer":
            return {"Authorization": f"Bearer {self.auth_config.get('token')}"}
        else:
            return None

    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the REST API.
        """
        auth = self._get_auth()
        headers = {}
        if isinstance(auth, dict):
            headers.update(auth)
            auth = None

        response = requests.get(self.url, auth=auth, headers=headers)
        response.raise_for_status()
        return pd.json_normalize(response.json())
