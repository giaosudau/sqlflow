"""REST export connector for SQLFlow."""

import json
import time
from enum import Enum
from typing import Any, Dict, Optional

import requests
from requests.auth import AuthBase, HTTPBasicAuth

from sqlflow.connectors.base import (
    ConnectionTestResult,
    ConnectorState,
    ExportConnector,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_export_connector
from sqlflow.core.errors import ConnectorError


class AuthMethod(Enum):
    """Authentication methods supported by the REST connector."""

    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    API_KEY = "api_key"
    OAUTH = "oauth"


@register_export_connector("REST")
class RESTExportConnector(ExportConnector):
    """Export connector for REST APIs."""

    def __init__(self):
        """Initialize a RESTExportConnector."""
        super().__init__()
        self.base_url: Optional[str] = None
        self.auth_method: AuthMethod = AuthMethod.NONE
        self.auth_params: Dict[str, str] = {}
        self.headers: Dict[str, str] = {}
        self.timeout: int = 30
        self.max_retries: int = 3
        self.retry_delay: int = 1
        self.batch_size: int = 100
        self.content_type: str = "application/json"
        self.request_params: Dict[str, Any] = {}
        self.streaming_mode: bool = False

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters including base_url, auth_method,
                   auth_params, headers, timeout, etc.

        Raises:
            ConnectorError: If configuration fails
        """
        try:
            self.base_url = params.get("base_url")
            if not self.base_url:
                raise ValueError("Base URL is required")

            auth_method_str = params.get("auth_method", "none").lower()
            try:
                self.auth_method = AuthMethod(auth_method_str)
            except ValueError:
                raise ValueError(
                    f"Invalid auth method: {auth_method_str}. "
                    f"Must be one of: {', '.join([m.value for m in AuthMethod])}"
                )

            self.auth_params = params.get("auth_params", {})
            self.headers = params.get("headers", {})
            self.timeout = int(params.get("timeout", 30))
            self.max_retries = int(params.get("max_retries", 3))
            self.retry_delay = int(params.get("retry_delay", 1))
            self.batch_size = int(params.get("batch_size", 100))
            self.content_type = params.get("content_type", "application/json")
            self.request_params = params.get("request_params", {})
            self.streaming_mode = params.get("streaming_mode", False)

            if "Content-Type" not in self.headers:
                self.headers["Content-Type"] = self.content_type

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "REST_EXPORT", f"Configuration failed: {str(e)}"
            )

    def _get_auth(self) -> Optional[AuthBase]:
        """Get authentication handler based on configured method.

        Returns:
            Authentication handler for requests or None if no auth is needed
        """
        if self.auth_method == AuthMethod.NONE:
            return None
        elif self.auth_method == AuthMethod.BASIC:
            username = self.auth_params.get("username", "")
            password = self.auth_params.get("password", "")
            return HTTPBasicAuth(username, password)
        elif self.auth_method == AuthMethod.BEARER:
            token = self.auth_params.get("token", "")
            self.headers["Authorization"] = f"Bearer {token}"
            return None
        elif self.auth_method == AuthMethod.API_KEY:
            key_name = self.auth_params.get("key_name", "api_key")
            key_value = self.auth_params.get("key_value", "")
            location = self.auth_params.get("location", "header").lower()

            if location == "header":
                self.headers[key_name] = key_value
            elif location == "query":
                if "params" not in self.request_params:
                    self.request_params["params"] = {}
                self.request_params["params"][key_name] = key_value
            return None
        elif self.auth_method == AuthMethod.OAUTH:
            token = self.auth_params.get("token", "")
            token_type = self.auth_params.get("token_type", "Bearer")
            self.headers["Authorization"] = f"{token_type} {token}"
            return None
        return None

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to the REST API.

        Returns:
            Result of the connection test
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.base_url:
                return ConnectionTestResult(False, "Base URL not configured")

            auth = self._get_auth()
            response = requests.get(
                self.base_url,
                headers=self.headers,
                auth=auth,
                timeout=self.timeout,
                **self.request_params,
            )

            if response.status_code < 400:
                self.state = ConnectorState.READY
                return ConnectionTestResult(True)
            else:
                self.state = ConnectorState.ERROR
                return ConnectionTestResult(
                    False, f"HTTP error: {response.status_code} - {response.text}"
                )
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, str(e))

    def _send_batch(self, url: str, data: Any) -> requests.Response:
        """Send a batch of data to the REST API with retries.

        Args:
            url: URL to send data to
            data: Data to send

        Returns:
            Response from the API

        Raises:
            ConnectorError: If sending data fails after all retries
        """
        auth = self._get_auth()
        payload = json.dumps(data)

        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    url,
                    data=payload,
                    headers=self.headers,
                    auth=auth,
                    timeout=self.timeout,
                    **self.request_params,
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise ConnectorError(
                        self.name or "REST_EXPORT", f"Failed to send data: {str(e)}"
                    )
                time.sleep(self.retry_delay * (2**attempt))  # Exponential backoff

        raise ConnectorError(self.name or "REST_EXPORT", "Failed to send data")

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to the REST API.

        Args:
            object_name: Endpoint path to append to base_url
            data_chunk: Data to write
            mode: Write mode (ignored for REST)

        Raises:
            ConnectorError: If write fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.base_url:
                raise ConnectorError(
                    self.name or "REST_EXPORT", "Base URL not configured"
                )

            url = self.base_url
            if object_name:
                if url.endswith("/") and object_name.startswith("/"):
                    url += object_name[1:]
                elif not url.endswith("/") and not object_name.startswith("/"):
                    url += "/" + object_name
                else:
                    url += object_name

            df = data_chunk.pandas_df
            records = df.to_dict(orient="records")

            if self.streaming_mode:
                for record in records:
                    self._send_batch(url, record)
            else:
                for i in range(0, len(records), self.batch_size):
                    batch = records[i : i + self.batch_size]
                    self._send_batch(url, batch)

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "REST_EXPORT", f"Write operation failed: {str(e)}"
            )

    def close(self) -> None:
        """Close resources used by the connector."""
