# This file is intentionally left blank.


class ConnectorError(Exception):
    """Base exception for connector-related errors."""

    def __init__(self, connector_name: str, message: str):
        self.connector_name = connector_name
        self.message = message
        super().__init__(f"[{connector_name}] {message}")


class ParameterError(ConnectorError):
    """Parameter validation errors."""

    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Parameter error: {message}")


class IncrementalError(ConnectorError):
    """Incremental loading errors."""

    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Incremental error: {message}")


class HealthCheckError(ConnectorError):
    """Health check errors."""

    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Health check error: {message}")
