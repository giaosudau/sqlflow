"""Data registration management for DuckDB engine."""

from typing import Any, Dict

from sqlflow.logging import get_logger

from .handlers import DataHandlerFactory

logger = get_logger(__name__)


class DataRegistrationManager:
    """Manages data registration with the DuckDB engine."""

    def __init__(self, connection: Any):
        """Initialize the data registration manager.

        Args:
            connection: DuckDB connection
        """
        self.connection = connection
        self.registered_data: Dict[str, Any] = {}

    def register(self, name: str, data: Any) -> None:
        """Register data with the engine.

        Args:
            name: Name to register the data as
            data: Data to register
        """
        logger.debug(f"Registering data: {name}")

        handler = DataHandlerFactory.create(data)
        handler.register(name, data, self.connection)

        self.registered_data[name] = data
        logger.debug(f"Successfully registered data: {name}")

    def unregister(self, name: str) -> None:
        """Unregister data from the engine.

        Args:
            name: Name of the data to unregister
        """
        if name in self.registered_data:
            del self.registered_data[name]
            logger.debug(f"Unregistered data: {name}")

    def is_registered(self, name: str) -> bool:
        """Check if data is registered.

        Args:
            name: Name of the data

        Returns:
            True if data is registered
        """
        return name in self.registered_data

    def get_registered_names(self) -> list[str]:
        """Get names of all registered data.

        Returns:
            List of registered data names
        """
        return list(self.registered_data.keys())
