"""Base storage manager for SQLFlow."""

from abc import ABC, abstractmethod
from typing import Any, Dict


class StorageManagerProtocol(ABC):
    """Protocol for storage managers."""

    @abstractmethod
    def store(self, key: str, data: Any) -> None:
        """Store data with a key.

        Args:
        ----
            key: Key to store data under
            data: Data to store

        """

    @abstractmethod
    def retrieve(self, key: str) -> Any:
        """Retrieve data by key.

        Args:
        ----
            key: Key to retrieve data for

        Returns:
        -------
            Retrieved data

        """

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists.

        Args:
        ----
            key: Key to check

        Returns:
        -------
            True if the key exists, False otherwise

        """


class MemoryStorageManager(StorageManagerProtocol):
    """In-memory storage manager."""

    def __init__(self):
        """Initialize a MemoryStorageManager."""
        self.storage: Dict[str, Any] = {}

    def store(self, key: str, data: Any) -> None:
        """Store data with a key.

        Args:
        ----
            key: Key to store data under
            data: Data to store

        """
        self.storage[key] = data

    def retrieve(self, key: str) -> Any:
        """Retrieve data by key.

        Args:
        ----
            key: Key to retrieve data for

        Returns:
        -------
            Retrieved data

        Raises:
        ------
            KeyError: If the key does not exist

        """
        if key not in self.storage:
            raise KeyError(f"Key '{key}' not found in storage")
        return self.storage[key]

    def exists(self, key: str) -> bool:
        """Check if a key exists.

        Args:
        ----
            key: Key to check

        Returns:
        -------
            True if the key exists, False otherwise

        """
        return key in self.storage
