"""Protocol definitions for SQLFlow components."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class WriterProtocol(Protocol):
    """Protocol for data writers."""

    @abstractmethod
    def write(self, data: Any, destination: str, options: Optional[Dict[str, Any]] = None) -> None:
        """Write data to a destination.

        Args:
            data: Data to write
            destination: Destination to write to
            options: Options for the writer
        """
        pass


@runtime_checkable
class ExecutorProtocol(Protocol):
    """Protocol for pipeline executors."""

    @abstractmethod
    def execute(self, plan: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a pipeline plan.

        Args:
            plan: List of operations to execute

        Returns:
            Dict containing execution results
        """
        pass


@runtime_checkable
class StorageManagerProtocol(Protocol):
    """Protocol for storage managers."""

    @abstractmethod
    def store(self, key: str, data: Any) -> None:
        """Store data with a key.

        Args:
            key: Key to store data under
            data: Data to store
        """
        pass

    @abstractmethod
    def retrieve(self, key: str) -> Any:
        """Retrieve data by key.

        Args:
            key: Key to retrieve data for

        Returns:
            Retrieved data
        """
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists.

        Args:
            key: Key to check

        Returns:
            True if the key exists, False otherwise
        """
        pass
