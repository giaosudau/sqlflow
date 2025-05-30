"""Registry for SQLFlow connectors."""

import importlib
import inspect
import pkgutil
from typing import Callable, Dict, Type, TypeVar, cast

from sqlflow.connectors.base import BidirectionalConnector, Connector, ExportConnector

CONNECTOR_REGISTRY: Dict[str, Type[Connector]] = {}
EXPORT_CONNECTOR_REGISTRY: Dict[str, Type[ExportConnector]] = {}
BIDIRECTIONAL_CONNECTOR_REGISTRY: Dict[str, Type[BidirectionalConnector]] = {}

T = TypeVar("T", bound=Connector)
E = TypeVar("E", bound=ExportConnector)
B = TypeVar("B", bound=BidirectionalConnector)


def register_connector(connector_type: str) -> Callable[[Type[T]], Type[T]]:
    """Register a connector class.

    Args:
    ----
        connector_type: Type name for the connector

    Returns:
    -------
        Decorator function

    """

    def decorator(cls: Type[T]) -> Type[T]:
        if connector_type in CONNECTOR_REGISTRY:
            raise ValueError(f"Connector type '{connector_type}' already registered")
        CONNECTOR_REGISTRY[connector_type] = cls
        return cls

    return decorator


def register_export_connector(connector_type: str) -> Callable[[Type[E]], Type[E]]:
    """Register an export connector class.

    Args:
    ----
        connector_type: Type name for the export connector

    Returns:
    -------
        Decorator function

    """

    def decorator(cls: Type[E]) -> Type[E]:
        if connector_type in EXPORT_CONNECTOR_REGISTRY:
            raise ValueError(
                f"Export connector type '{connector_type}' already registered"
            )
        EXPORT_CONNECTOR_REGISTRY[connector_type] = cls
        return cls

    return decorator


def register_bidirectional_connector(
    connector_type: str,
) -> Callable[[Type[B]], Type[B]]:
    """Register a bidirectional connector class that supports both source and export operations.

    This function also automatically registers the connector in both the source and export registries
    for backward compatibility.

    Args:
    ----
        connector_type: Type name for the bidirectional connector

    Returns:
    -------
        Decorator function

    Raises:
    ------
        ValueError: If connector type is already registered or if the class
                   does not properly implement required methods

    """

    def decorator(cls: Type[B]) -> Type[B]:
        if connector_type in BIDIRECTIONAL_CONNECTOR_REGISTRY:
            raise ValueError(
                f"Bidirectional connector type '{connector_type}' already registered"
            )

        # Validate that the class properly implements both interfaces using proper method inspection
        # This ensures we check the actual class methods, not inherited ones
        class_methods = {
            name: method for name, method in inspect.getmembers(cls, inspect.isfunction)
        }

        if "read" not in class_methods:
            raise ValueError(
                f"Bidirectional connector '{connector_type}' must implement read() method"
            )

        if "write" not in class_methods:
            raise ValueError(
                f"Bidirectional connector '{connector_type}' must implement write() method"
            )

        # Validate connector type is set correctly
        if not hasattr(cls, "connector_type"):
            import logging

            logging.warning(
                f"Bidirectional connector '{connector_type}' does not explicitly set connector_type"
            )

        BIDIRECTIONAL_CONNECTOR_REGISTRY[connector_type] = cls

        # Also register in source and export registries for backward compatibility
        if connector_type not in CONNECTOR_REGISTRY:
            CONNECTOR_REGISTRY[connector_type] = cls
        if connector_type not in EXPORT_CONNECTOR_REGISTRY:
            EXPORT_CONNECTOR_REGISTRY[connector_type] = cls

        return cls

    return decorator


def get_connector_class(connector_type: str) -> Type[Connector]:
    """Get a connector class by type.

    Args:
    ----
        connector_type: Type name for the connector

    Returns:
    -------
        Connector class

    Raises:
    ------
        ValueError: If connector type is not registered

    """
    if connector_type not in CONNECTOR_REGISTRY:
        raise ValueError(f"Unknown connector type: {connector_type}")
    return cast(Type[Connector], CONNECTOR_REGISTRY[connector_type])


def get_export_connector_class(connector_type: str) -> Type[ExportConnector]:
    """Get an export connector class by type.

    Args:
    ----
        connector_type: Type name for the export connector

    Returns:
    -------
        ExportConnector class

    Raises:
    ------
        ValueError: If export connector type is not registered

    """
    if connector_type not in EXPORT_CONNECTOR_REGISTRY:
        raise ValueError(f"Unknown export connector type: {connector_type}")
    return cast(Type[ExportConnector], EXPORT_CONNECTOR_REGISTRY[connector_type])


def get_bidirectional_connector_class(
    connector_type: str,
) -> Type[BidirectionalConnector]:
    """Get a bidirectional connector class by type.

    Args:
    ----
        connector_type: Type name for the bidirectional connector

    Returns:
    -------
        BidirectionalConnector class

    Raises:
    ------
        ValueError: If bidirectional connector type is not registered

    """
    if connector_type not in BIDIRECTIONAL_CONNECTOR_REGISTRY:
        raise ValueError(f"Unknown bidirectional connector type: {connector_type}")
    return cast(
        Type[BidirectionalConnector], BIDIRECTIONAL_CONNECTOR_REGISTRY[connector_type]
    )


def discover_connectors(package_name: str = "sqlflow.connectors") -> None:
    """Discover and register connectors from submodules.

    Args:
    ----
        package_name: Package to scan for connectors

    """
    package = importlib.import_module(package_name)
    for _, name, is_pkg in pkgutil.iter_modules(package.__path__):
        if not name.startswith("_"):  # Skip private modules
            module_name = f"{package_name}.{name}"
            try:
                importlib.import_module(module_name)
                if is_pkg:
                    discover_connectors(module_name)
            except ImportError:
                pass  # Skip modules with import errors
