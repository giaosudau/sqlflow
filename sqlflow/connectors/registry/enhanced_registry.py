"""Enhanced connector registry with profile support and configuration management.

This module provides an enhanced connector registry that supports:
- Profile-based connector creation
- Default configuration management
- Type validation for connector parameters
- Integration with ProfileManager for configuration resolution
"""

import inspect
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, Union

from sqlflow.connectors.base.connector import Connector
from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConnectorTypeInfo:
    """Information about a registered connector type."""

    connector_class: Type[Union[Connector, DestinationConnector]]
    connector_type: str
    default_config: Dict[str, Any] = field(default_factory=dict)
    required_params: List[str] = field(default_factory=list)
    optional_params: Dict[str, Any] = field(default_factory=dict)
    param_types: Dict[str, type] = field(default_factory=dict)
    description: str = ""


@dataclass
class ConfigurationResolutionResult:
    """Result of configuration resolution process."""

    resolved_config: Dict[str, Any]
    profile_used: Optional[str] = None
    overridden_params: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)


class EnhancedConnectorRegistry:
    """Enhanced connector registry with profile support and configuration management."""

    def __init__(self):
        self._source_connectors: Dict[str, ConnectorTypeInfo] = {}
        self._destination_connectors: Dict[str, ConnectorTypeInfo] = {}

    def register_source(
        self,
        connector_type: str,
        connector_class: Type[Connector],
        default_config: Optional[Dict[str, Any]] = None,
        required_params: Optional[List[str]] = None,
        optional_params: Optional[Dict[str, Any]] = None,
        description: str = "",
    ) -> None:
        """Register a source connector with enhanced metadata.

        Args:
            connector_type: Type identifier for the connector
            connector_class: Connector class to register
            default_config: Default configuration parameters
            required_params: List of required parameter names
            optional_params: Dictionary of optional parameters with defaults
            description: Human-readable description of the connector
        """
        logger.debug(f"Registering source connector: {connector_type}")

        # Extract parameter information from connector class
        param_types = self._extract_parameter_types(connector_class)

        connector_info = ConnectorTypeInfo(
            connector_class=connector_class,
            connector_type=connector_type,
            default_config=default_config or {},
            required_params=required_params or [],
            optional_params=optional_params or {},
            param_types=param_types,
            description=description,
        )

        self._source_connectors[connector_type] = connector_info
        logger.debug(f"Successfully registered source connector: {connector_type}")

    def register_destination(
        self,
        connector_type: str,
        connector_class: Type[DestinationConnector],
        default_config: Optional[Dict[str, Any]] = None,
        required_params: Optional[List[str]] = None,
        optional_params: Optional[Dict[str, Any]] = None,
        description: str = "",
    ) -> None:
        """Register a destination connector with enhanced metadata.

        Args:
            connector_type: Type identifier for the connector
            connector_class: Connector class to register
            default_config: Default configuration parameters
            required_params: List of required parameter names
            optional_params: Dictionary of optional parameters with defaults
            description: Human-readable description of the connector
        """
        logger.debug(f"Registering destination connector: {connector_type}")

        # Extract parameter information from connector class
        param_types = self._extract_parameter_types(connector_class)

        connector_info = ConnectorTypeInfo(
            connector_class=connector_class,
            connector_type=connector_type,
            default_config=default_config or {},
            required_params=required_params or [],
            optional_params=optional_params or {},
            param_types=param_types,
            description=description,
        )

        self._destination_connectors[connector_type] = connector_info
        logger.debug(f"Successfully registered destination connector: {connector_type}")

    def get_source_connector_class(self, connector_type: str) -> Type[Connector]:
        """Get a source connector class by type.

        Args:
            connector_type: Type identifier for the connector

        Returns:
            Source connector class

        Raises:
            ValueError: If connector type is not registered
        """
        if connector_type not in self._source_connectors:
            available = list(self._source_connectors.keys())
            raise ValueError(
                f"Unknown source connector type: {connector_type}. "
                f"Available types: {available}"
            )

        return self._source_connectors[connector_type].connector_class

    def get_destination_connector_class(
        self, connector_type: str
    ) -> Type[DestinationConnector]:
        """Get a destination connector class by type.

        Args:
            connector_type: Type identifier for the connector

        Returns:
            Destination connector class

        Raises:
            ValueError: If connector type is not registered
        """
        if connector_type not in self._destination_connectors:
            available = list(self._destination_connectors.keys())
            raise ValueError(
                f"Unknown destination connector type: {connector_type}. "
                f"Available types: {available}"
            )

        return self._destination_connectors[connector_type].connector_class

    def get_source_connector_info(self, connector_type: str) -> ConnectorTypeInfo:
        """Get detailed information about a source connector type.

        Args:
            connector_type: Type identifier for the connector

        Returns:
            Connector type information

        Raises:
            ValueError: If connector type is not registered
        """
        if connector_type not in self._source_connectors:
            available = list(self._source_connectors.keys())
            raise ValueError(
                f"Unknown source connector type: {connector_type}. "
                f"Available types: {available}"
            )

        return self._source_connectors[connector_type]

    def get_destination_connector_info(self, connector_type: str) -> ConnectorTypeInfo:
        """Get detailed information about a destination connector type.

        Args:
            connector_type: Type identifier for the connector

        Returns:
            Connector type information

        Raises:
            ValueError: If connector type is not registered
        """
        if connector_type not in self._destination_connectors:
            available = list(self._destination_connectors.keys())
            raise ValueError(
                f"Unknown destination connector type: {connector_type}. "
                f"Available types: {available}"
            )

        return self._destination_connectors[connector_type]

    def resolve_configuration(
        self,
        connector_type: str,
        is_source: bool = True,
        profile_params: Optional[Dict[str, Any]] = None,
        override_options: Optional[Dict[str, Any]] = None,
    ) -> ConfigurationResolutionResult:
        """Resolve configuration with proper precedence and validation.

        Configuration precedence (highest to lowest):
        1. Override options (from OPTIONS clause)
        2. Profile parameters (from profile PARAMS)
        3. Connector defaults

        Args:
            connector_type: Type identifier for the connector
            is_source: True for source connectors, False for destination
            profile_params: Parameters from profile configuration
            override_options: Parameters from OPTIONS override

        Returns:
            Configuration resolution result with resolved config and metadata

        Raises:
            ValueError: If connector type is not registered or configuration is invalid
        """
        # Get connector information
        if is_source:
            connector_info = self.get_source_connector_info(connector_type)
        else:
            connector_info = self.get_destination_connector_info(connector_type)

        # Start with connector defaults
        resolved_config = connector_info.default_config.copy()

        # Apply profile parameters (override defaults)
        profile_params = profile_params or {}
        resolved_config.update(profile_params)

        # Apply override options (highest precedence)
        override_options = override_options or {}
        overridden_params = []
        for key, value in override_options.items():
            if key in resolved_config and resolved_config[key] != value:
                overridden_params.append(key)
            resolved_config[key] = value

        # Validate configuration
        validation_warnings = self._validate_configuration(
            connector_info, resolved_config
        )

        return ConfigurationResolutionResult(
            resolved_config=resolved_config,
            profile_used=bool(profile_params),
            overridden_params=overridden_params,
            validation_warnings=validation_warnings,
        )

    def create_source_connector(
        self, connector_type: str, resolved_config: Dict[str, Any]
    ) -> Connector:
        """Create a source connector instance with resolved configuration.

        Args:
            connector_type: Type identifier for the connector
            resolved_config: Resolved configuration parameters

        Returns:
            Configured source connector instance

        Raises:
            ValueError: If connector type is not registered
        """
        connector_class = self.get_source_connector_class(connector_type)

        try:
            return connector_class(config=resolved_config)
        except TypeError:
            # Fallback for connectors that don't use config parameter
            instance = connector_class()
            if hasattr(instance, "configure"):
                instance.configure(resolved_config)
            return instance

    def create_destination_connector(
        self, connector_type: str, resolved_config: Dict[str, Any]
    ) -> DestinationConnector:
        """Create a destination connector instance with resolved configuration.

        Args:
            connector_type: Type identifier for the connector
            resolved_config: Resolved configuration parameters

        Returns:
            Configured destination connector instance

        Raises:
            ValueError: If connector type is not registered
        """
        connector_class = self.get_destination_connector_class(connector_type)

        try:
            return connector_class(config=resolved_config)
        except TypeError:
            # Fallback for connectors that don't use config parameter
            instance = connector_class()
            if hasattr(instance, "configure"):
                instance.configure(resolved_config)
            return instance

    def list_source_connectors(self) -> List[str]:
        """List all registered source connector types.

        Returns:
            List of source connector type identifiers
        """
        return list(self._source_connectors.keys())

    def list_destination_connectors(self) -> List[str]:
        """List all registered destination connector types.

        Returns:
            List of destination connector type identifiers
        """
        return list(self._destination_connectors.keys())

    def get_connector_defaults(
        self, connector_type: str, is_source: bool = True
    ) -> Dict[str, Any]:
        """Get default configuration for a connector type.

        Args:
            connector_type: Type identifier for the connector
            is_source: True for source connectors, False for destination

        Returns:
            Default configuration dictionary

        Raises:
            ValueError: If connector type is not registered
        """
        if is_source:
            connector_info = self.get_source_connector_info(connector_type)
        else:
            connector_info = self.get_destination_connector_info(connector_type)

        return connector_info.default_config.copy()

    def _extract_parameter_types(self, connector_class: type) -> Dict[str, type]:
        """Extract parameter types from connector class methods.

        Args:
            connector_class: Connector class to analyze

        Returns:
            Dictionary mapping parameter names to their types
        """
        param_types = {}

        try:
            # Check __init__ method for type hints
            init_signature = inspect.signature(connector_class.__init__)
            for param_name, param in init_signature.parameters.items():
                if param_name != "self" and param.annotation != inspect.Parameter.empty:
                    param_types[param_name] = param.annotation

            # Check configure method if it exists
            if hasattr(connector_class, "configure"):
                configure_signature = inspect.signature(connector_class.configure)
                for param_name, param in configure_signature.parameters.items():
                    if (
                        param_name != "self"
                        and param.annotation != inspect.Parameter.empty
                    ):
                        param_types[param_name] = param.annotation

        except Exception as e:
            logger.debug(
                f"Could not extract parameter types from {connector_class}: {e}"
            )

        return param_types

    def _validate_configuration(
        self, connector_info: ConnectorTypeInfo, config: Dict[str, Any]
    ) -> List[str]:
        """Validate configuration against connector requirements.

        Args:
            connector_info: Connector type information
            config: Configuration to validate

        Returns:
            List of validation warning messages
        """
        warnings = []

        # Check required parameters
        for required_param in connector_info.required_params:
            if required_param not in config:
                warnings.append(f"Required parameter '{required_param}' is missing")

        # Type validation (if type information is available)
        for param_name, expected_type in connector_info.param_types.items():
            if param_name in config:
                actual_value = config[param_name]
                if not isinstance(actual_value, expected_type):
                    warnings.append(
                        f"Parameter '{param_name}' expected type {expected_type.__name__}, "
                        f"got {type(actual_value).__name__}"
                    )

        return warnings


# Global enhanced registry instance
enhanced_registry = EnhancedConnectorRegistry()
