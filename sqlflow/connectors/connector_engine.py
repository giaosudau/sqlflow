"""Connector engine for SQLFlow."""

import logging
from typing import Any, Dict, Iterator, List, Optional

from sqlflow.connectors import (
    CONNECTOR_REGISTRY,
    get_connector_class,
    get_export_connector_class,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.errors import ConnectorError

logger = logging.getLogger(__name__)


class ConnectorEngine:
    """Orchestrates data flow from connectors to database."""

    def __init__(self):
        """Initialize a ConnectorEngine."""
        self.registered_connectors: Dict[str, Dict[str, Any]] = {}
        self.profile_config: Optional[Dict[str, Any]] = None
        logger.debug("ConnectorEngine initialized")

    def register_connector(
        self, name: str, connector_type: str, params: Dict[str, Any]
    ) -> None:
        """Register a connector.

        Args:
        ----
            name: Name of the connector
            connector_type: Type of the connector
            params: Parameters for the connector

        Raises:
        ------
            ValueError: If connector already exists or type is unknown

        """
        logger.debug(
            "Registering connector '%s' of type '%s' with params: %s",
            name,
            connector_type,
            params,
        )

        if name in self.registered_connectors:
            logger.debug("Connector '%s' already registered", name)
            raise ValueError(f"Connector '{name}' already registered")

        if connector_type not in CONNECTOR_REGISTRY:
            logger.debug("Unknown connector type: %s", connector_type)
            logger.debug(
                "Available connector types: %s", list(CONNECTOR_REGISTRY.keys())
            )
            raise ValueError(f"Unknown connector type: {connector_type}")

        self.registered_connectors[name] = {
            "type": connector_type,
            "params": params,
            "instance": None,
        }
        logger.debug("Successfully registered connector '%s'", name)

    def register_profile_connector(
        self,
        name: str,
        profile_connector_name: str,
        profile_connectors: Dict[str, Dict[str, Any]],
        options: Dict[str, Any],
    ) -> None:
        """Register a connector that references a connector defined in a profile.

        Args:
        ----
            name: Name of the connector in the pipeline
            profile_connector_name: Name of the connector in the profile
            profile_connectors: Dictionary of connectors from the profile
            options: Additional options to merge with the connector params

        Raises:
        ------
            ValueError: If connector already exists or profile connector is not found

        """
        logger.debug(
            "Registering profile-referenced connector '%s' from profile connector '%s'",
            name,
            profile_connector_name,
        )

        if name in self.registered_connectors:
            logger.debug("Connector '%s' already registered", name)
            raise ValueError(f"Connector '{name}' already registered")

        if profile_connector_name not in profile_connectors:
            logger.debug(
                "Profile connector '%s' not found in profile", profile_connector_name
            )
            raise ValueError(
                f"Profile connector '{profile_connector_name}' not found in profile"
            )

        profile_connector = profile_connectors[profile_connector_name]
        connector_type = profile_connector.get("type")

        if not connector_type:
            logger.debug("Profile connector '%s' missing type", profile_connector_name)
            raise ValueError(
                f"Profile connector '{profile_connector_name}' missing type"
            )

        if connector_type not in CONNECTOR_REGISTRY:
            logger.debug("Unknown connector type: %s", connector_type)
            logger.debug(
                "Available connector types: %s", list(CONNECTOR_REGISTRY.keys())
            )
            raise ValueError(f"Unknown connector type: {connector_type}")

        # Merge the profile connector params with the options
        base_params = profile_connector.get("params", {})
        merged_params = {**base_params}

        # Add options as top-level keys if they don't conflict
        for key, value in options.items():
            if key not in merged_params:
                merged_params[key] = value

        self.registered_connectors[name] = {
            "type": connector_type,
            "params": merged_params,
            "instance": None,
            "profile_referenced": True,
            "profile_connector_name": profile_connector_name,
        }
        logger.debug(
            "Successfully registered profile-referenced connector '%s' with type '%s' and merged params: %s",
            name,
            connector_type,
            merged_params,
        )

    def load_data(
        self,
        connector_name: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Iterator[DataChunk]:
        """Load data from a connector.

        Args:
        ----
            connector_name: Name of the connector
            table_name: Name of the table to load data into
            columns: Optional list of columns to load
            filters: Optional filters to apply

        Returns:
        -------
            Iterator of DataChunk objects

        Raises:
        ------
            ValueError: If connector is not registered
            ConnectorError: If loading fails

        """
        logger.debug(
            "Loading data from connector '%s' into table '%s'",
            connector_name,
            table_name,
        )
        logger.debug(
            "Registered connectors: %s", list(self.registered_connectors.keys())
        )

        if connector_name not in self.registered_connectors:
            logger.debug("Connector '%s' not registered", connector_name)
            raise ValueError(f"Connector '{connector_name}' not registered")

        connector_info = self.registered_connectors[connector_name]
        logger.debug("Found connector info: %s", connector_info)

        if connector_info["instance"] is None:
            logger.debug(
                "Creating new instance of connector type '%s'", connector_info["type"]
            )
            connector_class = get_connector_class(connector_info["type"])
            connector = connector_class()
            connector.name = connector_name
            try:
                logger.debug(
                    "Configuring connector '%s' with params: %s",
                    connector_name,
                    connector_info["params"],
                )
                connector.configure(connector_info["params"])
                connector_info["instance"] = connector
                logger.debug("Connector '%s' configured successfully", connector_name)
            except Exception as e:
                logger.debug(
                    "Error configuring connector '%s': %s", connector_name, str(e)
                )
                raise ConnectorError(connector_name, f"Configuration failed: {str(e)}")

        connector = connector_info["instance"]

        try:
            logger.debug("Testing connection for connector '%s'", connector_name)
            test_result = connector.test_connection()
            logger.debug("Connection test result: %s", test_result.success)
            if not test_result.success:
                logger.debug("Connection test failed: %s", test_result.message)
                raise ConnectorError(
                    connector_name, f"Connection test failed: {test_result.message}"
                )

            logger.debug(
                "Reading data from '%s' using connector '%s'",
                table_name,
                connector_name,
            )
            result_iter = connector.read(table_name, columns=columns, filters=filters)
            logger.debug("Successfully created data iterator")

            # Convert the iterator to a list and check for issues
            results = list(result_iter)
            logger.debug("Read %d data chunks", len(results))

            # Return as iterator
            for chunk in results:
                yield chunk

        except Exception as e:
            logger.debug("Error in load_data: %s", str(e))
            raise ConnectorError(connector_name, f"Loading data failed: {str(e)}")

    def _get_or_create_export_connector(
        self, connector_type: str, options: Optional[Dict[str, Any]] = None
    ):
        """Helper to instantiate and configure an export connector."""
        from sqlflow.connectors.registry import EXPORT_CONNECTOR_REGISTRY

        logger.debug(
            "Available export connector types: %s",
            list(EXPORT_CONNECTOR_REGISTRY.keys()),
        )
        export_connector_class = get_export_connector_class(connector_type)
        export_connector = export_connector_class()
        export_connector.name = f"{connector_type}_EXPORT"

        # Merge profile connector configuration for specific connector types
        final_options = self._merge_profile_connector_config(
            connector_type, options or {}
        )

        try:
            logger.debug("Configuring export connector with options: %s", final_options)
            export_connector.configure(final_options)
            logger.debug("Export connector configured successfully")
        except Exception as e:
            logger.debug("Error configuring export connector: %s", str(e))
            raise ConnectorError(
                export_connector.name, f"Configuration failed: {str(e)}"
            )
        return export_connector

    def _merge_profile_connector_config(
        self, connector_type: str, export_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge profile-based connector configuration with export-specific options.

        This ensures that export connectors like S3 get access to profile-configured
        connection parameters (bucket, credentials, etc.) while still allowing
        export-specific options to override or supplement them.

        Args:
        ----
            connector_type: The type of connector (e.g., 'S3', 'POSTGRES')
            export_options: Export-specific options from the EXPORT statement

        Returns:
        -------
            Merged configuration dictionary
        """
        # Start with export options as base
        final_options = export_options.copy()

        # For S3 connector, merge profile S3 configuration
        if connector_type.upper() == "S3":
            # Try to get S3 config from profile (if available via executor)
            if hasattr(self, "profile_config") and self.profile_config:
                profile_s3_config = self.profile_config.get("connectors", {}).get(
                    "s3", {}
                )
                if profile_s3_config:
                    logger.debug(f"Found S3 profile configuration: {profile_s3_config}")

                    # Merge profile config first, then export options override
                    merged_config = profile_s3_config.copy()
                    merged_config.update(final_options)
                    final_options = merged_config

                    logger.debug(f"Merged S3 configuration: {final_options}")
                else:
                    logger.debug("No S3 configuration found in profile")
            else:
                logger.debug("No profile configuration available for S3 connector")

        return final_options

    def _test_export_connector_connection(self, export_connector):
        try:
            logger.debug("Testing export connector connection")
            test_result = export_connector.test_connection()
            logger.debug("Export connection test result: %s", test_result.success)
            if not test_result.success:
                logger.debug("Export connection test failed: %s", test_result.message)
                raise ConnectorError(
                    export_connector.name,
                    f"Connection test failed: {test_result.message}",
                )
        except Exception as e:
            logger.debug("Error testing export connector connection: %s", str(e))
            raise ConnectorError(
                export_connector.name, f"Connection test failed: {str(e)}"
            )

    def _write_export_data(self, export_connector, destination, data):
        try:
            logger.debug("Writing data to destination: %s", destination)
            export_connector.write(destination, data)
            logger.debug("Data export successful")
        except Exception as e:
            logger.debug("Error writing data: %s", str(e))
            raise ConnectorError(export_connector.name, f"Writing failed: {str(e)}")

    def export_data(
        self,
        data: Any,
        destination: str,
        connector_type: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Export data using an export connector with smart destination detection."""
        logger.debug(
            "Exporting data to '%s' using connector type '%s'",
            destination,
            connector_type,
        )
        logger.debug("Export options: %s", options)

        try:
            if not isinstance(data, DataChunk):
                logger.debug("Converting data to DataChunk")
                data = DataChunk(data)

            # Smart connector selection based on destination URI
            actual_connector_type = self._detect_connector_type_from_destination(
                destination, connector_type, options
            )

            if actual_connector_type != connector_type:
                logger.info(
                    f"Auto-detected connector type '{actual_connector_type}' for destination '{destination}' "
                    f"(original type: '{connector_type}')"
                )

            logger.debug(
                "Getting export connector class for '%s'", actual_connector_type
            )
            export_connector = self._get_or_create_export_connector(
                actual_connector_type, options
            )
            self._test_export_connector_connection(export_connector)
            self._write_export_data(export_connector, destination, data)
        except Exception as e:
            logger.debug("Export failed: %s", str(e))
            raise ConnectorError(f"{connector_type}_EXPORT", f"Export failed: {str(e)}")

    def _detect_connector_type_from_destination(
        self,
        destination: str,
        original_type: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Detect the appropriate connector type based on destination URI patterns.

        This enables smart connector selection - for example, s3:// URIs automatically
        use the S3 connector regardless of the specified TYPE.

        Args:
        ----
            destination: The destination URI
            original_type: The originally specified connector type
            options: Export options that may influence connector selection

        Returns:
        -------
            The actual connector type to use
        """
        # S3 URI detection - any s3:// URI should use S3 connector
        if destination.startswith("s3://"):
            logger.debug(f"Detected S3 URI: {destination} - using S3 connector")

            # Merge the original type as format if it's a file format
            if original_type.upper() in ["CSV", "PARQUET", "JSON"]:
                options = options or {}
                if "file_format" not in options:
                    options["file_format"] = original_type.lower()
                    logger.debug(
                        f"Added file format '{original_type.lower()}' to S3 options"
                    )

            return "S3"

        # GCS URI detection - any gs:// URI should use GCS connector (if available)
        elif destination.startswith("gs://"):
            logger.debug(f"Detected GCS URI: {destination}")
            # For now, fall back to original type since GCS connector may not be implemented
            return original_type

        # Azure Blob detection - any azure:// or abfss:// URI
        elif destination.startswith(("azure://", "abfss://", "wasbs://")):
            logger.debug(f"Detected Azure URI: {destination}")
            # For now, fall back to original type since Azure connector may not be implemented
            return original_type

        # HTTP/HTTPS URIs might use REST connector
        elif destination.startswith(("http://", "https://")):
            logger.debug(f"Detected HTTP URI: {destination}")
            # Check if REST connector is available, otherwise use original
            from sqlflow.connectors.registry import EXPORT_CONNECTOR_REGISTRY

            if "REST" in EXPORT_CONNECTOR_REGISTRY:
                return "REST"
            return original_type

        # File paths - use original type (CSV, PARQUET, etc.)
        else:
            logger.debug(
                f"Using original connector type '{original_type}' for destination: {destination}"
            )
            return original_type
