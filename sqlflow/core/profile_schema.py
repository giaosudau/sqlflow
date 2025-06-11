"""YAML schema definition and validation for SQLFlow profiles.

This module provides formal schema definition and validation for SQLFlow
profiles with detailed error reporting and schema versioning support.
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SchemaValidationError:
    """Represents a schema validation error with detailed information."""

    path: str
    message: str
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    line_number: Optional[int] = None

    def __str__(self) -> str:
        """String representation of the validation error."""
        error_msg = f"{self.path}: {self.message}"
        if self.expected_type and self.actual_type:
            error_msg += f" (expected {self.expected_type}, got {self.actual_type})"
        if self.line_number:
            error_msg += f" at line {self.line_number}"
        return error_msg


@dataclass
class SchemaValidationResult:
    """Result of schema validation with errors and warnings."""

    is_valid: bool
    errors: List[SchemaValidationError]
    warnings: List[str]
    schema_version: Optional[str] = None

    def __bool__(self) -> bool:
        """Returns True if validation passed."""
        return self.is_valid

    def get_error_summary(self) -> str:
        """Get a formatted summary of all errors."""
        if not self.errors:
            return "No validation errors"

        error_lines = [str(error) for error in self.errors]
        return "\n".join(error_lines)


class ProfileSchemaValidator:
    """Validates SQLFlow profile YAML against the formal schema."""

    # Current supported schema version
    CURRENT_VERSION = "1.0"

    # Supported connector types and their required fields
    CONNECTOR_SCHEMAS = {
        "csv": {
            "required_fields": [],
            "optional_fields": [
                "has_header",
                "delimiter",
                "encoding",
                "quote_char",
                "escape_char",
                "null_value",
                "skip_rows",
                "max_rows",
            ],
            "field_types": {
                "has_header": bool,
                "delimiter": str,
                "encoding": str,
                "quote_char": str,
                "escape_char": str,
                "null_value": str,
                "skip_rows": int,
                "max_rows": int,
            },
        },
        "postgres": {
            "required_fields": [],
            "optional_fields": [
                "host",
                "port",
                "database",
                "schema",
                "user",
                "password",
                "sslmode",
                "connect_timeout",
                "application_name",
            ],
            "field_types": {
                "host": str,
                "port": (int, str),  # Can be int or string
                "database": str,
                "schema": str,
                "user": str,
                "password": str,
                "sslmode": str,
                "connect_timeout": int,
                "application_name": str,
            },
        },
        "s3": {
            "required_fields": [],
            "optional_fields": [
                "bucket",
                "key",
                "region",
                "access_key_id",
                "secret_access_key",
                "session_token",
                "endpoint_url",
                "use_ssl",
                "signature_version",
            ],
            "field_types": {
                "bucket": str,
                "key": str,
                "region": str,
                "access_key_id": str,
                "secret_access_key": str,
                "session_token": str,
                "endpoint_url": str,
                "use_ssl": bool,
                "signature_version": str,
            },
        },
        "rest": {
            "required_fields": [],
            "optional_fields": [
                "url",
                "method",
                "headers",
                "timeout",
                "verify_ssl",
                "max_retries",
                "auth",
                "params",
            ],
            "field_types": {
                "url": str,
                "method": str,
                "headers": dict,
                "timeout": int,
                "verify_ssl": bool,
                "max_retries": int,
                "auth": dict,
                "params": dict,
            },
        },
    }

    def __init__(self):
        """Initialize the schema validator."""
        logger.debug("Initialized ProfileSchemaValidator")

    def validate_profile(self, profile_data: Dict[str, Any]) -> SchemaValidationResult:
        """Validate profile data against the SQLFlow profile schema.

        Args:
            profile_data: Profile data to validate

        Returns:
            SchemaValidationResult with validation details
        """
        errors = []
        warnings = []

        logger.debug("Starting profile schema validation")

        # Basic structure validation
        if not isinstance(profile_data, dict):
            errors.append(
                SchemaValidationError(
                    path="root",
                    message="Profile must be a dictionary",
                    expected_type="dict",
                    actual_type=type(profile_data).__name__,
                )
            )
            return SchemaValidationResult(
                is_valid=False, errors=errors, warnings=warnings
            )

        # Validate version
        schema_version = self._validate_version(profile_data, errors, warnings)

        # Validate top-level sections
        self._validate_variables_section(profile_data, errors)
        self._validate_connectors_section(profile_data, errors, warnings)
        self._validate_engines_section(profile_data, errors)

        # Check for unknown top-level keys
        self._check_unknown_keys(
            profile_data,
            ["version", "variables", "connectors", "engines"],
            "root",
            errors,
            warnings,
        )

        is_valid = len(errors) == 0
        logger.debug(
            f"Schema validation completed: {len(errors)} errors, {len(warnings)} warnings"
        )

        return SchemaValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            schema_version=schema_version,
        )

    def _validate_version(
        self,
        profile_data: Dict[str, Any],
        errors: List[SchemaValidationError],
        warnings: List[str],
    ) -> Optional[str]:
        """Validate profile version."""
        version = profile_data.get("version")

        if version is None:
            warnings.append("Profile version not specified, assuming '1.0'")
            return self.CURRENT_VERSION

        if not isinstance(version, str):
            errors.append(
                SchemaValidationError(
                    path="version",
                    message="Version must be a string",
                    expected_type="str",
                    actual_type=type(version).__name__,
                )
            )
            return None

        if version != self.CURRENT_VERSION:
            warnings.append(
                f"Unknown profile version '{version}', expected '{self.CURRENT_VERSION}'"
            )

        return version

    def _validate_variables_section(
        self, profile_data: Dict[str, Any], errors: List[SchemaValidationError]
    ) -> None:
        """Validate variables section."""
        if "variables" not in profile_data:
            return

        variables = profile_data["variables"]
        if not isinstance(variables, dict):
            errors.append(
                SchemaValidationError(
                    path="variables",
                    message="Variables section must be a dictionary",
                    expected_type="dict",
                    actual_type=type(variables).__name__,
                )
            )
            return

        # Validate variable names (should be valid identifiers)
        for var_name, var_value in variables.items():
            if not isinstance(var_name, str):
                errors.append(
                    SchemaValidationError(
                        path=f"variables.{var_name}",
                        message="Variable name must be a string",
                    )
                )
                continue

            if not self._is_valid_variable_name(var_name):
                errors.append(
                    SchemaValidationError(
                        path=f"variables.{var_name}",
                        message="Variable name must be a valid identifier (letters, numbers, underscore)",
                    )
                )

            # Variable values should be strings, numbers, or booleans
            if not isinstance(var_value, (str, int, float, bool)):
                errors.append(
                    SchemaValidationError(
                        path=f"variables.{var_name}",
                        message="Variable value must be a string, number, or boolean",
                        expected_type="str|int|float|bool",
                        actual_type=type(var_value).__name__,
                    )
                )

    def _validate_connectors_section(
        self,
        profile_data: Dict[str, Any],
        errors: List[SchemaValidationError],
        warnings: List[str],
    ) -> None:
        """Validate connectors section."""
        if "connectors" not in profile_data:
            warnings.append(
                "No connectors section found - profile will not define any connectors"
            )
            return

        connectors = profile_data["connectors"]
        if not isinstance(connectors, dict):
            errors.append(
                SchemaValidationError(
                    path="connectors",
                    message="Connectors section must be a dictionary",
                    expected_type="dict",
                    actual_type=type(connectors).__name__,
                )
            )
            return

        if not connectors:
            warnings.append("Connectors section is empty")
            return

        # Validate each connector
        for conn_name, conn_config in connectors.items():
            self._validate_connector(conn_name, conn_config, errors, warnings)

    def _validate_connector(
        self,
        connector_name: str,
        connector_config: Any,
        errors: List[SchemaValidationError],
        warnings: List[str],
    ) -> None:
        """Validate a single connector configuration."""
        base_path = f"connectors.{connector_name}"

        if not isinstance(connector_config, dict):
            errors.append(
                SchemaValidationError(
                    path=base_path,
                    message="Connector configuration must be a dictionary",
                    expected_type="dict",
                    actual_type=type(connector_config).__name__,
                )
            )
            return

        # Validate required 'type' field
        connector_type = connector_config.get("type")
        if not connector_type:
            errors.append(
                SchemaValidationError(
                    path=f"{base_path}.type",
                    message="Connector must have a 'type' field",
                )
            )
            return

        if not isinstance(connector_type, str):
            errors.append(
                SchemaValidationError(
                    path=f"{base_path}.type",
                    message="Connector type must be a string",
                    expected_type="str",
                    actual_type=type(connector_type).__name__,
                )
            )
            return

        # Validate params section
        params = connector_config.get("params", {})
        if not isinstance(params, dict):
            errors.append(
                SchemaValidationError(
                    path=f"{base_path}.params",
                    message="Connector params must be a dictionary",
                    expected_type="dict",
                    actual_type=type(params).__name__,
                )
            )
            return

        # Validate connector-specific schema if available
        if connector_type in self.CONNECTOR_SCHEMAS:
            self._validate_connector_params(
                connector_name, connector_type, params, errors, warnings
            )
        else:
            warnings.append(
                f"Unknown connector type '{connector_type}' for connector '{connector_name}'"
            )

        # Check for unknown top-level connector keys
        self._check_unknown_keys(
            connector_config, ["type", "params"], base_path, errors, warnings
        )

    def _validate_connector_params(
        self,
        connector_name: str,
        connector_type: str,
        params: Dict[str, Any],
        errors: List[SchemaValidationError],
        warnings: List[str],
    ) -> None:
        """Validate connector parameters against type-specific schema."""
        schema = self.CONNECTOR_SCHEMAS[connector_type]
        base_path = f"connectors.{connector_name}.params"

        # Check for missing required fields
        for required_field in schema["required_fields"]:
            if required_field not in params:
                errors.append(
                    SchemaValidationError(
                        path=f"{base_path}.{required_field}",
                        message=f"Required field '{required_field}' missing for {connector_type} connector",
                    )
                )

        # Validate field types
        field_types = schema.get("field_types", {})
        for field_name, field_value in params.items():
            if field_name in field_types:
                expected_type = field_types[field_name]
                if not self._check_field_type(field_value, expected_type):
                    errors.append(
                        SchemaValidationError(
                            path=f"{base_path}.{field_name}",
                            message=f"Invalid type for field '{field_name}'",
                            expected_type=self._format_type(expected_type),
                            actual_type=type(field_value).__name__,
                        )
                    )
            else:
                # Unknown field
                all_fields = schema["required_fields"] + schema["optional_fields"]
                if field_name not in all_fields:
                    warnings.append(
                        f"Unknown parameter '{field_name}' for {connector_type} connector '{connector_name}'"
                    )

    def _validate_engines_section(
        self, profile_data: Dict[str, Any], errors: List[SchemaValidationError]
    ) -> None:
        """Validate engines section."""
        if "engines" not in profile_data:
            return

        engines = profile_data["engines"]
        if not isinstance(engines, dict):
            errors.append(
                SchemaValidationError(
                    path="engines",
                    message="Engines section must be a dictionary",
                    expected_type="dict",
                    actual_type=type(engines).__name__,
                )
            )
            return

        # Validate DuckDB engine if present
        if "duckdb" in engines:
            self._validate_duckdb_engine(engines["duckdb"], errors)

    def _validate_duckdb_engine(
        self, duckdb_config: Any, errors: List[SchemaValidationError]
    ) -> None:
        """Validate DuckDB engine configuration."""
        base_path = "engines.duckdb"

        if not isinstance(duckdb_config, dict):
            errors.append(
                SchemaValidationError(
                    path=base_path,
                    message="DuckDB engine configuration must be a dictionary",
                    expected_type="dict",
                    actual_type=type(duckdb_config).__name__,
                )
            )
            return

        # Validate mode field
        mode = duckdb_config.get("mode", "memory")
        if not isinstance(mode, str):
            errors.append(
                SchemaValidationError(
                    path=f"{base_path}.mode",
                    message="DuckDB mode must be a string",
                    expected_type="str",
                    actual_type=type(mode).__name__,
                )
            )
        elif mode not in ["memory", "persistent"]:
            errors.append(
                SchemaValidationError(
                    path=f"{base_path}.mode",
                    message=f"Invalid DuckDB mode '{mode}'. Must be 'memory' or 'persistent'",
                )
            )

        # Validate path field if mode is persistent
        if mode == "persistent":
            path = duckdb_config.get("path")
            if not path:
                errors.append(
                    SchemaValidationError(
                        path=f"{base_path}.path",
                        message="DuckDB path is required when mode is 'persistent'",
                    )
                )
            elif not isinstance(path, str):
                errors.append(
                    SchemaValidationError(
                        path=f"{base_path}.path",
                        message="DuckDB path must be a string",
                        expected_type="str",
                        actual_type=type(path).__name__,
                    )
                )

    def _check_unknown_keys(
        self,
        data: Dict[str, Any],
        known_keys: List[str],
        base_path: str,
        errors: List[SchemaValidationError],
        warnings: List[str],
    ) -> None:
        """Check for unknown keys in a dictionary."""
        for key in data.keys():
            if key not in known_keys:
                warnings.append(f"Unknown key '{key}' in {base_path}")

    def _is_valid_variable_name(self, name: str) -> bool:
        """Check if a variable name is valid (valid Python identifier)."""
        return re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name) is not None

    def _check_field_type(self, value: Any, expected_type: Union[type, tuple]) -> bool:
        """Check if value matches expected type(s)."""
        if isinstance(expected_type, tuple):
            return isinstance(value, expected_type)
        else:
            return isinstance(value, expected_type)

    def _format_type(self, type_spec: Union[type, tuple]) -> str:
        """Format type specification for error messages."""
        if isinstance(type_spec, tuple):
            type_names = [t.__name__ for t in type_spec]
            return "|".join(type_names)
        else:
            return type_spec.__name__

    def get_schema_documentation(self) -> str:
        """Get human-readable schema documentation.

        Returns:
            Formatted schema documentation string
        """
        docs = f"""
SQLFlow Profile Schema v{self.CURRENT_VERSION}
=================================

A SQLFlow profile is a YAML file that defines:
- Variables for reuse across connectors
- Connector configurations with parameters
- Engine configurations (e.g., DuckDB settings)

## Structure

```yaml
version: "1.0"                    # Optional: schema version
variables:                        # Optional: reusable variables
  key: value
connectors:                       # Required: connector definitions
  connector_name:
    type: connector_type          # Required: connector type
    params:                       # Required: connector parameters
      key: value
engines:                          # Optional: engine configurations
  duckdb:
    mode: memory|persistent       # DuckDB mode
    path: /path/to/db            # Required if mode is persistent
```

## Supported Connector Types

"""

        for conn_type, schema in self.CONNECTOR_SCHEMAS.items():
            docs += f"\n### {conn_type}\n"
            if schema["required_fields"]:
                docs += f"Required fields: {', '.join(schema['required_fields'])}\n"
            if schema["optional_fields"]:
                docs += f"Optional fields: {', '.join(schema['optional_fields'])}\n"

        return docs
