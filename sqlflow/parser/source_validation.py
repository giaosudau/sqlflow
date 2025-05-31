"""Industry-standard SOURCE parameter validation for SQLFlow.

This module provides validation for SOURCE parameters that are compatible
with Airbyte and Fivetran conventions, enabling easy migration from these platforms.
"""

from typing import Any, Dict, List

from sqlflow.logging import get_logger

logger = get_logger(__name__)

# Industry-standard parameter names (Airbyte/Fivetran compatible)
STANDARD_PARAMETERS = {
    "sync_mode": {"full_refresh", "incremental", "cdc"},
    "cursor_field": str,
    "primary_key": (str, list),
    "destination_sync_mode": {"overwrite", "append", "append_dedup"},
    "replication_method": {"FULL_TABLE", "INCREMENTAL", "LOG_BASED"},
    "cursor_field_datetime_format": str,
    "lookback_window": int,
    "start_date": str,
    "end_date": str,
}

# Parameter combinations that are incompatible
INCOMPATIBLE_COMBINATIONS = [
    ({"sync_mode": "full_refresh"}, {"cursor_field"}),
    ({"sync_mode": "cdc"}, {"cursor_field"}),
    (
        {"sync_mode": "incremental"},
        set(),
        "cursor_field",
    ),  # incremental requires cursor_field
]

# Parameters that require other parameters
REQUIRED_DEPENDENCIES = {
    "incremental": {"cursor_field"},
    "append_dedup": {"primary_key"},
}


class SourceParameterValidator:
    """Validates SOURCE parameters against industry standards."""

    def __init__(self):
        """Initialize the validator."""
        logger.debug("SourceParameterValidator initialized")

    def validate_parameters(self, params: Dict[str, Any]) -> List[str]:
        """Validate SOURCE parameters against industry standards.

        Args:
            params: Dictionary of SOURCE parameters

        Returns:
            List of validation error messages, empty if valid
        """
        errors = []

        # Validate individual parameters
        errors.extend(self._validate_sync_mode(params))
        errors.extend(self._validate_cursor_field(params))
        errors.extend(self._validate_primary_key(params))
        errors.extend(self._validate_destination_sync_mode(params))
        errors.extend(self._validate_replication_method(params))
        errors.extend(self._validate_datetime_format(params))
        errors.extend(self._validate_lookback_window(params))
        errors.extend(self._validate_date_parameters(params))

        # Validate parameter combinations
        errors.extend(self._validate_parameter_combinations(params))

        # Validate required dependencies
        errors.extend(self._validate_required_dependencies(params))

        logger.debug(f"Parameter validation completed with {len(errors)} errors")
        return errors

    def _validate_sync_mode(self, params: Dict[str, Any]) -> List[str]:
        """Validate sync_mode parameter."""
        errors = []
        sync_mode = params.get("sync_mode")

        if sync_mode is not None:
            if not isinstance(sync_mode, str):
                errors.append("sync_mode must be a string")
            elif sync_mode not in STANDARD_PARAMETERS["sync_mode"]:
                valid_modes = ", ".join(sorted(STANDARD_PARAMETERS["sync_mode"]))
                errors.append(
                    f"Invalid sync_mode '{sync_mode}'. Must be one of: {valid_modes}"
                )

        return errors

    def _validate_cursor_field(self, params: Dict[str, Any]) -> List[str]:
        """Validate cursor_field parameter."""
        errors = []
        cursor_field = params.get("cursor_field")

        if cursor_field is not None:
            if not isinstance(cursor_field, str):
                errors.append("cursor_field must be a string")
            elif not cursor_field.strip():
                errors.append("cursor_field cannot be empty")

        return errors

    def _validate_primary_key(self, params: Dict[str, Any]) -> List[str]:
        """Validate primary_key parameter."""
        errors = []
        primary_key = params.get("primary_key")

        if primary_key is not None:
            if isinstance(primary_key, str):
                if not primary_key.strip():
                    errors.append("primary_key cannot be empty")
            elif isinstance(primary_key, list):
                if not primary_key:
                    errors.append("primary_key list cannot be empty")
                elif not all(
                    isinstance(key, str) and key.strip() for key in primary_key
                ):
                    errors.append("All primary_key values must be non-empty strings")
            else:
                errors.append("primary_key must be a string or list of strings")

        return errors

    def _validate_destination_sync_mode(self, params: Dict[str, Any]) -> List[str]:
        """Validate destination_sync_mode parameter."""
        errors = []
        dest_sync_mode = params.get("destination_sync_mode")

        if dest_sync_mode is not None:
            if not isinstance(dest_sync_mode, str):
                errors.append("destination_sync_mode must be a string")
            elif dest_sync_mode not in STANDARD_PARAMETERS["destination_sync_mode"]:
                valid_modes = ", ".join(
                    sorted(STANDARD_PARAMETERS["destination_sync_mode"])
                )
                errors.append(
                    f"Invalid destination_sync_mode '{dest_sync_mode}'. Must be one of: {valid_modes}"
                )

        return errors

    def _validate_replication_method(self, params: Dict[str, Any]) -> List[str]:
        """Validate replication_method parameter."""
        errors = []
        replication_method = params.get("replication_method")

        if replication_method is not None:
            if not isinstance(replication_method, str):
                errors.append("replication_method must be a string")
            elif replication_method not in STANDARD_PARAMETERS["replication_method"]:
                valid_methods = ", ".join(
                    sorted(STANDARD_PARAMETERS["replication_method"])
                )
                errors.append(
                    f"Invalid replication_method '{replication_method}'. Must be one of: {valid_methods}"
                )

        return errors

    def _validate_datetime_format(self, params: Dict[str, Any]) -> List[str]:
        """Validate cursor_field_datetime_format parameter."""
        errors = []
        datetime_format = params.get("cursor_field_datetime_format")

        if datetime_format is not None:
            if not isinstance(datetime_format, str):
                errors.append("cursor_field_datetime_format must be a string")
            elif not datetime_format.strip():
                errors.append("cursor_field_datetime_format cannot be empty")

        return errors

    def _validate_lookback_window(self, params: Dict[str, Any]) -> List[str]:
        """Validate lookback_window parameter."""
        errors = []
        lookback_window = params.get("lookback_window")

        if lookback_window is not None:
            if not isinstance(lookback_window, int):
                errors.append("lookback_window must be an integer")
            elif lookback_window < 0:
                errors.append("lookback_window must be non-negative")

        return errors

    def _validate_date_parameters(self, params: Dict[str, Any]) -> List[str]:
        """Validate start_date and end_date parameters."""
        errors = []

        for date_param in ["start_date", "end_date"]:
            date_value = params.get(date_param)
            if date_value is not None:
                if not isinstance(date_value, str):
                    errors.append(f"{date_param} must be a string")
                elif not date_value.strip():
                    errors.append(f"{date_param} cannot be empty")

        return errors

    def _validate_parameter_combinations(self, params: Dict[str, Any]) -> List[str]:
        """Validate parameter combinations for compatibility."""
        errors = []
        sync_mode = params.get("sync_mode")
        cursor_field = params.get("cursor_field")

        # full_refresh and cdc don't use cursor_field
        if sync_mode in ["full_refresh", "cdc"] and cursor_field is not None:
            errors.append(f"sync_mode '{sync_mode}' cannot be used with cursor_field")

        return errors

    def _validate_required_dependencies(self, params: Dict[str, Any]) -> List[str]:
        """Validate that required parameter dependencies are met."""
        errors = []

        sync_mode = params.get("sync_mode")
        destination_sync_mode = params.get("destination_sync_mode")
        cursor_field = params.get("cursor_field")
        primary_key = params.get("primary_key")

        # incremental sync_mode requires cursor_field
        if sync_mode == "incremental" and cursor_field is None:
            errors.append(
                "sync_mode 'incremental' requires cursor_field to be specified"
            )

        # append_dedup destination_sync_mode requires primary_key
        if destination_sync_mode == "append_dedup" and primary_key is None:
            errors.append(
                "destination_sync_mode 'append_dedup' requires primary_key to be specified"
            )

        return errors

    def get_migration_suggestions(self, params: Dict[str, Any]) -> List[str]:
        """Get suggestions for migrating from other platforms.

        Args:
            params: Dictionary of SOURCE parameters

        Returns:
            List of migration suggestions
        """
        suggestions = []

        # Check for common Airbyte parameter patterns
        if "replication_method" in params and "sync_mode" not in params:
            replication_method = params["replication_method"]
            if replication_method == "FULL_TABLE":
                suggestions.append(
                    "Consider using sync_mode: 'full_refresh' instead of replication_method: 'FULL_TABLE'"
                )
            elif replication_method == "INCREMENTAL":
                suggestions.append(
                    "Consider using sync_mode: 'incremental' instead of replication_method: 'INCREMENTAL'"
                )
            elif replication_method == "LOG_BASED":
                suggestions.append(
                    "Consider using sync_mode: 'cdc' instead of replication_method: 'LOG_BASED'"
                )

        # Check for missing recommended parameters
        if params.get("sync_mode") == "incremental" and "lookback_window" not in params:
            suggestions.append(
                "Consider adding lookback_window for incremental sync to handle late-arriving data"
            )

        return suggestions
