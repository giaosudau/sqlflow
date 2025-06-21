"""Pure validation functions for pipeline steps.

Following Pythonic validation principles:
- Fail fast with clear error messages
- Use type hints for self-documenting APIs
- Pure functions that return results rather than modify state
- Composable validation functions
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationError:
    """Immutable validation error."""

    field: str
    message: str
    value: Any = None
    code: Optional[str] = None


@dataclass(frozen=True)
class ValidationResult:
    """Immutable validation result."""

    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(
        self, field: str, message: str, value: Any = None, code: Optional[str] = None
    ) -> "ValidationResult":
        """Add error and return new ValidationResult."""
        new_error = ValidationError(
            field=field, message=message, value=value, code=code
        )
        return ValidationResult(
            is_valid=False, errors=self.errors + [new_error], warnings=self.warnings
        )

    def add_warning(self, message: str) -> "ValidationResult":
        """Add warning and return new ValidationResult."""
        return ValidationResult(
            is_valid=self.is_valid,
            errors=self.errors,
            warnings=self.warnings + [message],
        )


def validate_step_type(step: Dict[str, Any]) -> ValidationResult:
    """Validate step has valid type field.

    Args:
        step: Pipeline step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    if not isinstance(step, dict):
        return result.add_error("step", "Step must be a dictionary", step)

    if "type" not in step:
        return result.add_error("type", "Step must have 'type' field")

    step_type = step["type"]
    valid_types = {"load", "transform", "export", "source_definition"}

    if step_type not in valid_types:
        return result.add_error(
            "type",
            f"Invalid step type '{step_type}'. Must be one of: {', '.join(valid_types)}",
            step_type,
        )

    return result


def validate_step_id(step: Dict[str, Any]) -> ValidationResult:
    """Validate step has valid ID field.

    Args:
        step: Pipeline step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    if "id" not in step:
        return result.add_error("id", "Step must have 'id' field")

    step_id = step["id"]

    if not isinstance(step_id, str):
        return result.add_error("id", "Step ID must be a string", step_id)

    if not step_id.strip():
        return result.add_error("id", "Step ID cannot be empty")

    # Check for valid identifier-like format
    if not step_id.replace("_", "").replace("-", "").isalnum():
        return result.add_warning(f"Step ID '{step_id}' contains special characters")

    return result


def validate_load_step(step: Dict[str, Any]) -> ValidationResult:
    """Validate load step configuration.

    Args:
        step: Load step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    # Basic step validation
    base_result = validate_step_base(step)
    if not base_result.is_valid:
        return base_result

    # Load step must have either source or source_name
    if "source" not in step and "source_name" not in step:
        result = result.add_error(
            "source", "Load step must have either 'source' or 'source_name' field"
        )

    # Must have target table
    if "target_table" not in step:
        result = result.add_error(
            "target_table", "Load step must have 'target_table' field"
        )

    # Validate load mode if present
    if "load_mode" in step:
        valid_modes = {"replace", "append", "incremental"}
        if step["load_mode"] not in valid_modes:
            result = result.add_error(
                "load_mode",
                f"Invalid load mode '{step['load_mode']}'. Must be one of: {', '.join(valid_modes)}",
                step["load_mode"],
            )

    return result


def validate_transform_step(step: Dict[str, Any]) -> ValidationResult:
    """Validate transform step configuration.

    Args:
        step: Transform step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    # Basic step validation
    base_result = validate_step_base(step)
    if not base_result.is_valid:
        return base_result

    # Transform step must have query
    if "query" not in step:
        result = result.add_error("query", "Transform step must have 'query' field")
    elif not isinstance(step["query"], str):
        result = result.add_error(
            "query", "Transform step query must be a string", step["query"]
        )
    elif not step["query"].strip():
        result = result.add_error("query", "Transform step query cannot be empty")

    # Must have target table
    if "target_table" not in step:
        result = result.add_error(
            "target_table", "Transform step must have 'target_table' field"
        )

    return result


def validate_export_step(step: Dict[str, Any]) -> ValidationResult:
    """Validate export step configuration.

    Args:
        step: Export step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    # Basic step validation
    base_result = validate_step_base(step)
    if not base_result.is_valid:
        return base_result

    # Export step must have source table
    if "source_table" not in step:
        result = result.add_error(
            "source_table", "Export step must have 'source_table' field"
        )

    # Must have destination
    if "destination" not in step:
        result = result.add_error(
            "destination", "Export step must have 'destination' field"
        )

    return result


def validate_source_definition_step(step: Dict[str, Any]) -> ValidationResult:
    """Validate source definition step.

    Args:
        step: Source definition step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    # Basic step validation
    base_result = validate_step_base(step)
    if not base_result.is_valid:
        return base_result

    # Must have name
    if "name" not in step:
        result = result.add_error("name", "Source definition must have 'name' field")

    # Must have source_connector_type
    if "source_connector_type" not in step:
        result = result.add_error(
            "source_connector_type",
            "Source definition must have 'source_connector_type' field",
        )

    return result


def validate_step_base(step: Dict[str, Any]) -> ValidationResult:
    """Validate basic step requirements.

    Args:
        step: Step to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    # Combine basic validations
    type_result = validate_step_type(step)
    if not type_result.is_valid:
        return type_result

    id_result = validate_step_id(step)
    if not id_result.is_valid:
        return id_result

    # Combine warnings
    all_warnings = type_result.warnings + id_result.warnings
    if all_warnings:
        for warning in all_warnings:
            result = result.add_warning(warning)

    return result


def validate_step(step: Dict[str, Any]) -> ValidationResult:
    """Validate a pipeline step based on its type.

    Args:
        step: Pipeline step to validate

    Returns:
        ValidationResult with any errors found
    """
    # Basic validation first
    base_result = validate_step_base(step)
    if not base_result.is_valid:
        return base_result

    step_type = step["type"]

    # Type-specific validation
    if step_type == "load":
        return validate_load_step(step)
    elif step_type == "transform":
        return validate_transform_step(step)
    elif step_type == "export":
        return validate_export_step(step)
    elif step_type == "source_definition":
        return validate_source_definition_step(step)
    else:
        # This shouldn't happen because validate_step_type should catch it first
        # But just in case there's a logic error, handle it here too
        return ValidationResult(is_valid=False).add_error(
            "type", f"Unknown step type: {step_type}"
        )


def validate_pipeline(steps: List[Dict[str, Any]]) -> ValidationResult:
    """Validate entire pipeline.

    Args:
        steps: List of pipeline steps

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    if not isinstance(steps, list):
        return result.add_error("pipeline", "Pipeline must be a list", steps)

    if not steps:
        return result.add_warning("Pipeline is empty")

    step_ids = []

    for i, step in enumerate(steps):
        # Validate individual step
        step_result = validate_step(step)

        # Accumulate errors
        for error in step_result.errors:
            result = result.add_error(
                f"step[{i}].{error.field}", error.message, error.value
            )

        # Accumulate warnings
        for warning in step_result.warnings:
            result = result.add_warning(f"Step {i}: {warning}")

        # Check for duplicate IDs
        if "id" in step:
            step_id = step["id"]
            if step_id in step_ids:
                result = result.add_error(
                    f"step[{i}].id", f"Duplicate step ID: {step_id}"
                )
            else:
                step_ids.append(step_id)

    return result


def validate_variables(variables: Any) -> ValidationResult:
    """Validate variables dictionary.

    Args:
        variables: Variables to validate

    Returns:
        ValidationResult with any errors found
    """
    result = ValidationResult(is_valid=True)

    if variables is None:
        return result

    if not isinstance(variables, dict):
        return result.add_error(
            "variables", "Variables must be a dictionary", variables
        )

    for key, value in variables.items():
        if not isinstance(key, str):
            result = result.add_error(
                f"variables.{key}", "Variable keys must be strings", key
            )

    return result
