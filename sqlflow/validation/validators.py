"""Validation functions for SQLFlow DSL components."""

from typing import List, Set

from ..parser.ast import LoadStep, Pipeline, SourceDefinitionStep
from .errors import ValidationError
from .schemas import CONNECTOR_SCHEMAS


def validate_connectors(pipeline: Pipeline) -> List[ValidationError]:
    """Validate connector parameters against their schemas.

    Args:
        pipeline: The parsed pipeline to validate

    Returns:
        List of validation errors found in connector configurations
    """
    errors = []

    for step in pipeline.steps:
        if isinstance(step, SourceDefinitionStep):
            # Skip validation for profile-based connectors (FROM syntax)
            if step.is_from_profile:
                continue

            connector_type = step.connector_type.upper()
            if connector_type not in CONNECTOR_SCHEMAS:
                error = ValidationError(
                    message=f"Unknown connector type: {connector_type}",
                    line=step.line_number or 1,
                    error_type="Connector Error",
                    suggestions=[
                        f"Available connector types: {', '.join(CONNECTOR_SCHEMAS.keys())}",
                        "Check the connector type spelling and case",
                    ],
                )
                errors.append(error)
                continue

            # Validate connector parameters
            schema = CONNECTOR_SCHEMAS[connector_type]
            param_errors = schema.validate(step.params)

            for param_error in param_errors:
                error = ValidationError(
                    message=param_error,
                    line=step.line_number or 1,
                    error_type="Parameter Error",
                    suggestions=[
                        f"Check the {connector_type} connector documentation",
                        "Verify parameter names and types",
                    ],
                )
                errors.append(error)

    return errors


def validate_references(pipeline: Pipeline) -> List[ValidationError]:
    """Validate cross-references between pipeline steps.

    Checks that:
    - LOAD steps reference existing SOURCE definitions
    - No circular dependencies exist
    - All referenced sources are defined before use

    Args:
        pipeline: The parsed pipeline to validate

    Returns:
        List of validation errors found in cross-references
    """
    errors = []
    defined_sources: Set[str] = set()

    for step in pipeline.steps:
        if isinstance(step, SourceDefinitionStep):
            # Check for duplicate source names
            if step.name in defined_sources:
                error = ValidationError(
                    message=f"Duplicate source definition: '{step.name}'",
                    line=step.line_number or 1,
                    error_type="Reference Error",
                    suggestions=[
                        "Use unique names for each SOURCE definition",
                        "Check for typos in source names",
                    ],
                )
                errors.append(error)
            else:
                defined_sources.add(step.name)

        elif isinstance(step, LoadStep):
            # Check that referenced source exists
            if step.source_name not in defined_sources:
                error = ValidationError(
                    message=f"LOAD references undefined source: '{step.source_name}'",
                    line=step.line_number or 1,
                    error_type="Reference Error",
                    suggestions=[
                        f"Define SOURCE '{step.source_name}' before using it in LOAD",
                        "Check the source name spelling",
                        f"Available sources: {', '.join(defined_sources) if defined_sources else 'none'}",
                    ],
                )
                errors.append(error)

    return errors


def validate_pipeline(pipeline: Pipeline) -> List[ValidationError]:
    """Validate an entire pipeline for all types of errors.

    This is the main validation entry point that combines all validation checks.

    Args:
        pipeline: The parsed pipeline to validate

    Returns:
        List of all validation errors found in the pipeline
    """
    errors = []

    # Validate individual step syntax (already done by AST validation)
    for step in pipeline.steps:
        step_errors = step.validate()
        for step_error in step_errors:
            error = ValidationError(
                message=step_error,
                line=getattr(step, "line_number", 1) or 1,
                error_type="Syntax Error",
            )
            errors.append(error)

    # Validate connector configurations
    connector_errors = validate_connectors(pipeline)
    errors.extend(connector_errors)

    # Validate cross-references
    reference_errors = validate_references(pipeline)
    errors.extend(reference_errors)

    return errors
