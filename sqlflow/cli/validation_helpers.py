"""Helper functions for CLI validation integration.

Provides utilities for validating pipelines and formatting errors for CLI output.
"""

import os
from pathlib import Path
from typing import List, Optional

import typer

from sqlflow.core.variable_substitution import VariableSubstitutionEngine
from sqlflow.logging import get_logger
from sqlflow.parser.parser import Parser
from sqlflow.project import Project
from sqlflow.validation import AggregatedValidationError
from sqlflow.validation.errors import ValidationError

logger = get_logger(__name__)


def _read_pipeline_file(pipeline_path: str) -> str:
    """Read pipeline file content with error handling.

    Args:
    ----
        pipeline_path: Path to the pipeline file

    Returns:
    -------
        Pipeline file content

    Raises:
    ------
        typer.Exit: If file cannot be read

    """
    pipeline_file = Path(pipeline_path)
    if not pipeline_file.exists():
        typer.echo(f"❌ Pipeline file not found: {pipeline_path}")
        raise typer.Exit(1)

    try:
        with open(pipeline_file, "r", encoding="utf-8") as f:
            return f.read()
    except (OSError, UnicodeDecodeError) as e:
        typer.echo(f"❌ Cannot read pipeline file {pipeline_path}: {str(e)}")
        raise typer.Exit(1)


def _get_profile_variables(profile_name: Optional[str] = None) -> dict:
    """Get profile variables from the current project.

    Args:
    ----
        profile_name: Name of the profile to use, None for default

    Returns:
    -------
        Dictionary of profile variables, empty if none found
    """
    try:
        project_dir = os.getcwd()
        project = Project(project_dir, profile_name=profile_name)
        profile_dict = project.get_profile()
        return (
            profile_dict.get("variables", {}) if isinstance(profile_dict, dict) else {}
        )
    except Exception as e:
        logger.debug(f"Could not load profile variables: {e}")
        return {}


def _apply_variable_substitution(pipeline_text: str, profile_variables: dict) -> str:
    """Apply variable substitution to pipeline text.

    Args:
    ----
        pipeline_text: Pipeline content to process
        profile_variables: Variables from the project profile

    Returns:
    -------
        Pipeline text with variables substituted
    """
    # Combine all variables with proper priority
    all_variables = {}

    # Add profile variables (lower priority)
    if profile_variables:
        all_variables.update(profile_variables)
        logger.debug(f"Added profile variables for validation: {profile_variables}")

    # Create substitution engine that also checks environment variables
    engine = VariableSubstitutionEngine(all_variables)
    result = engine.substitute(pipeline_text)

    logger.debug("Applied variable substitution for validation")

    # Log any missing variables (those without defaults)
    missing_vars = engine.validate_required_variables(pipeline_text)
    if missing_vars:
        logger.debug(f"Missing variables during validation: {', '.join(missing_vars)}")

    return result


def _enhance_table_reference_errors(
    errors: List[ValidationError], pipeline_text: str
) -> List[ValidationError]:
    """Enhance table reference errors with available tables/sources context.

    Args:
    ----
        errors: List of validation errors
        pipeline_text: Pipeline content to analyze

    Returns:
    -------
        Enhanced list of validation errors
    """
    enhanced_errors = []

    # Extract available tables and sources from pipeline
    available_tables = _extract_available_tables_from_pipeline(pipeline_text)
    available_sources = _extract_available_sources_from_pipeline(pipeline_text)

    for error in errors:
        # Check if this is a table reference error
        if "might not be defined" in error.message and "tables" in error.message:
            # Enhance the error with helpful context
            enhanced_suggestions = list(error.suggestions) if error.suggestions else []

            # Add table/source context like in runtime errors
            if available_tables:
                enhanced_suggestions.append(
                    f"Available tables: {', '.join(sorted(available_tables))}"
                )
            if available_sources:
                enhanced_suggestions.append(
                    f"Available sources: {', '.join(sorted(available_sources))}"
                )

            if not available_tables and not available_sources:
                enhanced_suggestions.append(
                    "No tables or sources are defined yet in this pipeline"
                )

            enhanced_suggestions.append(
                "Check that referenced tables are created by previous steps or defined as SOURCE statements"
            )

            enhanced_error = ValidationError(
                message=error.message,
                line=error.line,
                column=error.column,
                error_type=error.error_type,
                suggestions=enhanced_suggestions,
                help_url=error.help_url,
            )
            enhanced_errors.append(enhanced_error)
        else:
            # Keep other errors unchanged
            enhanced_errors.append(error)

    return enhanced_errors


def _extract_available_tables_from_pipeline(pipeline_text: str) -> List[str]:
    """Extract table names that can be queried from pipeline text.

    Args:
    ----
        pipeline_text: Pipeline content to analyze

    Returns:
    -------
        List of table names that can be used in SQL queries
    """
    import re

    tables = set()

    # Find LOAD statements that create tables
    load_pattern = r"LOAD\s+(\w+)\s+FROM"
    load_matches = re.findall(load_pattern, pipeline_text, re.IGNORECASE)
    for table_name in load_matches:
        tables.add(table_name.lower())

    # Find CREATE TABLE statements
    create_pattern = r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\w+)"
    create_matches = re.findall(create_pattern, pipeline_text, re.IGNORECASE)
    for table_name in create_matches:
        tables.add(table_name.lower())

    return list(tables)


def _extract_available_sources_from_pipeline(pipeline_text: str) -> List[str]:
    """Extract SOURCE definition names from pipeline text.

    Args:
    ----
        pipeline_text: Pipeline content to analyze

    Returns:
    -------
        List of source names that can be referenced in LOAD statements
    """
    import re

    sources = set()

    # Find SOURCE definitions
    source_pattern = r"SOURCE\s+(\w+)\s+TYPE"
    source_matches = re.findall(source_pattern, pipeline_text, re.IGNORECASE)
    for source_name in source_matches:
        sources.add(source_name.lower())

    return list(sources)


def _parse_and_validate_pipeline(
    pipeline_text: str, pipeline_path: str, profile_name: Optional[str] = None
) -> List[ValidationError]:
    """Parse and validate pipeline text.

    Args:
    ----
        pipeline_text: Pipeline content to parse
        pipeline_path: Path for logging purposes
        profile_name: Name of the profile to use for variable resolution

    Returns:
    -------
        List of validation errors

    """
    parser = Parser()
    errors = []

    try:
        # Get profile variables and apply substitution
        profile_variables = _get_profile_variables(profile_name)
        pipeline_text = _apply_variable_substitution(pipeline_text, profile_variables)

        # Parse with validation enabled (now with variables substituted)
        pipeline = parser.parse(pipeline_text, validate=True)
        logger.debug("Pipeline parsing passed for %s", pipeline_path)

        # ENHANCED: Also run planner-level validation to catch table reference errors
        planner_errors = _validate_with_planner(
            pipeline, pipeline_text, pipeline_path, profile_name
        )
        errors.extend(planner_errors)

    except ValidationError as e:
        # Single validation error
        errors = [e]
        logger.debug("Pipeline validation found 1 error for %s", pipeline_path)

    except AggregatedValidationError as e:
        # Multiple validation errors
        errors = e.errors
        logger.debug(
            "Pipeline validation found %d errors for %s", len(errors), pipeline_path
        )

    except Exception as e:
        # Parser error or other issues
        error_msg = str(e)
        if "Multiple errors found:" in error_msg:
            # Handle multiple parsing errors - convert to validation errors
            errors = [
                ValidationError(message=error_msg, line=1, error_type="Parser Error")
            ]
        else:
            # Single parser error
            errors = [
                ValidationError(message=error_msg, line=1, error_type="Parser Error")
            ]
        logger.debug("Pipeline parsing failed for %s: %s", pipeline_path, error_msg)

    # Enhance table reference errors with available tables/sources context
    if errors:
        errors = _enhance_table_reference_errors(errors, pipeline_text)

    return errors


def _validate_with_planner(
    pipeline, pipeline_text: str, pipeline_path: str, profile_name: Optional[str] = None
) -> List[ValidationError]:
    """Run planner-level validation to catch table reference errors.

    Args:
    ----
        pipeline: Parsed pipeline AST
        pipeline_text: Original pipeline text for context
        pipeline_path: Path for logging
        profile_name: Name of the profile to use for variable resolution

    Returns:
    -------
        List of validation errors from planner analysis
    """
    try:
        captured_warnings = _capture_planner_warnings(pipeline, profile_name)
        if captured_warnings:
            return _process_captured_warnings(
                captured_warnings, pipeline_text, profile_name
            )
        return []
    except Exception as e:
        logger.debug(f"Could not run planner validation for {pipeline_path}: {e}")
        return []


def _capture_planner_warnings(pipeline, profile_name: Optional[str] = None):
    """Capture warnings from planner execution.

    Args:
    ----
        pipeline: Parsed pipeline AST
        profile_name: Name of the profile to use for variable resolution
    """
    import logging

    from sqlflow.core.planner import Planner

    captured_warnings = []

    class ValidationWarningHandler(logging.Handler):
        def emit(self, record):
            if "might not be defined" in record.getMessage():
                captured_warnings.append(record.getMessage())

    # Set up warning capture
    planner_logger = logging.getLogger("sqlflow.core.planner")
    warning_handler = ValidationWarningHandler()
    warning_handler.setLevel(logging.WARNING)
    planner_logger.addHandler(warning_handler)

    try:
        planner = Planner()
        planner_variables = _get_planner_variables(profile_name)
        planner.create_plan(pipeline, variables=planner_variables)
    finally:
        planner_logger.removeHandler(warning_handler)

    return captured_warnings


def _get_planner_variables(profile_name: Optional[str] = None):
    """Get variables for planner context.

    Args:
    ----
        profile_name: Name of the profile to use for variable resolution
    """
    import os

    planner_variables = dict(os.environ)
    profile_variables = _get_profile_variables(profile_name)
    if profile_variables:
        planner_variables.update(profile_variables)

    # Add default variables for conditional pipeline validation
    # This helps with validating conditional pipelines that need context
    default_validation_vars = {
        "env": "dev",
        "environment": "dev",
        "region": "global",
        "target_region": "global",
        "enable_enrichment": "false",
        "segmentation_model": "basic",
        "enable_export_csv": "true",
        "enable_export_json": "false",
        "enable_export_warehouse": "false",
        "enable_address_verification": "false",
        "use_production_data": "false",
        "use_sample_data": "true",
    }

    # Only add defaults if not already set
    for key, value in default_validation_vars.items():
        if key not in planner_variables:
            planner_variables[key] = value

    return planner_variables


def _process_captured_warnings(
    captured_warnings, pipeline_text, profile_name: Optional[str] = None
):
    """Process captured warnings and convert to validation errors."""
    validation_errors = []
    available_tables = _extract_available_tables_from_pipeline(pipeline_text)
    available_sources = _extract_available_sources_from_pipeline(pipeline_text)

    for warning in captured_warnings:
        error = _process_warning_for_validation(
            warning, available_tables, available_sources, profile_name
        )
        if error:
            validation_errors.append(error)

    return validation_errors


def _process_warning_for_validation(
    warning, available_tables, available_sources, profile_name: Optional[str] = None
):
    """Process a single warning and create validation error if needed."""
    import re

    # Extract line number and table name
    line_number = 1
    undefined_table = None

    line_match = re.search(r"line (\d+)", warning)
    if line_match:
        line_number = int(line_match.group(1))

    if "references tables that might not be defined:" in warning:
        parts = warning.split("references tables that might not be defined:")
        if len(parts) > 1:
            undefined_table = parts[1].strip()

    if not undefined_table:
        return None

    # Skip validation for tables that are likely conditional context tables
    # These are created within conditional blocks and may not exist during validation
    conditional_table_patterns = [
        "customers_raw",
        "sales_raw",
        "products_raw",  # Raw loaded tables
        "dev_customers",
        "dev_products",
        "dev_sales",  # Development samples
        "filtered_sales",  # Filtered tables in conditional branches
    ]

    if undefined_table in conditional_table_patterns:
        # These are likely conditional tables - don't fail validation
        return None

    # Check for typos using fuzzy matching
    should_fail, suggestions = _check_for_typos(undefined_table, available_tables)

    # Add context
    suggestions.extend(_build_context_suggestions(available_tables, available_sources))

    if should_fail:
        return ValidationError(
            message=f"Referenced table '{undefined_table}' might not be defined",
            line=line_number,
            error_type="Table Reference Error",
            suggestions=suggestions,
        )

    return None


def _check_for_typos(undefined_table, available_tables):
    """Check if undefined table is likely a typo."""
    import difflib

    suggestions = []
    should_fail = False

    if undefined_table and available_tables:
        close_matches = difflib.get_close_matches(
            undefined_table, available_tables, n=3, cutoff=0.6
        )
        if close_matches:
            best_match = close_matches[0]
            similarity = difflib.SequenceMatcher(
                None, undefined_table, best_match
            ).ratio()

            if similarity >= 0.7:  # Very similar - likely a typo
                should_fail = True
                suggestions.append(f"Did you mean '{best_match}'?")
            else:
                suggestions.append(f"Similar table available: '{best_match}'")

    return should_fail, suggestions


def _build_context_suggestions(available_tables, available_sources):
    """Build context suggestions for validation errors."""
    suggestions = []

    if available_tables:
        suggestions.append(f"Available tables: {', '.join(sorted(available_tables))}")
    if available_sources:
        suggestions.append(f"Available sources: {', '.join(sorted(available_sources))}")

    suggestions.append(
        "Check that referenced tables are created by previous steps or defined as SOURCE statements"
    )

    return suggestions


def validate_pipeline(
    pipeline_path: str, profile_name: Optional[str] = None
) -> List[ValidationError]:
    """Validate a pipeline file.

    Args:
    ----
        pipeline_path: Path to the pipeline file
        profile_name: Name of the profile to use for variable resolution

    Returns:
    -------
        List of validation errors (empty if valid)

    Raises:
    ------
        typer.Exit: If pipeline file cannot be read

    """
    try:
        # Read and validate pipeline
        pipeline_text = _read_pipeline_file(pipeline_path)
        errors = _parse_and_validate_pipeline(
            pipeline_text, pipeline_path, profile_name
        )
        return errors

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(
            "Unexpected error during validation of %s: %s", pipeline_path, str(e)
        )
        return [
            ValidationError(
                message=f"Validation failed: {str(e)}",
                line=1,
                error_type="Internal Error",
            )
        ]


def _group_errors_by_type(errors: List[ValidationError]) -> dict:
    """Group validation errors by type.

    Args:
    ----
        errors: List of validation errors

    Returns:
    -------
        Dictionary mapping error types to lists of errors

    """
    error_groups = {}
    for error in errors:
        error_type = error.error_type
        if error_type not in error_groups:
            error_groups[error_type] = []
        error_groups[error_type].append(error)
    return error_groups


def _format_single_error(error: ValidationError, show_details: bool) -> List[str]:
    """Format a single validation error.

    Args:
    ----
        error: Validation error to format
        show_details: Whether to show detailed information

    Returns:
    -------
        List of formatted lines for this error

    """
    lines = []

    # Main error line
    if error.column > 0:
        lines.append(f"  Line {error.line}, Column {error.column}: {error.message}")
    else:
        lines.append(f"  Line {error.line}: {error.message}")

    # Show suggestions if available and details requested
    if show_details and error.suggestions:
        for suggestion in error.suggestions:
            lines.append(f"    💡 {suggestion}")

    # Show help URL if available
    if show_details and error.help_url:
        lines.append(f"    📖 Help: {error.help_url}")

    lines.append("")  # Empty line between errors
    return lines


def format_validation_errors_for_cli(
    errors: List[ValidationError], show_details: bool = True
) -> str:
    """Format validation errors for CLI output.

    Args:
    ----
        errors: List of validation errors
        show_details: Whether to show detailed error information

    Returns:
    -------
        Formatted error message string

    """
    if not errors:
        return "✅ Pipeline validation passed!"

    # Group errors by type for better organization
    error_groups = _group_errors_by_type(errors)

    # Format output
    lines = []
    lines.append(f"❌ Pipeline validation failed with {len(errors)} error(s):")
    lines.append("")

    for error_type, type_errors in error_groups.items():
        if len(error_groups) > 1:
            lines.append(f"📋 {error_type}s:")

        for error in type_errors:
            lines.extend(_format_single_error(error, show_details))

    # Add summary
    if not show_details:
        lines.append("Run with --verbose for detailed suggestions.")

    return "\n".join(lines).rstrip()


def print_validation_summary(
    errors: List[ValidationError], pipeline_name: str, quiet: bool = False
) -> None:
    """Print validation summary to console.

    Args:
    ----
        errors: List of validation errors
        pipeline_name: Name of the pipeline being validated
        quiet: Whether to use quiet output mode

    """
    if not errors:
        if not quiet:
            typer.echo(f"✅ Pipeline '{pipeline_name}' validation passed!")
        return

    # Show detailed errors unless in quiet mode
    show_details = not quiet
    formatted_errors = format_validation_errors_for_cli(
        errors, show_details=show_details
    )

    # Use stderr for errors
    typer.echo(formatted_errors, err=True)


def validate_and_exit_on_error(
    pipeline_path: str,
    pipeline_name: str,
    profile_name: Optional[str] = None,
    quiet: bool = False,
) -> None:
    """Validate pipeline and exit with error code if validation fails.

    This is a convenience function for CLI commands that should stop execution
    if validation fails.

    Args:
    ----
        pipeline_path: Path to the pipeline file
        pipeline_name: Name of the pipeline for display
        profile_name: Name of the profile to use for variable resolution
        quiet: Whether to use quiet output mode

    Raises:
    ------
        typer.Exit: If validation fails (exit code 1)

    """
    errors = validate_pipeline(pipeline_path, profile_name)

    if errors:
        print_validation_summary(errors, pipeline_name, quiet=quiet)
        raise typer.Exit(1)

    if not quiet:
        logger.debug("Pipeline validation passed for %s", pipeline_name)
