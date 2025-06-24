"""Legacy pipeline operations module for backward compatibility with tests.

This module provides backward compatibility for tests that import from
sqlflow.cli.pipeline_operations. All functionality has been moved to
sqlflow.cli.business_operations.
"""

# Import all functions from business_operations for backward compatibility
from sqlflow.cli.business_operations import (
    compile_pipeline_operation,
    init_project_operation,
    list_pipelines_operation,
    run_pipeline_operation,
    save_compilation_result,
    validate_pipeline_operation,
)


# Additional legacy functions that tests might expect
def compile_pipeline_to_plan(*args, **kwargs):
    """Legacy alias for compile_pipeline_operation."""
    from sqlflow.cli.errors import (
        PipelineCompilationError,
        PipelineNotFoundError,
        PipelineValidationError,
        ProfileNotFoundError,
    )

    # Handle save_plan parameter for backward compatibility
    save_plan = kwargs.pop("save_plan", True)

    try:
        # Extract parameters
        pipeline_name = args[0] if args else kwargs.get("pipeline_name")
        profile_name = args[1] if len(args) > 1 else kwargs.get("profile_name")
        variables = args[2] if len(args) > 2 else kwargs.get("variables")

        # Load project
        project = load_project(profile_name)

        # Load pipeline
        pipeline_content = load_pipeline(pipeline_name, project)

        # Substitute variables - ensure we pass string content, not Pipeline object
        if hasattr(pipeline_content, "text") or hasattr(pipeline_content, "__str__"):
            content_str = str(pipeline_content)
        else:
            content_str = pipeline_content
        substituted_content = substitute_variables(content_str, variables or {})

        # Plan pipeline
        operations = plan_pipeline(substituted_content, project, variables)

        if save_plan:
            # Save the plan
            save_compilation_result(operations, pipeline_name)

        # Display success
        display_compilation_success(operations, None)

        return operations
    except (PipelineNotFoundError, ProfileNotFoundError, PipelineValidationError):
        # Re-raise CLI-specific errors as-is
        raise
    except Exception as e:
        # Convert other exceptions to PipelineCompilationError for backward compatibility
        pipeline_name = args[0] if args else kwargs.get("pipeline_name", "unknown")
        raise PipelineCompilationError(
            f"Failed to compile pipeline '{pipeline_name}': {str(e)}"
        )


def load_project(*args, **kwargs):
    """Legacy function - replaced by load_project_for_command in factories."""
    from sqlflow.cli.factories import load_project_for_command

    return load_project_for_command(*args, **kwargs)


def _convert_value_type(value_str):
    """Convert string value to appropriate type."""
    value = value_str.strip()
    if value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False
    elif value.isdigit():
        return int(value)
    elif value.replace(".", "", 1).isdigit():
        return float(value)
    else:
        return value


def _parse_key_value_pairs(variables_str):
    """Parse key=value pairs from string."""
    from sqlflow.cli.errors import VariableParsingError

    result = {}
    for pair in variables_str.split(","):
        if "=" not in pair:
            raise VariableParsingError(
                variables_str, f"Invalid format: '{pair}' (expected key=value)"
            )
        key, value = pair.split("=", 1)
        result[key.strip()] = _convert_value_type(value)
    return result


def parse_variables(variables_str):
    """Parse variables from JSON or key=value format with proper type conversion."""
    import json

    from sqlflow.cli.errors import VariableParsingError

    if not variables_str:
        return {}

    try:
        # Try JSON format first
        return json.loads(variables_str)
    except json.JSONDecodeError as json_error:
        # If it looks like JSON (starts with { or [), preserve the JSON error
        if variables_str.strip().startswith(("{", "[")):
            raise VariableParsingError(variables_str, str(json_error))

        # Try key=value parsing with type conversion
        try:
            return _parse_key_value_pairs(variables_str)
        except VariableParsingError:
            raise
        except Exception as e:
            raise VariableParsingError(variables_str, str(e))


def substitute_variables(template: str, variables: dict) -> str:
    """Substitute variables in template string using V2 functions."""
    from sqlflow.core.variables import substitute_variables as v2_substitute

    # Use V2 substitute_variables directly - simpler and more efficient
    return v2_substitute(template, variables or {})


def plan_pipeline(pipeline_text, project, variables):
    """Legacy plan pipeline function."""
    from sqlflow.core.planner_main import Planner
    from sqlflow.parser import Parser

    # Parse the pipeline (now with variables substituted)
    parser = Parser()
    pipeline = parser.parse(pipeline_text)

    # Create a plan with planner
    planner = Planner()
    operations = planner.create_plan(pipeline, variables=variables or {})

    return operations


# Re-export all functions
__all__ = [
    "compile_pipeline_operation",
    "list_pipelines_operation",
    "run_pipeline_operation",
    "validate_pipeline_operation",
    "init_project_operation",
    "save_compilation_result",
    "compile_pipeline_to_plan",
    "load_project",
    "parse_variables",
    "substitute_variables",
    "plan_pipeline",
]

# Add missing logger for tests
import logging

logger = logging.getLogger(__name__)


# Add missing display functions for tests
def display_compilation_success(*args, **kwargs):
    """Legacy display function for tests."""
    from sqlflow.cli.display import display_compilation_success as real_func

    return real_func(*args, **kwargs)


def load_pipeline(*args, **kwargs):
    """Legacy load pipeline function."""
    from sqlflow.parser.parser import Parser

    parser = Parser()
    return parser.parse(*args, **kwargs)
