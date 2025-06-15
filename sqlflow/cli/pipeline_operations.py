"""Pipeline operations with type hints and single responsibility.

This module provides type-safe, single-responsibility functions for pipeline operations,
following the technical design's approach to break down large functions.
"""

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from sqlflow.project import Project

from sqlflow.cli.display import display_compilation_success
from sqlflow.cli.errors import (
    PipelineCompilationError,
    PipelineNotFoundError,
    ProfileNotFoundError,
    VariableParsingError,
)

logger = logging.getLogger(__name__)


def load_project(profile: str) -> "Project":
    """Load project configuration for given profile.

    Args:
        profile: Profile name to load

    Returns:
        Project instance with loaded configuration

    Raises:
        ProfileNotFoundError: If profile cannot be found
    """
    from sqlflow.project import Project

    try:
        project_dir = os.getcwd()
        project = Project(project_dir)
        # Validate profile exists by trying to get it
        profile_dict = project.get_profile()
        return project
    except Exception as e:
        # Extract available profiles for better error message
        available_profiles = []
        try:
            # Try to get available profiles from project
            available_profiles = (
                project.get_available_profiles() if "project" in locals() else []
            )
        except Exception:
            pass

        raise ProfileNotFoundError(profile, available_profiles) from e


def load_pipeline(name: str, project: "Project") -> str:
    """Load and validate pipeline file.

    Args:
        name: Pipeline name (with or without .sf extension)
        project: Project instance

    Returns:
        Pipeline file content as string

    Raises:
        PipelineNotFoundError: If pipeline file cannot be found
    """
    try:
        pipeline_path = _resolve_pipeline_path(project, name)
        return _read_pipeline_file(pipeline_path)
    except FileNotFoundError:
        # Get available pipelines for better error message
        available_pipelines = _get_available_pipelines(project)
        search_paths = [str(Path(project.project_dir) / "pipelines")]

        raise PipelineNotFoundError(name, search_paths, available_pipelines)


def parse_variables(variables_str: Optional[str]) -> Dict[str, Any]:
    """Parse variables from JSON string or key=value format.

    Args:
        variables_str: Variables as JSON string or key=value pairs

    Returns:
        Parsed variables dictionary

    Raises:
        VariableParsingError: If parsing fails
    """
    if not variables_str:
        return {}

    try:
        # Try JSON first
        if variables_str.strip().startswith("{"):
            return json.loads(variables_str)

        # Try key=value format
        variables = {}
        for pair in variables_str.split(","):
            if "=" not in pair:
                raise ValueError(f"Invalid format: {pair}")
            key, value = pair.split("=", 1)

            # Try to parse value as JSON, fallback to string
            try:
                variables[key.strip()] = json.loads(value.strip())
            except json.JSONDecodeError:
                variables[key.strip()] = value.strip()

        return variables

    except (json.JSONDecodeError, ValueError) as e:
        raise VariableParsingError(variables_str, str(e)) from e


def substitute_variables(
    pipeline_text: str,
    variables: Dict[str, Any],
    profile_variables: Optional[Dict[str, Any]] = None,
) -> str:
    """Substitute variables in pipeline text with priority-based resolution.

    Args:
        pipeline_text: Original pipeline content
        variables: CLI variables (highest priority)
        profile_variables: Profile variables (lower priority)

    Returns:
        Pipeline text with variables substituted
    """
    from sqlflow.core.variables.manager import VariableConfig, VariableManager

    # Use new VariableManager with priority-based resolution
    config = VariableConfig(
        cli_variables=variables,
        profile_variables=profile_variables or {},
        set_variables={},  # SET variables will be extracted during parsing
    )
    manager = VariableManager(config)
    substituted_text = manager.substitute(pipeline_text)

    # Log any missing variables using new validation system
    validation_result = manager.validate(substituted_text)
    missing_vars = validation_result.missing_variables
    if missing_vars:
        logger.warning(
            f"Missing variables during compilation: {', '.join(missing_vars)}"
        )

    return substituted_text


def plan_pipeline(
    pipeline_text: str, project: "Project", variables: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Plan pipeline operations from parsed content.

    Args:
        pipeline_text: Pipeline content with variables substituted
        project: Project instance
        variables: All variables for planner validation

    Returns:
        List of operations to execute

    Raises:
        PipelineCompilationError: If planning fails
    """
    from sqlflow.core.planner_main import Planner
    from sqlflow.parser import Parser

    try:
        # Parse the pipeline (now with variables substituted)
        parser = Parser()
        pipeline = parser.parse(pipeline_text)

        # Include environment variables for planner validation
        # Environment variables should have the lowest priority
        planner_variables = dict(os.environ)
        planner_variables.update(variables)

        # Create a plan with planner
        planner = Planner()
        operations = planner.create_plan(pipeline, variables=planner_variables)

        return operations

    except Exception as e:
        raise PipelineCompilationError("unknown", str(e)) from e


def save_compilation_result(
    operations: List[Dict[str, Any]],
    pipeline_name: str,
    output_dir: Optional[str] = None,
) -> str:
    """Save compilation result to disk.

    Args:
        operations: List of operations to save
        pipeline_name: Name of the pipeline
        output_dir: Optional custom output directory

    Returns:
        Path where the plan was saved
    """
    if output_dir:
        target_path = output_dir
    else:
        target_dir = "target/compiled"
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, f"{pipeline_name}.json")

    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    with open(target_path, "w") as f:
        json.dump(operations, f, indent=2)
        logger.debug(f"Saved execution plan to {target_path}")

    return target_path


def compile_pipeline_to_plan(
    pipeline_name: str,
    profile: str = "dev",
    variables: Optional[Dict[str, Any]] = None,
    output_dir: Optional[str] = None,
    save_plan: bool = True,
) -> List[Dict[str, Any]]:
    """Compile a pipeline to execution plan using type-safe functions.

    This function replaces the large _compile_pipeline_to_plan function
    by orchestrating smaller, single-responsibility functions.

    Args:
        pipeline_name: Name of the pipeline to compile
        profile: Profile to use for compilation
        variables: Variables to substitute in the pipeline
        output_dir: Optional custom output directory
        save_plan: Whether to save the plan to disk

    Returns:
        The execution plan as a list of operations

    Raises:
        Various CLI exceptions for specific error cases
    """
    try:
        # Load project configuration
        project = load_project(profile)

        # Load pipeline content
        pipeline_text = load_pipeline(pipeline_name, project)

        # Get profile variables
        profile_dict = project.get_profile()
        profile_variables = (
            profile_dict.get("variables", {}) if isinstance(profile_dict, dict) else {}
        )

        # Combine all variables with proper priority
        all_variables = {}
        if profile_variables:
            all_variables.update(profile_variables)
        if variables:
            all_variables.update(variables)

        # Substitute variables in pipeline text
        pipeline_text = substitute_variables(
            pipeline_text, variables or {}, profile_variables
        )

        # Plan the pipeline operations
        operations = plan_pipeline(pipeline_text, project, all_variables)

        # Save the plan if requested
        output_path = ""
        if save_plan:
            output_path = save_compilation_result(operations, pipeline_name, output_dir)

        # Display success with Rich formatting
        display_compilation_success(
            pipeline_name, profile, len(operations), output_path
        )

        return operations

    except (PipelineNotFoundError, VariableParsingError, PipelineCompilationError):
        # These are already properly formatted CLI errors
        raise
    except Exception as e:
        # Wrap any unexpected errors
        raise PipelineCompilationError(pipeline_name, str(e)) from e


# Helper functions (internal)


def _resolve_pipeline_path(project: "Project", pipeline_name: str) -> str:
    """Resolve pipeline path from name."""
    from sqlflow.cli.pipeline import _resolve_pipeline_path as original_resolve

    return original_resolve(project, pipeline_name)


def _read_pipeline_file(pipeline_path: str) -> str:
    """Read pipeline file content."""
    from sqlflow.cli.pipeline import _read_pipeline_file as original_read

    return original_read(pipeline_path)


def _get_available_pipelines(project: "Project") -> List[str]:
    """Get list of available pipeline names."""
    try:
        pipelines_dir = Path(project.project_dir) / "pipelines"
        if not pipelines_dir.exists():
            return []

        pipelines = []
        for file_path in pipelines_dir.glob("*.sf"):
            pipelines.append(file_path.stem)

        return sorted(pipelines)
    except Exception:
        return []
