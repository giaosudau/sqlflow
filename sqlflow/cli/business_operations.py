"""Business operations for SQLFlow CLI.

This module contains the core business logic functions that are called by CLI commands.
Each function has a single responsibility and uses factory functions for dependencies.

Following technical design principles:
- Single responsibility per function
- Type-safe function signatures
- Clean separation from CLI presentation layer
- Use factory functions for dependency management
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlflow.cli.errors import (
    PipelineNotFoundError,
    PipelineValidationError,
    ProfileNotFoundError,
)
from sqlflow.cli.factories import (
    create_executor_for_command,
    get_planner,
    load_project_for_command,
)
from sqlflow.logging import get_logger
from sqlflow.parser.parser import Parser

logger = get_logger(__name__)


# Pipeline Operations
def compile_pipeline_operation(
    pipeline_name: str,
    profile_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    output_dir: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    """Compile a pipeline to execution plan.

    Args:
        pipeline_name: Name of the pipeline to compile
        profile_name: Profile to use for compilation
        variables: Variables for substitution
        output_dir: Directory to save compilation output

    Returns:
        Tuple of (operations_list, output_path)

    Raises:
        PipelineNotFoundError: If pipeline file not found
        ProfileNotFoundError: If profile not found
        PipelineValidationError: If pipeline validation fails
    """
    # Load project using factory
    project = load_project_for_command(profile_name)
    if not project:
        raise ProfileNotFoundError(
            profile_name or "dev", ["No SQLFlow project found in current directory"]
        )

    # Find pipeline file
    pipeline_path = project.get_pipeline_path(pipeline_name)
    if not os.path.exists(pipeline_path):
        # Look for available pipelines to suggest
        pipelines_dir = os.path.dirname(pipeline_path)
        available_pipelines = []
        if os.path.exists(pipelines_dir):
            available_pipelines = [
                f[:-3] for f in os.listdir(pipelines_dir) if f.endswith(".sf")
            ]

        raise PipelineNotFoundError(pipeline_name, [pipelines_dir], available_pipelines)

    # Parse pipeline
    try:
        parser = Parser()
        with open(pipeline_path, "r") as f:
            pipeline_content = f.read()

        pipeline = parser.parse(pipeline_content)
        logger.debug(f"Parsed pipeline '{pipeline_name}' successfully")
    except Exception as e:
        raise PipelineValidationError(pipeline_name, [str(e)])

    # Create execution plan using factory
    planner = get_planner()
    try:
        operations = planner.create_plan(
            pipeline,
            variables=variables,
            profile_variables=project.profile.get("variables", {}),
        )
        logger.debug(f"Created execution plan with {len(operations)} operations")
    except Exception as e:
        raise PipelineValidationError(pipeline_name, [f"Planning failed: {str(e)}"])

    # Save compilation result
    output_path = save_compilation_result(
        operations, pipeline_name, output_dir or "target"
    )

    return operations, output_path


def run_pipeline_operation(
    pipeline_name: str,
    profile_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run a pipeline with execution.

    Args:
        pipeline_name: Name of the pipeline to run
        profile_name: Profile to use for execution
        variables: Variables for substitution

    Returns:
        Execution results dictionary

    Raises:
        PipelineNotFoundError: If pipeline file not found
        ProfileNotFoundError: If profile not found
        PipelineValidationError: If pipeline validation fails
    """
    # Compile pipeline first
    operations, _ = compile_pipeline_operation(pipeline_name, profile_name, variables)

    # Execute using factory-created executor
    executor = create_executor_for_command(profile_name, variables)

    try:
        results = executor.execute(operations, variables)
        logger.info(f"Pipeline '{pipeline_name}' executed successfully")
        return results
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise PipelineValidationError(pipeline_name, [f"Execution failed: {str(e)}"])


def list_pipelines_operation(
    profile_name: Optional[str] = None,
) -> List[Dict[str, str]]:
    """List available pipelines.

    Args:
        profile_name: Profile to use for listing

    Returns:
        List of pipeline dictionaries with name and path

    Raises:
        ProfileNotFoundError: If no project found
    """
    project = load_project_for_command(profile_name)
    if not project:
        raise ProfileNotFoundError(
            profile_name or "dev", ["No SQLFlow project found in current directory"]
        )

    # Find pipelines directory
    pipelines_path = project.get_path("pipelines") or "pipelines"
    pipelines_dir = os.path.join(project.project_dir, pipelines_path)

    pipelines = []
    if os.path.exists(pipelines_dir):
        for filename in os.listdir(pipelines_dir):
            if filename.endswith(".sf"):
                pipeline_name = filename[:-3]
                pipeline_path = os.path.join(pipelines_dir, filename)

                pipelines.append(
                    {
                        "name": pipeline_name,
                        "path": pipeline_path,
                        "size": str(os.path.getsize(pipeline_path)),
                        "modified": str(os.path.getmtime(pipeline_path)),
                    }
                )

    # Sort by name for consistent output
    pipelines.sort(key=lambda p: p["name"])
    logger.debug(f"Found {len(pipelines)} pipelines")

    return pipelines


def validate_pipeline_operation(
    pipeline_name: str, profile_name: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """Validate a pipeline without executing.

    Args:
        pipeline_name: Name of the pipeline to validate
        profile_name: Profile to use for validation

    Returns:
        Tuple of (is_valid, error_messages)

    Raises:
        PipelineNotFoundError: If pipeline file not found
        ProfileNotFoundError: If profile not found
    """
    try:
        # Try to compile pipeline - this validates syntax and planning
        operations, _ = compile_pipeline_operation(pipeline_name, profile_name)
        logger.debug(f"Pipeline '{pipeline_name}' validation passed")
        return True, []
    except PipelineValidationError as e:
        return False, e.errors
    except (PipelineNotFoundError, ProfileNotFoundError):
        # Re-raise these for proper CLI handling
        raise


# Profile Operations
def list_profiles_operation(project_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """List available profiles.

    Args:
        project_dir: Optional project directory (defaults to current directory)

    Returns:
        List of profile dictionaries

    Raises:
        ProfileNotFoundError: If no project found
    """
    # Use specified project directory or current directory
    current_dir = project_dir or os.getcwd()
    profiles_dir = os.path.join(current_dir, "profiles")
    profiles = []

    # Check if profiles directory exists
    if not os.path.exists(profiles_dir):
        raise ProfileNotFoundError(
            "profiles", [f"Profiles directory not found: {profiles_dir}"]
        )

    if os.path.exists(profiles_dir):
        for filename in os.listdir(profiles_dir):
            if filename.endswith(".yml") or filename.endswith(".yaml"):
                profile_name = filename.rsplit(".", 1)[0]
                profile_path = os.path.join(profiles_dir, filename)

                # Try to determine if profile is valid
                # Use simpler validation - just check if YAML is parseable
                try:
                    import yaml

                    with open(profile_path, "r") as f:
                        yaml.safe_load(f)
                    status = "valid"
                    description = "Profile loaded successfully"
                except Exception as e:
                    status = "invalid"
                    description = f"Error: {str(e)[:50]}..."

                profiles.append(
                    {
                        "name": profile_name,
                        "path": profile_path,
                        "status": status,
                        "description": description,
                    }
                )

    # Check if no profiles were found
    if not profiles:
        raise ProfileNotFoundError(
            "profiles", [f"No profile files found in: {profiles_dir}"]
        )

    # Sort by name for consistent output
    profiles.sort(key=lambda p: p["name"])
    logger.debug(f"Found {len(profiles)} profiles")

    return profiles


def validate_profile_operation(profile_name: str) -> Tuple[bool, List[str]]:
    """Validate a profile configuration.

    Args:
        profile_name: Name of the profile to validate

    Returns:
        Tuple of (is_valid, error_messages)

    Raises:
        ProfileNotFoundError: If profile not found
    """
    try:
        project = load_project_for_command(profile_name)
        if not project:
            return False, [f"Profile '{profile_name}' not found"]

        # If we got here, profile loaded successfully
        logger.debug(f"Profile '{profile_name}' validation passed")
        return True, []
    except ValueError as e:
        # Profile validation errors
        return False, [str(e)]
    except Exception as e:
        return False, [f"Error validating profile: {str(e)}"]


# Utility Operations
def init_project_operation(project_dir: str, project_name: str) -> str:
    """Initialize a new SQLFlow project.

    Args:
        project_dir: Directory to create project in
        project_name: Name of the project

    Returns:
        Path to created project

    Raises:
        OSError: If directory creation fails
    """
    from sqlflow.project import Project

    try:
        # Create project directory
        os.makedirs(project_dir, exist_ok=True)

        # Initialize project structure
        Project.init(project_dir, project_name)

        logger.info(f"Initialized SQLFlow project '{project_name}' in {project_dir}")
        return project_dir
    except Exception as e:
        logger.error(f"Failed to initialize project: {e}")
        raise


# Helper Functions
def save_compilation_result(
    operations: List[Dict[str, Any]], pipeline_name: str, output_dir: str = "target"
) -> str:
    """Save compilation result to file.

    Args:
        operations: List of compiled operations
        pipeline_name: Name of the pipeline
        output_dir: Directory to save output, or complete file path if ends with .json

    Returns:
        Path to saved file
    """
    # Check if output_dir is actually a complete file path
    if output_dir.endswith(".json"):
        output_path = output_dir
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
    else:
        # Create compiled subdirectory in output directory
        compiled_dir = os.path.join(output_dir, "compiled")

        # Ensure output directory exists
        os.makedirs(compiled_dir, exist_ok=True)

        # Create output filename
        output_filename = f"{pipeline_name}.json"
        output_path = os.path.join(compiled_dir, output_filename)

    # Save compilation result
    with open(output_path, "w") as f:
        json.dump(
            {
                "pipeline_name": pipeline_name,
                "operations": operations,
                "operation_count": len(operations),
            },
            f,
            indent=2,
        )

    logger.debug(f"Saved compilation result to {output_path}")
    return output_path
