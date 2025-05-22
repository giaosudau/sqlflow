"""Utility functions for the SQLFlow CLI."""

import json
import os
from typing import Any, Dict, Optional


def resolve_pipeline_name(pipeline_name: str, pipelines_dir: str) -> str:
    """Resolve a pipeline name to a full path.

    Args:
        pipeline_name: Name of the pipeline, with or without .sf extension
        pipelines_dir: Directory containing pipeline files

    Returns:
        Full path to the pipeline file

    Raises:
        FileNotFoundError: If the pipeline file cannot be found
    """
    if pipeline_name.endswith(".sf"):
        base_name = pipeline_name[:-3]
        file_name = pipeline_name
    else:
        base_name = pipeline_name
        file_name = f"{pipeline_name}.sf"

    pipeline_path = os.path.join(pipelines_dir, file_name)
    if os.path.exists(pipeline_path):
        return pipeline_path

    raise FileNotFoundError(
        f"Pipeline '{base_name}' not found in '{pipelines_dir}'. "
        f"Make sure the file '{file_name}' exists."
    )


def parse_vars(vars_input: Optional[str]) -> Dict[str, Any]:
    """Parse pipeline variables from string input.

    Supports both JSON format and key=value pairs.

    Args:
        vars_input: String containing variables in JSON or key=value format

    Returns:
        Dict of parsed variables

    Raises:
        ValueError: If the input cannot be parsed
    """
    if not vars_input:
        return {}

    try:
        return json.loads(vars_input)
    except json.JSONDecodeError:
        result = {}
        try:
            # Support both space-separated and comma-separated key=value pairs
            # Convert commas to spaces first to handle both formats consistently
            normalized_input = vars_input.replace(",", " ")
            for pair in normalized_input.split():
                if "=" not in pair:
                    raise ValueError(f"Invalid key=value pair: {pair}")
                key, value = pair.split("=", 1)
                result[key] = value
            return result
        except Exception as e:
            raise ValueError(
                f"Failed to parse variables. Use JSON format or 'key=value' pairs (space or comma-separated). Error: {e}"
            )
