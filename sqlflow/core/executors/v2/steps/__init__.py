"""Step executors package for V2 architecture.

Provides focused step executors following SOLID principles:
- Individual step executors with Single Responsibility
- Clean protocols and interfaces for dependency inversion
- Registry pattern for extensible step execution
- Proper separation of concerns across step types
"""

import uuid
from typing import Any, Dict

from .base import BaseStepExecutor, StepExecutor
from .export import ExportStepExecutor
from .load import LoadStepExecutor
from .registry import StepRegistry, create_default_registry
from .transform import TransformStepExecutor

# Import step classes from the parent v2 step_definitions.py module
try:
    # Import step classes from the step_definitions.py file in parent directory
    from ..step_definitions import (
        BaseStep,
        ExportStep,
        LoadStep,
        SetVariableStep,
        SourceDefinitionStep,
        TransformStep,
    )
except ImportError:
    # This should not happen in normal operation but provides a fallback
    # Create basic classes to prevent import errors
    from dataclasses import dataclass
    from typing import List

    @dataclass(frozen=True)
    class BaseStep:
        id: str
        type: str
        depends_on: List[str] = None

        def __post_init__(self):
            if self.depends_on is None:
                object.__setattr__(self, "depends_on", [])

    @dataclass(frozen=True)
    class LoadStep(BaseStep):
        source: str = ""
        target_table: str = ""
        type: str = "load"

    @dataclass(frozen=True)
    class TransformStep(BaseStep):
        sql: str = ""
        type: str = "transform"

    @dataclass(frozen=True)
    class ExportStep(BaseStep):
        source_table: str = ""
        target: str = ""
        type: str = "export"

    @dataclass(frozen=True)
    class SourceDefinitionStep(BaseStep):
        source_name: str = ""
        source_config: Dict[str, Any] = None
        type: str = "source_definition"

    @dataclass(frozen=True)
    class SetVariableStep(BaseStep):
        variable_name: str = ""
        value: Any = None
        type: str = "set_variable"


# Step type registry for factory pattern
STEP_TYPE_REGISTRY = {
    "load": LoadStep,
    "transform": TransformStep,
    "export": ExportStep,
    "source_definition": SourceDefinitionStep,
    "set_variable": SetVariableStep,
}


def _validate_step_dict(step_dict: Dict[str, Any]) -> str:
    """Validate step dictionary and return step type.

    Args:
        step_dict: Dictionary containing step configuration

    Returns:
        str: The step type

    Raises:
        ValueError: For invalid step_dict or missing fields
    """
    if not isinstance(step_dict, dict):
        raise ValueError("step_dict must be a dictionary")

    step_type = step_dict.get("type")
    if not step_type:
        raise ValueError("step_dict must contain a 'type' field")

    if step_type not in STEP_TYPE_REGISTRY:
        raise ValueError(f"Unknown step type: {step_type}")

    return step_type


def _ensure_step_id(step_dict: Dict[str, Any], step_type: str) -> Dict[str, Any]:
    """Ensure step dictionary has an ID field.

    Args:
        step_dict: Dictionary containing step configuration
        step_type: The step type

    Returns:
        Dict with ID field guaranteed to exist
    """
    if "id" not in step_dict:
        return {**step_dict, "id": f"{step_type}_{uuid.uuid4().hex[:8]}"}
    return step_dict


def create_step_from_dict(step_dict: Dict[str, Any]):
    """Create a step object from a dictionary.

    Converts dictionary-based step definitions to proper step objects.

    Args:
        step_dict: Dictionary containing step configuration

    Returns:
        Appropriate step object (LoadStep, TransformStep, etc.)

    Raises:
        ValueError: For invalid step_dict or unknown step types
    """
    step_type = _validate_step_dict(step_dict)
    step_dict_with_id = _ensure_step_id(step_dict, step_type)

    step_class = STEP_TYPE_REGISTRY[step_type]

    try:
        return step_class(**step_dict_with_id)
    except TypeError as e:
        # Re-raise with more context
        raise ValueError(
            f"Invalid step configuration for type '{step_type}': {e}"
        ) from e


__all__ = [
    "BaseStepExecutor",
    "StepExecutor",
    "LoadStepExecutor",
    "TransformStepExecutor",
    "ExportStepExecutor",
    "StepRegistry",
    "create_default_registry",
    "create_step_from_dict",
    # Re-export step classes for backward compatibility
    "BaseStep",
    "LoadStep",
    "TransformStep",
    "ExportStep",
    "SourceDefinitionStep",
    "SetVariableStep",
]
