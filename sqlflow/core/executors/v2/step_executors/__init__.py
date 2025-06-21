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

# Import step classes from the original steps.py module to maintain backward compatibility
# We need to be careful about circular imports since both modules are in the same package
try:
    # Use sys.modules to check if the steps module is already loaded
    import sys

    if "sqlflow.core.executors.v2.steps" in sys.modules:
        # The steps.py module is already loaded, get it from sys.modules
        steps_module = sys.modules["sqlflow.core.executors.v2.steps"]
        BaseStep = steps_module.BaseStep
        LoadStep = steps_module.LoadStep
        TransformStep = steps_module.TransformStep
        ExportStep = steps_module.ExportStep
        SourceDefinitionStep = steps_module.SourceDefinitionStep
        SetVariableStep = steps_module.SetVariableStep
    else:
        # Import directly from the module path - this should work during normal imports
        # We use absolute imports to avoid circular import issues
        from sqlflow.core.executors.v2.steps import BaseStep as _BaseStep
        from sqlflow.core.executors.v2.steps import ExportStep as _ExportStep
        from sqlflow.core.executors.v2.steps import LoadStep as _LoadStep
        from sqlflow.core.executors.v2.steps import SetVariableStep as _SetVariableStep
        from sqlflow.core.executors.v2.steps import (
            SourceDefinitionStep as _SourceDefinitionStep,
        )
        from sqlflow.core.executors.v2.steps import TransformStep as _TransformStep

        BaseStep = _BaseStep
        LoadStep = _LoadStep
        TransformStep = _TransformStep
        ExportStep = _ExportStep
        SourceDefinitionStep = _SourceDefinitionStep
        SetVariableStep = _SetVariableStep
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


def create_step_from_dict(step_dict: Dict[str, Any]):
    """Create a step object from a dictionary.

    Provides migration path from dictionary-based to step execution.
    For now, returns the dictionary itself since our executors handle both.

    Args:
        step_dict: Dictionary representation of a step

    Returns:
        Step object (currently returns the dict itself)

    Raises:
        ValueError: If step_dict is malformed
    """
    if not isinstance(step_dict, dict):
        raise ValueError("step_dict must be a dictionary")

    step_type = step_dict.get("type")
    if not step_type:
        raise ValueError("step_dict must contain a 'type' field")

    # Ensure ID is present
    if "id" not in step_dict:
        step_dict = {**step_dict, "id": f"{step_type}_{uuid.uuid4().hex[:8]}"}

    # For now, return the dict as our executors can handle both dict and object format
    return step_dict


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
