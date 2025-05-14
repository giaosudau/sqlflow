"""Abstract Syntax Tree (AST) for SQLFlow DSL."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union


class PipelineStep(ABC):
    """Base class for all pipeline steps."""

    @abstractmethod
    def validate(self) -> List[str]:
        """Validate the pipeline step.

        Returns:
            List of validation error messages, empty if valid
        """
        pass


@dataclass
class SourceDefinitionStep(PipelineStep):
    """Represents a SOURCE directive in the pipeline.

    Example:
        SOURCE users TYPE POSTGRES PARAMS {
            "connection": "${DB_CONN}",
            "table": "users"
        };
    """

    name: str
    connector_type: str
    params: Dict[str, Any]
    line_number: Optional[int] = None

    def validate(self) -> List[str]:
        """Validate the SOURCE directive.

        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        if not self.name:
            errors.append("SOURCE directive requires a name")
        if not self.connector_type:
            errors.append("SOURCE directive requires a TYPE")
        if not self.params:
            errors.append("SOURCE directive requires PARAMS")
        return errors


@dataclass
class LoadStep(PipelineStep):
    """Represents a LOAD directive in the pipeline.
    
    Example:
        LOAD users_table FROM users_source;
    """
    
    table_name: str
    source_name: str
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the LOAD directive.
        
        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        if not self.table_name:
            errors.append("LOAD directive requires a table name")
        if not self.source_name:
            errors.append("LOAD directive requires a source name")
        return errors


@dataclass
class Pipeline:
    """Represents a complete parsed pipeline.

    A pipeline consists of a sequence of pipeline steps.
    """

    steps: List[PipelineStep] = field(default_factory=list)
    name: Optional[str] = None
    source_file: Optional[str] = None

    def add_step(self, step: PipelineStep) -> None:
        """Add a step to the pipeline.

        Args:
            step: The pipeline step to add
        """
        self.steps.append(step)

    def validate(self) -> List[str]:
        """Validate the entire pipeline.

        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        for i, step in enumerate(self.steps):
            step_errors = step.validate()
            for error in step_errors:
                errors.append(f"Step {i+1}: {error}")
        return errors
