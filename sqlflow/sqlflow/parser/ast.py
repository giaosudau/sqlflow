"""Abstract Syntax Tree (AST) for SQLFlow DSL."""

import os
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
class ExportStep(PipelineStep):
    """Represents an EXPORT directive in the pipeline.
    
    Example:
        EXPORT
          SELECT * FROM users
        TO "s3://bucket/users.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true
        };
    """
    
    sql_query: str
    destination_uri: str
    connector_type: str
    options: Dict[str, Any]
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the EXPORT directive.
        
        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        if not self.sql_query:
            errors.append("EXPORT directive requires a SQL query")
        if not self.destination_uri:
            errors.append("EXPORT directive requires a destination URI")
        if not self.connector_type:
            errors.append("EXPORT directive requires a connector TYPE")
        if not self.options:
            errors.append("EXPORT directive requires OPTIONS")
        return errors


@dataclass
class IncludeStep(PipelineStep):
    """Represents an INCLUDE directive in the pipeline.
    
    Example:
        INCLUDE "common/utils.sf" AS utils;
    """
    
    file_path: str
    alias: str
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the INCLUDE directive.
        
        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        if not self.file_path:
            errors.append("INCLUDE directive requires a file path")
        if not self.alias:
            errors.append("INCLUDE directive requires an alias (AS keyword)")
        
        _, ext = os.path.splitext(self.file_path)
        if not ext:
            errors.append("INCLUDE file path must have an extension")
        
        return errors


@dataclass
class SetStep(PipelineStep):
    """Represents a SET directive in the pipeline.
    
    Example:
        SET table_name = "users";
    """
    
    variable_name: str
    variable_value: str
    line_number: Optional[int] = None
    
    def validate(self) -> List[str]:
        """Validate the SET directive.
        
        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        if not self.variable_name:
            errors.append("SET directive requires a variable name")
        if not self.variable_value:
            errors.append("SET directive requires a variable value")
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
