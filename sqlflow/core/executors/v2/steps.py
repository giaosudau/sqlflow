"""Step Data Classes for V2 Executor.

This module defines strongly-typed representations of each step in the execution plan.
These replace generic dictionaries with proper type hints, validation, and observability
metadata, following the principle of explicitness over implicitness.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional


@dataclass(frozen=True)
class BaseStep:
    """
    Base class for all execution steps.

    Provides common functionality for step identification, dependency tracking,
    and observability metadata that applies to all step types.

    Attributes:
        id: Unique identifier for this step
        type: Step type identifier (load, transform, export, etc.)
        depends_on: List of step IDs this step depends on
        expected_duration_ms: Expected execution time for performance monitoring
        criticality: Business criticality level (affects retry policies)
        retry_policy: Optional retry configuration for this step
        metadata: Additional step-specific metadata
        created_at: Timestamp when step was created
    """

    id: str
    type: str
    depends_on: List[str] = field(default_factory=list)

    # Observability metadata
    expected_duration_ms: Optional[float] = None
    criticality: Literal["low", "normal", "high", "critical"] = "normal"
    retry_policy: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        """Post-initialization validation."""
        if not self.id:
            raise ValueError("Step ID cannot be empty")
        if not self.type:
            raise ValueError("Step type cannot be empty")
        if self.expected_duration_ms is not None and self.expected_duration_ms < 0:
            raise ValueError("Expected duration must be non-negative")


@dataclass(frozen=True)
class LoadStep(BaseStep):
    """
    Represents a 'load' operation from a source into a table.

    This step type handles data ingestion from various sources (files, APIs, databases)
    into the target SQL engine, with support for different load modes and incremental
    loading patterns.

    Attributes:
        source: Source identifier (file path, API endpoint, database table, etc.)
        target_table: Name of the destination table in the SQL engine
        load_mode: How to handle existing data (replace, append, upsert)
        options: Source-specific configuration options
        schema_options: Schema handling options (infer, enforce, evolve)
        incremental_config: Configuration for incremental loading
    """

    source: str = ""
    target_table: str = ""
    load_mode: Literal["replace", "append", "upsert"] = "replace"
    options: Dict[str, Any] = field(default_factory=dict)
    schema_options: Dict[str, Any] = field(default_factory=dict)
    incremental_config: Optional[Dict[str, Any]] = None
    type: str = "load"

    def __post_init__(self):
        """Post-initialization validation."""
        super().__post_init__()
        if not self.source:
            raise ValueError("Load step source cannot be empty")
        if not self.target_table:
            raise ValueError("Load step target_table cannot be empty")


@dataclass(frozen=True)
class TransformStep(BaseStep):
    """
    Represents a SQL transformation step.

    This step type executes SQL queries to transform data, with support for
    UDF registration, variable substitution, and schema evolution.

    Attributes:
        sql: SQL query or queries to execute
        target_table: Name of the destination table (for CREATE TABLE AS operations)
        operation_type: Type of SQL operation (select, create_table, insert, etc.)
        udf_dependencies: List of UDFs required by this transformation
        schema_evolution: Whether to allow schema changes
        materialization: How to materialize the results (table, view, temporary)
    """

    sql: str = ""
    target_table: Optional[str] = None
    operation_type: Literal[
        "select", "create_table", "insert", "update", "delete", "merge"
    ] = "select"
    udf_dependencies: List[str] = field(default_factory=list)
    schema_evolution: bool = False
    materialization: Literal["table", "view", "temporary"] = "table"
    type: str = "transform"

    def __post_init__(self):
        """Post-initialization validation."""
        super().__post_init__()
        if not self.sql.strip():
            raise ValueError("Transform step SQL cannot be empty")
        if (
            self.operation_type
            in ["create_table", "insert", "update", "delete", "merge"]
            and not self.target_table
        ):
            raise ValueError(
                f"Transform step with operation_type '{self.operation_type}' requires target_table"
            )


@dataclass(frozen=True)
class ExportStep(BaseStep):
    """
    Represents an 'export' operation from a table to a destination.

    This step type handles data export from the SQL engine to various destinations
    (files, APIs, databases) with support for different formats and partitioning.

    Attributes:
        source_table: Name of the source table in the SQL engine
        target: Target identifier (file path, API endpoint, database table, etc.)
        export_format: Output format (csv, parquet, json, etc.)
        options: Target-specific configuration options
        partitioning: Optional partitioning configuration
        compression: Optional compression settings
    """

    source_table: str = ""
    target: str = ""
    export_format: str = "csv"
    options: Dict[str, Any] = field(default_factory=dict)
    partitioning: Optional[Dict[str, Any]] = None
    compression: Optional[str] = None
    type: str = "export"

    def __post_init__(self):
        """Post-initialization validation."""
        super().__post_init__()
        if not self.source_table:
            raise ValueError("Export step source_table cannot be empty")
        if not self.target:
            raise ValueError("Export step target cannot be empty")


@dataclass(frozen=True)
class SourceDefinitionStep(BaseStep):
    """
    Represents a source definition/configuration step.

    This step type handles source configuration, validation, and setup,
    including profile resolution and connector initialization.

    Attributes:
        source_name: Name of the source being defined
        source_config: Source configuration (connector type, connection details, etc.)
        profile_name: Optional profile name for configuration resolution
        validation_rules: Optional validation rules to apply
        setup_sql: Optional SQL to run during source setup
    """

    source_name: str = ""
    source_config: Dict[str, Any] = field(default_factory=dict)
    profile_name: Optional[str] = None
    validation_rules: List[Dict[str, Any]] = field(default_factory=list)
    setup_sql: Optional[str] = None
    type: str = "source_definition"

    def __post_init__(self):
        """Post-initialization validation."""
        super().__post_init__()
        if not self.source_name:
            raise ValueError("Source definition step source_name cannot be empty")
        if not self.source_config:
            raise ValueError("Source definition step source_config cannot be empty")


@dataclass(frozen=True)
class SetVariableStep(BaseStep):
    """
    Represents a variable assignment step.

    This step type handles setting variables that can be used in subsequent steps,
    supporting both static values and dynamic computation.

    Attributes:
        variable_name: Name of the variable to set
        value: Static value to assign
        sql_expression: Optional SQL expression to compute the value
        scope: Variable scope (step, pipeline, global)
    """

    variable_name: str = ""
    value: Optional[Any] = None
    sql_expression: Optional[str] = None
    scope: Literal["step", "pipeline", "global"] = "pipeline"
    type: str = "set_variable"

    def __post_init__(self):
        """Post-initialization validation."""
        super().__post_init__()
        if not self.variable_name:
            raise ValueError("Set variable step variable_name cannot be empty")
        if self.value is None and not self.sql_expression:
            raise ValueError(
                "Set variable step must have either value or sql_expression"
            )
        if self.value is not None and self.sql_expression:
            raise ValueError(
                "Set variable step cannot have both value and sql_expression"
            )


# Type alias for step union types
StepType = (
    BaseStep
    | LoadStep
    | TransformStep
    | ExportStep
    | SourceDefinitionStep
    | SetVariableStep
)


def create_step_from_dict(step_dict: Dict[str, Any]) -> StepType:
    """
    Create a strongly-typed Step object from a dictionary.

    This function provides a migration path from the existing dictionary-based
    step representation to the new strongly-typed system.

    Args:
        step_dict: Dictionary representation of a step

    Returns:
        Strongly-typed Step object

    Raises:
        ValueError: If step_dict is malformed or has unknown step type
    """
    if not isinstance(step_dict, dict):
        raise ValueError("step_dict must be a dictionary")

    step_type = step_dict.get("type")
    if not step_type:
        raise ValueError("step_dict must contain a 'type' field")

    # Ensure ID is present
    if "id" not in step_dict:
        step_dict = {**step_dict, "id": f"{step_type}_{uuid.uuid4().hex[:8]}"}

    try:
        match step_type:
            case "load":
                return LoadStep(**step_dict)
            case "transform":
                return TransformStep(**step_dict)
            case "export":
                return ExportStep(**step_dict)
            case "source_definition":
                return SourceDefinitionStep(**step_dict)
            case "set_variable":
                return SetVariableStep(**step_dict)
            case _:
                raise ValueError(f"Unknown step type: {step_type}")

    except TypeError as e:
        raise ValueError(
            f"Invalid step configuration for type '{step_type}': {e}"
        ) from e
