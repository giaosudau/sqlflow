"""Abstract Syntax Tree (AST) for SQLFlow DSL."""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class PipelineStep(ABC):
    """Base class for all pipeline steps."""

    @abstractmethod
    def validate(self) -> List[str]:
        """Validate the pipeline step.

        Returns
        -------
            List of validation error messages, empty if valid

        """


@dataclass
class SourceDefinitionStep(PipelineStep):
    """Represents a SOURCE directive in the pipeline.

    Example 1:
        SOURCE users TYPE POSTGRES PARAMS {
            "connection": "${DB_CONN}",
            "table": "users",
            "sync_mode": "incremental",
            "cursor_field": "updated_at"
        };

    Example 2:
        SOURCE users FROM "postgres" OPTIONS {
            "table": "users",
            "sync_mode": "full_refresh"
        };
    """

    name: str
    connector_type: str
    params: Dict[str, Any]
    is_from_profile: bool = False
    profile_connector_name: Optional[str] = None
    line_number: Optional[int] = None

    def validate(self) -> List[str]:
        """Validate the SOURCE directive.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.name:
            errors.append("SOURCE directive requires a name")

        # Check for mixing of FROM and TYPE syntax patterns
        if self.is_from_profile and self.connector_type:
            errors.append(
                "Invalid SOURCE syntax: Cannot mix FROM and TYPE syntax patterns.\n\n"
                "Choose one of these formats:\n"
                '1. SOURCE name FROM "connector_name" OPTIONS {...};\n'
                "2. SOURCE name TYPE connector_type PARAMS {...};\n"
            )

        # Check correct format based on selected syntax pattern
        if self.is_from_profile:
            if not self.profile_connector_name:
                errors.append(
                    "SOURCE directive with FROM syntax requires a connector name in quotes:\n\n"
                    'SOURCE name FROM "connector_name" OPTIONS {...};\n'
                )
            # Check for PARAMS with FROM (incorrect)
            if any(param == "PARAMS" for param in self.params.keys()):
                errors.append(
                    "Invalid SOURCE syntax: Cannot use PARAMS with FROM-based syntax.\n\n"
                    "Correct syntax:\n"
                    'SOURCE name FROM "connector_name" OPTIONS {...};\n'
                )
        else:
            # TYPE syntax validation
            if not self.connector_type:
                errors.append(
                    "SOURCE directive with TYPE syntax requires a connector type:\n\n"
                    "SOURCE name TYPE connector_type PARAMS {...};\n"
                )
            if not self.params:
                errors.append(
                    "SOURCE directive with TYPE syntax requires PARAMS:\n\n"
                    "SOURCE name TYPE connector_type PARAMS {...};\n"
                )
            # Check for OPTIONS with TYPE (incorrect)
            if any(param == "OPTIONS" for param in self.params.keys()):
                errors.append(
                    "Invalid SOURCE syntax: Cannot use OPTIONS with TYPE-based syntax.\n\n"
                    "Correct syntax:\n"
                    "SOURCE name TYPE connector_type PARAMS {...};\n"
                )

        # Validate industry-standard parameters
        errors.extend(self._validate_industry_standard_parameters())

        return errors

    def _validate_industry_standard_parameters(self) -> List[str]:
        """Validate industry-standard SOURCE parameters.

        Returns:
            List of validation error messages for parameter issues
        """
        # Skip validation if parameters contain variables - validation will happen after substitution
        if self._contains_variables():
            return []

        # Import here to avoid circular imports
        from sqlflow.parser.source_validation import SourceParameterValidator

        validator = SourceParameterValidator()
        return validator.validate_parameters(self.params)

    def _contains_variables(self) -> bool:
        """Check if parameters contain variable references (${...}).

        Returns:
            True if any parameter value contains variable references
        """
        import json

        # Convert params to string to check for variable patterns
        params_str = json.dumps(self.params)
        return "${" in params_str

    def get_migration_suggestions(self) -> List[str]:
        """Get migration suggestions for parameters.

        Returns:
            List of migration suggestions for better parameter usage
        """
        # Import here to avoid circular imports
        from sqlflow.parser.source_validation import SourceParameterValidator

        validator = SourceParameterValidator()
        return validator.get_migration_suggestions(self.params)


@dataclass
class LoadStep(PipelineStep):
    """Represents a LOAD directive in the pipeline.

    Example:
    -------
        LOAD users_table FROM users_source;

    Example with MODE:
        LOAD users_table FROM users_source MODE REPLACE;

    Example with MERGE and MERGE_KEYS:
        LOAD users_table FROM users_source MODE MERGE MERGE_KEYS user_id;

    """

    table_name: str
    source_name: str
    mode: str = "REPLACE"  # Default mode is REPLACE
    merge_keys: List[str] = field(default_factory=list)  # For MERGE mode
    line_number: Optional[int] = None

    def validate(self) -> List[str]:
        """Validate the LOAD directive.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.table_name:
            errors.append("LOAD directive requires a table name")
        if not self.source_name:
            errors.append("LOAD directive requires a source name")

        # Validate mode
        valid_modes = ["REPLACE", "APPEND", "MERGE"]
        if self.mode.upper() not in valid_modes:
            errors.append(
                f"Invalid MODE: {self.mode}. Must be one of: {', '.join(valid_modes)}"
            )

        # Validate merge keys when MODE is MERGE
        if self.mode.upper() == "MERGE" and not self.merge_keys:
            errors.append("MERGE mode requires MERGE_KEYS to be specified")

        return errors


@dataclass
class ExportStep(PipelineStep):
    """Represents an EXPORT directive in the pipeline.

    Example:
    -------
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

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.sql_query:
            errors.append("EXPORT directive requires a SQL query")
        if not self.destination_uri:
            errors.append("EXPORT directive requires a destination URI")
        if not self.connector_type:
            errors.append("EXPORT directive requires a connector TYPE")
        if self.options is None or (
            isinstance(self.options, dict) and len(self.options) == 0
        ):
            errors.append("EXPORT directive requires OPTIONS")
        return errors


@dataclass
class IncludeStep(PipelineStep):
    """Represents an INCLUDE directive in the pipeline.

    Example:
    -------
        INCLUDE "common/utils.sf" AS utils;

    """

    file_path: str
    alias: str
    line_number: Optional[int] = None

    def validate(self) -> List[str]:
        """Validate the INCLUDE directive.

        Returns
        -------
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
    -------
        SET table_name = "users";

    """

    variable_name: str
    variable_value: str
    line_number: Optional[int] = None

    def validate(self) -> List[str]:
        """Validate the SET directive.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.variable_name:
            errors.append("SET directive requires a variable name")
        if not self.variable_value:
            errors.append("SET directive requires a variable value")
        return errors
        if not self.variable_name:
            errors.append("SET directive requires a variable name")
        if not self.variable_value:
            errors.append("SET directive requires a variable value")
        return errors


@dataclass
class SQLBlockStep(PipelineStep):
    """Represents a SQL block in the pipeline, such as CREATE TABLE.

    Example:
    -------
        CREATE TABLE customer_ltv AS
        SELECT
          customer_id,
          PYTHON_FUNC("helpers.calculate_ltv", raw_sales, 0.08) AS ltv
        FROM raw_sales;

    Transform Mode Examples:
    -------
        CREATE TABLE daily_sales MODE REPLACE AS
        SELECT order_date, SUM(amount) FROM orders GROUP BY order_date;

        CREATE TABLE incremental_events MODE INCREMENTAL BY event_date AS
        SELECT * FROM events WHERE event_date BETWEEN @start_date AND @end_date;

        CREATE TABLE customer_summary MODE MERGE KEY customer_id AS
        SELECT customer_id, COUNT(*) as orders FROM orders GROUP BY customer_id;

    """

    table_name: str
    sql_query: str
    line_number: Optional[int] = None
    is_replace: bool = False  # True if CREATE OR REPLACE was used

    # NEW: Transform mode fields
    mode: Optional[str] = None  # REPLACE/APPEND/MERGE/INCREMENTAL
    time_column: Optional[str] = None  # For INCREMENTAL BY column
    merge_keys: List[str] = field(default_factory=list)  # For MERGE KEY (...)
    lookback: Optional[str] = None  # For LOOKBACK duration

    def validate(self) -> List[str]:
        """Validate the SQL block.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.table_name:
            errors.append("SQL block requires a table name")
        if not self.sql_query:
            errors.append("SQL block requires a SQL query")

        # Validate transform mode fields
        errors.extend(self.validate_transform_fields())

        return errors

    def validate_transform_fields(self) -> List[str]:
        """Validate transform mode specific fields.

        Returns:
            List of validation error messages for transform mode issues
        """
        errors = []

        # If no mode is specified, this is a standard SQL block
        if self.mode is None:
            return errors

        # Validate mode value
        valid_modes = ["REPLACE", "APPEND", "MERGE", "INCREMENTAL"]
        if self.mode.upper() not in valid_modes:
            errors.append(
                f"Invalid MODE '{self.mode}'. Expected: {', '.join(valid_modes)}"
            )
            return errors  # Don't continue validation if mode is invalid

        mode_upper = self.mode.upper()

        # Mode-specific field validation
        errors.extend(self._validate_mode_specific_fields(mode_upper))

        # LOOKBACK can only be used with INCREMENTAL
        if self.lookback and mode_upper != "INCREMENTAL":
            errors.append("LOOKBACK can only be used with INCREMENTAL mode")

        return errors

    def _validate_mode_specific_fields(self, mode_upper: str) -> List[str]:
        """Validate fields specific to each mode.

        Args:
            mode_upper: The mode in uppercase

        Returns:
            List of validation errors for mode-specific field violations
        """
        if mode_upper == "INCREMENTAL":
            return self._validate_incremental_mode()
        elif mode_upper == "MERGE":
            return self._validate_merge_mode()
        elif mode_upper in ["REPLACE", "APPEND"]:
            return self._validate_simple_mode(mode_upper)
        return []

    def _validate_incremental_mode(self) -> List[str]:
        """Validate INCREMENTAL mode fields."""
        errors = []
        if not self.time_column:
            errors.append("INCREMENTAL mode requires BY <time_column>")
        if self.merge_keys:
            errors.append("INCREMENTAL mode cannot use merge keys")
        return errors

    def _validate_merge_mode(self) -> List[str]:
        """Validate MERGE mode fields."""
        errors = []
        if not self.merge_keys:
            errors.append(
                "MERGE mode requires KEY <column> or KEY (<col1>, <col2>, ...)"
            )
        if self.time_column:
            errors.append("MERGE mode cannot use time column")
        if self.lookback:
            errors.append("MERGE mode cannot use LOOKBACK")
        return errors

    def _validate_simple_mode(self, mode_upper: str) -> List[str]:
        """Validate REPLACE/APPEND mode fields."""
        errors = []
        if self.time_column:
            errors.append(f"{mode_upper} mode cannot use time column")
        if self.merge_keys:
            errors.append(f"{mode_upper} mode cannot use merge keys")
        if self.lookback:
            errors.append(f"{mode_upper} mode cannot use LOOKBACK")
        return errors

    def is_transform_mode(self) -> bool:
        """Check if this SQL block uses transform mode syntax.

        Returns:
            True if this block uses MODE syntax, False for standard SQL
        """
        return self.mode is not None


@dataclass
class ConditionalBranchStep(PipelineStep):
    """A single branch within a conditional block."""

    condition: str  # Raw condition expression
    steps: List[PipelineStep]  # Steps to execute if condition is true
    line_number: int

    def validate(self) -> List[str]:
        """Validate the conditional branch.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.condition:
            errors.append("Conditional branch requires a condition expression")

        # Validate nested steps
        for i, step in enumerate(self.steps):
            step_errors = step.validate()
            for error in step_errors:
                errors.append(f"Branch step {i + 1}: {error}")

        return errors


@dataclass
class ConditionalBlockStep(PipelineStep):
    """Block containing multiple conditional branches and optional else."""

    branches: List[ConditionalBranchStep]  # IF/ELSEIF branches
    else_branch: Optional[List[PipelineStep]]  # ELSE branch (may be None)
    line_number: int

    def validate(self) -> List[str]:
        """Validate the conditional block.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        if not self.branches:
            errors.append("Conditional block requires at least one branch")

        # Validate all branches
        for i, branch in enumerate(self.branches):
            branch_errors = branch.validate()
            for error in branch_errors:
                errors.append(f"Branch {i + 1}: {error}")

        # Validate else branch if present
        if self.else_branch:
            for i, step in enumerate(self.else_branch):
                step_errors = step.validate()
                for error in step_errors:
                    errors.append(f"Else branch step {i + 1}: {error}")

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
        ----
            step: The pipeline step to add

        """
        self.steps.append(step)

    def validate(self) -> List[str]:
        """Validate the entire pipeline.

        Returns
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        for i, step in enumerate(self.steps):
            step_errors = step.validate()
            for error in step_errors:
                errors.append(f"Step {i + 1}: {error}")
        return errors
