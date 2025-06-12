"""Planner for SQLFlow pipelines.

This module contains the planner that converts a validated SQLFlow DAG
into a linear, JSON-serialized ExecutionPlan consumable by an executor.

Error Handling Convention:
- To avoid duplicate error messages, planner methods should avoid logging at ERROR level
- Instead, log detailed information at DEBUG level and raise appropriately-formatted exceptions
- PlanningError and EvaluationError objects should contain all user-facing information
- CLI code is responsible for presenting errors to users in a clean, readable format
- Variable missing/validation errors are logged at INFO level to help troubleshooting
"""

import json
import re
from typing import Any, Dict, List, Optional

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.errors import PlanningError
from sqlflow.core.evaluator import ConditionEvaluator, EvaluationError
from sqlflow.core.planner.dependency_analyzer import DependencyAnalyzer
from sqlflow.core.planner.factory import PlannerConfig, PlannerFactory
from sqlflow.core.planner.interfaces import (
    IDependencyAnalyzer,
    IExecutionOrderResolver,
    IStepBuilder,
)
from sqlflow.core.planner.order_resolver import ExecutionOrderResolver
from sqlflow.core.planner.step_builder import StepBuilder
from sqlflow.core.variable_substitution import VariableSubstitutionEngine
from sqlflow.logging import get_logger
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    ExportStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)

logger = get_logger(__name__)


# --- UTILITY FUNCTIONS ---
def _format_error(msg: str, *lines: str) -> str:
    return msg + ("\n" + "\n".join(lines) if lines else "")


# --- EXECUTION PLAN BUILDER ---
class ExecutionPlanBuilder:
    """Builds an execution plan from a validated SQLFlow DAG.

    Following Zen of Python: Simple is better than complex.
    Delegates to specialized components for single responsibilities.

    Supports optional dependency injection for testability while maintaining
    backward compatibility through default component construction.
    """

    def __init__(
        self,
        dependency_analyzer: Optional[IDependencyAnalyzer] = None,
        order_resolver: Optional[IExecutionOrderResolver] = None,
        step_builder: Optional[IStepBuilder] = None,
        config: Optional[PlannerConfig] = None,
    ):
        """Initialize ExecutionPlanBuilder with optional dependency injection.

        Args:
            dependency_analyzer: Optional custom dependency analyzer
            order_resolver: Optional custom execution order resolver
            step_builder: Optional custom step builder
            config: Optional configuration object (overrides individual components)

        Following Zen of Python:
        - Simple is better than complex: Default components for simple usage
        - Explicit is better than implicit: Clear injection points for testing
        - Practicality beats purity: Backward compatibility maintained
        """
        # Legacy compatibility attributes
        self.dependency_resolver = DependencyResolver()
        self.step_id_map: Dict[int, str] = {}
        self.step_dependencies: Dict[str, List[str]] = {}
        self._source_definitions: Dict[str, Dict[str, Any]] = {}

        # Component injection with fallback to defaults
        if config is not None:
            # Use factory to create components from config
            self._dependency_analyzer, self._order_resolver, self._step_builder = (
                PlannerFactory.create_components_from_config(config)
            )
            logger.debug(
                "ExecutionPlanBuilder initialized with configuration-based components"
            )
        else:
            # Individual component injection or defaults
            self._dependency_analyzer = dependency_analyzer or DependencyAnalyzer()
            self._order_resolver = order_resolver or ExecutionOrderResolver()
            self._step_builder = step_builder or StepBuilder()

            if dependency_analyzer or order_resolver or step_builder:
                logger.debug(
                    "ExecutionPlanBuilder initialized with injected components"
                )
            else:
                logger.debug("ExecutionPlanBuilder initialized with default components")

    # --- PIPELINE VALIDATION ---
    def _validate_variable_references(
        self, pipeline: Pipeline, variables: Dict[str, Any]
    ) -> None:
        """Validate variable references and substitution.

        Following Zen of Python: Errors should never pass silently.
        Comprehensive validation with clear error messages.
        """
        logger.debug("Validating variable references and values")  # Reduced from INFO

        # Use modern variable validation approach
        referenced_vars = self._collect_all_referenced_variables(pipeline)

        # Get all variables (including set variables from pipeline)
        all_variables = self._get_effective_variables(pipeline, variables)

        # Create engine for validation
        engine = VariableSubstitutionEngine(all_variables)

        # Log comprehensive variable reference report
        self._log_modern_variable_reference_report(referenced_vars, engine, pipeline)

        # Verify all variables have values or defaults
        self._verify_variable_values_modern(referenced_vars, engine, pipeline)

    def _serialize_pipeline_for_validation(self, pipeline: Pipeline) -> str:
        """Serialize pipeline steps to text for variable validation."""
        texts = []
        for step in pipeline.steps:
            if hasattr(step, "sql_query") and step.sql_query:
                texts.append(step.sql_query)
            # Add more step types as needed for comprehensive validation
        return "\n".join(texts)

    def _log_modern_variable_reference_report(
        self,
        referenced_vars: set,
        engine: VariableSubstitutionEngine,
        pipeline: Pipeline,
    ) -> None:
        """Log a detailed report of all variable references using the modern engine."""
        logger.debug("----- Variable Reference Report -----")
        for var in sorted(referenced_vars):
            value = engine._get_variable_value(var)
            if value is not None:
                logger.debug(f"  ✓ ${{{var}}} = '{value}'")
            elif self._has_default_in_pipeline(var, pipeline):
                logger.debug(f"  ✓ ${{{var}}} = [Using default value]")
            else:
                logger.debug(f"  ✗ ${{{var}}} = UNDEFINED")
        logger.debug("-----------------------------------")

    def _verify_variable_values_modern(
        self,
        referenced_vars: set,
        engine: VariableSubstitutionEngine,
        pipeline: Pipeline,
    ) -> None:
        """Verify all referenced variables have values or defaults.

        Following Zen of Python: Errors should never pass silently.
        Comprehensive validation with clear error messages.
        """
        missing_vars = []
        invalid_defaults = []

        for var in referenced_vars:
            # Check if variable has a value through the engine
            value = engine._get_variable_value(var)
            has_default = self._has_default_in_pipeline(var, pipeline)

            if value is None and not has_default:
                missing_vars.append(var)

        # Check for invalid default values (unquoted values with spaces)
        invalid_defaults = self._find_invalid_defaults(referenced_vars, pipeline)

        # Raise errors for missing variables
        if missing_vars:
            self._raise_missing_variables_error(missing_vars, pipeline)

        # Raise errors for invalid defaults
        if invalid_defaults:
            self._raise_invalid_defaults_error(invalid_defaults)

    def _raise_missing_variables_error(
        self, missing_vars: List[str], pipeline: Pipeline
    ) -> None:
        """Raise PlanningError with details about missing variables."""
        # Skip logging here - the calling function already logs missing variables
        error_msg = "Pipeline references undefined variables:\n" + "".join(
            f"  - ${{{var}}} is used but not defined\n" for var in missing_vars
        )
        error_msg += "\nPlease define these variables using SET statements or provide them when running the pipeline."

        # Add reference locations for better context
        error_msg += "\n\nVariable reference locations:"
        for var in missing_vars:
            locations = self._find_variable_reference_locations(var, pipeline)
            if locations:
                error_msg += f"\n  ${{{var}}} referenced at: {', '.join(locations)}"

        # Raise PlanningError without additional logging
        raise PlanningError(error_msg)

    def _raise_invalid_defaults_error(self, invalid_defaults: List[str]) -> None:
        """Raise PlanningError with details about invalid default values."""
        # Skip logging here - just raise the error
        error_msg = (
            "Invalid default values for variables (must not contain spaces unless quoted):\n"
            + "".join(f"  - {expr}\n" for expr in invalid_defaults)
        )
        error_msg += (
            '\nDefault values with spaces must be quoted, e.g. ${var|"us-east"}'
        )
        raise PlanningError(error_msg)

    def _get_effective_variables(
        self, pipeline: Pipeline, variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Combine provided variables with those defined by SET statements in the pipeline."""
        # Extract variables defined by SET statements
        defined_vars = self._extract_set_defined_variables(pipeline)
        logger.debug(f"Variables defined by SET statements: {defined_vars}")

        # Create a copy of provided variables and add pipeline-defined ones
        effective_variables = variables.copy()
        for var_name, var_value in defined_vars.items():
            if var_name not in effective_variables:
                logger.debug(
                    f"Adding pipeline-defined variable: {var_name}={var_value}"
                )
                effective_variables[var_name] = var_value

        # Log the complete variables dictionary
        logger.debug(f"Effective variables for validation: {effective_variables}")
        return effective_variables

    def _collect_all_referenced_variables(self, pipeline: Pipeline) -> set:
        """Extract all variable references from all steps in the pipeline."""
        referenced_vars = set()
        for step in pipeline.steps:
            logger.debug(f"Checking variable references in step: {type(step).__name__}")
            self._extract_step_variable_references(step, referenced_vars)
        logger.debug(f"Found referenced variables: {referenced_vars}")
        return referenced_vars

    def _extract_step_variable_references(
        self, step: PipelineStep, referenced_vars: set
    ) -> None:
        """Extract variable references from a specific pipeline step."""
        if isinstance(step, ConditionalBlockStep):
            for branch in step.branches:
                self._extract_variable_references(branch.condition, referenced_vars)
        elif isinstance(step, ExportStep):
            self._extract_variable_references(step.destination_uri, referenced_vars)
            self._extract_variable_references(json.dumps(step.options), referenced_vars)
            # Also check for variables in SQL queries
            if hasattr(step, "sql_query") and step.sql_query:
                self._extract_variable_references(step.sql_query, referenced_vars)
        elif isinstance(step, SourceDefinitionStep):
            self._extract_variable_references(json.dumps(step.params), referenced_vars)
        elif isinstance(step, SQLBlockStep):
            # Check for variables in SQL queries
            self._extract_variable_references(step.sql_query, referenced_vars)
        elif isinstance(step, SetStep):
            # Check for variables in variable values
            self._extract_variable_references(step.variable_value, referenced_vars)

    def _find_missing_vars(self, referenced_vars, variables, pipeline):
        return [
            var
            for var in referenced_vars
            if var not in variables and not self._has_default_in_pipeline(var, pipeline)
        ]

    def _find_invalid_defaults(self, referenced_vars, pipeline):
        invalid_defaults = []
        for var in referenced_vars:
            if self._has_default_in_pipeline(var, pipeline):
                var_with_default_pattern = (
                    rf"\$\{{[ ]*{re.escape(var)}[ ]*\|([^{{}}]*)\}}"
                )
                for step in pipeline.steps:
                    texts = self._get_texts_for_var_check(step)
                    for text in texts:
                        if not text:
                            continue
                        for match in re.finditer(var_with_default_pattern, text):
                            default_val = match.group(1).strip()
                            if self._is_invalid_default_value(default_val):
                                invalid_defaults.append(f"${{{var}|{default_val}}}")
        return invalid_defaults

    def _verify_variable_values(self, referenced_vars, variables, pipeline):
        """Verify that all variable values are valid (non-empty, type-compatible, etc.)

        Args:
        ----
            referenced_vars: Set of referenced variable names
            variables: Dictionary of variable values
            pipeline: The pipeline AST

        Raises:
        ------
            PlanningError: If any variable values are invalid

        """
        logger.debug("Verifying all variable values")
        invalid_vars = []

        for var in referenced_vars:
            if var in variables:
                value = variables[var]
                # Empty values are now allowed (behavior change)
                if value == "":
                    # Just log a warning but don't treat as invalid
                    logger.debug(f"Found empty value for variable: ${{{var}}}")
                # Add more validation logic here as needed
                # For example, you could check for type compatibility,
                # numeric ranges, format validity, etc.
            elif self._has_default_in_pipeline(var, pipeline):
                # Variable has a default value which is used in the absence of a provided value
                # We could parse and verify the default value here if needed
                logger.debug(f"Using default value for variable: ${{{var}}}")

        if invalid_vars:
            error_msg = "Invalid variable values detected:\n" + "\n".join(
                f"  - {err}" for err in invalid_vars
            )
            error_msg += "\n\nPlease provide valid values for these variables."

            # Add additional debugging info about where variables are used
            error_msg += "\n\nVariable reference locations:"
            for var in [v.split()[0].strip("${}") for v in invalid_vars]:
                locations = self._find_variable_reference_locations(var, pipeline)
                if locations:
                    error_msg += f"\n  ${{{var}}} referenced at: {', '.join(locations)}"

            logger.warning(f"Variable value validation failed: {error_msg}")
            raise PlanningError(error_msg)

        logger.debug("All variable values verified successfully")

    def _get_texts_for_var_check(self, step):
        texts = []
        if isinstance(step, ExportStep):
            texts.append(step.destination_uri)
            texts.append(json.dumps(step.options))
            # Also check SQL queries if they exist
            if hasattr(step, "sql_query") and step.sql_query:
                texts.append(step.sql_query)
        elif isinstance(step, SourceDefinitionStep):
            texts.append(json.dumps(step.params))
        elif isinstance(step, ConditionalBlockStep):
            for branch in step.branches:
                texts.append(branch.condition)
        elif isinstance(step, SQLBlockStep):
            texts.append(step.sql_query)
        elif isinstance(step, SetStep):
            texts.append(step.variable_value)
        return texts

    def _is_invalid_default_value(self, default_val: str) -> bool:
        """Return True if the default value is invalid (contains spaces and is not quoted)."""
        if " " in default_val:
            if (default_val.startswith('"') and default_val.endswith('"')) or (
                default_val.startswith("'") and default_val.endswith("'")
            ):
                return False
            return True
        return False

    def _extract_variable_references(self, text: str, result: set) -> None:
        if not text:
            return
        var_pattern = r"\$\{([^|{}]+)(?:\|[^{}]*)?\}"
        matches = re.findall(var_pattern, text)
        for match in matches:
            result.add(match.strip())

    def _has_default_in_pipeline(self, var_name: str, pipeline: Pipeline) -> bool:
        """Check if a variable has a default value in any step of the pipeline.

        Args:
        ----
            var_name: The name of the variable to check
            pipeline: The pipeline to search in

        Returns:
        -------
            True if the variable has a default value, False otherwise

        """
        var_with_default_pattern = rf"\$\{{[ ]*{re.escape(var_name)}[ ]*\|[^{{}}]*\}}"

        for step in pipeline.steps:
            if self._step_has_variable_default(step, var_with_default_pattern):
                return True
        return False

    def _step_has_variable_default(self, step: PipelineStep, pattern: str) -> bool:
        """Check if a step contains a variable with a default value.

        Args:
        ----
            step: The pipeline step to check
            pattern: The regex pattern to search for

        Returns:
        -------
            True if the step contains the pattern, False otherwise

        """
        if isinstance(step, ExportStep):
            return self._export_step_has_default(step, pattern)
        elif isinstance(step, SourceDefinitionStep):
            return self._source_step_has_default(step, pattern)
        elif isinstance(step, ConditionalBlockStep):
            return self._conditional_step_has_default(step, pattern)
        elif isinstance(step, SQLBlockStep):
            return self._sql_step_has_default(step, pattern)
        elif isinstance(step, SetStep):
            return self._set_step_has_default(step, pattern)
        return False

    def _export_step_has_default(self, step: ExportStep, pattern: str) -> bool:
        """Check if an export step contains a variable with a default value."""
        if re.search(pattern, step.destination_uri):
            return True
        if re.search(pattern, json.dumps(step.options)):
            return True
        # Also check SQL queries if they exist
        if hasattr(step, "sql_query") and step.sql_query:
            if re.search(pattern, step.sql_query):
                return True
        return False

    def _source_step_has_default(
        self, step: SourceDefinitionStep, pattern: str
    ) -> bool:
        """Check if a source step contains a variable with a default value."""
        return bool(re.search(pattern, json.dumps(step.params)))

    def _conditional_step_has_default(
        self, step: ConditionalBlockStep, pattern: str
    ) -> bool:
        """Check if a conditional step contains a variable with a default value."""
        for branch in step.branches:
            if re.search(pattern, branch.condition):
                return True
        return False

    def _sql_step_has_default(self, step: SQLBlockStep, pattern: str) -> bool:
        """Check if a SQL step contains a variable with a default value."""
        return bool(re.search(pattern, step.sql_query))

    def _set_step_has_default(self, step: SetStep, pattern: str) -> bool:
        """Check if a SET step contains a variable with a default value."""
        return bool(re.search(pattern, step.variable_value))

    def _extract_set_defined_variables(self, pipeline: Pipeline) -> Dict[str, Any]:
        """Extract variables defined by SET statements in the pipeline.

        Args:
        ----
            pipeline: The pipeline to analyze

        Returns:
        -------
            A dictionary of variable names to values

        """
        from sqlflow.parser.ast import SetStep

        defined_vars = {}
        for step in pipeline.steps:
            if isinstance(step, SetStep):
                var_name = step.variable_name.strip()
                var_value = step.variable_value.strip()

                # Process the variable value
                processed_value = self._process_variable_value(var_name, var_value)
                defined_vars[var_name] = processed_value

        logger.debug(f"Extracted SET-defined variables: {defined_vars}")
        return defined_vars

    def _process_variable_value(self, var_name: str, var_value: str) -> Any:
        """Process a variable value from a SET statement.

        Handles references with defaults and type conversion.

        Args:
        ----
            var_name: The name of the variable
            var_value: The raw value of the variable

        Returns:
        -------
            The processed value

        """
        # If the value is itself a variable reference with default, extract it
        var_ref_match = re.match(r"\$\{([^|{}]+)\|([^{}]*)\}", var_value)
        if var_ref_match:
            var_ref_match.group(1).strip()
            default_val = var_ref_match.group(2).strip()

            # Always use the default value for variables defined in SET statements with defaults
            # This ensures SET var = "${var|default}" properly uses the default value
            logger.debug(f"Variable ${{{var_name}}} has default value '{default_val}'")
            return self._convert_value_to_appropriate_type(default_val)

        # Not a variable reference with default, return the value with quotes removed if present
        return self._remove_quotes_if_present(var_value)

    def _convert_value_to_appropriate_type(self, value: str) -> Any:
        """Convert a string value to an appropriate type.

        Args:
        ----
            value: The string value to convert

        Returns:
        -------
            The converted value

        """
        # Handle boolean values
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False

        # Handle numeric values
        if re.match(r"^[0-9]+$", value):
            return int(value)
        elif re.match(r"^[0-9]*\.[0-9]+$", value):
            return float(value)

        # Otherwise keep as string, with quotes removed if present
        return self._remove_quotes_if_present(value)

    def _remove_quotes_if_present(self, value: str) -> str:
        """Remove quotes from a string if they're present.

        Args:
        ----
            value: The string to process

        Returns:
        -------
            The string with outer quotes removed if present

        """
        if (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        ):
            return value[1:-1]
        return value

    def _find_variable_reference_locations(
        self, var_name: str, pipeline: Pipeline
    ) -> List[str]:
        """Find all locations where a variable is referenced in the pipeline.

        Args:
        ----
            var_name: The name of the variable to find
            pipeline: The pipeline to analyze

        Returns:
        -------
            A list of location descriptions

        """
        locations = []
        var_pattern = rf"\$\{{[ ]*{re.escape(var_name)}[ ]*(?:\|[^{{}}]*)?\}}"

        for step in pipeline.steps:
            line_info = f"line {getattr(step, 'line_number', 'unknown')}"
            self._check_step_for_variable_references(
                step, var_pattern, line_info, locations
            )

        return locations

    def _check_step_for_variable_references(
        self, step: PipelineStep, var_pattern: str, line_info: str, locations: List[str]
    ) -> None:
        """Check a specific step for variable references and add locations to the list.

        Args:
        ----
            step: The pipeline step to check
            var_pattern: The regex pattern to search for
            line_info: Line information string for error reporting
            locations: List to add location information to

        """
        if isinstance(step, ConditionalBlockStep):
            self._check_conditional_step_for_references(
                step, var_pattern, line_info, locations
            )
        elif isinstance(step, ExportStep):
            self._check_export_step_for_references(
                step, var_pattern, line_info, locations
            )
        elif isinstance(step, SourceDefinitionStep):
            self._check_source_step_for_references(
                step, var_pattern, line_info, locations
            )
        elif isinstance(step, SetStep):
            self._check_set_step_for_references(step, var_pattern, line_info, locations)
        elif isinstance(step, SQLBlockStep):
            self._check_sql_step_for_references(step, var_pattern, line_info, locations)

    def _check_conditional_step_for_references(
        self,
        step: ConditionalBlockStep,
        var_pattern: str,
        line_info: str,
        locations: List[str],
    ) -> None:
        """Check a conditional step for variable references."""
        for branch in step.branches:
            if re.search(var_pattern, branch.condition):
                locations.append(f"IF condition at {line_info}")

    def _check_export_step_for_references(
        self, step: ExportStep, var_pattern: str, line_info: str, locations: List[str]
    ) -> None:
        """Check an export step for variable references."""
        if re.search(var_pattern, step.destination_uri):
            locations.append(f"EXPORT destination at {line_info}")
        if re.search(var_pattern, json.dumps(step.options)):
            locations.append(f"EXPORT options at {line_info}")

    def _check_source_step_for_references(
        self,
        step: SourceDefinitionStep,
        var_pattern: str,
        line_info: str,
        locations: List[str],
    ) -> None:
        """Check a source step for variable references."""
        if re.search(var_pattern, json.dumps(step.params)):
            locations.append(f"SOURCE params at {line_info}")

    def _check_set_step_for_references(
        self, step: SetStep, var_pattern: str, line_info: str, locations: List[str]
    ) -> None:
        """Check a SET step for variable references."""
        if re.search(var_pattern, step.variable_value):
            locations.append(f"SET statement at {line_info}")

    def _check_sql_step_for_references(
        self, step: SQLBlockStep, var_pattern: str, line_info: str, locations: List[str]
    ) -> None:
        """Check a SQL step for variable references."""
        if re.search(var_pattern, step.sql_query):
            locations.append(f"SQL query at {line_info}")

    # --- TABLE & DEPENDENCY ANALYSIS ---
    def _build_table_to_step_mapping(
        self, pipeline: Pipeline
    ) -> Dict[str, PipelineStep]:
        """Build mapping from table names to pipeline steps.

        Following Zen of Python: Explicit is better than implicit.
        Clear mapping for dependency analysis with duplicate detection.

        CRITICAL FIX: Multiple LoadSteps on same table must be preserved for execution.
        Only the table-to-step mapping should avoid duplicates for dependency purposes.
        """
        self._table_to_step_mapping = {}
        duplicate_tables = []
        # NEW: Track all load steps separately for execution planning
        self._all_load_steps = []

        for step in pipeline.steps:
            self._process_step_for_mapping(step, duplicate_tables)

        self._validate_no_duplicate_tables(duplicate_tables)
        return self._table_to_step_mapping

    def _process_step_for_mapping(
        self, step: PipelineStep, duplicate_tables: List
    ) -> None:
        """Process a single step for table mapping."""
        # Collect ALL LoadSteps for execution (CRITICAL FIX)
        if isinstance(step, LoadStep):
            self._all_load_steps.append(step)

        table_name = self._extract_table_name_from_step(step)
        if table_name:
            self._handle_table_mapping(step, table_name, duplicate_tables)

    def _extract_table_name_from_step(self, step: PipelineStep) -> Optional[str]:
        """Extract table name from different step types."""
        if hasattr(step, "table_name") and step.table_name:
            return step.table_name
        elif isinstance(step, SQLBlockStep):
            # Extract table name from CREATE TABLE statements
            table_name = self._extract_table_name_from_sql(step.sql_query)
            if table_name:
                step.table_name = table_name  # Set it for consistency
            return table_name
        return None

    def _handle_table_mapping(
        self, step: PipelineStep, table_name: str, duplicate_tables: List
    ) -> None:
        """Handle table mapping with duplicate detection."""
        if table_name in self._table_to_step_mapping:
            self._handle_duplicate_table(step, table_name, duplicate_tables)
        else:
            self._table_to_step_mapping[table_name] = step

    def _handle_duplicate_table(
        self, step: PipelineStep, table_name: str, duplicate_tables: List
    ) -> None:
        """Handle duplicate table definitions."""
        existing_step = self._table_to_step_mapping[table_name]

        if self._is_allowed_load_duplicate(step, existing_step, table_name):
            return
        elif self._is_allowed_sql_duplicate(step, existing_step, table_name):
            return
        else:
            # Different step types creating same table - this is a duplicate
            duplicate_tables.append((table_name, step.line_number))

    def _is_allowed_load_duplicate(
        self, step: PipelineStep, existing_step: PipelineStep, table_name: str
    ) -> bool:
        """Check if multiple LoadSteps on same table are allowed."""
        if isinstance(step, LoadStep) and isinstance(existing_step, LoadStep):
            # Multiple LoadSteps on same table are allowed for load modes
            # Keep the first one in the mapping for dependency purposes ONLY
            # BUT still preserve all LoadSteps for execution (via _all_load_steps)
            logger.debug(
                f"Multiple LOAD steps detected for table '{table_name}' - preserving all for execution"
            )
            return True
        return False

    def _is_allowed_sql_duplicate(
        self, step: PipelineStep, existing_step: PipelineStep, table_name: str
    ) -> bool:
        """Check if multiple SQLBlockSteps on same table are allowed."""
        if isinstance(step, SQLBlockStep) and isinstance(existing_step, SQLBlockStep):
            # FIXED: Allow SQLBlockStep to create multiple operations on same table
            # when using CREATE OR REPLACE - don't merge, preserve both
            if getattr(step, "is_replace", False):
                # CREATE OR REPLACE can coexist with regular CREATE
                # Don't update mapping - keep both operations separate
                # Add this step to separate tracking for execution
                if not hasattr(self, "_all_sql_block_steps"):
                    self._all_sql_block_steps = []
                self._all_sql_block_steps.append(step)
                logger.debug(
                    f"CREATE OR REPLACE operation for table '{table_name}' - preserving both CREATE and CREATE OR REPLACE"
                )
                return True
        return False

    def _validate_no_duplicate_tables(self, duplicate_tables: List) -> None:
        """Validate that no duplicate tables exist."""
        if duplicate_tables:
            error_msg = "Duplicate table definitions found:\n" + "".join(
                f"  - Table '{table}' defined at line {line}, but already defined at line {getattr(self._table_to_step_mapping[table], 'line_number', 'unknown')}\n"
                for table, line in duplicate_tables
            )
            raise PlanningError(error_msg)

    def _extract_referenced_tables(self, sql_query: str) -> List[str]:
        sql_lower = sql_query.lower()
        tables = []

        # DuckDB built-in functions that are not table references
        builtin_functions = {
            "read_csv_auto",
            "read_csv",
            "read_parquet",
            "read_json",
            "information_schema",
            "pg_catalog",
            "main",
        }

        # Handle standard SQL FROM clauses
        from_matches = re.finditer(
            r"from\s+([a-zA-Z0-9_]+(?:\s*,\s*[a-zA-Z0-9_]+)*)", sql_lower
        )
        for match in from_matches:
            table_list = match.group(1).split(",")
            for table in table_list:
                table_name = table.strip()
                if (
                    table_name
                    and table_name not in tables
                    and table_name not in builtin_functions
                ):
                    tables.append(table_name)

        # Handle standard SQL JOINs
        join_matches = re.finditer(r"join\s+([a-zA-Z0-9_]+)", sql_lower)
        for match in join_matches:
            table_name = match.group(1).strip()
            if (
                table_name
                and table_name not in tables
                and table_name not in builtin_functions
            ):
                tables.append(table_name)

        # Handle table UDF pattern: PYTHON_FUNC("module.function", table_name)
        udf_table_matches = re.finditer(
            r"python_func\s*\(\s*['\"][\w\.]+['\"]\s*,\s*([a-zA-Z0-9_]+)", sql_lower
        )
        for match in udf_table_matches:
            table_name = match.group(1).strip()
            if (
                table_name
                and table_name not in tables
                and table_name not in builtin_functions
            ):
                tables.append(table_name)

        return tables

    def _find_table_references(
        self, step: PipelineStep, sql_query: str, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        referenced_tables = self._extract_referenced_tables(sql_query)
        undefined_tables = []
        for table_name in referenced_tables:
            if table_name in table_to_step:
                table_step = table_to_step.get(table_name)
                if table_step and table_step != step:
                    self._add_dependency(step, table_step)
            else:
                undefined_tables.append(table_name)
        if undefined_tables:
            line_number = getattr(step, "line_number", "unknown")
            logger.warning(
                f"Step at line {line_number} references tables that might not be defined: {', '.join(undefined_tables)}"
            )
            # Store undefined tables for validation error raising
            if not hasattr(self, "_undefined_tables"):
                self._undefined_tables = []
            self._undefined_tables.extend(
                [(table, step, line_number) for table in undefined_tables]
            )

    def _validate_table_references(self) -> None:
        """Validate that all table references are defined.

        Following Zen of Python: Errors should never pass silently.
        Comprehensive validation with clear error messages.
        """
        if not hasattr(self, "_undefined_tables") or not self._undefined_tables:
            return

        defined_tables = self._get_defined_tables()
        typo_analysis = self._analyze_undefined_tables(defined_tables)

        if typo_analysis["likely_typos"]:
            self._raise_typo_validation_error(typo_analysis, defined_tables)

        self._log_external_table_warnings(typo_analysis["likely_typos"])

    def _get_defined_tables(self) -> set:
        """Get all defined table names for similarity checking."""
        if hasattr(self, "_table_to_step_mapping"):
            return set(self._table_to_step_mapping.keys())
        return set()

    def _analyze_undefined_tables(self, defined_tables: set) -> Dict[str, Any]:
        """Analyze undefined tables to identify likely typos."""
        likely_typos = []
        context_locations = {}
        suggestions_map = {}

        for table, step, line_number in self._undefined_tables:
            self._collect_table_context(table, step, line_number, context_locations)

            if self._is_likely_typo(table, defined_tables):
                likely_typos.append(table)
                best_suggestion = self._find_best_suggestion(table, defined_tables)
                if best_suggestion:
                    suggestions_map[table] = best_suggestion

        return {
            "likely_typos": likely_typos,
            "context_locations": context_locations,
            "suggestions_map": suggestions_map,
        }

    def _collect_table_context(
        self, table: str, step: PipelineStep, line_number: int, context_locations: Dict
    ) -> None:
        """Collect context information for undefined table references."""
        if table not in context_locations:
            context_locations[table] = []
        step_name = getattr(step, "table_name", "unknown step")
        context_locations[table].append(f"line {line_number} in {step_name}")

    def _raise_typo_validation_error(
        self, typo_analysis: Dict[str, Any], defined_tables: set
    ) -> None:
        """Raise validation error for likely typos."""
        suggestions = self._build_typo_suggestions(typo_analysis, defined_tables)
        error_message = self._build_typo_error_message(typo_analysis["likely_typos"])

        from sqlflow.validation.errors import ValidationError as StandardValidationError

        raise StandardValidationError(
            message=error_message,
            line=1,  # Use first undefined table's line or default
            error_type="Table Reference Error",
            suggestions=suggestions,
        )

    def _build_typo_suggestions(
        self, typo_analysis: Dict[str, Any], defined_tables: set
    ) -> List[str]:
        """Build suggestions for typo validation error."""
        suggestions = []

        # Add specific suggestions for each typo
        for table in typo_analysis["likely_typos"]:
            if table in typo_analysis["suggestions_map"]:
                suggestions.append(
                    f"Did you mean '{typo_analysis['suggestions_map'][table]}' instead of '{table}'?"
                )

        # Add context information that verbose mode expects
        if defined_tables:
            suggestions.append(f"Available tables: {', '.join(sorted(defined_tables))}")

        # Add reference locations
        for table in typo_analysis["likely_typos"]:
            if table in typo_analysis["context_locations"]:
                locations_str = ", ".join(typo_analysis["context_locations"][table])
                suggestions.append(f"{table} referenced at: {locations_str}")

        return suggestions

    def _build_typo_error_message(self, likely_typos: List[str]) -> str:
        """Build error message for typo validation."""
        if len(likely_typos) == 1:
            return f"Referenced table '{likely_typos[0]}' might not be defined"
        else:
            return f"Referenced table '{', '.join(likely_typos)}' might not be defined"

    def _log_external_table_warnings(self, likely_typos: List[str]) -> None:
        """Log warnings for tables that might be external/intentional."""
        for table in set(table for table, _, _ in self._undefined_tables):
            if table not in likely_typos:
                logger.warning(
                    f"Table '{table}' is referenced but not defined - this might be an external table"
                )

    def _find_best_suggestion(
        self, undefined_table: str, defined_tables: set
    ) -> Optional[str]:
        """Find the best suggestion for a typo among defined tables."""
        best_match = None
        best_score = float("inf")

        for defined_table in defined_tables:
            # Check if this matches our typo detection criteria
            if self._tables_are_similar(undefined_table, defined_table):
                # Use edit distance as score (lower is better)
                score = self._edit_distance(undefined_table, defined_table)
                if score < best_score:
                    best_score = score
                    best_match = defined_table

        return best_match

    def _tables_are_similar(self, table1: str, table2: str) -> bool:
        """Check if two table names are similar enough to be considered typos.

        Following Zen of Python: Simple is better than complex.
        Practical patterns for real-world typos.
        """
        # Same length with small edit distance (1-2 character changes)
        if abs(len(table1) - len(table2)) <= 2:
            edit_dist = self._edit_distance(table1, table2)
            if edit_dist <= 2:
                return True

        # ENHANCED: Common typo patterns (appending or removing suffixes)
        # Examples: users_table vs users_table_failed, users_table_wrong, etc.
        longer, shorter = (
            (table1, table2) if len(table1) > len(table2) else (table2, table1)
        )

        if shorter in longer:
            # Check if it's a suffix pattern
            if longer.startswith(shorter):
                suffix = longer[len(shorter) :]
                # Common typo suffixes that people add when debugging/testing
                common_suffixes = [
                    "_failed",
                    "_wrong",
                    "_test",
                    "_old",
                    "_new",
                    "_backup",
                    "_temp",
                    "_copy",
                    "_typo",
                    "_error",
                    "_bad",
                    "_fixed",
                ]
                if suffix in common_suffixes or (
                    suffix.startswith("_") and len(suffix) <= 10
                ):
                    return True

            # Check if it's a prefix pattern
            if longer.endswith(shorter):
                prefix = longer[: -len(shorter)]
                # Common typo prefixes
                if prefix.endswith("_") and len(prefix) <= 10:
                    return True

        # Check for single character insertions/deletions that are common typos
        if abs(len(table1) - len(table2)) == 1:
            edit_dist = self._edit_distance(table1, table2)
            if edit_dist == 1:  # Single character difference
                return True

        return False

    def _is_likely_typo(self, undefined_table: str, defined_tables: set) -> bool:
        """Check if an undefined table is likely a typo of a defined table.

        Following Zen of Python: Simple is better than complex.
        Practicality beats purity - be conservative about flagging typos but catch obvious ones.

        Only flag as typos when there's strong evidence:
        1. Very close edit distance (1-2 characters different)
        2. OR similar table names with common typo patterns (appending/removing suffixes)
        3. BUT not very short table names (common test table names like 'data', 'users')
        """
        if not defined_tables:
            return False

        # Don't flag very short table names as typos (common test table names)
        if len(undefined_table) <= 3:
            return False

        for defined_table in defined_tables:
            # ENHANCED: Check for common typo patterns
            if self._tables_are_similar(undefined_table, defined_table):
                return True

        return False

    def _edit_distance(self, s1: str, s2: str) -> int:
        """Calculate edit distance between two strings.

        Following Zen of Python: Simple is better than complex.
        Basic edit distance calculation for typo detection.
        """
        if len(s1) < len(s2):
            return self._edit_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    # --- CYCLE DETECTION ---
    def _detect_cycles(self, resolver: DependencyResolver) -> List[List[str]]:
        cycles = []
        visited = set()
        path = []

        def dfs(node):
            if node in path:
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
                return
            if node in visited:
                return
            visited.add(node)
            path.append(node)
            for dep in resolver.dependencies.get(node, []):
                dfs(dep)
            path.pop()

        for node in resolver.dependencies:
            if node not in visited:
                dfs(node)
        return cycles

    def _format_cycle_error(self, cycles: List[List[str]]) -> str:
        if not cycles:
            return "No cycles found"
        lines = []
        for i, cycle in enumerate(cycles):
            readable_cycle = []
            for step_id in cycle:
                if step_id.startswith("transform_"):
                    readable_cycle.append(f"CREATE TABLE {step_id[10:]}")
                elif step_id.startswith("load_"):
                    readable_cycle.append(f"LOAD {step_id[5:]}")
                elif step_id.startswith("source_"):
                    readable_cycle.append(f"SOURCE {step_id[7:]}")
                elif step_id.startswith("export_"):
                    parts = step_id.split("_", 2)
                    if len(parts) > 2:
                        readable_cycle.append(f"EXPORT {parts[2]} to {parts[1]}")
                    else:
                        readable_cycle.append(step_id)
                else:
                    readable_cycle.append(step_id)
            cycle_str = " → ".join(readable_cycle)
            lines.append(f"Cycle {i + 1}: {cycle_str}")
        return "\n".join(lines)

    # --- SQL SYNTAX VALIDATION ---
    def _validate_sql_syntax(
        self, sql_query: str, step_id: str, line_number: int
    ) -> None:
        sql = sql_query.lower()
        if sql.count("(") != sql.count(")"):
            logger.warning(
                f"Possible syntax error in step {step_id} at line {line_number}: Unmatched parentheses - {sql.count('(')} opening vs {sql.count(')')} closing"
            )
        if not re.search(r"\bselect\b", sql):
            logger.warning(
                f"Possible issue in step {step_id} at line {line_number}: SQL query doesn't contain SELECT keyword"
            )
        if re.search(r"\bfrom\s*$", sql) or re.search(r"\bfrom\s+where\b", sql):
            logger.warning(
                f"Possible syntax error in step {step_id} at line {line_number}: FROM clause appears to be incomplete"
            )
        if sql.count("'") % 2 != 0:
            logger.warning(
                f"Possible syntax error in step {step_id} at line {line_number}: Unclosed single quotes"
            )
        if sql.count('"') % 2 != 0:
            logger.warning(
                f"Possible syntax error in step {step_id} at line {line_number}: Unclosed double quotes"
            )
        if ";" in sql[:-1]:
            statements = sql.split(";")
            if not statements[-1].strip():
                statements = statements[:-1]
            if len(statements) > 1:
                logger.info(
                    f"Step {step_id} at line {line_number} contains multiple SQL statements ({len(statements)}). Ensure this is intentional."
                )

    # --- JSON PARSING ---
    def _parse_json_token(self, json_str: str, context: str = "") -> Dict[str, Any]:
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            line_col = f"line {e.lineno}, column {e.colno}"
            error_msg = f"Invalid JSON in {context}: {str(e)} at {line_col}"
            if e.lineno > 1 and "\n" in json_str:
                lines = json_str.split("\n")
                if e.lineno <= len(lines):
                    error_line = lines[e.lineno - 1]
                    pointer = " " * (e.colno - 1) + "^"
                    error_msg += f"\n\n{error_line}\n{pointer}"
            if "Expecting property name" in str(e):
                error_msg += '\nTip: Property names must be in double quotes, e.g. {"name": "value"}'
            elif "Expecting ',' delimiter" in str(e):
                error_msg += "\nTip: Check for missing commas between items or an extra comma after the last item"
            elif "Expecting value" in str(e):
                error_msg += "\nTip: Make sure all property values are valid (string, number, object, array, true, false, null)"
            raise PlanningError(error_msg) from e

    # --- MAIN ENTRY POINT ---
    def build_plan(
        self, pipeline: Pipeline, variables: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Build execution plan from pipeline.

        Following Zen of Python: Explicit is better than implicit.
        Clean separation of concerns with comprehensive validation.
        """
        logger.debug("Building execution plan using extracted components")

        if variables is None:
            variables = {}

        # 1. Variable validation (critical - must happen first)
        self._validate_variable_references(pipeline, variables)
        logger.debug("Variable validation successful")

        # 2. Flatten conditional blocks based on variable values
        flattened_pipeline = self._flatten_conditional_blocks(pipeline, variables)
        logger.debug("Conditional blocks flattened")

        # 3. Build table-to-step mapping (includes duplicate detection)
        self._build_table_to_step_mapping(flattened_pipeline)
        logger.debug("Table-to-step mapping built")

        # 4. Build dependency graph for execution ordering
        self._build_dependency_graph(flattened_pipeline)
        logger.debug("Dependency graph built")

        # 5. Validate table references (raise errors for likely typos)
        self._validate_table_references()
        logger.debug("Table references validated")

        # 6. Resolve execution order from dependencies
        execution_order = self._resolve_execution_order()
        logger.debug(f"Execution order resolved: {len(execution_order)} steps")

        # 7. Build execution steps using existing working logic
        execution_steps = self._build_execution_steps(
            flattened_pipeline, execution_order
        )

        logger.debug(
            f"Successfully built execution plan with {len(execution_steps)} steps"
        )
        return execution_steps

    # --- CONDITIONALS & FLATTENING ---
    def _flatten_conditional_blocks(
        self, pipeline: Pipeline, variables: Dict[str, Any]
    ) -> Pipeline:
        """Process conditional blocks based on variable evaluation.

        Args:
        ----
            pipeline: Pipeline with conditional blocks
            variables: Variables for condition evaluation

        Returns:
        -------
            Flattened pipeline with only steps from true conditions

        """
        # Get pipeline-defined variables including those with defaults
        defined_vars = self._extract_set_defined_variables(pipeline)

        # Create a complete variable dictionary by combining provided and defined variables
        all_variables = variables.copy()
        for var_name, var_value in defined_vars.items():
            if var_name not in all_variables:
                all_variables[var_name] = var_value
                logger.debug(
                    f"Added pipeline-defined variable for conditional evaluation: {var_name}={var_value}"
                )

        # Create a modern VariableSubstitutionEngine with priority-based resolution
        # Note: We don't have access to CLI/profile variables here, so we use all_variables as CLI variables
        substitution_engine = VariableSubstitutionEngine(
            cli_variables=all_variables,
            profile_variables={},
            set_variables=defined_vars,
        )

        # Create evaluator with complete variable set and modern substitution engine
        logger.debug(f"Creating ConditionEvaluator with variables: {all_variables}")
        evaluator = ConditionEvaluator(all_variables, substitution_engine)

        flattened_pipeline = Pipeline()
        for step in pipeline.steps:
            if isinstance(step, ConditionalBlockStep):
                try:
                    active_steps = self._resolve_conditional_block(step, evaluator)
                    for active_step in active_steps:
                        flattened_pipeline.add_step(active_step)
                except (PlanningError, EvaluationError):
                    # Just pass through existing errors without adding more context
                    # They already have sufficient information
                    raise
                except Exception as e:
                    # For other unexpected errors, add context
                    error_msg = f"Error processing conditional block at line {step.line_number}."
                    error_detail = (
                        "\nPlease check your variable syntax. Common issues include:"
                        "\n- Incomplete variable references (e.g. '$' without '{name}')"
                        "\n- Missing variable definitions (use SET statements to define variables)"
                        "\n- Invalid variable names or syntax in conditional expressions"
                    )
                    # Log details but don't duplicate in the error message
                    logger.debug(f"{error_msg} Details: {str(e)}")
                    raise PlanningError(f"{error_msg}{error_detail}") from e
            else:
                flattened_pipeline.add_step(step)
        return flattened_pipeline

    def _resolve_conditional_block(
        self, conditional_block: ConditionalBlockStep, evaluator: ConditionEvaluator
    ) -> List[PipelineStep]:
        """Determine active branch based on condition evaluation."""
        logger.debug(
            f"Resolving conditional block at line {conditional_block.line_number}"
        )

        # Process each branch until a true condition is found
        for branch in conditional_block.branches:
            branch_result = self._try_evaluate_branch(branch, evaluator)
            if branch_result:
                return branch_result

        # If no branch condition is true, use the else branch if available
        if conditional_block.else_branch:
            logger.info("No conditions were true - using ELSE branch")
            return self._flatten_steps(conditional_block.else_branch, evaluator)

        # No condition was true and no else branch
        logger.warning(
            "No conditions were true and no else branch exists - skipping entire block"
        )
        return []

    def _try_evaluate_branch(
        self, branch: ConditionalBranchStep, evaluator: ConditionEvaluator
    ) -> Optional[List[PipelineStep]]:
        """Try to evaluate a condition branch and return steps if condition is true."""
        logger.debug(f"Branch condition from AST: '{branch.condition}'")
        try:
            # Do NOT catch EvaluationError here - let it propagate up with line information
            condition_result = evaluator.evaluate(branch.condition)
            if condition_result:
                logger.info(
                    f"Condition '{branch.condition}' evaluated to TRUE - using this branch"
                )
                return self._flatten_steps(branch.steps, evaluator)
            else:
                logger.debug(
                    f"Condition '{branch.condition}' evaluated to FALSE - skipping branch"
                )
                return None
        except EvaluationError as e:
            # Add line number context and re-raise
            error_msg = f"Error in condition: '{branch.condition}' at line {branch.line_number}.\n{str(e)}"
            # Log at DEBUG level only - let CLI handle user-facing errors
            logger.debug(error_msg)
            raise PlanningError(error_msg) from e
        except Exception as e:
            # Only catch other generic exceptions and log a warning
            logger.warning(
                f"Unexpected error evaluating condition: {branch.condition} at line {branch.line_number}. Error: {str(e)}"
            )
            return None

    def _flatten_steps(
        self, steps: List[PipelineStep], evaluator: ConditionEvaluator
    ) -> List[PipelineStep]:
        """Process steps and flatten any nested conditionals."""
        flat_steps = []
        for step in steps:
            if isinstance(step, ConditionalBlockStep):
                flat_steps.extend(self._resolve_conditional_block(step, evaluator))
            else:
                flat_steps.append(step)
        return flat_steps

    # --- DEPENDENCY GRAPH & EXECUTION ORDER ---
    def _build_dependency_graph(self, pipeline: Pipeline) -> None:
        """Build dependency graph for pipeline execution.

        Following Zen of Python: Explicit is better than implicit.
        Clear dependency relationships for proper execution order.
        """
        # Initialize dependencies tracking
        self.step_dependencies = {}

        # Generate step IDs for all steps (needed for dependency tracking)
        self._generate_step_ids(pipeline)

        # Use the table mapping we built earlier
        table_to_step = self._table_to_step_mapping or {}

        # Analyze dependencies for each step
        for step in pipeline.steps:
            if isinstance(step, SQLBlockStep):
                self._analyze_sql_dependencies(step, table_to_step)
            elif isinstance(step, ExportStep):
                self._analyze_export_dependencies(step, table_to_step)

        # Add load dependencies
        source_steps, load_steps = self._get_sources_and_loads(pipeline)
        self._add_load_dependencies(source_steps, load_steps)

        # Ensure all steps have a dependency entry (even if empty)
        for step in pipeline.steps:
            step_id = self._get_step_id(step)
            if step_id and step_id not in self.step_dependencies:
                self.step_dependencies[step_id] = []

    def _analyze_sql_dependencies(
        self, step: SQLBlockStep, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        sql_query = step.sql_query.lower()
        self._find_table_references(step, sql_query, table_to_step)

    def _analyze_export_dependencies(
        self, step: ExportStep, table_to_step: Dict[str, PipelineStep]
    ) -> None:
        """Analyze dependencies for an export step.

        Args:
        ----
            step: Export step to analyze
            table_to_step: Mapping of table names to steps

        """
        # First handle exports with SQL queries
        if hasattr(step, "sql_query") and step.sql_query:
            sql_query = step.sql_query.lower()
            self._find_table_references(step, sql_query, table_to_step)
            logger.debug(
                f"Found SQL dependencies for export step: {self._get_step_id(step)}"
            )

        # Handle direct table references (simple exports)
        elif hasattr(step, "table_name") and getattr(step, "table_name", None):
            table_name = getattr(step, "table_name", "").lower()
            if table_name in table_to_step:
                dependency_step = table_to_step[table_name]
                step_id = self._get_step_id(step)
                dependency_id = self._get_step_id(dependency_step)

                if step_id not in self.step_dependencies:
                    self.step_dependencies[step_id] = []

                if (
                    dependency_id
                    and dependency_id not in self.step_dependencies[step_id]
                ):
                    self.step_dependencies[step_id].append(dependency_id)
                    logger.debug(f"Added dependency: {step_id} -> {dependency_id}")

        # Ensure every export step has an entry in dependencies
        step_id = self._get_step_id(step)
        if step_id and step_id not in self.step_dependencies:
            self.step_dependencies[step_id] = []
            logger.debug(f"Added empty dependency entry for export step: {step_id}")

    def _add_dependency(
        self, dependent_step: PipelineStep, dependency_step: PipelineStep
    ) -> None:
        # FIXED: Use step_id_map for consistent step IDs instead of generating with index 0
        # This ensures dependency IDs match the actual step IDs in the final plan
        dependent_id = self.step_id_map.get(id(dependent_step))
        dependency_id = self.step_id_map.get(id(dependency_step))

        # Only add dependency if both step IDs exist
        if dependent_id and dependency_id:
            # Add to step_dependencies directly
            if dependent_id not in self.step_dependencies:
                self.step_dependencies[dependent_id] = []

            if dependency_id not in self.step_dependencies[dependent_id]:
                self.step_dependencies[dependent_id].append(dependency_id)
                logger.debug(f"Added dependency: {dependent_id} -> {dependency_id}")
        else:
            # Debug info for missing step IDs
            if not dependent_id:
                logger.warning(
                    f"Could not find step ID for dependent step: {type(dependent_step).__name__}"
                )
            if not dependency_id:
                logger.warning(
                    f"Could not find step ID for dependency step: {type(dependency_step).__name__}"
                )

    def _get_sources_and_loads(
        self, pipeline: Pipeline
    ) -> tuple[Dict[str, SourceDefinitionStep], List[LoadStep]]:
        source_steps = {}
        load_steps = []
        for step in pipeline.steps:
            if isinstance(step, SourceDefinitionStep):
                source_steps[step.name] = step
            elif isinstance(step, LoadStep):
                load_steps.append(step)
        return source_steps, load_steps

    def _add_load_dependencies(
        self, source_steps: Dict[str, SourceDefinitionStep], load_steps: List[LoadStep]
    ) -> None:
        for load_step in load_steps:
            source_name = load_step.source_name
            if source_name in source_steps:
                source_step = source_steps[source_name]
                self._add_dependency(load_step, source_step)

    def _generate_step_ids(self, pipeline: Pipeline) -> None:
        """Generate step IDs for all pipeline steps and create clean dependency mapping."""
        # First pass: Generate all step IDs
        self._create_step_id_mapping(pipeline)

        # Second pass: Create step dependencies using the new IDs
        self._create_clean_dependencies()

    def _create_step_id_mapping(self, pipeline: Pipeline) -> None:
        """Create mapping from object IDs to step IDs."""
        for i, step in enumerate(pipeline.steps):
            step_id = self._generate_step_id(step, i)
            self.step_id_map[id(step)] = step_id

    def _create_clean_dependencies(self) -> None:
        """Create clean step dependencies using the new IDs."""
        # Note: Dependencies are now added directly to step_dependencies in _add_dependency
        # so this method mainly ensures all steps have a dependency entry (even if empty)

        # Ensure all steps have a dependency entry (even if empty)
        for step_id in self.step_id_map.values():
            if step_id not in self.step_dependencies:
                self.step_dependencies[step_id] = []

    def _generate_step_id(self, step: PipelineStep, index: int) -> str:
        if isinstance(step, SourceDefinitionStep):
            return f"source_{step.name}"
        elif isinstance(step, LoadStep):
            # CRITICAL FIX: Make LoadStep IDs unique by including mode and index
            # This prevents multiple LOAD operations on same table from being merged
            mode = getattr(step, "mode", "REPLACE").lower()
            return f"load_{step.table_name}_{mode}_{index}"
        elif isinstance(step, SQLBlockStep):
            # Make SQL block step IDs unique by including index for CREATE OR REPLACE scenarios
            is_replace = getattr(step, "is_replace", False)
            replace_suffix = "_replace" if is_replace else ""
            return f"transform_{step.table_name}{replace_suffix}_{index}"
        elif isinstance(step, ExportStep):
            table_name = getattr(
                step, "table_name", None
            ) or self._extract_table_name_from_sql(getattr(step, "sql_query", ""))
            connector_type = getattr(step, "connector_type", "unknown").lower()
            if table_name:
                return f"export_{connector_type}_{table_name}"
            else:
                return f"export_{connector_type}_{index}"
        elif isinstance(step, SetStep):
            # SET statements are not execution steps but variable definitions
            # Give them a unique ID but they won't appear in the final execution plan
            return f"var_def_{step.variable_name}"
        else:
            return f"step_{index}"

    def _resolve_execution_order(self) -> List[str]:
        resolver = self._create_dependency_resolver()
        all_step_ids = list(self.step_id_map.values())
        if not all_step_ids:
            return []
        entry_points = self._find_entry_points(resolver, all_step_ids)
        try:
            execution_order = self._build_execution_order(resolver, entry_points)
        except Exception as e:
            try:
                cycles = self._detect_cycles(resolver)
                if cycles:
                    cycle_msg = self._format_cycle_error(cycles)
                    raise PlanningError(
                        f"Circular dependencies detected in pipeline:\n{cycle_msg}"
                    ) from e
            except Exception:
                pass
            raise PlanningError(f"Failed to resolve execution order: {str(e)}") from e
        self._ensure_all_steps_included(execution_order, all_step_ids)
        return execution_order

    def _create_dependency_resolver(self) -> DependencyResolver:
        resolver = DependencyResolver()
        for step_id, dependencies in self.step_dependencies.items():
            for dependency in dependencies:
                resolver.add_dependency(step_id, dependency)
        return resolver

    def _find_entry_points(
        self, resolver: DependencyResolver, all_step_ids: List[str]
    ) -> List[str]:
        entry_points = [
            step_id for step_id in all_step_ids if step_id not in resolver.dependencies
        ]
        if not entry_points and all_step_ids:
            entry_points = [all_step_ids[0]]
        return entry_points

    def _build_execution_order(
        self, resolver: DependencyResolver, entry_points: List[str]
    ) -> List[str]:
        execution_order = []
        for entry_point in entry_points:
            if entry_point in execution_order:
                continue
            step_order = resolver.resolve_dependencies(entry_point)
            for step_id in step_order:
                if step_id not in execution_order:
                    execution_order.append(step_id)
        return execution_order

    def _ensure_all_steps_included(
        self, execution_order: List[str], all_step_ids: List[str]
    ) -> None:
        for step_id in all_step_ids:
            if step_id not in execution_order:
                execution_order.append(step_id)

    def _build_execution_steps(
        self, pipeline: Pipeline, execution_order: List[str]
    ) -> List[Dict[str, Any]]:
        """Build final execution steps from pipeline.

        Following Zen of Python: Simple is better than complex.
        Clear step building with proper dependency ordering.

        CRITICAL FIX: Include ALL LoadSteps, not just those in table mapping.
        """
        logger.debug(
            f"Building execution steps for {len(execution_order)} ordered steps"
        )

        # Create step ID to pipeline step mapping for ordered steps
        step_id_to_pipeline_step = self._create_step_lookup_mapping(pipeline)

        # Process steps in dependency order
        execution_steps = self._process_steps_in_execution_order(
            execution_order, step_id_to_pipeline_step
        )

        # CRITICAL FIX: Add any missing LoadSteps that weren't in the dependency order
        # This handles multiple LOAD operations on the same table
        execution_steps = self._add_missing_load_steps(
            pipeline, execution_steps, step_id_to_pipeline_step
        )

        # CRITICAL FIX: Add any missing SQL block steps (CREATE OR REPLACE operations)
        execution_steps = self._add_missing_sql_block_steps(
            pipeline, execution_steps, step_id_to_pipeline_step
        )

        # Add any other missing steps (backward compatibility)
        execution_steps = self._add_missing_steps(
            pipeline, execution_steps, step_id_to_pipeline_step
        )

        logger.debug(f"Built {len(execution_steps)} execution steps total")
        return execution_steps

    def _create_step_lookup_mapping(
        self, pipeline: Pipeline
    ) -> Dict[str, PipelineStep]:
        """Create mapping from step_id to pipeline_step for faster lookup."""
        step_id_to_pipeline_step = {}
        for pipeline_step in pipeline.steps:
            # Skip SET statements from being added to the execution plan
            if isinstance(pipeline_step, SetStep):
                continue

            step_id = self._get_step_id(pipeline_step)
            if step_id:
                step_id_to_pipeline_step[step_id] = pipeline_step

        return step_id_to_pipeline_step

    def _process_steps_in_execution_order(
        self,
        execution_order: List[str],
        step_id_to_pipeline_step: Dict[str, PipelineStep],
    ) -> List[Dict[str, Any]]:
        """Process steps in the execution order."""
        execution_steps = []
        for step_id in execution_order:
            if step_id in step_id_to_pipeline_step:
                pipeline_step = step_id_to_pipeline_step[step_id]
                execution_step = self._build_execution_step(pipeline_step)
                if execution_step:  # Skip None returns (like SET statements)
                    execution_steps.append(execution_step)
        return execution_steps

    def _add_missing_steps(
        self,
        pipeline: Pipeline,
        execution_steps: List[Dict[str, Any]],
        step_id_to_pipeline_step: Dict[str, PipelineStep],
    ) -> List[Dict[str, Any]]:
        """Add steps that weren't included in the execution order."""
        existing_step_ids = [s["id"] for s in execution_steps]

        for pipeline_step in pipeline.steps:
            # Skip SET statements
            if isinstance(pipeline_step, SetStep):
                continue

            step_id = self._get_step_id(pipeline_step)
            if step_id and step_id not in existing_step_ids:
                logger.debug(f"Adding missing step to execution plan: {step_id}")
                execution_step = self._build_execution_step(pipeline_step)
                if execution_step:  # Skip None returns (like SET statements)
                    execution_steps.append(execution_step)

        return execution_steps

    def _get_step_id(self, step: PipelineStep) -> str:
        return self.step_id_map.get(id(step), "")

    def _extract_table_name_from_sql(self, sql_query: str) -> Optional[str]:
        from_match = re.search(r"FROM\s+([a-zA-Z0-9_]+)", sql_query, re.IGNORECASE)
        if from_match:
            return from_match.group(1)
        insert_match = re.search(
            r"INSERT\s+INTO\s+([a-zA-Z0-9_]+)", sql_query, re.IGNORECASE
        )
        if insert_match:
            return insert_match.group(1)
        update_match = re.search(r"UPDATE\s+([a-zA-Z0-9_]+)", sql_query, re.IGNORECASE)
        if update_match:
            return update_match.group(1)
        create_match = re.search(
            r"CREATE\s+TABLE\s+([a-zA-Z0-9_]+)", sql_query, re.IGNORECASE
        )
        if create_match:
            return create_match.group(1)
        return None

    def _build_execution_step(
        self, pipeline_step: PipelineStep
    ) -> Optional[Dict[str, Any]]:
        """Build a single execution step from a pipeline step.

        Args:
        ----
            pipeline_step: The pipeline step to convert

        Returns:
        -------
            An execution step dictionary or None for steps like SET that don't
            correspond to executable steps

        """
        step_id = self._get_step_id(pipeline_step)
        depends_on = self.step_dependencies.get(step_id, [])

        # Skip SET statements as they aren't execution steps, just variable definitions
        if isinstance(pipeline_step, SetStep):
            logger.debug(
                f"Skipping SET statement for {pipeline_step.variable_name} as it's not an execution step"
            )
            return None

        # Delegate to specific builders based on step type
        if isinstance(pipeline_step, SourceDefinitionStep):
            return self._generate_source_definition_step(pipeline_step)
        elif isinstance(pipeline_step, LoadStep):
            return self._build_load_step(pipeline_step, step_id, depends_on)
        elif isinstance(pipeline_step, SQLBlockStep):
            return self._build_sql_block_step(pipeline_step, step_id, depends_on)
        elif isinstance(pipeline_step, ExportStep):
            return self._build_export_step(pipeline_step, step_id, depends_on)
        else:
            return {
                "id": step_id,
                "type": "unknown",
                "depends_on": depends_on,
            }

    def _generate_source_definition_step(
        self, step: SourceDefinitionStep
    ) -> Dict[str, Any]:
        """Generate an execution step for a source definition with industry-standard parameters."""
        step_id = self.step_id_map.get(id(step), f"source_{step.name}")

        # Extract industry-standard parameters from SOURCE params
        params = step.params.copy()
        sync_mode = params.get("sync_mode", "full_refresh")
        cursor_field = params.get("cursor_field")
        primary_key = params.get("primary_key", [])

        # Ensure primary_key is always a list for consistency
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Check if this is a profile-based source definition (FROM syntax)
        if step.is_from_profile:
            # Handle FROM-based syntax
            return {
                "id": step_id,
                "type": "source_definition",
                "name": step.name,
                "is_from_profile": True,
                "profile_connector_name": step.profile_connector_name,
                "query": params,  # These are the OPTIONS for the source
                # Industry-standard parameters for incremental loading
                "sync_mode": sync_mode,
                "cursor_field": cursor_field,
                "primary_key": primary_key,
                "depends_on": [],
            }
        else:
            # Handle standard SOURCE syntax with enhanced parameter propagation
            return {
                "id": step_id,
                "type": "source_definition",
                "name": step.name,
                "connector_type": step.connector_type,  # Add connector_type at top level
                "source_connector_type": step.connector_type,  # Keep for backward compatibility
                "query": params,  # All SOURCE params including industry-standard ones
                # Industry-standard parameters promoted to top level for easy access
                "sync_mode": sync_mode,
                "cursor_field": cursor_field,
                "primary_key": primary_key,
                "depends_on": [],
            }

    def _build_load_step(
        self, step: LoadStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a load step."""
        source_name = step.source_name

        # Try to find the SOURCE definition to get the real connector type
        source_connector_type = "csv"  # Default fallback

        # Look for the source definition in the source_definitions mapping
        # This mapping is built during the build_plan process
        if (
            hasattr(self, "_source_definitions")
            and source_name in self._source_definitions
        ):
            source_def = self._source_definitions[source_name]
            source_connector_type = source_def.get("connector_type", "csv")

        return {
            "id": step_id,
            "type": "load",
            "name": step.table_name,
            "source_name": step.source_name,  # Top-level for executor compatibility
            "target_table": step.table_name,  # Top-level for executor compatibility
            "source_connector_type": source_connector_type,
            "mode": getattr(step, "mode", "REPLACE"),  # Include load mode
            "upsert_keys": getattr(step, "upsert_keys", []),
            "query": {
                "source_name": step.source_name,
                "table_name": step.table_name,
            },
            "depends_on": depends_on,
        }

    def _build_sql_block_step(
        self, step: SQLBlockStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a SQL block."""
        sql_query = step.sql_query
        if not sql_query.strip():
            logger.warning(f"Empty SQL query in step {step_id}")

        # Validate SQL syntax
        self._validate_sql_syntax(sql_query, step_id, getattr(step, "line_number", -1))

        return {
            "id": step_id,
            "type": "transform",
            "name": step.table_name,
            "query": sql_query,
            "is_replace": getattr(
                step, "is_replace", False
            ),  # Include the is_replace flag
            "depends_on": depends_on,
        }

    def _build_export_step(
        self, step: ExportStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for an export step."""
        # Determine table name from step or SQL query
        table_name = getattr(
            step, "table_name", None
        ) or self._extract_table_name_from_sql(getattr(step, "sql_query", ""))
        connector_type = getattr(step, "connector_type", "unknown")

        # Use the actual step_id or generate a fallback
        export_id = step_id
        if not export_id:
            export_id = f"export_{connector_type.lower()}_{table_name or 'unknown'}"

        # Get destination URI and substitute variables if any are provided
        destination_uri = getattr(step, "destination_uri", "")
        logger.debug(f"Export step {export_id} with destination: {destination_uri}")

        # Get options and substitute variables if any
        options = getattr(step, "options", {})

        # Return the execution step
        return {
            "id": export_id,
            "type": "export",
            "source_table": table_name,
            "source_connector_type": connector_type,
            "query": {
                "sql_query": getattr(step, "sql_query", ""),
                "destination_uri": destination_uri,
                "options": options,
                "type": connector_type,
            },
            "depends_on": depends_on,
        }

    def _build_source_definition_step(
        self, step: SourceDefinitionStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a source definition.

        This is a compatibility method that redirects to _generate_source_definition_step.
        """
        # Save step_id and dependencies for _generate_source_definition_step to use
        self.step_id_map[id(step)] = step_id
        self.step_dependencies[step_id] = depends_on

        # Delegate to the new method
        return self._generate_source_definition_step(step)

    def _add_missing_load_steps(
        self,
        pipeline: Pipeline,
        execution_steps: List[Dict[str, Any]],
        step_id_to_pipeline_step: Dict[str, PipelineStep],
    ) -> List[Dict[str, Any]]:
        """Add missing LoadSteps that weren't included in dependency ordering.

        CRITICAL FIX: Multiple LOAD operations on same table must all execute.
        The dependency ordering might only include the first LoadStep per table,
        but we need ALL LoadSteps to execute in order.
        """
        if not hasattr(self, "_all_load_steps"):
            return execution_steps  # No LoadSteps tracked

        existing_step_ids = {step.get("id") for step in execution_steps}

        for load_step in self._all_load_steps:
            # Generate step ID for this load step
            step_id = self._get_step_id(load_step)

            # If this LoadStep is not already in execution steps, add it
            if step_id not in existing_step_ids:
                logger.debug(f"Adding missing LoadStep: {step_id}")

                # Build execution step for this LoadStep
                execution_step = self._build_execution_step(load_step)
                if execution_step:
                    # Insert in the right position to maintain dependency order
                    # For now, append - more sophisticated ordering can be added later
                    execution_steps.append(execution_step)
                    existing_step_ids.add(step_id)

        return execution_steps

    def _add_missing_sql_block_steps(
        self,
        pipeline: Pipeline,
        execution_steps: List[Dict[str, Any]],
        step_id_to_pipeline_step: Dict[str, PipelineStep],
    ) -> List[Dict[str, Any]]:
        """Add missing SQL block steps (CREATE OR REPLACE operations) that weren't included in dependency ordering.

        CRITICAL FIX: Multiple SQL block steps on same table must all execute.
        The dependency ordering might only include the first SQL block step per table,
        but we need ALL SQL block steps to execute in order.
        """
        if not hasattr(self, "_all_sql_block_steps"):
            return execution_steps  # No SQL block steps tracked

        existing_step_ids = {step.get("id") for step in execution_steps}

        for sql_block_step in self._all_sql_block_steps:
            # Generate step ID for this SQL block step
            step_id = self._get_step_id(sql_block_step)

            # If this SQL block step is not already in execution steps, add it
            if step_id not in existing_step_ids:
                logger.debug(f"Adding missing SQL block step: {step_id}")

                # Build execution step for this SQL block step
                execution_step = self._build_execution_step(sql_block_step)
                if execution_step:
                    # Insert in the right position to maintain dependency order
                    # For now, append - more sophisticated ordering can be added later
                    execution_steps.append(execution_step)
                    existing_step_ids.add(step_id)

        return execution_steps


# --- OPERATION PLANNER ---
class OperationPlanner:
    def __init__(self):
        self.plan_builder = ExecutionPlanBuilder()

    def plan(
        self, pipeline: Pipeline, variables: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        try:
            return self.plan_builder.build_plan(pipeline, variables)
        except Exception as e:
            raise PlanningError(f"Failed to plan operations: {str(e)}") from e

    def to_json(self, plan: List[Dict[str, Any]]) -> str:
        return json.dumps(plan, indent=2)

    def from_json(self, json_str: str) -> List[Dict[str, Any]]:
        return json.loads(json_str)


# --- MAIN PLANNER ---
class Planner:
    """Interface to the ExecutionPlanBuilder with a simplified API.

    Following Zen of Python: Simple is better than complex.
    Supports optional dependency injection for testability.
    """

    def __init__(
        self,
        builder: Optional[ExecutionPlanBuilder] = None,
        config: Optional[PlannerConfig] = None,
    ):
        """Initialize the planner with optional dependency injection.

        Args:
            builder: Optional custom ExecutionPlanBuilder
            config: Optional configuration for component injection

        Following Zen of Python:
        - Simple is better than complex: Default construction for simple usage
        - Explicit is better than implicit: Clear injection points for testing
        - Practicality beats purity: Backward compatibility maintained
        """
        if builder is not None:
            self.builder = builder
            logger.debug("Planner initialized with injected builder")
        elif config is not None:
            self.builder = ExecutionPlanBuilder(config=config)
            logger.debug("Planner initialized with configuration-based builder")
        else:
            self.builder = ExecutionPlanBuilder()
            logger.debug("Planner initialized with default builder")

    def create_plan(
        self,
        pipeline: Pipeline,
        variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Create an execution plan from a pipeline.

        Args:
        ----
            pipeline: The pipeline to build a plan for
            variables: Variables to substitute in the plan (CLI variables - highest priority)
            profile_variables: Profile variables (medium priority)

        Returns:
        -------
            The execution plan as a list of operation dictionaries

        Priority order for variable substitution:
        1. CLI variables (highest priority)
        2. Profile variables (medium priority)
        3. SET variables in pipeline (lower priority)
        4. Environment variables (fallback)
        5. Default values in ${var|default} expressions (only used when no other value is found)

        """
        # First extract SET variables including default values from the pipeline
        set_variables = self.builder._extract_set_defined_variables(pipeline)
        logger.debug(f"SET variables with defaults from pipeline: {set_variables}")

        # Create VariableSubstitutionEngine with priority-based resolution
        engine = VariableSubstitutionEngine(
            cli_variables=variables,
            profile_variables=profile_variables,
            set_variables=set_variables,
        )

        # Build the plan using all variables for validation purposes
        # For backward compatibility with builder expectations, provide merged variables
        all_variables = {}
        if set_variables:
            all_variables.update(set_variables)
        if profile_variables:
            all_variables.update(profile_variables)
        if variables:
            all_variables.update(variables)

        execution_plan = self.builder.build_plan(pipeline, all_variables)

        # Apply variable substitution to the execution plan using the modern engine
        execution_plan = engine.substitute(execution_plan)

        # Check for any missing variables and log warnings
        # Convert the plan back to text to validate variable usage
        plan_text = json.dumps(execution_plan)
        missing_vars = engine.validate_required_variables(plan_text)
        if missing_vars:
            logger.warning(
                f"Plan contains unresolved variables: {', '.join(missing_vars)}"
            )

        return execution_plan
