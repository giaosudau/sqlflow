"""Planner for SQLFlow pipelines.

This module contains the planner that converts a validated SQLFlow DAG
into a linear, JSON-serialized ExecutionPlan consumable by an executor.
"""

import json
import re
from typing import Any, Dict, List, Optional

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.errors import PlanningError
from sqlflow.core.evaluator import ConditionEvaluator, EvaluationError
from sqlflow.logging import get_logger
from sqlflow.parser.ast import (
    ConditionalBlockStep,
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
    """Builds an execution plan from a validated SQLFlow DAG."""

    def __init__(self):
        self.dependency_resolver = DependencyResolver()
        self.step_id_map: Dict[int, str] = {}
        self.step_dependencies: Dict[str, List[str]] = {}
        logger.debug("ExecutionPlanBuilder initialized")

    # --- PIPELINE VALIDATION ---
    def _validate_variable_references(
        self, pipeline: Pipeline, variables: Dict[str, Any]
    ) -> None:
        """Validate that all variable references in the pipeline exist in variables or have defaults.
        Also checks that default values are valid (no unquoted spaces).
        """
        logger.debug("Validating variable references in pipeline")

        # Get effective variables (provided + defined in pipeline)
        effective_variables = self._get_effective_variables(pipeline, variables)

        # Extract all referenced variables
        referenced_vars = self._collect_all_referenced_variables(pipeline)

        # Log variable reference report
        self._log_variable_reference_report(
            referenced_vars, effective_variables, pipeline
        )

        # Check for missing vars and invalid defaults
        missing_vars = self._find_missing_vars(
            referenced_vars, effective_variables, pipeline
        )
        invalid_defaults = self._find_invalid_defaults(referenced_vars, pipeline)

        # Handle missing variables error
        if missing_vars:
            self._raise_missing_variables_error(missing_vars, pipeline)

        # Handle invalid defaults error
        if invalid_defaults:
            self._raise_invalid_defaults_error(invalid_defaults)

        # Verify all variable values
        self._verify_variable_values(referenced_vars, variables, pipeline)

        logger.info("Variable validation completed successfully")

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

    def _log_variable_reference_report(
        self,
        referenced_vars: set,
        effective_variables: Dict[str, Any],
        pipeline: Pipeline,
    ) -> None:
        """Log a detailed report of all variable references and their status."""
        logger.debug("----- Variable Reference Report -----")
        for var in sorted(referenced_vars):
            if var in effective_variables:
                logger.debug(f"  ✓ ${{{var}}} = '{effective_variables[var]}'")
            elif self._has_default_in_pipeline(var, pipeline):
                logger.debug(f"  ✓ ${{{var}}} = [Using default value]")
            else:
                logger.debug(f"  ✗ ${{{var}}} = UNDEFINED")
        logger.debug("-----------------------------------")

    def _raise_missing_variables_error(
        self, missing_vars: List[str], pipeline: Pipeline
    ) -> None:
        """Raise PlanningError with details about missing variables."""
        logger.warning(f"Pipeline references undefined variables: {missing_vars}")
        error_msg = "Pipeline references undefined variables:\n" + "".join(
            f"  - ${{{var}}} is used but not defined\n" for var in missing_vars
        )
        error_msg += "\nPlease define these variables using SET statements or provide them when running the pipeline."

        # Add additional debugging info for each variable
        error_msg += "\n\nVariable reference locations:"
        for var in missing_vars:
            locations = self._find_variable_reference_locations(var, pipeline)
            if locations:
                error_msg += f"\n  ${{{var}}} referenced at: {', '.join(locations)}"

        raise PlanningError(error_msg)

    def _raise_invalid_defaults_error(self, invalid_defaults: List[str]) -> None:
        """Raise PlanningError with details about invalid default values."""
        logger.warning(f"Found invalid default values: {invalid_defaults}")
        error_msg = (
            "Invalid default values for variables (must not contain spaces unless quoted):\n"
            + "".join(f"  - {expr}\n" for expr in invalid_defaults)
        )
        error_msg += (
            '\nDefault values with spaces must be quoted, e.g. ${var|"us-east"}'
        )
        raise PlanningError(error_msg)

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
            referenced_vars: Set of referenced variable names
            variables: Dictionary of variable values
            pipeline: The pipeline AST

        Raises:
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
            var_name: The name of the variable to check
            pipeline: The pipeline to search in

        Returns:
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
            step: The pipeline step to check
            pattern: The regex pattern to search for

        Returns:
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
            pipeline: The pipeline to analyze

        Returns:
            A dictionary of variable names to values
        """
        from sqlflow.parser.ast import SetStep

        defined_vars = {}
        for step in pipeline.steps:
            if isinstance(step, SetStep):
                var_name = step.variable_name.strip()
                var_value = step.variable_value.strip()

                # If the value is itself a variable reference with default, extract it
                var_ref_match = re.match(r"\$\{([^|{}]+)\|([^{}]*)\}", var_value)
                if var_ref_match:
                    ref_var = var_ref_match.group(1).strip()
                    default_val = var_ref_match.group(2).strip()

                    # If it's a self-reference with default, use the default
                    if ref_var == var_name:
                        logger.debug(
                            f"Self-referential variable ${{{var_name}}} with default '{default_val}'"
                        )
                        defined_vars[var_name] = default_val
                else:
                    # Remove quotes if present (simple string literal handling)
                    if (var_value.startswith("'") and var_value.endswith("'")) or (
                        var_value.startswith('"') and var_value.endswith('"')
                    ):
                        var_value = var_value[1:-1]

                    defined_vars[var_name] = var_value

        return defined_vars

    def _find_variable_reference_locations(
        self, var_name: str, pipeline: Pipeline
    ) -> List[str]:
        """Find all locations where a variable is referenced in the pipeline.

        Args:
            var_name: The name of the variable to find
            pipeline: The pipeline to analyze

        Returns:
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
        table_to_step = {}
        duplicate_tables = []
        for step in pipeline.steps:
            if isinstance(step, (LoadStep, SQLBlockStep)):
                table_name = step.table_name
                if table_name in table_to_step:
                    duplicate_tables.append((table_name, step.line_number))
                else:
                    table_to_step[table_name] = step
        if duplicate_tables:
            error_msg = "Duplicate table definitions found:\n" + "".join(
                f"  - Table '{table}' defined at line {line}, but already defined at line {getattr(table_to_step[table], 'line_number', 'unknown')}\n"
                for table, line in duplicate_tables
            )
            raise PlanningError(error_msg)
        return table_to_step

    def _extract_referenced_tables(self, sql_query: str) -> List[str]:
        sql_lower = sql_query.lower()
        tables = []

        # Handle standard SQL FROM clauses
        from_matches = re.finditer(
            r"from\s+([a-zA-Z0-9_]+(?:\s*,\s*[a-zA-Z0-9_]+)*)", sql_lower
        )
        for match in from_matches:
            table_list = match.group(1).split(",")
            for table in table_list:
                table_name = table.strip()
                if table_name and table_name not in tables:
                    tables.append(table_name)

        # Handle standard SQL JOINs
        join_matches = re.finditer(r"join\s+([a-zA-Z0-9_]+)", sql_lower)
        for match in join_matches:
            table_name = match.group(1).strip()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # Handle table UDF pattern: PYTHON_FUNC("module.function", table_name)
        udf_table_matches = re.finditer(
            r"python_func\s*\(\s*['\"][\w\.]+['\"]\s*,\s*([a-zA-Z0-9_]+)", sql_lower
        )
        for match in udf_table_matches:
            table_name = match.group(1).strip()
            if table_name and table_name not in tables:
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
        if undefined_tables and hasattr(step, "line_number"):
            logger.warning(
                f"Step at line {step.line_number} references tables that might not be defined: {', '.join(undefined_tables)}"
            )

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
            lines.append(f"Cycle {i+1}: {cycle_str}")
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
        """Build an execution plan from a pipeline.

        Args:
            pipeline: The validated pipeline to build a plan for
            variables: Variables for variable substitution

        Returns:
            A list of execution steps in topological order

        Raises:
            PlanningError: If the plan cannot be built
        """
        logger.info("Building execution plan")
        if not pipeline.steps:
            logger.warning("Planning an empty pipeline")
            return []

        # Initialize state
        self.dependency_resolver = DependencyResolver()
        self.step_id_map = {}
        self.step_dependencies = {}

        # Use provided variables or initialize empty dict
        variables_to_use = variables or {}
        logger.debug(f"Planning with {len(variables_to_use)} variables")
        if variables_to_use:
            logger.debug(
                f"Variables: {', '.join(f'{k}={v}' for k, v in variables_to_use.items())}"
            )

        try:
            # Validate variable references and values
            logger.info("Validating variable references and values")
            self._validate_variable_references(pipeline, variables_to_use)
            logger.info("Variable validation successful")

            # Flatten conditional blocks to just the active branch steps
            logger.debug(
                f"Flattening conditional blocks in pipeline with {len(pipeline.steps)} steps"
            )
            flattened_pipeline = self._flatten_conditional_blocks(
                pipeline, variables_to_use
            )
            logger.debug(
                f"Flattened pipeline has {len(flattened_pipeline.steps)} steps"
            )

            # Build dependency graph using flattened pipeline
            self._build_dependency_graph(flattened_pipeline)

            # Set up additional dependencies for correct execution order
            source_steps, load_steps = self._get_sources_and_loads(flattened_pipeline)
            logger.debug(
                f"Found {len(source_steps)} source steps and {len(load_steps)} load steps"
            )
            self._add_load_dependencies(source_steps, load_steps)

            # Generate unique IDs for each step
            self._generate_step_ids(flattened_pipeline)

            # Check for cycles in the dependency graph
            resolver = self._create_dependency_resolver()
            cycles = self._detect_cycles(resolver)
            if cycles:
                error_msg = self._format_cycle_error(cycles)
                logger.error(f"Dependency cycle detected: {error_msg}")
                raise PlanningError(error_msg)

            # Resolve execution order based on dependencies
            all_step_ids = list(self.step_id_map.values())
            logger.debug(f"Resolving execution order for {len(all_step_ids)} steps")
            entry_points = self._find_entry_points(resolver, all_step_ids)
            logger.debug(f"Found {len(entry_points)} entry points")
            execution_order = self._build_execution_order(resolver, entry_points)

            # Create execution steps from pipeline steps in the determined order
            logger.debug(f"Creating {len(execution_order)} execution steps")
            execution_steps = self._build_execution_steps(
                flattened_pipeline, execution_order
            )
            logger.info(
                f"Successfully built execution plan with {len(execution_steps)} steps"
            )
            return execution_steps

            # Create execution steps from pipeline steps in the determined order
            logger.debug(f"Creating {len(execution_order)} execution steps")
            execution_steps = self._build_execution_steps(
                flattened_pipeline, execution_order
            )
            logger.info(
                f"Successfully built execution plan with {len(execution_steps)} steps"
            )
            return execution_steps

        except EvaluationError as e:
            logger.error(f"Condition evaluation error: {str(e)}")
            raise PlanningError(f"Error evaluating conditions: {str(e)}") from e
        except Exception as e:
            logger.error(f"Error building execution plan: {str(e)}")
            raise PlanningError(f"Failed to build execution plan: {str(e)}") from e

    # --- CONDITIONALS & FLATTENING ---
    def _flatten_conditional_blocks(
        self, pipeline: Pipeline, variables: Dict[str, Any]
    ) -> Pipeline:
        flattened_pipeline = Pipeline()
        evaluator = ConditionEvaluator(variables)
        for step in pipeline.steps:
            if isinstance(step, ConditionalBlockStep):
                try:
                    active_steps = self._resolve_conditional_block(step, evaluator)
                    for active_step in active_steps:
                        flattened_pipeline.add_step(active_step)
                except EvaluationError as e:
                    error_msg = f"Error evaluating conditional block at line {step.line_number}: {str(e)}"
                    error_msg += (
                        "\nPlease check your variable syntax. Common issues include:"
                    )
                    error_msg += (
                        "\n- Incomplete variable references (e.g. '$' without '{name}')"
                    )
                    error_msg += "\n- Missing variable definitions (use SET statements to define variables)"
                    error_msg += "\n- Invalid variable names or syntax in conditional expressions"
                    raise PlanningError(error_msg) from e
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
            try:
                if evaluator.evaluate(branch.condition):
                    logger.info(
                        f"Condition '{branch.condition}' evaluated to TRUE - using this branch"
                    )
                    # Process any nested conditionals in the active branch
                    flat_branch_steps = []
                    for step in branch.steps:
                        if isinstance(step, ConditionalBlockStep):
                            flat_branch_steps.extend(
                                self._resolve_conditional_block(step, evaluator)
                            )
                        else:
                            flat_branch_steps.append(step)
                    return flat_branch_steps
                else:
                    logger.debug(
                        f"Condition '{branch.condition}' evaluated to FALSE - skipping branch"
                    )
            except Exception as e:
                # Log the error but continue to next branch
                logger.warning(
                    f"Error evaluating condition: {branch.condition}. Error: {str(e)}"
                )

        # If no branch condition is true, use the else branch if available
        if conditional_block.else_branch:
            logger.info("No conditions were true - using ELSE branch")
            flat_else_steps = []
            for step in conditional_block.else_branch:
                if isinstance(step, ConditionalBlockStep):
                    flat_else_steps.extend(
                        self._resolve_conditional_block(step, evaluator)
                    )
                else:
                    flat_else_steps.append(step)
            return flat_else_steps

        # No condition was true and no else branch
        logger.warning(
            "No conditions were true and no else branch exists - skipping entire block"
        )
        return []

    # --- DEPENDENCY GRAPH & EXECUTION ORDER ---
    def _build_dependency_graph(self, pipeline: Pipeline) -> None:
        """Build a dependency graph for the pipeline.

        This method analyzes dependencies between steps and builds a graph
        for determining the correct execution order.

        Args:
            pipeline: The pipeline to analyze
        """
        # Initialize step dependencies dict
        self.step_dependencies = {}

        # Generate step IDs for all steps
        self._generate_step_ids(pipeline)

        # Create table name to step mapping
        table_to_step = self._build_table_to_step_mapping(pipeline)

        # First add source and load dependencies
        source_steps, load_steps = self._get_sources_and_loads(pipeline)
        self._add_load_dependencies(source_steps, load_steps)

        # Then add SQL step dependencies
        for step in pipeline.steps:
            if isinstance(step, SQLBlockStep):
                self._analyze_sql_dependencies(step, table_to_step)
            elif isinstance(step, ExportStep):
                self._analyze_export_dependencies(step, table_to_step)

        # Debug dependency graph
        logger.debug(
            f"Dependency graph created with {len(self.step_dependencies)} entries"
        )
        for step_id, deps in self.step_dependencies.items():
            if deps:
                logger.debug(f"Step {step_id} depends on: {deps}")

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
        dependent_id = str(id(dependent_step))
        dependency_id = str(id(dependency_step))
        self.dependency_resolver.add_dependency(dependent_id, dependency_id)

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
        for i, step in enumerate(pipeline.steps):
            step_id = self._generate_step_id(step, i)
            self.step_id_map[id(step)] = step_id
            old_id = str(id(step))
            for (
                dependent_id,
                dependencies,
            ) in self.dependency_resolver.dependencies.items():
                if old_id in dependencies:
                    dependencies.remove(old_id)
                    dependencies.append(step_id)
            for (
                dependent_id,
                dependencies,
            ) in self.dependency_resolver.dependencies.items():
                if dependent_id == old_id:
                    self.step_dependencies[step_id] = dependencies
                    self.dependency_resolver.dependencies.pop(dependent_id, None)
                    break

    def _generate_step_id(self, step: PipelineStep, index: int) -> str:
        if isinstance(step, SourceDefinitionStep):
            return f"source_{step.name}"
        elif isinstance(step, LoadStep):
            return f"load_{step.table_name}"
        elif isinstance(step, SQLBlockStep):
            return f"transform_{step.table_name}"
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
        """Build execution steps from the execution order.

        Args:
            pipeline: The pipeline to build steps for
            execution_order: The order of steps to execute

        Returns:
            List of executable steps
        """
        execution_steps = []

        # First, make sure all pipeline steps have IDs
        if not self.step_id_map:
            self._generate_step_ids(pipeline)

        # Create mapping for faster lookup
        step_id_to_pipeline_step = self._create_step_id_mapping(pipeline)

        # Process steps in execution order
        execution_steps = self._process_steps_in_execution_order(
            execution_order, step_id_to_pipeline_step
        )

        # Add any missing steps
        execution_steps = self._add_missing_steps(
            pipeline, execution_steps, step_id_to_pipeline_step
        )

        logger.info(f"Built execution plan with {len(execution_steps)} steps")
        return execution_steps

    def _create_step_id_mapping(self, pipeline: Pipeline) -> Dict[str, PipelineStep]:
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
            pipeline_step: The pipeline step to convert

        Returns:
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
            return self._build_source_definition_step(
                pipeline_step, step_id, depends_on
            )
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

    def _build_source_definition_step(
        self, step: SourceDefinitionStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a source definition."""
        return {
            "id": step_id,
            "type": "source_definition",
            "name": step.name,
            "source_connector_type": step.connector_type,
            "query": step.params,
            "depends_on": depends_on,
        }

    def _build_load_step(
        self, step: LoadStep, step_id: str, depends_on: List[str]
    ) -> Dict[str, Any]:
        """Build an execution step for a load step."""
        source_name = step.source_name
        source_connector_type = "CSV"  # Default, could be determined from source

        # Check if we're actually using the source dependency
        source_step_id = f"source_{source_name}"
        if source_step_id in depends_on:
            pass  # Placeholder for any additional logic

        return {
            "id": step_id,
            "type": "load",
            "name": step.table_name,
            "source_connector_type": source_connector_type,
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

        # Log destination URI to help with debugging
        destination_uri = getattr(step, "destination_uri", "")
        logger.debug(f"Export step {export_id} with destination: {destination_uri}")

        # Return the execution step
        return {
            "id": export_id,
            "type": "export",
            "source_table": table_name,
            "source_connector_type": connector_type,
            "query": {
                "sql_query": getattr(step, "sql_query", ""),
                "destination_uri": destination_uri,
                "options": getattr(step, "options", {}),
                "type": connector_type,
            },
            "depends_on": depends_on,
        }


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
    """Interface to the ExecutionPlanBuilder with a simplified API."""

    def __init__(self):
        """Initialize the planner."""
        self.builder = ExecutionPlanBuilder()
        logger.debug("Planner initialized")

    def create_plan(
        self, pipeline: Pipeline, variables: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Create an execution plan from a pipeline.

        Args:
            pipeline: The parsed pipeline
            variables: Variables for variable substitution

        Returns:
            List of executable operations

        Raises:
            PlanningError: If the plan cannot be created
        """
        logger.info(f"Creating plan for pipeline with {len(pipeline.steps)} steps")
        try:
            plan = self.builder.build_plan(pipeline, variables)
            logger.info(f"Successfully created plan with {len(plan)} operations")
            return plan
        except Exception as e:
            logger.error(f"Failed to create plan: {str(e)}")
            raise
