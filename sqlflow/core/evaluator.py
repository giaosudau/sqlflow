"""Evaluator for conditional expressions in SQLFlow.

This module provides a class to evaluate conditional expressions with variable substitution.
"""

import ast
import logging
import re
from typing import Any, Dict

logger = logging.getLogger(__name__)


class EvaluationError(Exception):
    """Exception raised for evaluation errors."""

    def __init__(self, message: str):
        """Initialize an EvaluationError.

        Args:
            message: Error message
        """
        self.message = message
        super().__init__(message)


class ConditionEvaluator:
    """Evaluates conditional expressions with variable substitution."""

    def __init__(self, variables: Dict[str, Any]):
        """Initialize with a variables dictionary.

        Args:
            variables: Dictionary of variable names to values
        """
        self.variables = variables
        # Define operators that are allowed
        self.operators = {
            # Comparison operators
            ast.Eq: lambda a, b: a == b,  # ==
            ast.NotEq: lambda a, b: a != b,  # !=
            ast.Lt: lambda a, b: a < b,  # <
            ast.LtE: lambda a, b: a <= b,  # <=
            ast.Gt: lambda a, b: a > b,  # >
            ast.GtE: lambda a, b: a >= b,  # >=
            # Logical operators
            ast.And: lambda a, b: a and b,  # and
            ast.Or: lambda a, b: a or b,  # or
            ast.Not: lambda a: not a,  # not
            # Constants and literals
            ast.Constant: lambda node: node.value,
        }

    def evaluate(self, condition: str) -> bool:
        """Evaluate a condition expression to a boolean result.

        Args:
            condition: String containing the condition to evaluate

        Returns:
            Boolean result of the condition evaluation

        Raises:
            EvaluationError: If the condition cannot be evaluated
        """
        # First substitute variables
        substituted_condition = self._substitute_variables(condition)

        # Detect accidental use of '=' instead of '==' (not part of '==', '!=', '>=', '<=')
        if re.search(r"(?<![=!<>])=(?![=])", substituted_condition):
            raise EvaluationError(
                f"Syntax error in condition: '{condition}'.\n"
                "Hint: Use '==' for equality, not '='. "
                "Example: IF ${var} == 'value' THEN ..."
            )

        # Handle case-insensitive true/false by replacing them with True/False
        # This allows for consistent handling of boolean literals regardless of case
        substituted_condition = re.sub(r"(?i)\btrue\b", "True", substituted_condition)
        substituted_condition = re.sub(r"(?i)\bfalse\b", "False", substituted_condition)

        try:
            # Safe evaluation using Python's ast module
            return self._safe_eval(substituted_condition)
        except Exception as e:
            raise EvaluationError(
                f"Failed to evaluate condition: {condition}. Error: {str(e)}"
            )

    def _substitute_variables(self, condition: str) -> str:
        """Replace ${var} with the variable value.

        Args:
            condition: Condition containing variable references

        Returns:
            Condition with variables substituted
        """
        pattern = r"\$\{([^}]+)\}"

        def replace_var(match):
            var_expr = match.group(1)
            if "|" in var_expr:
                # Handle default value
                var_name, default = var_expr.split("|", 1)
                var_name = var_name.strip()
                default = default.strip()

                if var_name in self.variables:
                    value = self.variables[var_name]
                else:
                    # Use the default value, properly typed
                    logger.debug(
                        f"Using default value '{default}' for variable '{var_name}'"
                    )
                    return self._format_value_with_type_inference(default)
            else:
                var_name = var_expr.strip()
                if var_name not in self.variables:
                    # Check if this is a self-reference to a variable that was defined with a default
                    # For example: SET use_csv = "${use_csv|true}";
                    # The variable would be defined in the SET but the evaluator might not see it
                    # We should still log a warning but use the default that was set
                    match_with_default = re.search(
                        r"\$\{" + re.escape(var_name) + r"\|([^}]+)\}", condition
                    )
                    if match_with_default:
                        default = match_with_default.group(1).strip()
                        logger.debug(
                            f"Found self-referential variable ${{{var_name}}} with default '{default}'"
                        )
                        return self._format_value_with_type_inference(default)

                    logger.warning(f"Variable {var_name} not found in context")
                    return "None"  # Missing variable becomes None

                value = self.variables[var_name]

            return self._format_value(value)

        # Substitute variables with their values
        return re.sub(pattern, replace_var, condition)

    def _format_value(self, value: Any) -> str:
        """Format a value for substitution in a condition.

        Args:
            value: Value to format

        Returns:
            Formatted value as a string
        """
        if isinstance(value, str):
            # Keep strings as quoted strings
            if value.startswith("'") and value.endswith("'"):
                return value  # Already properly quoted
            if value.startswith('"') and value.endswith('"'):
                return value  # Already properly quoted
            return f"'{value}'"
        elif isinstance(value, bool):
            return str(value)  # Returns 'True' or 'False'
        elif value is None:
            return "None"
        else:
            return str(value)  # Numbers and other types

    def _format_value_with_type_inference(self, value: str) -> str:
        """Format a value with type inference for default values.

        Args:
            value: String value to format with type inference

        Returns:
            Properly typed and formatted value as a string
        """
        # Strip quotes if present
        if (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        ):
            return value  # Already a string literal

        # Handle boolean values
        if value.lower() == "true":
            return "True"
        if value.lower() == "false":
            return "False"

        # Handle None/null
        if value.lower() == "none" or value.lower() == "null":
            return "None"

        # Try to parse as number
        try:
            # Check if it's an integer
            int_val = int(value)
            return str(int_val)
        except ValueError:
            try:
                # Check if it's a float
                float_val = float(value)
                return str(float_val)
            except ValueError:
                # If not a number, treat as string
                return f"'{value}'"

    def _safe_eval(self, expr: str) -> bool:
        """Safely evaluate an expression to a boolean result.

        Args:
            expr: Expression to evaluate

        Returns:
            Boolean result of the evaluation

        Raises:
            EvaluationError: If the expression cannot be evaluated safely
        """
        try:
            # Parse the expression into an AST
            tree = ast.parse(expr, mode="eval").body

            # Evaluate the AST
            result = self._eval_node(tree)

            # Ensure result is boolean
            if not isinstance(result, bool):
                raise EvaluationError(
                    f"Expression does not evaluate to a boolean: {expr}"
                )

            return result
        except Exception as e:
            if isinstance(e, EvaluationError):
                raise
            raise EvaluationError(f"Error evaluating expression: {expr}. {str(e)}")

    def _eval_node(self, node: ast.AST) -> Any:
        """Evaluate a single AST node.

        Args:
            node: The AST node to evaluate

        Returns:
            The result of evaluating the node

        Raises:
            EvaluationError: If the node cannot be evaluated
        """
        if isinstance(node, ast.BoolOp):
            return self._eval_bool_op(node)
        elif isinstance(node, ast.UnaryOp):
            return self._eval_unary_op(node)
        elif isinstance(node, ast.Compare):
            return self._eval_compare(node)
        elif isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Name):
            return self._eval_name(node)
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Num):
            return node.n
        else:
            raise EvaluationError(f"Unsupported AST node type: {type(node).__name__}")

    def _eval_bool_op(self, node: ast.BoolOp) -> bool:
        """Evaluate a boolean operation (AND/OR).

        Args:
            node: The boolean operation node

        Returns:
            The result of the boolean operation
        """
        if isinstance(node.op, ast.And):
            # Short-circuit AND
            result = True
            for value in node.values:
                val = self._eval_node(value)
                result = result and val
                if not result:
                    break
            return result
        elif isinstance(node.op, ast.Or):
            # Short-circuit OR
            result = False
            for value in node.values:
                val = self._eval_node(value)
                result = result or val
                if result:
                    break
            return result
        else:
            raise EvaluationError(
                f"Unsupported boolean operator: {type(node.op).__name__}"
            )

    def _eval_unary_op(self, node: ast.UnaryOp) -> bool:
        """Evaluate a unary operation (NOT).

        Args:
            node: The unary operation node

        Returns:
            The result of the unary operation
        """
        if isinstance(node.op, ast.Not):
            return not self._eval_node(node.operand)
        raise EvaluationError(f"Unsupported unary operator: {type(node.op).__name__}")

    def _eval_compare(self, node: ast.Compare) -> bool:
        """Evaluate a comparison operation.

        Args:
            node: The comparison node

        Returns:
            The result of the comparison
        """
        left = self._eval_node(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            op_type = type(op)
            if op_type not in self.operators:
                raise EvaluationError(
                    f"Unsupported comparison operator: {op_type.__name__}"
                )
            right = self._eval_node(comparator)

            # Use helper method for string-boolean comparisons
            if self._is_string_boolean_comparison(op_type, left, right):
                return self._evaluate_string_boolean_comparison(op_type, left, right)

            return self.operators[op_type](left, right)

        # This should never happen as there's always at least one operator
        raise EvaluationError("Invalid comparison expression")

    def _is_string_boolean_comparison(
        self, op_type: type, left: Any, right: Any
    ) -> bool:
        """Check if this is a comparison between a string and a boolean.

        Args:
            op_type: The operator type
            left: Left operand
            right: Right operand

        Returns:
            True if this is a string-boolean comparison that needs special handling
        """
        is_eq_op = op_type in (ast.Eq, ast.NotEq)
        is_bool_string_pair = (isinstance(left, bool) and isinstance(right, str)) or (
            isinstance(right, bool) and isinstance(left, str)
        )
        return is_eq_op and is_bool_string_pair

    def _evaluate_string_boolean_comparison(
        self, op_type: type, left: Any, right: Any
    ) -> bool:
        """Handle special case for string-boolean comparisons.

        Args:
            op_type: The operator type
            left: Left operand
            right: Right operand

        Returns:
            Result of the comparison
        """
        # Ensure bool_val is the boolean and str_val is the string
        if isinstance(left, bool) and isinstance(right, str):
            bool_val, str_val = left, right
        else:
            bool_val, str_val = right, left

        # Normalize the string for comparison
        normalized_str = str_val.lower()

        # Handle equality and inequality differently
        if op_type == ast.Eq:
            return (bool_val and normalized_str == "true") or (
                not bool_val and normalized_str == "false"
            )
        elif op_type == ast.NotEq:
            return not (
                (bool_val and normalized_str == "true")
                or (not bool_val and normalized_str == "false")
            )

        # Should not happen due to check in _is_string_boolean_comparison
        return False

    def _eval_name(self, node: ast.Name) -> Any:
        """Evaluate a name node.

        Args:
            node: The name node

        Returns:
            The value of the name
        """
        if node.id == "True":
            return True
        elif node.id == "False":
            return False
        elif node.id == "None":
            return None
        raise EvaluationError(f"Unknown identifier: {node.id}")
