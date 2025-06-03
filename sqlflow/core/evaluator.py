"""Evaluator for conditional expressions in SQLFlow.

This module provides a class to evaluate conditional expressions with variable substitution.
"""

import ast
import re
from typing import Any, Dict, Optional

from sqlflow.core.variable_substitution import VariableSubstitutionEngine
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class EvaluationError(Exception):
    """Exception raised for evaluation errors."""

    def __init__(self, message: str):
        """Initialize an EvaluationError.

        Args:
        ----
            message: Error message

        """
        self.message = message
        super().__init__(message)


class ConditionEvaluator:
    """Evaluates conditional expressions with variable substitution."""

    def __init__(
        self,
        variables: Dict[str, Any],
        substitution_engine: Optional[VariableSubstitutionEngine] = None,
    ):
        """Initialize with a variables dictionary.

        Args:
        ----
            variables: Dictionary of variable names to values
            substitution_engine: Optional VariableSubstitutionEngine to use for substitution.
                                If not provided, creates a backward-compatible one.

        """
        self.variables = variables
        if substitution_engine is not None:
            self.substitution_engine = substitution_engine
        else:
            # Backward compatibility: create engine with just variables
            self.substitution_engine = VariableSubstitutionEngine(variables)
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
        ----
            condition: String containing the condition to evaluate

        Returns:
        -------
            Boolean result of the condition evaluation

        Raises:
        ------
            EvaluationError: If the condition cannot be evaluated

        """
        logger.debug(f"Evaluating condition: '{condition}'")

        # First substitute variables using centralized engine
        substituted_condition = self._substitute_variables(condition)
        logger.debug(f"After substitution: '{substituted_condition}'")

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

        logger.debug(f"After true/false replacement: '{substituted_condition}'")

        try:
            # Safe evaluation using Python's ast module
            return self._safe_eval(substituted_condition)
        except Exception as e:
            logger.debug(f"Error during evaluation: {e}")
            raise EvaluationError(
                f"Failed to evaluate condition: {condition}. Error: {str(e)}"
            )

    def _substitute_variables(self, condition: str) -> str:
        """Replace ${var} with the variable value using centralized engine.

        Args:
        ----
            condition: Condition containing variable references

        Returns:
        -------
            Condition with variables substituted

        """
        # Use the centralized substitution engine
        substituted = self.substitution_engine._substitute_string(condition)

        # The centralized engine returns values as-is, but for condition evaluation
        # we need to ensure proper formatting for Python AST parsing
        return self._format_for_ast_evaluation(substituted)

    def _format_for_ast_evaluation(self, condition: str) -> str:
        """Format substituted condition for AST evaluation.

        Args:
        ----
            condition: Condition with variables already substituted

        Returns:
        -------
            Condition formatted for Python AST evaluation
        """
        # This method handles any additional formatting needed for AST evaluation
        # The centralized engine should handle most cases correctly
        return condition

    def _safe_eval(self, expr: str) -> bool:
        """Safely evaluate an expression to a boolean result.

        Args:
        ----
            expr: Expression to evaluate

        Returns:
        -------
            Boolean result of the evaluation

        Raises:
        ------
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
        ----
            node: The AST node to evaluate

        Returns:
        -------
            The result of evaluating the node

        Raises:
        ------
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
        elif isinstance(node, ast.BinOp):
            return self._eval_binop(node)
        else:
            raise EvaluationError(f"Unsupported AST node type: {type(node).__name__}")

    def _eval_bool_op(self, node: ast.BoolOp) -> bool:
        """Evaluate a boolean operation (AND/OR).

        Args:
        ----
            node: The boolean operation node

        Returns:
        -------
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
        ----
            node: The unary operation node

        Returns:
        -------
            The result of the unary operation

        """
        if isinstance(node.op, ast.Not):
            return not self._eval_node(node.operand)
        raise EvaluationError(f"Unsupported unary operator: {type(node.op).__name__}")

    def _eval_compare(self, node: ast.Compare) -> bool:
        """Evaluate a comparison operation.

        Args:
        ----
            node: The comparison node

        Returns:
        -------
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
        ----
            op_type: The operator type
            left: Left operand
            right: Right operand

        Returns:
        -------
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
        ----
            op_type: The operator type
            left: Left operand
            right: Right operand

        Returns:
        -------
            Result of the comparison

        """
        # Ensure bool_val is the boolean and str_val is the string
        if isinstance(left, bool) and isinstance(right, str):
            bool_val, str_val = left, right
        elif isinstance(right, bool) and isinstance(left, str):
            bool_val, str_val = right, left
        else:
            # This should not happen due to check in _is_string_boolean_comparison
            return False

        # Normalize the string for comparison (str_val is guaranteed to be str here)
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
        ----
            node: The name node

        Returns:
        -------
            The value of the name

        """
        if node.id == "True":
            return True
        elif node.id == "False":
            return False
        elif node.id == "None":
            return None
        else:
            # Treat unknown identifiers as string literals
            # This handles cases where unquoted strings are parsed as identifiers
            # including Python reserved keywords like 'global'
            logger.debug(f"Treating unknown identifier '{node.id}' as string literal")
            return node.id

    def _eval_binop(self, node: ast.BinOp) -> Any:
        """Evaluate a binary operation.

        This handles cases where unquoted strings with hyphens are incorrectly
        parsed as subtraction operations (e.g., 'us-east' becomes 'us - east').

        Args:
        ----
            node: The binary operation node

        Returns:
        -------
            The result of the binary operation

        Raises:
        ------
            EvaluationError: If the operation is not supported
        """
        # Handle subtraction operations that might be misinterpreted string literals
        if isinstance(node.op, ast.Sub):
            left = self._eval_node(node.left)
            right = self._eval_node(node.right)

            # If both operands are strings/names, this might be a misinterpreted string literal
            if isinstance(left, str) and isinstance(right, str):
                # Reconstruct the original string (e.g., 'us' - 'east' -> 'us-east')
                reconstructed = f"{left}-{right}"
                logger.debug(
                    f"Reconstructed string from binary operation: {reconstructed}"
                )
                return reconstructed

            # If they're numbers, perform actual subtraction
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                return left - right

        # For other binary operations, raise an error as they're not supported in conditions
        raise EvaluationError(
            f"Unsupported binary operation: {type(node.op).__name__}. "
            "Conditions should use comparison operators (==, !=, <, >, <=, >=) and logical operators (and, or, not)."
        )
