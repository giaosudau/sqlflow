"""Evaluator for conditional expressions in SQLFlow.

This module provides a class to evaluate conditional expressions with variable substitution.
"""

import ast
import re
from typing import Any, Dict, Optional

from sqlflow.core.variables.manager import VariableConfig, VariableManager
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
        variable_manager: Optional[VariableManager] = None,
    ):
        """Initialize with a variables dictionary.

        Args:
        ----
            variables: Dictionary of variable names to values
            variable_manager: Optional VariableManager to use for substitution.
                                If not provided, creates one from variables.

        """
        self.variables = variables
        if variable_manager is not None:
            self.variable_manager = variable_manager
        else:
            # Create manager with variables
            config = VariableConfig(cli_variables=variables)
            self.variable_manager = VariableManager(config)
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
        substituted_condition = self.substitute_variables(condition)
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

    def substitute_variables(self, condition: str) -> str:
        """Substitute variables in a condition string for condition evaluation.

        Uses the new unified variable management system for consistent variable handling,
        then applies condition-specific formatting for AST evaluation.

        Args:
        ----
            condition: Original condition string with variables

        Returns:
        -------
            Condition with variables substituted and formatted for AST evaluation

        """
        from sqlflow.core.variables.parser import StandardVariableParser

        parse_result = StandardVariableParser.find_variables(condition)

        if not parse_result.has_variables:
            return condition

        new_parts = []
        last_end = 0

        for expr in parse_result.expressions:
            # Append the text between the last match and this one
            new_parts.append(condition[last_end : expr.span[0]])

            # Check if variable is inside quotes using proper context detection
            start_pos = expr.span[0]
            end_pos = expr.span[1]
            inside_quotes = self._is_inside_quoted_string(condition, start_pos, end_pos)

            formatted_value = None
            if expr.variable_name in self.variables:
                value = self.variables[expr.variable_name]
                formatted_value = self._format_value_for_ast(value, inside_quotes)
            elif expr.default_value is not None:
                formatted_value = self._format_value_for_ast(
                    expr.default_value, inside_quotes
                )
            else:
                # For AST evaluation, missing variables become None
                formatted_value = "None"

            # Append the substituted value
            new_parts.append(formatted_value)
            last_end = expr.span[1]

        # Append the rest of the string after the last match
        new_parts.append(condition[last_end:])

        return "".join(new_parts)

    def _is_inside_quotes(self, condition: str, start_pos: int, end_pos: int) -> bool:
        """Check if a variable is inside quotes."""
        # Check if we're immediately surrounded by quotes
        if start_pos > 0 and end_pos < len(condition):
            char_before = condition[start_pos - 1]
            char_after = condition[end_pos]
            if (char_before == "'" and char_after == "'") or (
                char_before == '"' and char_after == '"'
            ):
                return True
        return False

    def _is_inside_quoted_string(
        self, condition: str, start_pos: int, end_pos: int
    ) -> bool:
        """Check if a variable is inside quotes using proper context detection."""
        # Track quote state by scanning from beginning (same logic as SQLGenerator)
        in_single_quotes = False
        in_double_quotes = False
        i = 0

        while i < start_pos:
            char = condition[i]

            if char == "'" and not in_double_quotes:
                # Check for escaped quote
                if i > 0 and condition[i - 1] == "\\":
                    i += 1
                    continue
                in_single_quotes = not in_single_quotes
            elif char == '"' and not in_single_quotes:
                # Check for escaped quote
                if i > 0 and condition[i - 1] == "\\":
                    i += 1
                    continue
                in_double_quotes = not in_double_quotes

            i += 1

        # Variable is inside quotes if we're currently in a quoted section
        return in_single_quotes or in_double_quotes

    def _format_value_for_ast(self, value: Any, inside_quotes: bool = False) -> str:
        """Format a value for AST evaluation context.

        Args:
        ----
            value: The value to format
            inside_quotes: Whether the variable is already inside quotes in the template

        Returns:
        -------
            String representation suitable for AST evaluation
        """
        if value is None:
            return "None"
        elif isinstance(value, str):
            return self._format_string_for_ast(value, inside_quotes)
        elif isinstance(value, bool):
            # Keep boolean values as unquoted for AST evaluation
            return str(value)
        elif isinstance(value, (int, float)):
            # Keep numeric values as unquoted for AST evaluation
            return str(value)
        else:
            # For other types, convert to string and quote
            if inside_quotes:
                return str(value)
            return f"'{value}'"

    def _format_string_for_ast(self, value: str, inside_quotes: bool = False) -> str:
        """Format a string for AST evaluation context.

        Args:
        ----
            value: The string to format
            inside_quotes: Whether the string is already inside quotes in the template

        Returns:
        -------
            String representation suitable for AST evaluation
        """
        if value.lower() == "true":
            # Convert string 'true' to unquoted boolean for AST evaluation
            return "True"
        elif value.lower() == "false":
            # Convert string 'false' to unquoted boolean for AST evaluation
            return "False"
        elif value.isdigit():
            # Convert string numbers to unquoted numbers for AST evaluation
            return value
        elif self._is_sql_expression(value):
            # Don't quote SQL expressions like lists
            return value
        else:
            # Quote string values for AST evaluation unless already inside quotes
            if inside_quotes:
                return value
            return f"'{value}'"

    def _is_sql_expression(self, value: str) -> bool:
        """Check if a string value is already a properly formatted SQL expression."""
        if not isinstance(value, str):
            return False

        value = value.strip()

        # Check for SQL lists (values separated by commas, with quoted elements)
        if "," in value and ("'" in value or '"' in value):
            # This looks like a SQL list: 'value1','value2' or "value1","value2"
            return True

        # Check for function calls
        if "(" in value and value.endswith(")"):
            return True

        # Check for other SQL expressions (this can be expanded as needed)
        # For now, we'll be conservative and only handle the list case
        return False

    def _format_for_ast_evaluation(self, condition: str) -> str:
        """Format substituted condition for AST evaluation.

        Args:
        ----
            condition: Condition with variables already substituted

        Returns:
        -------
            Condition formatted for Python AST evaluation
        """
        from sqlflow.core.variables.parser import StandardVariableParser

        # Handle any remaining unsubstituted variables (missing variables) - convert to None
        parse_result = StandardVariableParser.find_variables(condition)

        if parse_result.has_variables:
            new_parts = []
            last_end = 0

            for expr in parse_result.expressions:
                # Append the text between the last match and this one
                new_parts.append(condition[last_end : expr.span[0]])

                # Convert missing variables to None for AST evaluation
                new_parts.append("None")
                last_end = expr.span[1]

            # Append the rest of the string after the last match
            new_parts.append(condition[last_end:])

            condition = "".join(new_parts)

        # Find unquoted identifiers and quote them if they look like string values
        # This handles cases like: global == 'us-east' -> 'global' == 'us-east'

        # Pattern to find unquoted identifiers that could be string values
        # Look for word characters (possibly with hyphens) that aren't already quoted
        # and appear in comparison contexts

        # First, handle identifiers with hyphens (like us-east-1)
        pattern_hyphen = (
            r"(?<!')(?<!\")\b([a-zA-Z][a-zA-Z0-9]*(-[a-zA-Z0-9]+)+)\b(?!')(?!\")"
        )

        def quote_identifier(match):
            identifier = match.group(1)
            return f"'{identifier}'"

        condition = re.sub(pattern_hyphen, quote_identifier, condition)

        # Then, handle simple word identifiers that appear before comparison operators
        # Look for patterns like: word == 'value' or word != 'value'
        pattern_word = (
            r"(?<!')(?<!\")\b([a-zA-Z][a-zA-Z0-9_]*)\b(?!')(?!\") *(?===|!=|<|>)"
        )

        def quote_word_before_comparison(match):
            word = match.group(1)
            # Don't quote Python keywords or boolean values
            python_keywords = {"True", "False", "None", "and", "or", "not", "in", "is"}
            if word in python_keywords:
                return match.group(0)  # Return unchanged
            return f"'{word}'" + match.group(0)[len(word) :]

        condition = re.sub(pattern_word, quote_word_before_comparison, condition)

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
