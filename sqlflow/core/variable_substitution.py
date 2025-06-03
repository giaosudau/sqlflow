"""Centralized variable substitution utilities for SQLFlow.

This module provides a unified interface for variable substitution across the entire codebase,
eliminating code duplication and ensuring consistent behavior.

Environment variables are automatically available in variable substitution, providing
seamless integration with .env files and system environment variables.
"""

import keyword
import os
import re
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class VariableSubstitutionEngine:
    """Centralized engine for variable substitution with consistent behavior.

    Supports both explicit variables and automatic environment variable lookup with
    priority-based resolution. Priority order:
    1. CLI variables (highest priority)
    2. Profile variables
    3. SET variables from pipeline
    4. Environment variables
    5. Default values (${var|default})
    """

    # Compile regex patterns once for better performance
    VARIABLE_PATTERN = re.compile(r"\$\{([^}]+)\}")
    VARIABLE_WITH_DEFAULT_PATTERN = re.compile(r"\$\{([^|}]+)\|([^}]+)\}")

    def __init__(
        self,
        variables: Optional[Dict[str, Any]] = None,
        cli_variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
        set_variables: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the substitution engine.

        Args:
            variables: Dictionary of variable name-value pairs (for backward compatibility)
            cli_variables: CLI variables (highest priority)
            profile_variables: Profile variables (medium priority)
            set_variables: SET variables from pipeline (lower priority)

        Note: Environment variables are automatically available as fallback.
        If only 'variables' is provided, it maintains backward compatibility.
        """
        # Backward compatibility: if only 'variables' provided, use it directly
        if (
            variables is not None
            and cli_variables is None
            and profile_variables is None
            and set_variables is None
        ):
            self.variables = variables
            self.cli_variables = None
            self.profile_variables = None
            self.set_variables = None
            self._use_priority_mode = False
        else:
            # Priority mode: use separate variable sources
            self.variables = {}  # Not used in priority mode
            self.cli_variables = cli_variables or {}
            self.profile_variables = profile_variables or {}
            self.set_variables = set_variables or {}
            self._use_priority_mode = True

    def _get_variable_value(self, var_name: str) -> Optional[str]:
        """Get variable value respecting priority order.

        Priority order:
        1. CLI variables (highest priority)
        2. Profile variables
        3. SET variables from pipeline
        4. Environment variables
        5. Default values handled separately

        Args:
            var_name: Name of the variable to look up

        Returns:
            Variable value if found, None otherwise
        """
        if self._use_priority_mode:
            # Priority-based resolution
            # 1. CLI variables (highest priority)
            if var_name in self.cli_variables:
                return self.cli_variables[var_name]

            # 2. Profile variables
            if var_name in self.profile_variables:
                return self.profile_variables[var_name]

            # 3. SET variables from pipeline
            if var_name in self.set_variables:
                return self.set_variables[var_name]

            # 4. Environment variables (fallback)
            env_value = os.environ.get(var_name)
            if env_value is not None:
                logger.debug(
                    f"Using environment variable ${{{var_name}}} = '{env_value}'"
                )
                return env_value
        else:
            # Backward compatibility mode
            # First check explicit variables (highest priority)
            if var_name in self.variables:
                return self.variables[var_name]

            # Then check environment variables (fallback)
            env_value = os.environ.get(var_name)
            if env_value is not None:
                logger.debug(
                    f"Using environment variable ${{{var_name}}} = '{env_value}'"
                )
                return env_value

        return None

    def substitute(self, data: Any) -> Any:
        """Substitute variables in any data structure.

        Args:
            data: String, dict, list, or any other data that may contain variables

        Returns:
            Data with variables substituted
        """
        if isinstance(data, str):
            return self._substitute_string(data)
        elif isinstance(data, dict):
            return self._substitute_dict(data)
        elif isinstance(data, list):
            return self._substitute_list(data)
        else:
            return data

    def _substitute_string(self, text: str) -> str:
        """Substitute variables in a string.

        Args:
            text: String containing variable references

        Returns:
            String with variables substituted
        """
        logger.debug(f"Substituting variables in text: {text[:100]}...")

        def replace_variable(match: re.Match) -> str:
            var_expr = match.group(1)

            # Get local context around the variable (50 chars before and after)
            start_pos = max(0, match.start() - 50)
            end_pos = min(len(text), match.end() + 50)
            local_context = text[start_pos:end_pos]

            # Handle expressions with defaults: ${var|default}
            if "|" in var_expr:
                var_name, default_value = var_expr.split("|", 1)
                var_name = var_name.strip()
                default_value = default_value.strip()

                # Remove quotes from default value if present
                default_value = self._unquote_value(default_value)

                # Check explicit variables first, then environment variables
                value = self._get_variable_value(var_name)
                if value is not None:
                    logger.debug(f"Using variable ${{{var_name}}} = '{value}'")
                    return self._format_value_for_context(value, local_context)
                else:
                    logger.debug(
                        f"Using default value '{default_value}' for variable ${{{var_name}}}"
                    )
                    return self._format_value_for_context(default_value, local_context)
            else:
                # Simple variable reference: ${var}
                var_name = var_expr.strip()
                value = self._get_variable_value(var_name)
                if value is not None:
                    logger.debug(f"Using variable ${{{var_name}}} = '{value}'")
                    return self._format_value_for_context(value, local_context)
                else:
                    logger.warning(
                        f"Variable '{var_name}' not found in variables or environment and no default provided"
                    )
                    # In condition contexts, treat missing variables as None
                    if self._is_condition_context(local_context):
                        logger.debug(
                            f"Treating missing variable ${{{var_name}}} as None in condition context"
                        )
                        return "None"
                    else:
                        return match.group(
                            0
                        )  # Keep original text in non-condition contexts

        result = self.VARIABLE_PATTERN.sub(replace_variable, text)
        logger.debug(f"Substitution result: {result}")
        return result

    def _format_value_for_context(self, value: Any, context: str) -> str:
        """Format a value appropriately for its context.

        Args:
            value: The value to format
            context: The surrounding text context

        Returns:
            Formatted value as string
        """
        str_value = str(value)

        # Don't add quotes in JSON contexts (contains quotes and colons)
        if self._is_json_context(context):
            return str_value

        # If this looks like a condition context (contains ==, !=, etc.)
        # and the value is a string that could cause parsing issues, quote it
        is_condition = self._is_condition_context(context)
        needs_quotes = self._needs_quoting(str_value)

        if is_condition and needs_quotes:
            result = f"'{str_value}'"
            return result

        return str_value

    def _is_json_context(self, text: str) -> bool:
        """Check if the text appears to be a JSON context."""
        # Look for JSON-like patterns: quotes and colons
        return '"' in text and ":" in text

    def _is_condition_context(self, text: str) -> bool:
        """Check if the text appears to be a condition expression."""
        condition_operators = [
            "==",
            "!=",
            "<=",
            ">=",
            "<",
            ">",
            " and ",
            " or ",
            " not ",
        ]
        return any(op in text for op in condition_operators)

    def _needs_quoting(self, value: str) -> bool:
        """Check if a string value needs to be quoted to avoid parsing issues."""
        # Quote if it's a Python keyword
        if keyword.iskeyword(value):
            return True

        # Quote if it contains hyphens (to avoid being parsed as subtraction)
        if "-" in value:
            return True

        # Quote if it contains spaces
        if " " in value:
            return True

        # Quote if it's not a pure numeric value (int, float, etc.)
        # This ensures that string values like "global", "test", etc. are properly quoted
        try:
            # Try to parse as a number (int or float)
            float(value)
            return False  # It's a number, no quotes needed
        except ValueError:
            # It's not a number, so it should be quoted in conditional contexts
            return True

    def _substitute_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute variables in a dictionary recursively."""
        result = {}
        for key, value in data_dict.items():
            result[key] = self.substitute(value)
        return result

    def _substitute_list(self, data_list: List[Any]) -> List[Any]:
        """Substitute variables in a list recursively."""
        return [self.substitute(item) for item in data_list]

    def _unquote_value(self, value: str) -> str:
        """Remove surrounding quotes from a string value."""
        if len(value) >= 2:
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                return value[1:-1]
        return value

    def update_variables(self, new_variables: Dict[str, Any]) -> None:
        """Update the variables dictionary.

        Args:
            new_variables: New variables to add or update
        """
        if self._use_priority_mode:
            # In priority mode, update CLI variables (highest priority)
            self.cli_variables.update(new_variables)
            logger.debug(f"Updated CLI variables: {new_variables}")
        else:
            # Backward compatibility mode
            self.variables.update(new_variables)
            logger.debug(f"Updated variables: {new_variables}")

    def validate_required_variables(
        self, text: str, required_vars: Optional[List[str]] = None
    ) -> List[str]:
        """Validate that all required variables are available.

        Checks both explicit variables and environment variables.

        Args:
            text: Text to check for variable references
            required_vars: Optional list of required variable names

        Returns:
            List of missing variable names
        """
        missing_vars = []

        # Find all variable references in the text
        for match in self.VARIABLE_PATTERN.finditer(text):
            var_expr = match.group(1)

            # Extract variable name (handle both ${var} and ${var|default})
            if "|" in var_expr:
                var_name = var_expr.split("|", 1)[0].strip()
                # Variables with defaults are not considered missing
                continue
            else:
                var_name = var_expr.strip()

            # Check if variable is available (explicit or environment)
            if self._get_variable_value(var_name) is None:
                missing_vars.append(var_name)

        # Also check explicitly required variables
        if required_vars:
            for var_name in required_vars:
                if (
                    self._get_variable_value(var_name) is None
                    and var_name not in missing_vars
                ):
                    missing_vars.append(var_name)

        return list(set(missing_vars))  # Remove duplicates


# Convenience functions for backward compatibility
def substitute_variables(data: Any, variables: Dict[str, Any]) -> Any:
    """Convenience function for variable substitution.

    Args:
        data: Data structure to substitute variables in
        variables: Dictionary of variable values

    Returns:
        Data with variables substituted
    """
    engine = VariableSubstitutionEngine(variables)
    return engine.substitute(data)


def validate_variables(text: str, variables: Dict[str, Any]) -> List[str]:
    """Convenience function to validate required variables.

    Args:
        text: Text to check for variable references
        variables: Available variables

    Returns:
        List of missing variable names
    """
    engine = VariableSubstitutionEngine(variables)
    return engine.validate_required_variables(text)
