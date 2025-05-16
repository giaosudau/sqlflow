"""Variable handling utilities for SQLFlow CLI."""

import logging
import re
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class VariableHandler:
    """Handles variable substitution in SQLFlow pipeline text."""

    def __init__(self, variables: Optional[Dict[str, Any]] = None):
        """Initialize the variable handler.

        Args:
            variables: Dictionary of variable name-value pairs
        """
        self.variables = variables or {}
        self.var_pattern = re.compile(r"\$\{([^}|]+)(?:\|([^}]+))?\}")

    def substitute_variables(self, text: str) -> str:
        """Substitute variables in the text.

        Args:
            text: Text containing variables in ${var} or ${var|default} format

        Returns:
            Text with variables substituted
        """

        def replace(match: re.Match) -> str:
            var_name, default = self._parse_variable_expr(match.group(0))
            value = self.variables.get(var_name)

            if value is None and default is None:
                logger.warning(
                    f"Variable '{var_name}' not found and no default provided"
                )
                return match.group(0)  # Keep original text

            if value is None:
                logger.debug(
                    f"Using default value '{default}' for variable '{var_name}'"
                )
                return str(default)

            return str(value)

        return self.var_pattern.sub(replace, text)

    def _parse_variable_expr(self, expr: str) -> Tuple[str, Optional[str]]:
        """Parse a variable expression into name and default value.

        Args:
            expr: Variable expression like ${var} or ${var|default}

        Returns:
            Tuple of (variable_name, default_value)
        """
        match = self.var_pattern.match(expr)
        if not match:
            return expr.strip("${}"), None

        var_name = match.group(1)
        default = match.group(2) if len(match.groups()) > 1 else None

        return var_name, default

    def validate_variable_usage(self, text: str) -> bool:
        """Validate that all required variables are provided.

        Args:
            text: Text containing variables

        Returns:
            True if all required variables are available or have defaults
        """
        missing_vars = []
        for match in self.var_pattern.finditer(text):
            var_name, default = self._parse_variable_expr(match.group(0))
            if var_name not in self.variables and default is None:
                missing_vars.append(var_name)

        if missing_vars:
            logger.error(f"Missing required variables: {', '.join(missing_vars)}")
            return False

        return True
