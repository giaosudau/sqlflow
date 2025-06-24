"""Variable handling utilities for SQLFlow CLI."""

import logging
from typing import Any, Dict, Optional, Tuple

from sqlflow.core.variables import (
    find_variables,
    resolve_variables,
    substitute_variables,
)

logger = logging.getLogger(__name__)


class VariableHandler:
    """Handles variable substitution in SQLFlow pipeline text using V2 functions."""

    def __init__(self, variables: Optional[Dict[str, Any]] = None):
        """Initialize the variable handler.

        Args:
        ----
            variables: Dictionary of variable name-value pairs

        """
        self._variables = variables or {}
        logger.debug("VariableHandler initialized with V2 functions")

    @property
    def variables(self) -> Dict[str, Any]:
        """Get the variables dictionary for backward compatibility."""
        return self._variables

    def substitute_variables(self, text: str) -> str:
        """Substitute variables in the text.

        Args:
        ----
            text: Text containing variables in ${var} or ${var|default} format

        Returns:
        -------
            Text with variables substituted

        """
        if not text:
            return text

        # Use V2 substitute_variables function
        return substitute_variables(text, self._variables)

    def validate_variable_usage(self, text: str) -> bool:
        """Validate that all required variables are provided.

        Args:
        ----
            text: Text containing variables

        Returns:
        -------
            True if all required variables are available or have defaults

        """
        # Use V2 validation functions
        referenced_vars = find_variables(text)
        resolved_vars = resolve_variables(self._variables, self._variables)
        # Extract variable names from VariableInfo objects
        missing_vars = [
            var.name for var in referenced_vars if var.name not in resolved_vars
        ]

        if missing_vars:
            logger.error(f"Missing required variables: {', '.join(missing_vars)}")
            return False
        return True

    def _parse_variable_expr(self, expr: str) -> Tuple[str, Optional[str]]:
        """Parse a variable expression into name and default value.

        Args:
        ----
            expr: Variable expression like ${var} or ${var|default}

        Returns:
        -------
            Tuple of (variable_name, default_value)

        """
        # Strip ${ and } from expression
        if expr.startswith("${") and expr.endswith("}"):
            expr = expr[2:-1]

        if "|" in expr:
            var_name, default = expr.split("|", 1)
            return var_name.strip(), default.strip()
        else:
            return expr.strip(), None
