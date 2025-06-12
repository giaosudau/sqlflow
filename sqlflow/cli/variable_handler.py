"""Variable handling utilities for SQLFlow CLI."""

import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class VariableHandler:
    """Handles variable substitution in SQLFlow pipeline text using centralized engine."""

    def __init__(self, variables: Optional[Dict[str, Any]] = None):
        """Initialize the variable handler.

        Args:
        ----
            variables: Dictionary of variable name-value pairs

        """
        self._variables = variables or {}

        # Use new VariableManager system (Phase 4: Feature flags removed)
        from sqlflow.core.variables.manager import VariableConfig, VariableManager

        config = VariableConfig(cli_variables=self._variables)
        self._manager = VariableManager(config)
        logger.debug("VariableHandler initialized with VariableManager system")

        # Keep var_pattern for backward compatibility with existing tests
        import re

        self.var_pattern = re.compile(r"\$\{([^}|]+)(?:\|([^}]+))?\}")

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

        # Use new VariableManager system
        return self._manager.substitute(text)

    def _substitute_variables_legacy(self, text: str) -> str:
        """Legacy variable substitution implementation.

        Maintains exact original behavior for backward compatibility.
        """
        # Use custom implementation to maintain exact logging behavior expected by tests
        import re

        var_pattern = re.compile(r"\$\{([^}|]+)(?:\|([^}]+))?\}")

        def replace(match: re.Match) -> str:
            var_name, default = self._parse_variable_expr(match.group(0))
            value = self._variables.get(var_name)

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

        return var_pattern.sub(replace, text)

    def validate_variable_usage(self, text: str) -> bool:
        """Validate that all required variables are provided.

        Args:
        ----
            text: Text containing variables

        Returns:
        -------
            True if all required variables are available or have defaults

        """
        # Use new VariableManager validation
        result = self._manager.validate(text)
        if not result.is_valid:
            logger.error(
                f"Missing required variables: {', '.join(result.missing_variables)}"
            )
            return False
        return True

    def _parse_variable_expr(self, expr: str) -> Tuple[str, Optional[str]]:
        """Parse a variable expression into name and default value.

        This method is kept for backward compatibility but delegates to the engine.

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
