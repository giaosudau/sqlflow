"""Variable resolution and substitution for SQLFlow.

This module centralizes variable handling for SQLFlow, providing consistent
variable resolution and substitution throughout the application.

The module follows these priorities for variable resolution:
1. CLI variables (highest priority)
2. Profile variables (medium priority)
3. SET variables in pipeline (lowest priority)
4. Default values in ${var|default} expressions (only used when no other value is found)
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class VariableContext:
    """Manages variables from different sources with priority resolution.

    This class collects variables from different sources (SET statements in pipelines,
    profile configuration, CLI arguments, and default values) and resolves them
    according to the priority order.
    """

    def __init__(
        self,
        cli_variables: Optional[Dict[str, Any]] = None,
        profile_variables: Optional[Dict[str, Any]] = None,
        set_variables: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a variable context with variables from different sources.

        Args:
            cli_variables: Variables from CLI arguments (highest priority)
            profile_variables: Variables from profile configuration (medium priority)
            set_variables: Variables from SET statements in pipeline (lowest priority)
        """
        self._cli_variables = cli_variables or {}
        self._profile_variables = profile_variables or {}
        self._set_variables = set_variables or {}
        self._unresolved_variables: Set[str] = set()

        # Build the effective variables dictionary with proper priority
        self._effective_variables = self._resolve_variables()

        logger.debug(
            f"Created VariableContext with {len(self._effective_variables)} effective variables"
        )

    def _resolve_variables(self) -> Dict[str, Any]:
        """Resolve variables according to priority order.

        Returns:
            Dictionary of resolved variables with proper priority applied
        """
        # Start with lowest priority (SET variables)
        effective_vars = self._set_variables.copy()

        # Add profile variables (overriding SET variables)
        for name, value in self._profile_variables.items():
            effective_vars[name] = value

        # Add CLI variables (highest priority, overriding both SET and profile)
        for name, value in self._cli_variables.items():
            effective_vars[name] = value

        return effective_vars

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get a variable value by name.

        Args:
            name: Name of the variable to retrieve
            default: Default value to return if variable doesn't exist

        Returns:
            The variable value or default if not found
        """
        return self._effective_variables.get(name, default)

    def has_variable(self, name: str) -> bool:
        """Check if a variable exists.

        Args:
            name: Name of the variable to check

        Returns:
            True if the variable exists, False otherwise
        """
        return name in self._effective_variables

    def get_all_variables(self) -> Dict[str, Any]:
        """Get all effective variables.

        Returns:
            Dictionary of all variables with priority resolution applied
        """
        return self._effective_variables.copy()

    def add_unresolved_variable(self, name: str) -> None:
        """Add a variable to the unresolved set.

        Args:
            name: Name of the unresolved variable
        """
        self._unresolved_variables.add(name)

    def get_unresolved_variables(self) -> Set[str]:
        """Get all unresolved variables.

        Returns:
            Set of unresolved variable names
        """
        return self._unresolved_variables.copy()

    def has_unresolved_variables(self) -> bool:
        """Check if there are any unresolved variables.

        Returns:
            True if there are unresolved variables, False otherwise
        """
        return len(self._unresolved_variables) > 0

    def __contains__(self, name: str) -> bool:
        """Check if a variable exists using the 'in' operator.

        Args:
            name: Name of the variable to check

        Returns:
            True if the variable exists, False otherwise
        """
        return self.has_variable(name)

    def __getitem__(self, name: str) -> Any:
        """Get a variable value using dictionary-like access.

        Args:
            name: Name of the variable to retrieve

        Returns:
            The variable value

        Raises:
            KeyError: If the variable doesn't exist
        """
        if name not in self._effective_variables:
            raise KeyError(f"Variable '{name}' not found")
        return self._effective_variables[name]

    def merge(self, other: "VariableContext") -> "VariableContext":
        """Create a new context by merging with another context.

        Variables from the other context take precedence.

        Args:
            other: Another VariableContext to merge with

        Returns:
            A new VariableContext with merged variables
        """
        new_cli = self._cli_variables.copy()
        new_cli.update(other._cli_variables)

        new_profile = self._profile_variables.copy()
        new_profile.update(other._profile_variables)

        new_set = self._set_variables.copy()
        new_set.update(other._set_variables)

        return VariableContext(
            cli_variables=new_cli, profile_variables=new_profile, set_variables=new_set
        )


class VariableSubstitutor:
    """Performs variable substitution in various data structures.

    This class provides methods to substitute variables in strings, lists, and
    dictionaries based on a VariableContext.
    """

    def __init__(self, context: VariableContext):
        """Initialize a variable substitutor with a variable context.

        Args:
            context: The variable context to use for substitution
        """
        self.context = context
        self._regex_pattern = re.compile(r"\$\{([^}]+)\}")

    def substitute_string(self, text: str) -> str:
        """Substitute variables in a string.

        Variables can be in the format ${var} or ${var|default}.

        Args:
            text: String with variables to substitute

        Returns:
            String with variables substituted
        """
        if not isinstance(text, str):
            return text

        logger.debug(f"Before substitution: {text}")
        result = self._regex_pattern.sub(self._replace_variable, text)
        logger.debug(f"After substitution: {result}")
        return result

    def _replace_variable(self, match: re.Match) -> str:
        """Replace a variable match with its value.

        Args:
            match: Regex match object

        Returns:
            Replacement string
        """
        var_expr = match.group(1)

        # Handle expressions with default values: ${var|default}
        if "|" in var_expr:
            var_name, default_value = var_expr.split("|", 1)
            var_name = var_name.strip()
            default_value = default_value.strip()

            # Remove quotes if present in default value
            if (default_value.startswith('"') and default_value.endswith('"')) or (
                default_value.startswith("'") and default_value.endswith("'")
            ):
                default_value = default_value[1:-1]

            # Check if variable exists
            if self.context.has_variable(var_name):
                value = self.context.get_variable(var_name)
                logger.debug(f"Using variable ${{{var_name}}} = '{value}'")
                return str(value)
            else:
                logger.debug(
                    f"Using default value '{default_value}' for variable ${{{var_name}}}"
                )
                return default_value
        else:
            # Simple variable reference: ${var}
            var_name = var_expr.strip()

            if self.context.has_variable(var_name):
                value = self.context.get_variable(var_name)
                logger.debug(f"Using variable ${{{var_name}}} = '{value}'")
                return str(value)
            else:
                # Track unresolved variables
                self.context.add_unresolved_variable(var_name)
                logger.warning(f"Variable {var_name} not found during substitution")
                return match.group(0)  # Keep the original reference

    def substitute_list(self, data_list: List[Any]) -> List[Any]:
        """Substitute variables in a list.

        Args:
            data_list: List with items that may contain variables

        Returns:
            List with variables substituted
        """
        return [
            (
                self.substitute_dict(item)
                if isinstance(item, dict)
                else (
                    self.substitute_list(item)
                    if isinstance(item, list)
                    else self.substitute_string(item) if isinstance(item, str) else item
                )
            )
            for item in data_list
        ]

    def substitute_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute variables in a dictionary.

        Args:
            data_dict: Dictionary with values that may contain variables

        Returns:
            Dictionary with variables substituted
        """
        if not isinstance(data_dict, dict):
            return data_dict

        result = {}
        for key, value in data_dict.items():
            if isinstance(value, dict):
                result[key] = self.substitute_dict(value)
            elif isinstance(value, list):
                result[key] = self.substitute_list(value)
            elif isinstance(value, str):
                result[key] = self.substitute_string(value)
            else:
                result[key] = value
        return result

    def substitute_any(self, data: Any) -> Any:
        """Substitute variables in any data structure.

        Args:
            data: Data structure that may contain variables

        Returns:
            Data structure with variables substituted
        """
        if isinstance(data, dict):
            return self.substitute_dict(data)
        elif isinstance(data, list):
            return self.substitute_list(data)
        elif isinstance(data, str):
            return self.substitute_string(data)
        else:
            return data
