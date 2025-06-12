"""Simplified variable validator for SQLFlow.

This module provides a focused, efficient validator that works alongside existing
validation systems. Following Zen of Python: Simple is better than complex.

The validator provides comprehensive validation while maintaining compatibility
with existing validation logic.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from sqlflow.logging import get_logger

from .manager import ValidationResult, VariableConfig

logger = get_logger(__name__)


class VariableValidator:
    """Simplified, focused validator that coexists with existing validators.

    Following Zen of Python principles:
    - Simple is better than complex: Single method for all validation
    - Readability counts: Clear, understandable validation logic
    - There should be one obvious way to do it: One validator interface
    """

    # Pre-compiled patterns for performance
    VARIABLE_PATTERN = re.compile(r"\$\{([^}]+)\}")
    DEFAULT_PATTERN = re.compile(r"^([^|]+)\|(.+)$")

    def __init__(self, variables: Optional[Dict[str, Any]] = None):
        """Initialize validator with optional pre-resolved variables.

        Args:
            variables: Pre-resolved variables for validation
        """
        self.variables = variables or {}
        logger.debug(
            f"VariableValidator initialized with {len(self.variables)} variables"
        )

    def validate(
        self, content: str, config: Optional[VariableConfig] = None
    ) -> ValidationResult:
        """Single method to validate all variable usage.

        Following Zen of Python: There should be one obvious way to do it.
        This method handles all validation scenarios in one place.

        Args:
            content: Content to validate for variable usage
            config: Optional variable configuration for priority resolution

        Returns:
            ValidationResult with comprehensive validation information
        """
        if not content:
            return ValidationResult(
                is_valid=True,
                missing_variables=[],
                invalid_defaults=[],
                warnings=[],
                context_locations={},
            )

        logger.debug(f"Validating content ({len(content)} chars)")

        # Get all available variables
        if config:
            all_vars = config.resolve_priority()
        else:
            all_vars = self.variables

        missing_vars = []
        invalid_defaults = []
        warnings = []
        context_locations = {}

        # Find all variable references and validate them
        for match in self.VARIABLE_PATTERN.finditer(content):
            var_expr = match.group(1)
            var_name, default = self._parse_variable_expression(var_expr)

            # Check if variable is missing
            if var_name not in all_vars and default is None:
                missing_vars.append(var_name)
                context_locations[var_name] = self._find_context(content, match.start())
                logger.debug(f"Missing variable: {var_name}")

            # Check if default value is invalid
            elif default and self._is_invalid_default(default):
                invalid_defaults.append(f"${{{var_expr}}}")
                logger.debug(f"Invalid default for: {var_name}")

            # Check for potential warnings
            warning = self._check_for_warnings(var_name, default, all_vars)
            if warning:
                warnings.append(warning)

        is_valid = len(missing_vars) == 0 and len(invalid_defaults) == 0

        result = ValidationResult(
            is_valid=is_valid,
            missing_variables=missing_vars,
            invalid_defaults=invalid_defaults,
            warnings=warnings,
            context_locations=context_locations,
        )

        logger.debug(f"Validation completed: {'PASS' if is_valid else 'FAIL'}")
        return result

    def validate_quick(self, content: str, variables: Dict[str, Any]) -> List[str]:
        """Quick validation returning only missing variables.

        Optimized for performance when only missing variables are needed.

        Args:
            content: Content to validate
            variables: Variables available for substitution

        Returns:
            List of missing variable names
        """
        if not content:
            return []

        missing = []
        for match in self.VARIABLE_PATTERN.finditer(content):
            var_expr = match.group(1)
            var_name, default = self._parse_variable_expression(var_expr)

            if var_name not in variables and default is None:
                missing.append(var_name)

        return list(set(missing))  # Remove duplicates

    def _parse_variable_expression(self, var_expr: str) -> Tuple[str, Optional[str]]:
        """Parse variable expression to extract name and default value.

        Args:
            var_expr: Variable expression like 'name' or 'name|default'

        Returns:
            Tuple of (variable_name, default_value_or_none)
        """
        var_expr = var_expr.strip()

        # Check for default value pattern: var|default
        default_match = self.DEFAULT_PATTERN.match(var_expr)
        if default_match:
            var_name = default_match.group(1).strip()
            default_value = default_match.group(2).strip()

            # Remove quotes from default value if present
            default_value = self._unquote_value(default_value)
            return var_name, default_value
        else:
            return var_expr, None

    def _unquote_value(self, value: str) -> str:
        """Remove quotes from a value if present.

        Args:
            value: Value that may be quoted

        Returns:
            Unquoted value
        """
        if len(value) >= 2:
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                return value[1:-1]
        return value

    def _is_invalid_default(self, default: str) -> bool:
        """Check if a default value is invalid.

        Args:
            default: Default value to validate

        Returns:
            True if default is invalid, False otherwise
        """
        if not default:
            return True

        # Check for common invalid patterns
        invalid_patterns = [
            r"^\s*$",  # Empty or whitespace only
            r'^[\'"]\s*[\'"]\s*$',  # Empty quotes
            r"^\$\{",  # Starts with variable reference (circular)
        ]

        for pattern in invalid_patterns:
            if re.match(pattern, default):
                return True

        return False

    def _find_context(self, content: str, position: int) -> List[str]:
        """Find context around a variable position for better error messages.

        Args:
            content: Full content
            position: Position of the variable

        Returns:
            List of context strings showing where the variable appears
        """
        # Find line number and column
        lines = content[:position].split("\n")
        line_num = len(lines)
        len(lines[-1]) + 1

        # Get surrounding context
        content_lines = content.split("\n")
        start_line = max(0, line_num - 2)
        end_line = min(len(content_lines), line_num + 2)

        context = []
        for i in range(start_line, end_line):
            marker = ">>> " if i == line_num - 1 else "    "
            context.append(f"{marker}Line {i+1}: {content_lines[i]}")

        return context

    def _check_for_warnings(
        self, var_name: str, default: Optional[str], all_vars: Dict[str, Any]
    ) -> Optional[str]:
        """Check for potential warnings about variable usage.

        Args:
            var_name: Variable name
            default: Default value if any
            all_vars: All available variables

        Returns:
            Warning message if any, None otherwise
        """
        # Check for variables that might be typos
        if var_name not in all_vars:
            similar = self._find_similar_variables(var_name, all_vars.keys())
            if similar:
                return f"Variable '{var_name}' not found. Did you mean: {', '.join(similar)}?"

        # Check for unused defaults
        if var_name in all_vars and default:
            return f"Variable '{var_name}' has a default value but is already defined"

        return None

    def _find_similar_variables(
        self, var_name: str, available_vars: List[str]
    ) -> List[str]:
        """Find variables with similar names for suggestion.

        Simple similarity check based on common prefixes/suffixes.

        Args:
            var_name: Variable name to find similar ones for
            available_vars: List of available variable names

        Returns:
            List of similar variable names
        """
        similar = []
        var_lower = var_name.lower()

        for available in available_vars:
            available_lower = available.lower()

            # Check for common patterns
            if (
                # Same length, different by 1-2 characters
                (
                    len(var_name) == len(available)
                    and sum(c1 != c2 for c1, c2 in zip(var_lower, available_lower)) <= 2
                )
                or
                # One is substring of the other
                var_lower in available_lower
                or available_lower in var_lower
                or
                # Similar prefix/suffix
                (
                    len(var_name) > 3
                    and (
                        var_lower[:3] == available_lower[:3]
                        or var_lower[-3:] == available_lower[-3:]
                    )
                )
            ):
                similar.append(available)

        return similar[:3]  # Return max 3 suggestions

    # Compatibility methods for integration with existing systems

    def is_valid_variable_name(self, name: str) -> bool:
        """Check if a variable name is valid.

        Args:
            name: Variable name to validate

        Returns:
            True if valid, False otherwise
        """
        # Basic validation: not empty, no special characters except underscore
        return bool(name and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name))

    def extract_variables(self, content: str) -> List[str]:
        """Extract all variable names from content.

        Args:
            content: Content to extract variables from

        Returns:
            List of unique variable names found
        """
        variables = []
        for match in self.VARIABLE_PATTERN.finditer(content):
            var_expr = match.group(1)
            var_name, _ = self._parse_variable_expression(var_expr)
            variables.append(var_name)

        return list(set(variables))  # Remove duplicates

    def count_variable_references(self, content: str) -> Dict[str, int]:
        """Count how many times each variable is referenced.

        Args:
            content: Content to analyze

        Returns:
            Dictionary mapping variable names to reference counts
        """
        counts = {}
        for match in self.VARIABLE_PATTERN.finditer(content):
            var_expr = match.group(1)
            var_name, _ = self._parse_variable_expression(var_expr)
            counts[var_name] = counts.get(var_name, 0) + 1

        return counts
