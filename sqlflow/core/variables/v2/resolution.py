"""Priority resolution system for SQLFlow Variables V2

This module implements pure functions for variable resolution with V1 priority
order: CLI > Profile > SET > ENV.

Following Zen of Python principles:
- Simple is better than complex
- Explicit is better than implicit
- Pure functions with no side effects
"""

import os
from typing import Any, Dict, Optional

from .types import VariableSources


def resolve_variables(sources: VariableSources) -> Dict[str, Any]:
    """Resolve variables with V1 priority order: CLI > Profile > SET > ENV.

    Args:
        sources: VariableSources object containing all variable sources

    Returns:
        Dictionary with variables resolved by priority
    """
    result = {}

    # Apply in priority order (lowest to highest)
    if sources.env:
        result.update(sources.env)
    if sources.set:
        result.update(sources.set)
    if sources.profile:
        result.update(sources.profile)
    if sources.cli:
        result.update(sources.cli)

    return result


def resolve_from_environment() -> Dict[str, Any]:
    """Get all environment variables as a dictionary.

    Returns:
        Dictionary of all environment variables
    """
    return dict(os.environ)


def get_variable_priority(var_name: str, sources: VariableSources) -> Optional[str]:
    """Get the source priority level for a specific variable.

    Args:
        var_name: Name of the variable to check
        sources: VariableSources object

    Returns:
        Priority source name ('cli', 'profile', 'set', 'env') or None if not found
    """
    if sources.cli and var_name in sources.cli:
        return "cli"
    elif sources.profile and var_name in sources.profile:
        return "profile"
    elif sources.set and var_name in sources.set:
        return "set"
    elif sources.env and var_name in sources.env:
        return "env"
    else:
        return None


def resolve_with_sources(
    var_name: str, sources: VariableSources
) -> tuple[Any, Optional[str]]:
    """Resolve a single variable and return its value with source.

    Args:
        var_name: Name of the variable to resolve
        sources: VariableSources object

    Returns:
        Tuple of (value, source) where source is the priority level name
        Returns (None, None) if variable not found
    """
    source = get_variable_priority(var_name, sources)

    if source == "cli" and sources.cli:
        return sources.cli[var_name], "cli"
    elif source == "profile" and sources.profile:
        return sources.profile[var_name], "profile"
    elif source == "set" and sources.set:
        return sources.set[var_name], "set"
    elif source == "env" and sources.env:
        return sources.env[var_name], "env"
    else:
        return None, None


# Backward compatibility functions
def resolve_variables_legacy(
    cli_vars: Optional[Dict[str, Any]] = None,
    profile_vars: Optional[Dict[str, Any]] = None,
    set_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Legacy function for backward compatibility."""
    sources = VariableSources(
        cli=cli_vars or {},
        profile=profile_vars or {},
        set=set_vars or {},
        env=env_vars or {},
    )
    return resolve_variables(sources)


def merge_variable_sources(sources: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Merge multiple variable sources with explicit priority.

    Args:
        sources: Dictionary mapping priority names to variable dictionaries
                Expected keys: 'env', 'set', 'profile', 'cli'

    Returns:
        Merged variables with proper priority resolution
    """
    variable_sources = VariableSources(
        cli=sources.get("cli", {}),
        profile=sources.get("profile", {}),
        set=sources.get("set", {}),
        env=sources.get("env", {}),
    )
    return resolve_variables(variable_sources)
