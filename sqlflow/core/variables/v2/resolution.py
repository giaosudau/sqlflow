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


def resolve_variables(
    cli_vars: Optional[Dict[str, Any]] = None,
    profile_vars: Optional[Dict[str, Any]] = None,
    set_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Resolve variables with V1 priority order: CLI > Profile > SET > ENV.

    Args:
        cli_vars: CLI variables (highest priority)
        profile_vars: Profile variables
        set_vars: SET statement variables
        env_vars: Environment variables (lowest priority)

    Returns:
        Dictionary with variables resolved by priority
    """
    result = {}

    # Apply in priority order (lowest to highest)
    if env_vars:
        result.update(env_vars)
    if set_vars:
        result.update(set_vars)
    if profile_vars:
        result.update(profile_vars)
    if cli_vars:
        result.update(cli_vars)

    return result


def resolve_from_environment() -> Dict[str, Any]:
    """Get all environment variables as a dictionary.

    Returns:
        Dictionary of all environment variables
    """
    return dict(os.environ)


def merge_variable_sources(sources: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Merge multiple variable sources with explicit priority.

    Args:
        sources: Dictionary mapping priority names to variable dictionaries
                Expected keys: 'env', 'set', 'profile', 'cli'

    Returns:
        Merged variables with proper priority resolution
    """
    return resolve_variables(
        cli_vars=sources.get("cli"),
        profile_vars=sources.get("profile"),
        set_vars=sources.get("set"),
        env_vars=sources.get("env"),
    )


def get_variable_priority(
    var_name: str,
    cli_vars: Optional[Dict[str, Any]] = None,
    profile_vars: Optional[Dict[str, Any]] = None,
    set_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Get the source priority level for a specific variable.

    Args:
        var_name: Name of the variable to check
        cli_vars: CLI variables
        profile_vars: Profile variables
        set_vars: SET statement variables
        env_vars: Environment variables

    Returns:
        Priority source name ('cli', 'profile', 'set', 'env') or None if not found
    """
    if cli_vars and var_name in cli_vars:
        return "cli"
    elif profile_vars and var_name in profile_vars:
        return "profile"
    elif set_vars and var_name in set_vars:
        return "set"
    elif env_vars and var_name in env_vars:
        return "env"
    else:
        return None


def resolve_with_sources(
    var_name: str,
    cli_vars: Optional[Dict[str, Any]] = None,
    profile_vars: Optional[Dict[str, Any]] = None,
    set_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None,
) -> tuple[Any, Optional[str]]:
    """Resolve a single variable and return its value with source.

    Args:
        var_name: Name of the variable to resolve
        cli_vars: CLI variables
        profile_vars: Profile variables
        set_vars: SET statement variables
        env_vars: Environment variables

    Returns:
        Tuple of (value, source) where source is the priority level name
        Returns (None, None) if variable not found
    """
    source = get_variable_priority(var_name, cli_vars, profile_vars, set_vars, env_vars)

    if source == "cli" and cli_vars:
        return cli_vars[var_name], "cli"
    elif source == "profile" and profile_vars:
        return profile_vars[var_name], "profile"
    elif source == "set" and set_vars:
        return set_vars[var_name], "set"
    elif source == "env" and env_vars:
        return env_vars[var_name], "env"
    else:
        return None, None
