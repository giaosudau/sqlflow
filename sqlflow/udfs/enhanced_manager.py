"""
UDF automatic discovery and registration for SQLFlow.

Utilities for handling UDFs with default parameters when integrating with DuckDB.
This module provides functions for discovering UDFs with default parameters
and registering specialized versions for them.
"""

import inspect
import logging
from typing import Any, Callable, Dict, List, Tuple

from sqlflow.udfs import python_scalar_udf

logger = logging.getLogger(__name__)


def _find_default_parameters(func: Callable) -> List[Tuple[str, Any]]:
    """
    Extract default parameters from a function's signature.

    Args:
        func: The function to inspect

    Returns:
        List of (param_name, default_value) tuples
    """
    sig = inspect.signature(func)
    return [
        (p.name, p.default)
        for p in sig.parameters.values()
        if p.default is not inspect.Parameter.empty
    ]


def _create_wrapper_function(
    original_func: Callable, param: str, default_val: Any
) -> Callable:
    """
    Create a wrapper function that applies a default parameter value.

    Args:
        original_func: The original UDF function
        param: Parameter name to set default for
        default_val: Default value to use

    Returns:
        Wrapped function with the default parameter applied
    """

    @python_scalar_udf
    def wrapper(*args, **kwargs):
        new_kwargs = dict(kwargs)
        if param not in new_kwargs:
            new_kwargs[param] = default_val
        return original_func(*args, **new_kwargs)

    # Set the specialized function name
    wrapper.__name__ = f"{original_func.__name__}_without_{param}"

    # Copy relevant attributes
    for attr in dir(original_func):
        if attr.startswith("_") and not attr.startswith("__"):
            try:
                setattr(wrapper, attr, getattr(original_func, attr))
            except AttributeError:
                # Handle the case where the attribute can't be set
                logger.debug(
                    f"Could not copy attribute {attr} from {original_func.__name__}"
                )

    return wrapper


def create_specialized_udfs(
    manager: Any, udfs: Dict[str, Callable]
) -> Dict[str, Callable]:
    """
    Create specialized versions of UDFs that have default parameters.

    For each UDF with default parameters, create specialized versions
    that don't require those parameters with defaults.

    Args:
        manager: The UDFManager instance
        udfs: Dictionary of UDFs

    Returns:
        Dictionary with original and specialized UDFs
    """
    new_udfs = dict(udfs)  # Make a copy of the original UDFs

    for udf_name, func in udfs.items():
        # Only process scalar UDFs
        if getattr(func, "_udf_type", None) != "scalar":
            continue

        # Check if function has default parameters
        default_params = _find_default_parameters(func)

        if not default_params:
            continue

        logger.info(f"UDF {udf_name} has default parameters: {default_params}")

        # Get the module and function name parts
        module_path = ".".join(udf_name.split(".")[:-1])
        func_name = udf_name.split(".")[-1]

        # Create specialized versions that don't require the parameters with defaults
        for param_name, default_value in default_params:
            # Create a specialized name
            specialized_name = f"{module_path}.{func_name}_without_{param_name}"

            # Skip if already exists
            if specialized_name in new_udfs:
                continue

            # Create a wrapper function that automatically applies the default
            specialized_udf = _create_wrapper_function(func, param_name, default_value)

            # Add to the UDFs dictionary
            new_udfs[specialized_name] = specialized_udf
            logger.info(f"Created specialized UDF: {specialized_name}")

    return new_udfs


def enhance_udf_manager(manager: Any) -> None:
    """
    Enhance the UDF manager to properly handle default parameters.

    This function modifies the UDF manager's discover_udfs method
    to create specialized versions of UDFs with default parameters.

    Args:
        manager: The UDFManager instance
    """
    original_discover = manager.discover_udfs

    def enhanced_discover(*args, **kwargs):
        # Call the original discover method
        udfs = original_discover(*args, **kwargs)

        # Create specialized versions of UDFs with default parameters
        enhanced_udfs = create_specialized_udfs(manager, udfs)

        # Update the manager's UDFs dictionary
        manager.udfs = enhanced_udfs

        return enhanced_udfs

    # Replace the discover_udfs method
    manager.discover_udfs = enhanced_discover

    logger.info("Enhanced UDF manager with default parameter handling")
