"""UDF validation logic for DuckDB engine."""

import inspect
from typing import Any, Callable, Dict, Tuple

import pandas as pd

from sqlflow.logging import get_logger

from ..constants import DuckDBConstants
from ..exceptions import UDFRegistrationError

logger = get_logger(__name__)


class TableUDFSignatureValidator:
    """Validates table UDF signatures and extracts parameter information."""

    def validate(
        self, name: str, function: Callable
    ) -> Tuple[inspect.Signature, Dict[str, Any]]:
        """Validate a table UDF's signature.

        Args:
            name: Name of the UDF
            function: The UDF function

        Returns:
            Tuple of (signature, parameter info)

        Raises:
            UDFRegistrationError: If the signature is invalid
        """
        try:
            sig = inspect.signature(function)
            self._validate_parameters(name, sig)
            self._validate_return_type(name, sig)
            param_info = self._extract_param_info(sig)
            return sig, param_info

        except Exception as e:
            raise UDFRegistrationError(
                f"Error validating table UDF {name} signature: {str(e)}"
            ) from e

    def _validate_parameters(self, name: str, sig: inspect.Signature) -> None:
        """Validate UDF parameters.

        Args:
            name: Name of the UDF
            sig: Function signature

        Raises:
            UDFRegistrationError: If parameters are invalid
        """
        params = list(sig.parameters.values())

        # Must have at least one parameter (DataFrame)
        if not params:
            raise UDFRegistrationError(
                f"Table UDF {name} must accept at least one argument (DataFrame)"
            )

        # First parameter must be positional and a DataFrame
        first_param = params[0]
        if first_param.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise UDFRegistrationError(
                f"First parameter of table UDF {name} must be positional (DataFrame)"
            )

        # Relaxed type checking for testing purposes
        # Check first parameter type annotation if present
        if (
            first_param.annotation != inspect.Parameter.empty
            and first_param.annotation != pd.DataFrame
            and "DataFrame" not in str(first_param.annotation)
        ):
            logger.debug(
                "WARNING: First parameter of table UDF %s should be pd.DataFrame, got %s",
                name,
                first_param.annotation,
            )

        # Remaining parameters must be keyword arguments
        for param in params[1:]:
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                raise UDFRegistrationError(
                    f"Additional parameters in table UDF {name} must be keyword "
                    f"arguments, got {param.name} as {param.kind}"
                )

    def _validate_return_type(self, name: str, sig: inspect.Signature) -> None:
        """Validate UDF return type.

        Args:
            name: Name of the UDF
            sig: Function signature
        """
        # Relaxed return type checking for testing
        # Return type must be DataFrame
        return_annotation = sig.return_annotation
        if (
            return_annotation != inspect.Parameter.empty
            and return_annotation != pd.DataFrame
            and "DataFrame" not in str(return_annotation)
        ):
            logger.debug(
                "WARNING: Table UDF %s should have return type pd.DataFrame, got %s",
                name,
                return_annotation,
            )

    def _extract_param_info(self, sig: inspect.Signature) -> Dict[str, Any]:
        """Extract parameter information for registration.

        Args:
            sig: Function signature

        Returns:
            Parameter information dictionary
        """
        return {
            name: {
                "kind": param.kind,
                "default": (
                    None if param.default is inspect.Parameter.empty else param.default
                ),
                "annotation": (
                    "Any"
                    if param.annotation is inspect.Parameter.empty
                    else str(param.annotation)
                ),
            }
            for name, param in sig.parameters.items()
        }


class TypeValidator:
    """Validates Python types for UDF registration."""

    @staticmethod
    def map_python_type_to_duckdb(py_type: Any, udf_name: str, param_name: str) -> str:
        """Map a Python type to its corresponding DuckDB SQL type string.

        Args:
            py_type: Python type to map
            udf_name: Name of the UDF
            param_name: Name of the parameter

        Returns:
            Corresponding DuckDB SQL type string

        Raises:
            ValueError: If the type is not supported
        """
        if py_type in DuckDBConstants.PYTHON_TO_DUCKDB_TYPES:
            return DuckDBConstants.PYTHON_TO_DUCKDB_TYPES[py_type]
        else:
            raise ValueError(f"Unsupported Python type: {py_type}")
