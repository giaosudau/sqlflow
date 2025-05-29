"""UDF handlers for different UDF types in DuckDB engine."""

import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable

from sqlflow.logging import get_logger

from ..constants import DuckDBConstants
from ..exceptions import UDFRegistrationError

logger = get_logger(__name__)


class UDFHandler(ABC):
    """Abstract base class for UDF handlers."""

    @abstractmethod
    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register a UDF with the database connection.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: Database connection
        """


class ScalarUDFHandler(UDFHandler):
    """Handler for scalar UDF registration."""

    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register a scalar UDF with DuckDB.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: DuckDB connection
        """
        logger.info("Registering scalar UDF: {}".format(name))

        actual_function = getattr(function, "__wrapped__", function)
        registration_function = self._prepare_function_for_registration(actual_function)

        return_type = self._get_return_type(actual_function)

        if return_type:
            connection.create_function(
                name, registration_function, return_type=return_type
            )
            logger.info(
                "Registered scalar UDF: {} with return type {}".format(
                    name, return_type
                )
            )
        else:
            self._register_with_fallback(name, registration_function, connection)

    def _prepare_function_for_registration(self, function: Callable) -> Callable:
        """Prepare function for registration by handling default parameters.

        Args:
            function: Function to prepare

        Returns:
            Function ready for registration
        """
        sig = inspect.signature(function)
        has_default_params = any(
            p.default is not inspect.Parameter.empty for p in sig.parameters.values()
        )

        if has_default_params:
            logger.debug("Function has parameters with default values")

            def udf_wrapper(*args):
                """Wrapper that handles missing default parameters."""
                bound_args = sig.bind_partial(*args)
                bound_args.apply_defaults()
                return function(*bound_args.args, **bound_args.kwargs)

            # Copy over relevant attributes from the original function
            for attr in dir(function):
                if attr.startswith("_") and not attr.startswith("__"):
                    try:
                        setattr(udf_wrapper, attr, getattr(function, attr))
                    except (AttributeError, TypeError):
                        # Some attributes can't be set, ignore them
                        pass

            return udf_wrapper
        else:
            return function

    def _get_return_type(self, function: Callable) -> str | None:
        """Get the DuckDB return type for a function.

        Args:
            function: Function to analyze

        Returns:
            DuckDB type string or None if not determinable
        """
        annotations = getattr(function, "__annotations__", {})
        return_type = annotations.get("return", None)

        if return_type and return_type in DuckDBConstants.PYTHON_TO_DUCKDB_TYPES:
            return DuckDBConstants.PYTHON_TO_DUCKDB_TYPES[return_type]

        return None

    def _register_with_fallback(
        self, name: str, function: Callable, connection: Any
    ) -> None:
        """Register UDF with fallback strategies.

        Args:
            name: Name to register the UDF as
            function: Function to register
            connection: DuckDB connection
        """
        try:
            connection.create_function(name, function)
            logger.info(
                "Registered scalar UDF: {} with inferred return type".format(name)
            )
        except Exception as e:
            logger.warning(
                "Could not infer return type for {}, using DOUBLE: {}".format(name, e)
            )
            connection.create_function(name, function, return_type="DOUBLE")
            logger.info(
                "Registered scalar UDF: {} with default DOUBLE return type".format(name)
            )


class TableUDFHandler(UDFHandler):
    """Handler for table UDF registration."""

    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register a table UDF with DuckDB.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: DuckDB connection
        """
        logger.info("Registering table UDF: {}".format(name))

        # Import here to avoid circular import
        from .registration import UDFRegistrationStrategyFactory

        strategy = UDFRegistrationStrategyFactory.create(function)
        strategy.register(name, function, connection)


class UDFHandlerFactory:
    """Factory for creating appropriate UDF handlers."""

    @staticmethod
    def create(function: Callable) -> UDFHandler:
        """Create appropriate UDF handler based on function type.

        Args:
            function: Function to create handler for

        Returns:
            Appropriate UDF handler

        Raises:
            UDFRegistrationError: If UDF type is unknown
        """
        udf_type = getattr(function, "_udf_type", DuckDBConstants.UDF_TYPE_SCALAR)

        if udf_type == DuckDBConstants.UDF_TYPE_SCALAR:
            return ScalarUDFHandler()
        elif udf_type == DuckDBConstants.UDF_TYPE_TABLE:
            return TableUDFHandler()
        else:
            raise UDFRegistrationError("Unknown UDF type: {}".format(udf_type))
