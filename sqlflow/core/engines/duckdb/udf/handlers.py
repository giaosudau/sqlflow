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
        ----
            name: Name to register the UDF as
            function: Python function to register
            connection: Database connection

        """


class ScalarUDFHandler(UDFHandler):
    """Handler for scalar UDF registration."""

    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register a scalar UDF with DuckDB.

        Args:
        ----
            name: Name to register the UDF as
            function: Python function to register
            connection: DuckDB connection

        """
        logger.info("Registering scalar UDF: {}".format(name))

        # Only unwrap if it's not already a bound method
        # Bound methods should be used as-is to preserve their binding
        if inspect.ismethod(function):
            actual_function = function
        else:
            actual_function = getattr(function, "__wrapped__", function)

        # Debug logging for the registration process
        logger.debug(f"Function type: {type(actual_function)}")
        logger.debug(f"Is method: {inspect.ismethod(actual_function)}")
        logger.debug(f"Function signature: {inspect.signature(actual_function)}")

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
        """Prepare function for registration by handling default parameters and instance methods.

        Args:
        ----
            function: Function to prepare

        Returns:
        -------
            Function ready for registration

        """
        base_function = self._handle_instance_method_wrapper(function)
        return self._handle_default_parameters(base_function)

    def _handle_instance_method_wrapper(self, function: Callable) -> Callable:
        """Handle instance method wrapping to exclude 'self' parameter.

        Args:
        ----
            function: Function to check and possibly wrap

        Returns:
        -------
            Function ready for registration (with or without wrapper)

        """
        # Check if this is already a bound method
        is_bound_method = inspect.ismethod(function)

        if is_bound_method:
            logger.debug("Function is a bound method, using directly")
            return function

        # Check if this looks like an unbound instance method
        sig = inspect.signature(function)
        params = list(sig.parameters.values())
        is_instance_method = self._is_instance_method(params)

        if is_instance_method:
            logger.debug(
                "Function appears to be an unbound instance method - this may cause issues"
            )
            # For unbound instance methods, we can't easily create a working wrapper
            # since we don't have access to the instance. The caller should pass a bound method.
            logger.warning(
                f"Function {function.__name__} appears to be an unbound instance method. "
                "Consider passing a bound method (e.g., instance.method) instead."
            )
            return function
        else:
            logger.debug("Function is a regular function, using as-is")
            return function

    def _is_instance_method(self, params: list) -> bool:
        """Check if function signature indicates an instance method.

        Args:
        ----
            params: List of function parameters

        Returns:
        -------
            True if function appears to be an instance method

        """
        return (
            len(params) > 0
            and params[0].name == "self"
            and params[0].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
        )

    def _create_instance_method_wrapper(
        self, function: Callable, params: list
    ) -> Callable:
        """Create a wrapper for instance methods that handles 'self' parameter correctly.

        Args:
        ----
            function: Original instance method
            params: Function parameters

        Returns:
        -------
            Wrapped function with proper instance handling

        """
        logger.debug(
            "Function is an unbound instance method, creating wrapper to exclude 'self'"
        )

        # Create new signature without the 'self' parameter
        new_params = params[1:]  # Skip the 'self' parameter

        if new_params:
            return self._create_parameterized_wrapper(function, new_params)
        else:
            return self._create_simple_wrapper(function)

    def _create_parameterized_wrapper(
        self, function: Callable, new_params: list
    ) -> Callable:
        """Create wrapper for instance methods with parameters.

        Args:
        ----
            function: Original function
            new_params: Parameters excluding 'self'

        Returns:
        -------
            Wrapper function

        """
        # For instance methods, we need to create a wrapper that calls the function
        # with the proper instance. Since this is an unbound method, we need to
        # ensure the function has access to its instance when called.

        def instance_method_wrapper(*args):
            # The function should have been properly bound before we get here
            # If not, we need to handle it gracefully
            if hasattr(function, "__self__"):
                # This is actually a bound method, use it directly
                return function(*args)
            else:
                # This is an unbound method - we need to find the instance
                # In our case, we expect the function to have been registered
                # with its instance context preserved
                raise RuntimeError(
                    f"Cannot call unbound instance method {function.__name__} - "
                    "function must be bound to an instance before registration"
                )

        return self._copy_function_attributes(function, instance_method_wrapper)

    def _create_simple_wrapper(self, function: Callable) -> Callable:
        """Create wrapper for instance methods with no parameters.

        Args:
        ----
            function: Original function

        Returns:
        -------
            Simple wrapper function

        """

        def instance_method_wrapper():
            # Same logic as parameterized wrapper
            if hasattr(function, "__self__"):
                return function()
            else:
                raise RuntimeError(
                    f"Cannot call unbound instance method {function.__name__} - "
                    "function must be bound to an instance before registration"
                )

        return self._copy_function_attributes(function, instance_method_wrapper)

    def _copy_function_attributes(self, source: Callable, target: Callable) -> Callable:
        """Copy relevant attributes from source function to target function.

        Args:
        ----
            source: Source function
            target: Target function

        Returns:
        -------
            Target function with copied attributes

        """
        for attr in dir(source):
            if attr.startswith("_") and not attr.startswith("__"):
                try:
                    setattr(target, attr, getattr(source, attr))
                except (AttributeError, TypeError):
                    pass
        return target

    def _handle_default_parameters(self, base_function: Callable) -> Callable:
        """Handle functions with default parameters.

        Args:
        ----
            base_function: Function to check for default parameters

        Returns:
        -------
            Function with default parameter handling (wrapped if needed)

        """
        current_sig = inspect.signature(base_function)
        has_default_params = any(
            p.default is not inspect.Parameter.empty
            for p in current_sig.parameters.values()
        )

        if has_default_params:
            logger.debug("Function has parameters with default values")
            return self._create_default_parameter_wrapper(base_function, current_sig)
        else:
            return base_function

    def _create_default_parameter_wrapper(
        self, base_function: Callable, current_sig: inspect.Signature
    ) -> Callable:
        """Create wrapper that handles missing default parameters.

        Args:
        ----
            base_function: Function to wrap
            current_sig: Function signature

        Returns:
        -------
            Wrapper function with default parameter handling

        """

        def udf_wrapper(*args):
            """Wrapper that handles missing default parameters."""
            bound_args = current_sig.bind_partial(*args)
            bound_args.apply_defaults()
            return base_function(*bound_args.args, **bound_args.kwargs)

        return self._copy_function_attributes(base_function, udf_wrapper)

    def _get_return_type(self, function: Callable) -> str | None:
        """Get the DuckDB return type for a function.

        Args:
        ----
            function: Function to analyze

        Returns:
        -------
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
        ----
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
        ----
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
        ----
            function: Function to create handler for

        Returns:
        -------
            Appropriate UDF handler

        Raises:
        ------
            UDFRegistrationError: If UDF type is unknown

        """
        udf_type = getattr(function, "_udf_type", DuckDBConstants.UDF_TYPE_SCALAR)

        if udf_type == DuckDBConstants.UDF_TYPE_SCALAR:
            return ScalarUDFHandler()
        elif udf_type == DuckDBConstants.UDF_TYPE_TABLE:
            return TableUDFHandler()
        else:
            raise UDFRegistrationError("Unknown UDF type: {}".format(udf_type))
