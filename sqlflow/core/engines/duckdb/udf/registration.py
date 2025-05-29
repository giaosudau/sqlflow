"""UDF registration strategies for DuckDB engine."""

import traceback
from abc import ABC, abstractmethod
from typing import Any, Callable

from sqlflow.core.engines.duckdb.exceptions import UDFRegistrationError
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class UDFRegistrationStrategy(ABC):
    """Abstract base class for UDF registration strategies."""

    @abstractmethod
    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register a UDF using this strategy.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: Database connection
        """


class ExplicitSchemaStrategy(UDFRegistrationStrategy):
    """Registration strategy for UDFs with explicit output schema."""

    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register UDF with explicit schema information.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: Database connection
        """
        output_schema = getattr(function, "_output_schema", None)
        if not output_schema:
            raise UDFRegistrationError(f"No output schema found for UDF {name}")

        logger.info(f"UDF {name} has defined output_schema: {output_schema}")

        try:
            # Create a DuckDB struct type that describes the output schema
            struct_fields = []
            for col_name, col_type in output_schema.items():
                # Standardize type names to ensure compatibility
                type_name = self._normalize_type(col_type)
                struct_fields.append(f"{col_name} {type_name}")

            # Create a SQL STRUCT type string
            return_type_str = f"STRUCT({', '.join(struct_fields)})"

            logger.debug(
                f"Registering table UDF {name} with output schema: {return_type_str}"
            )

            # Register with the structured return type
            connection.create_function(
                name,
                function,
                return_type=return_type_str,
            )

            logger.info(
                f"Successfully registered table UDF {name} with structured return type."
            )

        except Exception as e:
            logger.warning(
                f"Structured schema registration failed for UDF {name}, error: {str(e)}"
            )
            logger.debug(traceback.format_exc())
            raise UDFRegistrationError(
                f"Failed to register UDF {name} with explicit schema: {e}"
            ) from e

    def _normalize_type(self, col_type: str) -> str:
        """Normalize column type for DuckDB compatibility.

        Args:
            col_type: Column type string

        Returns:
            Normalized type string
        """
        type_name = col_type.upper()
        if (
            "VARCHAR" in type_name
            or "TEXT" in type_name
            or "CHAR" in type_name
            or "STRING" in type_name
        ):
            return "VARCHAR"
        elif "INT" in type_name:
            return "INTEGER"
        elif (
            "FLOAT" in type_name
            or "DOUBLE" in type_name
            or "DECIMAL" in type_name
            or "NUMERIC" in type_name
        ):
            return "DOUBLE"
        elif "BOOL" in type_name:
            return "BOOLEAN"
        else:
            return type_name


class InferSchemaStrategy(UDFRegistrationStrategy):
    """Registration strategy for UDFs with schema inference."""

    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register UDF with schema inference.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: Database connection
        """
        logger.debug(f"Registering UDF {name} with infer=True")

        try:
            connection.create_function(
                name,
                function,
                return_type=None,  # Let DuckDB infer the type
            )
            logger.info(f"Successfully registered table UDF {name} with type inference")

        except Exception as e:
            logger.debug(f"Failed to register with inference: {e}")
            raise UDFRegistrationError(
                f"Failed to register UDF {name} with schema inference: {e}"
            ) from e


class FallbackStrategy(UDFRegistrationStrategy):
    """Fallback registration strategy for UDFs."""

    def register(self, name: str, function: Callable, connection: Any) -> None:
        """Register UDF with fallback approach.

        Args:
            name: Name to register the UDF as
            function: Python function to register
            connection: Database connection
        """
        logger.debug(f"Last attempt to register UDF {name} with standard approach")

        try:
            connection.create_function(
                name,
                function,
            )
            logger.info(
                f"Successfully registered table UDF {name} with standard approach"
            )

        except Exception as e:
            logger.error(f"Error registering table UDF {name} with DuckDB: {str(e)}")
            logger.error(traceback.format_exc())
            raise UDFRegistrationError(
                f"Failed to register table UDF {name} with DuckDB: {e}"
            ) from e


class UDFRegistrationStrategyFactory:
    """Factory for creating appropriate UDF registration strategies."""

    @staticmethod
    def create(function: Callable) -> UDFRegistrationStrategy:
        """Create appropriate registration strategy based on function attributes.

        Args:
            function: Function to create strategy for

        Returns:
            Appropriate registration strategy
        """
        # Check if the UDF has an output schema defined
        output_schema = getattr(function, "_output_schema", None)
        infer_schema = getattr(function, "_infer_schema", False)

        # Check for wrapped function and extract attributes if needed
        wrapped_func = getattr(function, "__wrapped__", None)
        if wrapped_func:
            logger.debug(f"Function has wrapped function: {wrapped_func}")

            # If the wrapped function has better metadata, use it
            if not output_schema:
                wrapped_output_schema = getattr(wrapped_func, "_output_schema", None)
                if wrapped_output_schema:
                    logger.debug(
                        f"Using output_schema from wrapped function: {wrapped_output_schema}"
                    )
                    output_schema = wrapped_output_schema
                    # Copy the attribute to the function for future use
                    setattr(function, "_output_schema", output_schema)

            if not infer_schema:
                wrapped_infer = getattr(wrapped_func, "_infer_schema", False)
                if wrapped_infer:
                    logger.debug(
                        f"Using infer_schema from wrapped function: {wrapped_infer}"
                    )
                    infer_schema = wrapped_infer
                    # Copy the attribute to the function
                    setattr(function, "_infer_schema", infer_schema)

        # Choose strategy based on available metadata
        if output_schema:
            return ExplicitSchemaStrategy()
        elif infer_schema:
            return InferSchemaStrategy()
        else:
            return FallbackStrategy()
