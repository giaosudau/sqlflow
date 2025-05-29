"""Custom exceptions for DuckDB engine."""

from typing import Optional


class DuckDBEngineError(Exception):
    """Base exception for DuckDB engine errors."""


class UDFRegistrationError(DuckDBEngineError):
    """Error raised when UDF registration fails."""


class UDFError(DuckDBEngineError):
    """Error for UDF-related issues in the DuckDB engine."""

    def __init__(
        self, message: str, udf_name: Optional[str] = None, query: Optional[str] = None
    ):
        """Initialize a UDF error.

        Args:
            message: Error message
            udf_name: Optional name of the UDF that caused the error
            query: Optional query where the error occurred
        """
        self.udf_name = udf_name
        self.query = query
        super().__init__(message)


class TransactionError(DuckDBEngineError):
    """Exception raised when a transaction operation fails."""


class PersistenceError(DuckDBEngineError):
    """Exception raised when a persistence operation fails."""


class DuckDBConnectionError(DuckDBEngineError):
    """Exception raised when database connection operations fail."""


class SchemaValidationError(DuckDBEngineError):
    """Exception raised when schema validation fails."""

    def __init__(
        self,
        message: str,
        source_schema: Optional[dict] = None,
        target_schema: Optional[dict] = None,
    ):
        """Initialize a schema validation error.

        Args:
            message: Error message
            source_schema: Optional source schema that caused the error
            target_schema: Optional target schema that caused the error
        """
        self.source_schema = source_schema
        self.target_schema = target_schema
        super().__init__(message)
