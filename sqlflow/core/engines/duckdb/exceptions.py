"""Custom exceptions for DuckDB engine."""

from typing import List, Optional


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


class InvalidLoadModeError(DuckDBEngineError):
    """Exception raised when an invalid load mode is specified."""

    def __init__(self, mode: str, valid_modes: List[str]):
        """Initialize an invalid load mode error.

        Args:
            mode: The invalid mode that was specified
            valid_modes: List of valid load modes
        """
        self.mode = mode
        self.valid_modes = valid_modes
        message = f"Invalid load mode: {mode}. Must be one of: {', '.join(valid_modes)}"
        super().__init__(message)


class MergeKeyValidationError(DuckDBEngineError):
    """Exception raised when merge keys are missing or invalid."""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        merge_keys: Optional[List[str]] = None,
    ):
        """Initialize a merge key validation error.

        Args:
            message: Error message
            table_name: Optional table name where the error occurred
            merge_keys: Optional merge keys that caused the error
        """
        self.table_name = table_name
        self.merge_keys = merge_keys
        super().__init__(message)


class DataRegistrationError(DuckDBEngineError):
    """Exception raised when data registration operations fail."""

    def __init__(self, message: str, data_name: Optional[str] = None):
        """Initialize a data registration error.

        Args:
            message: Error message
            data_name: Optional name of the data that caused the error
        """
        self.data_name = data_name
        super().__init__(message)
