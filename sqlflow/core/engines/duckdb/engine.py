"""Main DuckDB engine implementation."""

import os
import re
import time
from typing import Any, Callable, Dict, List, Optional

import duckdb
import pandas as pd
import pyarrow as pa

from sqlflow.core.engines.base import SQLEngine
from sqlflow.logging import get_logger

from .constants import DuckDBConstants, RegexPatterns, SQLTemplates
from .exceptions import DuckDBConnectionError, UDFError, UDFRegistrationError
from .load.handlers import LoadModeHandlerFactory
from .transaction_manager import TransactionManager
from .udf import AdvancedUDFQueryProcessor, UDFHandlerFactory

logger = get_logger(__name__)


class ExecutionStats:
    """Track execution statistics for the engine."""

    def __init__(self):
        """Initialize execution statistics."""
        self.query_count = 0
        self.udf_executions = 0
        self.udf_errors = 0
        self.last_error = None
        self.query_times = []

    def record_query(self, duration: float):
        """Record a query execution.

        Args:
            duration: Query execution time in seconds
        """
        self.query_count += 1
        self.query_times.append(duration)

    def record_udf_execution(self, success: bool, error: Optional[Exception] = None):
        """Record a UDF execution.

        Args:
            success: Whether the execution was successful
            error: Optional error if the execution failed
        """
        self.udf_executions += 1
        if not success:
            self.udf_errors += 1
            self.last_error = error

    def get_avg_query_time(self) -> float:
        """Get the average query execution time.

        Returns:
            Average query execution time in seconds
        """
        if not self.query_times:
            return 0.0
        return sum(self.query_times) / len(self.query_times)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of execution statistics.

        Returns:
            Dictionary with execution statistics
        """
        return {
            "query_count": self.query_count,
            "udf_executions": self.udf_executions,
            "udf_errors": self.udf_errors,
            "avg_query_time": self.get_avg_query_time(),
            "last_error": str(self.last_error) if self.last_error else None,
        }


class UDFExecutionContext:
    """Context for executing UDFs with consistent error handling and logging."""

    def __init__(self, engine: "DuckDBEngine", udf_name: str):
        """Initialize an execution context.

        Args:
            engine: DuckDB engine instance
            udf_name: Name of the UDF
        """
        self.engine = engine
        self.udf_name = udf_name
        self.start_time: float = 0.0  # Will be set in __enter__
        self.logger = get_logger(f"sqlflow.udf.{udf_name}")

    def __enter__(self):
        """Enter the execution context."""
        self.start_time = time.time()
        self.logger.debug(f"Starting execution of UDF {self.udf_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the execution context.

        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised

        Returns:
            True if the exception was handled, False otherwise
        """
        end_time = time.time()
        duration = end_time - self.start_time

        if exc_type is None:
            self.logger.debug(
                f"UDF {self.udf_name} completed successfully in {duration:.3f}s"
            )
        else:
            self.logger.error(
                f"UDF {self.udf_name} failed after {duration:.3f}s: {exc_val}"
            )
            # Don't suppress the exception
            return False
        return True


class DuckDBEngine(SQLEngine):
    """Primary execution engine using DuckDB."""

    def __init__(self, database_path: Optional[str] = None):
        """Initialize DuckDB engine.

        Args:
            database_path: Path to DuckDB database file, or ":memory:" for in-memory database
        """
        self._initialize_state()
        self._setup_database_connection(database_path)
        self._initialize_components()

        logger.info(
            f"DuckDBEngine initialized: persistent={self.is_persistent}, "
            f"path={self.database_path}"
        )

    def _initialize_state(self):
        """Initialize engine state variables."""
        self.stats = ExecutionStats()
        self.connection: Optional[Any] = None  # Use Any instead of specific DuckDB type
        self.variables = {}
        self.registered_udfs = {}

    def _setup_database_connection(self, database_path: Optional[str]):
        """Set up the database connection.

        Args:
            database_path: Path to the database file
        """
        self.database_path = self._setup_database_path(database_path)
        self.is_persistent = self.database_path != DuckDBConstants.MEMORY_DATABASE

        if self.is_persistent:
            self._ensure_directory_exists()

        self._establish_connection()
        self._configure_persistence()
        self._verify_connection()

    def _initialize_components(self):
        """Initialize engine components."""
        self.transaction_manager = TransactionManager(self)

    def _setup_database_path(self, database_path: Optional[str] = None) -> str:
        """Set up the database path based on input.

        Args:
            database_path: Path to the DuckDB database file, or None

        Returns:
            The resolved database path
        """
        if database_path == DuckDBConstants.MEMORY_DATABASE:
            logger.debug("Using true in-memory database")
            return DuckDBConstants.MEMORY_DATABASE
        elif not database_path:
            default_path = DuckDBConstants.DEFAULT_DATABASE_PATH
            logger.debug("No database path provided, using default: %s", default_path)
            return default_path
        else:
            logger.debug("DuckDB engine initializing with path: %s", database_path)
            return database_path

    def _ensure_directory_exists(self) -> None:
        """Ensure directory exists for file-based databases."""
        if self.database_path != DuckDBConstants.MEMORY_DATABASE:
            dir_path = os.path.dirname(self.database_path)
            if dir_path:
                logger.debug("Creating directory for DuckDB file: %s", dir_path)
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    logger.debug("Directory created/verified: %s", dir_path)
                except Exception as e:
                    logger.debug("Error creating directory: %s", e)
                    raise DuckDBConnectionError(
                        f"Failed to create directory for DuckDB database: {e}"
                    )

    def _establish_connection(self):
        """Establish connection to DuckDB."""
        try:
            self.connection = duckdb.connect(self.database_path)
            logger.debug(f"Connected to DuckDB: {self.database_path}")
        except Exception as e:
            error_msg = f"Error initializing DuckDB: {str(e)}"
            logger.error(error_msg)

            if self.database_path != DuckDBConstants.MEMORY_DATABASE:
                logger.warning(
                    f"Falling back to in-memory database due to error: {str(e)}"
                )
                self.database_path = DuckDBConstants.MEMORY_DATABASE
                self.is_persistent = False
                try:
                    self.connection = duckdb.connect(DuckDBConstants.MEMORY_DATABASE)
                    logger.info(
                        "Successfully connected to in-memory DuckDB as fallback"
                    )
                except Exception as fallback_error:
                    logger.error(f"Fallback to in-memory also failed: {fallback_error}")
                    raise

    def _configure_persistence(self) -> None:
        """Configure persistence settings for the database."""
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        if self.database_path != DuckDBConstants.MEMORY_DATABASE:
            try:
                # Try to get DuckDB version
                version_result = self.connection.execute(
                    DuckDBConstants.SQL_SELECT_VERSION
                ).fetchone()
                duckdb_version = version_result[0] if version_result else "unknown"
                logger.debug("DuckDB version: %s", duckdb_version)

                # Apply settings based on what's likely to be supported
                try:
                    memory_sql = DuckDBConstants.SQL_PRAGMA_MEMORY_LIMIT.format(
                        memory_limit=DuckDBConstants.DEFAULT_MEMORY_LIMIT
                    )
                    self.connection.execute(memory_sql)
                    logger.debug(
                        "Set memory limit to %s", DuckDBConstants.DEFAULT_MEMORY_LIMIT
                    )
                except Exception as e:
                    logger.debug("Could not set memory limit: %s", e)

                # Force a checkpoint to ensure data is committed
                self.connection.execute(DuckDBConstants.SQL_CHECKPOINT)
                logger.debug("Initial checkpoint executed successfully")

                logger.debug("DuckDB persistence settings applied.")
            except Exception as e:
                logger.debug("Could not apply all DuckDB settings: %s", e)

    def _verify_connection(self) -> None:
        """Verify the connection is working.

        Raises:
            DuckDBConnectionError: If test query fails
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        try:
            self.connection.execute(DuckDBConstants.SQL_SELECT_ONE).fetchone()
            logger.debug("DuckDB connection verified with test query")
        except Exception as e:
            logger.debug("DuckDB test query failed: %s", e)
            raise DuckDBConnectionError(f"DuckDB connection test failed: {e}")

    def execute_query(self, query: str) -> Any:
        """Execute SQL query.

        Args:
            query: SQL query to execute

        Returns:
            DuckDB query result

        Raises:
            Exception: If query execution fails
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        start_time = time.time()
        try:
            logger.debug(f"Executing query: {query}")
            result = self.connection.execute(query)
            duration = time.time() - start_time
            self.stats.record_query(duration)
            logger.debug(f"Query executed in {duration:.6f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Query execution failed: {str(e)}")
            logger.debug(f"Failed query: {query}")
            raise

    def register_python_udf(self, name: str, function: Callable) -> None:
        """Register a Python UDF with the engine.

        Args:
            name: Name to register the UDF as
            function: Python function to register
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        logger.info(f"Registering Python UDF: {name}")

        # Use only the last part of the name (function name)
        flat_name = name.split(".")[-1]

        try:
            udf_handler = UDFHandlerFactory.create(function)
            udf_handler.register(flat_name, function, self.connection)
            self.registered_udfs[flat_name] = function

            # Check if the function was registered in the custom table function registry
            if self.connection and hasattr(self.connection, "_sqlflow_table_functions"):
                logger.info(
                    f"Table UDF {flat_name} registered in custom SQLFlow registry"
                )
                # Mark this as a table function for special handling
                if not hasattr(function, "_udf_type"):
                    setattr(function, "_udf_type", "table")

        except Exception as e:
            raise UDFRegistrationError(
                f"Error registering Python UDF {flat_name}: {str(e)}"
            ) from e

    def execute_table_udf(self, name: str, input_data: Any, **kwargs) -> Any:
        """Execute a table UDF programmatically.

        Args:
            name: Name of the table UDF
            input_data: Input data (typically a pandas DataFrame)
            **kwargs: Additional arguments for the UDF

        Returns:
            Result of the UDF execution
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        # Check if it's in our custom table function registry
        if hasattr(self.connection, "_sqlflow_table_functions"):
            if name in self.connection._sqlflow_table_functions:
                function = self.connection._sqlflow_table_functions[name]
                logger.debug(f"Executing table UDF {name} from custom registry")

                try:
                    with UDFExecutionContext(self, name):
                        result = function(input_data, **kwargs)
                        logger.debug(f"Table UDF {name} executed successfully")
                        return result
                except Exception as e:
                    logger.error(f"Error executing table UDF {name}: {e}")
                    raise UDFRegistrationError(
                        f"Table UDF execution failed for {name}: {e}"
                    ) from e

        # Check if it's in the regular UDF registry
        if name in self.registered_udfs:
            function = self.registered_udfs[name]
            udf_type = getattr(function, "_udf_type", None)

            if udf_type == "table":
                logger.debug(f"Executing table UDF {name} from regular registry")
                try:
                    with UDFExecutionContext(self, name):
                        result = function(input_data, **kwargs)
                        logger.debug(f"Table UDF {name} executed successfully")
                        return result
                except Exception as e:
                    logger.error(f"Error executing table UDF {name}: {e}")
                    raise UDFRegistrationError(
                        f"Table UDF execution failed for {name}: {e}"
                    ) from e

        raise UDFRegistrationError(f"Table UDF {name} not found in any registry")

    def process_query_for_udfs(self, query: str, udfs: Dict[str, Callable]) -> str:
        """Process a query to handle UDF references.

        Args:
            query: SQL query
            udfs: Dictionary of UDFs to consider for the query

        Returns:
            Processed query
        """
        if not udfs:
            logger.debug("No UDF replacements made in query")
            return query

        processor = AdvancedUDFQueryProcessor(self, udfs)
        return processor.process(query)

    def register_table(self, name: str, data: Any, manage_transaction: bool = True):
        """Register a table in DuckDB.

        Args:
            name: Name of the table
            data: Data to register (pandas DataFrame or similar)
            manage_transaction: Whether this method should handle transaction
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        logger.debug("Registering table %s", name)
        logger.debug(f"Registering table {name} with schema: {data.dtypes}")

        if manage_transaction:
            with self.transaction_manager:
                self._register_table_internal(name, data)
        else:
            self._register_table_internal(name, data)

    def _register_table_internal(self, name: str, data: Any):
        """Internal table registration logic.

        Args:
            name: Name of the table
            data: Data to register
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        # Register the table
        self.connection.register(name, data)
        logger.debug("Table %s registered successfully", name)

        # If using file-based storage, create a persistent table directly
        if self.is_persistent:
            self._create_persistent_table(name, data)

    def _create_persistent_table(self, name: str, data: Any):
        """Create a persistent table from registered data.

        Args:
            name: Name of the table
            data: Data to persist
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        try:
            if hasattr(data, "columns"):
                column_names = list(data.columns)
                if column_names:
                    # Create a select statement that explicitly names each column
                    columns_sql = ", ".join(
                        [f'"{col}" AS "{col}"' for col in column_names]
                    )
                    create_sql = SQLTemplates.CREATE_TABLE_WITH_COLUMNS.format(
                        table_name=name, columns=columns_sql, source_name=name
                    )
                    self.connection.execute(create_sql)
                else:
                    # Fallback for tables without column names
                    create_sql = SQLTemplates.CREATE_TABLE_AS.format(
                        table_name=name, source_name=name
                    )
                    self.connection.execute(create_sql)

                logger.debug(
                    f"Created persistent table {name} with column names: {column_names}"
                )
            else:
                # No column information available, use original approach
                create_sql = SQLTemplates.CREATE_TABLE_AS.format(
                    table_name=name, source_name=name
                )
                self.connection.execute(create_sql)
                logger.debug(
                    f"Created persistent table {name} without explicit column names"
                )

        except Exception as e:
            logger.debug("Error during table persistence: %s", e)
            raise

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table.

        Args:
            table_name: Name of the table

        Returns:
            Dict mapping column names to their types
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        logger.debug("Getting schema for table %s", table_name)
        try:
            # Try PRAGMA approach first (newer DuckDB)
            try:
                pragma_sql = DuckDBConstants.SQL_PRAGMA_TABLE_INFO.format(
                    table_name=table_name
                )
                result = self.connection.sql(pragma_sql)
                schema = {
                    row["name"]: row["type"]
                    for row in result.fetchdf().to_dict("records")
                }
            except Exception:
                # Fall back to DESCRIBE for older versions
                describe_sql = DuckDBConstants.SQL_DESCRIBE_TABLE.format(
                    table_name=table_name
                )
                result = self.connection.sql(describe_sql)
                schema = {
                    row["column_name"]: row["column_type"]
                    for row in result.fetchdf().to_dict("records")
                }

            logger.debug("Schema for table %s: %s", table_name, schema)
            return schema
        except Exception as e:
            logger.debug("Error getting schema for table %s: %s", table_name, e)
            logger.error(f"Error getting schema for table {table_name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table to check

        Returns:
            True if the table exists, False otherwise
        """
        if not self.connection:
            logger.warning(
                "No database connection available, assuming table doesn't exist"
            )
            return False

        logger.debug("Checking if table %s exists", table_name)
        try:
            # Try to check if the table exists using information_schema
            try:
                # First try the information_schema approach (standard)
                check_sql = SQLTemplates.CHECK_TABLE_EXISTS.format(
                    table_name=table_name
                )
                result = self.connection.sql(check_sql)
                exists = len(result.fetchdf()) > 0
            except Exception:
                # Fall back to direct query
                try:
                    limit_sql = SQLTemplates.CHECK_TABLE_EXISTS_LIMIT.format(
                        table_name=table_name
                    )
                    self.connection.sql(limit_sql)
                    exists = True
                except Exception:
                    exists = False

            logger.debug("Table %s exists: %s", table_name, exists)
            return exists
        except Exception as e:
            logger.debug("Error checking if table %s exists: %s", table_name, e)
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False

    def generate_load_sql(self, load_step: Any) -> str:
        """Generate SQL for a LOAD step based on its mode.

        Args:
            load_step: The LoadStep containing table_name, source_name, mode, and merge_keys

        Returns:
            SQL string for executing the LOAD operation
        """
        # Convert parser LoadStep to our internal format if needed
        from .load.handlers import LoadStep as InternalLoadStep

        if isinstance(load_step, InternalLoadStep):
            internal_load_step = load_step
        else:
            # Convert from parser's LoadStep to our internal LoadStep
            internal_load_step = InternalLoadStep(
                table_name=load_step.table_name,
                source_name=load_step.source_name,
                mode=load_step.mode,
                merge_keys=(
                    load_step.merge_keys if hasattr(load_step, "merge_keys") else None
                ),
            )

        handler = LoadModeHandlerFactory.create(internal_load_step.mode, self)
        return handler.generate_sql(internal_load_step)

    # Simplified methods that delegate to other components
    def substitute_variables(self, template: str) -> str:
        """Substitute variables in a template.

        Args:
            template: Template string with variables in the form ${var_name}

        Returns:
            Template with variables substituted
        """
        # Replace variables
        result = re.sub(
            RegexPatterns.VARIABLE_SUBSTITUTION, self._replace_variable_match, template
        )
        logger.debug("Substitution result: %s", result)
        return result

    def _replace_variable_match(self, match: re.Match) -> str:
        """Replace a variable match with its formatted value."""
        var_expr = match.group(1)

        if "|" in var_expr:
            return self._handle_variable_with_default(var_expr)
        else:
            return self._handle_simple_variable(var_expr)

    def _handle_variable_with_default(self, var_expr: str) -> str:
        """Handle variable substitution with default value."""
        var_name, default = var_expr.split("|", 1)
        var_name = var_name.strip()
        default = default.strip()

        if var_name in self.variables:
            return self._format_sql_value(self.variables[var_name])

        # Handle quoted default values
        if self._is_quoted_string(default):
            return default.strip("\"'")
        return self._format_sql_value(default)

    def _handle_simple_variable(self, var_expr: str) -> str:
        """Handle simple variable substitution without default."""
        var_name = var_expr.strip()
        if var_name in self.variables:
            return self._format_sql_value(self.variables[var_name])

        logger.warning(f"Variable {var_name} not found and no default provided")
        return "NULL"

    def _is_quoted_string(self, value: str) -> bool:
        """Check if a string is quoted."""
        return (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        )

    def _format_sql_value(self, value: Any) -> str:
        """Format a value for SQL based on its type."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Check if the value is already quoted
            if value.startswith("'") and value.endswith("'"):
                return value
            # Escape single quotes and wrap in quotes
            escaped_value = value.replace("'", "''")
            return "'" + escaped_value + "'"
        else:
            # For any other type, convert to string and quote
            return "'" + str(value) + "'"

    def register_variable(self, name: str, value: Any) -> None:
        """Register a variable for use in queries.

        Args:
            name: Variable name
            value: Variable value
        """
        self.variables[name] = value

    def get_variable(self, name: str) -> Any:
        """Get the value of a variable.

        Args:
            name: Variable name

        Returns:
            Variable value
        """
        return self.variables.get(name)

    def configure(
        self, config: Dict[str, Any], profile_variables: Dict[str, Any]
    ) -> None:
        """Configure the engine with settings from the profile.

        Args:
            config: Engine configuration from the profile
            profile_variables: Variables defined in the profile
        """
        logger.info("Configuring DuckDB engine")

        # Register profile variables
        for name, value in profile_variables.items():
            self.register_variable(name, value)

        # Apply specific DuckDB settings from config
        if "memory_limit" in config and self.connection:
            try:
                memory_sql = DuckDBConstants.SQL_PRAGMA_MEMORY_LIMIT.format(
                    memory_limit=config["memory_limit"]
                )
                self.connection.execute(memory_sql)
                logger.info(f"Set memory limit to {config['memory_limit']}")
                logger.debug("Set memory limit to %s", config["memory_limit"])
            except Exception as e:
                logger.warning(f"Could not set memory limit: {e}")
                logger.debug("Could not set memory limit: %s", e)

    def close(self):
        """Close the database connection and release resources."""
        if self.connection is not None:
            try:
                logger.debug(f"Closing DuckDB connection for {self.database_path}")
                self.connection.close()
                self.connection = None
                logger.debug("DuckDB connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {str(e)}")

    def __del__(self):
        """Clean up resources when the object is garbage collected."""
        self.close()

    # Additional methods for UDF execution and management
    def execute_udf_with_context(
        self, udf_name: str, udf_func: Callable, *args, **kwargs
    ) -> Any:
        """Execute a UDF with proper error handling and context.

        Args:
            udf_name: Name of the UDF to execute
            udf_func: UDF function
            *args: Positional arguments for the UDF
            **kwargs: Keyword arguments for the UDF

        Returns:
            Result of UDF execution

        Raises:
            UDFError: If UDF execution fails
        """
        with UDFExecutionContext(self, udf_name) as ctx:
            try:
                result = udf_func(*args, **kwargs)
                self.stats.record_udf_execution(True)
                return result
            except Exception as e:
                self.stats.record_udf_execution(False, e)
                raise UDFError(
                    f"Error executing UDF {udf_name}: {str(e)}", udf_name=udf_name
                ) from e

    def get_stats(self) -> Dict[str, Any]:
        """Get current execution statistics.

        Returns:
            Dictionary with execution statistics
        """
        return self.stats.get_summary()

    def reset_stats(self) -> None:
        """Reset execution statistics."""
        self.stats = ExecutionStats()

    def supports_feature(self, feature: str) -> bool:
        """Check if the engine supports a specific feature.

        Args:
            feature: Feature to check for support

        Returns:
            True if the feature is supported, False otherwise
        """
        # List of supported features
        supported_features = {
            "python_udfs": True,
            "arrow": True,
            "json": True,
            "merge": True,
            "window_functions": True,
            "ctes": True,
        }

        return supported_features.get(feature, False)

    # Schema validation methods
    def validate_schema_compatibility(
        self, target_table: str, source_schema: Dict[str, str]
    ) -> bool:
        """Validate schema compatibility between source and target tables.

        Args:
            target_table: Name of the target table
            source_schema: Schema of the source table

        Returns:
            True if schemas are compatible

        Raises:
            ValueError: If schemas are incompatible
        """
        logger.debug(
            f"Validating schema compatibility between source and target {target_table}"
        )

        # If target table doesn't exist, any schema is compatible
        if not self.table_exists(target_table):
            logger.debug(
                f"Target table {target_table} doesn't exist, no schema validation needed"
            )
            return True

        # Get target table schema
        target_schema = self.get_table_schema(target_table)

        # Check source columns exist in target with compatible types
        for col_name, col_type in source_schema.items():
            if col_name not in target_schema:
                error_msg = f"Column '{col_name}' in source does not exist in target table '{target_table}'"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Normalize types for comparison
            source_type = col_type.upper()
            target_type = target_schema[col_name].upper()

            # Check type compatibility
            if not self._are_types_compatible(source_type, target_type):
                error_msg = f"Column '{col_name}' has incompatible types: source={source_type}, target={target_type}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        logger.debug(f"Schema validation successful for {target_table}")
        return True

    def _are_types_compatible(self, source_type: str, target_type: str) -> bool:
        """Check if two SQL types are compatible.

        Args:
            source_type: Source column type
            target_type: Target column type

        Returns:
            True if types are compatible, False otherwise
        """

        def normalize_type(type_str):
            if (
                "VARCHAR" in type_str
                or "CHAR" in type_str
                or "TEXT" in type_str
                or "STRING" in type_str
            ):
                return "STRING"
            elif "INT" in type_str:
                return "INTEGER"
            elif (
                "FLOAT" in type_str
                or "DOUBLE" in type_str
                or "DECIMAL" in type_str
                or "NUMERIC" in type_str
            ):
                return "FLOAT"
            elif "BOOL" in type_str:
                return "BOOLEAN"
            elif "DATE" in type_str:
                return "DATE"
            elif "TIME" in type_str and "TIMESTAMP" not in type_str:
                return "TIME"
            elif "TIMESTAMP" in type_str:
                return "TIMESTAMP"
            else:
                return type_str

        norm_source = normalize_type(source_type)
        norm_target = normalize_type(target_type)

        return norm_source == norm_target

    def validate_merge_keys(
        self, target_table: str, source_name: str, merge_keys: List[str]
    ) -> bool:
        """Validate that merge keys exist in both source and target tables.

        Args:
            target_table: Name of the target table
            source_name: Name of the source table/view
            merge_keys: List of column names to be used as merge keys

        Returns:
            True if merge keys are valid

        Raises:
            ValueError: If merge keys are invalid
        """
        if not merge_keys:
            raise ValueError("MERGE operation requires at least one merge key")

        logger.debug(
            f"Validating merge keys for MERGE operation: {', '.join(merge_keys)}"
        )

        # If target table doesn't exist, no validation needed
        if not self.table_exists(target_table):
            logger.debug(
                f"Target table {target_table} doesn't exist, no merge key validation needed"
            )
            return True

        # Get schemas for source and target
        source_schema = self.get_table_schema(source_name)
        target_schema = self.get_table_schema(target_table)

        # Validate each merge key
        for key in merge_keys:
            # Check if key exists in source
            if key not in source_schema:
                error_msg = (
                    f"Merge key '{key}' does not exist in source '{source_name}'"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Check if key exists in target
            if key not in target_schema:
                error_msg = (
                    f"Merge key '{key}' does not exist in target table '{target_table}'"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Check type compatibility of merge keys
            source_type = source_schema[key].upper()
            target_type = target_schema[key].upper()

            if not self._are_types_compatible(source_type, target_type):
                error_msg = (
                    f"Merge key '{key}' has incompatible types: "
                    f"source={source_type}, target={target_type}. "
                    f"Merge keys must have compatible types."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

        logger.debug(f"Merge key validation successful for {', '.join(merge_keys)}")
        return True

    # Simplified stubs for methods that need full implementation
    def create_temp_table(self, name: str, data: Any) -> None:
        """Create a temporary table with the given data."""
        logger.info(f"Creating temporary table {name}")
        if isinstance(data, pd.DataFrame):
            self.register_table(name, data)
        elif isinstance(data, pa.Table):
            self.register_arrow(name, data)
        else:
            raise TypeError(f"Unsupported data type for temp table: {type(data)}")

    def register_arrow(
        self, table_name: str, arrow_table: pa.Table, manage_transaction: bool = True
    ) -> None:
        """Register an Arrow table with the engine."""
        logger.info(f"Registering Arrow table {table_name}")
        # Convert to pandas if needed for now - this would be expanded in full implementation
        df = arrow_table.to_pandas()
        self.register_table(table_name, df, manage_transaction)

    def commit(self):
        """Commit any pending changes to the database."""
        if not self.connection:
            logger.warning("No database connection available, cannot commit")
            return

        logger.debug("Committing changes")
        try:
            self.connection.commit()
            logger.debug("Changes committed successfully")

            # Checkpoint for persistent databases
            if self.is_persistent:
                try:
                    self.connection.execute(DuckDBConstants.SQL_CHECKPOINT)
                    logger.debug("Checkpoint executed after commit")
                except Exception as e:
                    logger.debug("Error performing checkpoint: %s", e)

            logger.info("Changes committed successfully")
        except Exception as e:
            logger.debug("Error committing changes: %s", e)
            logger.error(f"Error committing changes: {e}")

    def execute_pipeline_file(
        self, file_path: str, compile_only: bool = False
    ) -> Dict[str, Any]:
        """Execute a pipeline file."""
        logger.debug(
            "Executing pipeline file: %s, compile_only: %s", file_path, compile_only
        )
        return {}

    def batch_execute_table_udf(
        self, udf_name: str, dataframes: List[pd.DataFrame], **kwargs
    ) -> List[pd.DataFrame]:
        """Batch execute table UDFs for performance.

        Phase 3 enhancement for executing table UDFs across multiple DataFrames
        with optimized batch processing and resource management.

        Args:
            udf_name: Name of the table UDF
            dataframes: List of DataFrames to process
            **kwargs: Additional arguments for the UDF

        Returns:
            List of processed DataFrames
        """
        if not self.connection:
            raise DuckDBConnectionError("No database connection available")

        if not dataframes:
            logger.warning(f"No dataframes provided for batch execution of {udf_name}")
            return []

        logger.info(
            f"Batch executing table UDF {udf_name} on {len(dataframes)} DataFrames"
        )

        results = []
        successful_executions = 0
        failed_executions = 0

        for i, df in enumerate(dataframes):
            try:
                logger.debug(
                    f"Processing batch {i+1}/{len(dataframes)} for UDF {udf_name}"
                )
                result = self.execute_table_udf(udf_name, df, **kwargs)
                results.append(result)
                successful_executions += 1

            except Exception as e:
                logger.error(f"Error in batch {i+1} for UDF {udf_name}: {e}")
                failed_executions += 1
                # Add empty DataFrame as placeholder
                results.append(pd.DataFrame())

        logger.info(
            f"Batch execution complete: {successful_executions} successful, {failed_executions} failed"
        )
        return results

    def validate_table_udf_schema_compatibility(
        self, table_name: str, udf_schema: Dict[str, str]
    ) -> bool:
        """Validate UDF schema compatibility with existing tables.

        Phase 3 enhancement for comprehensive schema validation between
        table UDF outputs and target tables.

        Args:
            table_name: Name of the target table
            udf_schema: Expected schema of the UDF output

        Returns:
            True if schemas are compatible
        """
        logger.debug(f"Validating UDF schema compatibility with table {table_name}")

        if not self.table_exists(table_name):
            logger.info(
                f"Target table {table_name} doesn't exist - UDF schema is valid"
            )
            return True

        try:
            target_schema = self.get_table_schema(table_name)

            # Validate each UDF output column
            for col_name, col_type in udf_schema.items():
                if col_name not in target_schema:
                    logger.error(
                        f"UDF output column '{col_name}' not found in target table {table_name}"
                    )
                    return False

                # Check type compatibility
                target_type = target_schema[col_name]
                if not self._are_types_compatible(col_type, target_type):
                    logger.error(
                        f"Incompatible types for column '{col_name}': UDF={col_type}, Table={target_type}"
                    )
                    return False

            logger.info(f"UDF schema is compatible with table {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error validating schema compatibility: {e}")
            return False

    def debug_table_udf_registration(self, udf_name: str) -> Dict[str, Any]:
        """Comprehensive debugging information for table UDF registration.

        Phase 3 enhancement providing detailed debugging information
        for troubleshooting table UDF registration issues.

        Args:
            udf_name: Name of the UDF to debug

        Returns:
            Dictionary with comprehensive debugging information
        """
        debug_info = {
            "udf_name": udf_name,
            "timestamp": time.time(),
            "engine_state": {},
            "registration_status": {},
            "metadata": {},
            "recommendations": [],
        }

        # Engine state information
        debug_info["engine_state"] = {
            "connection_available": self.connection is not None,
            "database_path": self.database_path,
            "is_persistent": self.is_persistent,
            "registered_udfs_count": len(self.registered_udfs),
            "stats": self.get_stats(),
        }

        # Registration status
        flat_name = udf_name.split(".")[-1]
        debug_info["registration_status"] = {
            "flat_name": flat_name,
            "in_registered_udfs": flat_name in self.registered_udfs,
            "in_custom_registry": False,
            "registration_error": None,
        }

        # Check custom table function registry
        if self.connection and hasattr(self.connection, "_sqlflow_table_functions"):
            debug_info["registration_status"]["in_custom_registry"] = (
                flat_name in self.connection._sqlflow_table_functions
            )

        # UDF metadata if available
        if flat_name in self.registered_udfs:
            udf_function = self.registered_udfs[flat_name]
            debug_info["metadata"] = {
                "udf_type": getattr(udf_function, "_udf_type", "unknown"),
                "output_schema": getattr(udf_function, "_output_schema", None),
                "infer_schema": getattr(udf_function, "_infer_schema", False),
                "table_dependencies": getattr(
                    udf_function, "_table_dependencies", None
                ),
                "vectorized": getattr(udf_function, "_vectorized", False),
                "arrow_compatible": getattr(udf_function, "_arrow_compatible", False),
                "enable_batch_processing": getattr(
                    udf_function, "_enable_batch_processing", False
                ),
            }

        # Generate recommendations
        debug_info["recommendations"] = self._generate_udf_recommendations(
            udf_name, debug_info
        )

        logger.debug(f"Generated debug information for UDF {udf_name}")
        return debug_info

    def _generate_udf_recommendations(
        self, udf_name: str, debug_info: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations for UDF troubleshooting.

        Args:
            udf_name: Name of the UDF
            debug_info: Current debug information

        Returns:
            List of recommendation strings
        """
        recommendations = []

        # Check connection
        if not debug_info["engine_state"]["connection_available"]:
            recommendations.append(
                "Establish database connection before registering UDFs"
            )

        # Check registration status
        reg_status = debug_info["registration_status"]
        if (
            not reg_status["in_registered_udfs"]
            and not reg_status["in_custom_registry"]
        ):
            recommendations.append(
                f"UDF {udf_name} is not registered. Call register_python_udf() first"
            )

        # Check metadata
        metadata = debug_info.get("metadata", {})
        if metadata:
            udf_type = metadata.get("udf_type", "unknown")

            if udf_type == "table":
                if not metadata.get("output_schema") and not metadata.get(
                    "infer_schema"
                ):
                    recommendations.append(
                        "Consider adding output_schema or infer_schema for better table UDF support"
                    )

                if not metadata.get("vectorized"):
                    recommendations.append(
                        "Consider enabling vectorization for large dataset processing"
                    )

                if not metadata.get("arrow_compatible"):
                    recommendations.append(
                        "Consider making UDF Arrow-compatible for better performance"
                    )

        # Check performance
        stats = debug_info["engine_state"].get("stats", {})
        if stats.get("udf_errors", 0) > 0:
            recommendations.append(
                "Review UDF implementation - recent execution errors detected"
            )

        return recommendations

    def get_table_udf_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics specific to table UDF operations.

        Phase 3 enhancement providing detailed performance tracking
        for table UDF operations and optimization insights.

        Returns:
            Dictionary with table UDF performance metrics
        """
        base_stats = self.get_stats()

        # Enhanced metrics for table UDFs
        table_udf_metrics = {
            "base_stats": base_stats,
            "table_udf_specific": {
                "total_table_udfs": 0,
                "vectorized_udfs": 0,
                "arrow_optimized_udfs": 0,
                "batch_enabled_udfs": 0,
            },
            "performance_insights": [],
            "optimization_opportunities": [],
        }

        # Analyze registered UDFs
        for udf_name, udf_function in self.registered_udfs.items():
            udf_type = getattr(udf_function, "_udf_type", "scalar")

            if udf_type == "table":
                table_udf_metrics["table_udf_specific"]["total_table_udfs"] += 1

                if getattr(udf_function, "_vectorized", False):
                    table_udf_metrics["table_udf_specific"]["vectorized_udfs"] += 1

                if getattr(udf_function, "_arrow_compatible", False):
                    table_udf_metrics["table_udf_specific"]["arrow_optimized_udfs"] += 1

                if getattr(udf_function, "_enable_batch_processing", False):
                    table_udf_metrics["table_udf_specific"]["batch_enabled_udfs"] += 1

        # Generate performance insights
        total_table_udfs = table_udf_metrics["table_udf_specific"]["total_table_udfs"]
        if total_table_udfs > 0:
            vectorized_pct = (
                table_udf_metrics["table_udf_specific"]["vectorized_udfs"]
                / total_table_udfs
            ) * 100
            arrow_pct = (
                table_udf_metrics["table_udf_specific"]["arrow_optimized_udfs"]
                / total_table_udfs
            ) * 100

            table_udf_metrics["performance_insights"] = [
                f"Table UDF vectorization coverage: {vectorized_pct:.1f}%",
                f"Arrow optimization coverage: {arrow_pct:.1f}%",
                f"Average query time: {base_stats.get('avg_query_time', 0):.4f}s",
            ]

            # Optimization opportunities
            if vectorized_pct < 50:
                table_udf_metrics["optimization_opportunities"].append(
                    "Consider enabling vectorization for more table UDFs to improve performance"
                )

            if arrow_pct < 50:
                table_udf_metrics["optimization_opportunities"].append(
                    "Consider making more table UDFs Arrow-compatible for zero-copy performance"
                )

        return table_udf_metrics

    def optimize_table_udf_for_performance(self, udf_name: str) -> Dict[str, Any]:
        """Optimize a table UDF for performance using available enhancement strategies.

        Phase 3 enhancement automatically applying performance optimizations
        to registered table UDFs based on their characteristics.

        Args:
            udf_name: Name of the table UDF to optimize

        Returns:
            Dictionary with optimization results and recommendations
        """
        flat_name = udf_name.split(".")[-1]

        if flat_name not in self.registered_udfs:
            return {
                "error": f"UDF {udf_name} not found in registered UDFs",
                "optimizations_applied": [],
                "recommendations": [f"Register UDF {udf_name} first"],
            }

        udf_function = self.registered_udfs[flat_name]
        udf_type = getattr(udf_function, "_udf_type", "scalar")

        if udf_type != "table":
            return {
                "error": f"UDF {udf_name} is not a table UDF",
                "optimizations_applied": [],
                "recommendations": [
                    "Only table UDFs can be optimized with this method"
                ],
            }

        optimization_results = {
            "udf_name": udf_name,
            "optimizations_applied": [],
            "recommendations": [],
            "performance_impact": "unknown",
        }

        # Apply available optimizations
        from .udf.performance import ArrowPerformanceOptimizer

        optimizer = ArrowPerformanceOptimizer()

        # Get recommended optimizations
        recommendations = optimizer.get_recommended_optimizations(udf_function)

        for optimization in recommendations:
            if optimization == "serialization_optimization":
                optimized_function = optimizer.minimize_serialization_overhead(
                    udf_function
                )
                self.registered_udfs[flat_name] = optimized_function
                optimization_results["optimizations_applied"].append(
                    "serialization_optimization"
                )

            elif optimization == "vectorization":
                vectorized_function = optimizer.enable_vectorized_processing(
                    udf_function
                )
                self.registered_udfs[flat_name] = vectorized_function
                optimization_results["optimizations_applied"].append("vectorization")

            elif optimization == "arrow_optimization":
                # Mark as Arrow-compatible if not already
                if not getattr(udf_function, "_arrow_compatible", False):
                    setattr(udf_function, "_arrow_compatible", True)
                    optimization_results["optimizations_applied"].append(
                        "arrow_compatibility"
                    )

        # Generate additional recommendations
        if not optimization_results["optimizations_applied"]:
            optimization_results["recommendations"].append(
                "UDF already optimized or no optimizations available"
            )
        else:
            optimization_results["performance_impact"] = "improved"
            optimization_results["recommendations"].append(
                "Monitor performance metrics to validate optimization impact"
            )

        logger.info(
            f"Applied {len(optimization_results['optimizations_applied'])} optimizations to UDF {udf_name}"
        )
        return optimization_results
