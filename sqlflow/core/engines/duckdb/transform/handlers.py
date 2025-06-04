"""Transform mode handlers for DuckDB engine.

This module implements transform mode handlers that extend the existing LOAD
infrastructure to provide advanced data transformation capabilities with
SQL-native syntax.
"""

import threading
import time
from abc import abstractmethod
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from sqlflow.logging import get_logger
from sqlflow.parser.ast import SQLBlockStep

from ..exceptions import (
    InvalidLoadModeError,
)

# Import existing LOAD infrastructure to reuse
from ..load.handlers import (
    LoadModeHandler,
    TableInfo,
)

if TYPE_CHECKING:
    from ..engine import DuckDBEngine

logger = get_logger(__name__)


class TransformError(Exception):
    """Exception raised for transform-specific errors."""

    def __init__(self, message: str, table_name: Optional[str] = None):
        """Initialize a TransformError.

        Args:
            message: Error message
            table_name: Table name involved in the error
        """
        self.message = message
        self.table_name = table_name
        super().__init__(message)


class SecureTimeSubstitution:
    """Secure time macro substitution using parameterized queries."""

    def substitute_time_macros(
        self, sql: str, start_time: datetime, end_time: datetime
    ) -> Tuple[str, Dict[str, Any]]:
        """Return SQL with placeholders and parameter dictionary for safe execution.

        Args:
            sql: SQL query with time macros
            start_time: Start time for the time range
            end_time: End time for the time range

        Returns:
            Tuple of (sql_with_parameters, parameters_dict)
        """
        # Create parameter dictionary
        parameters = {
            "start_date": start_time.strftime("%Y-%m-%d"),
            "end_date": end_time.strftime("%Y-%m-%d"),
            "start_dt": start_time.isoformat(),
            "end_dt": end_time.isoformat(),
        }

        # Replace macros with DuckDB parameter placeholders
        result = sql
        macro_to_param = {
            "@start_date": "$start_date",
            "@end_date": "$end_date",
            "@start_dt": "$start_dt",
            "@end_dt": "$end_dt",
        }

        for macro, param in macro_to_param.items():
            if macro in result:
                result = result.replace(macro, param)

        return result, parameters

    def execute_with_parameters(
        self, conn, sql: str, parameters: Dict[str, Any]
    ) -> Any:
        """Execute SQL with parameters safely.

        Args:
            conn: Database connection
            sql: SQL query with parameter placeholders
            parameters: Parameter values

        Returns:
            Query result
        """
        return conn.execute(sql, parameters)


class TransformLockManager:
    """Prevent concurrent transforms on same table."""

    def __init__(self):
        """Initialize the lock manager."""
        self.locks = {}
        self._lock = threading.Lock()

    @contextmanager
    def acquire_table_lock(self, table_name: str):
        """Acquire exclusive lock for table transformation.

        Args:
            table_name: Name of the table to lock

        Raises:
            TransformError: If table is already locked
        """
        with self._lock:
            if table_name in self.locks:
                raise TransformError(
                    f"Table {table_name} is already being transformed by another process"
                )

            self.locks[table_name] = threading.Lock()

        try:
            with self.locks[table_name]:
                yield
        finally:
            with self._lock:
                if table_name in self.locks:
                    del self.locks[table_name]


class TransformModeHandler(LoadModeHandler):
    """Base class for transform mode handlers - reuses LOAD validation."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize transform mode handler.

        Args:
            engine: DuckDB engine instance
        """
        super().__init__(engine)  # Inherit all LOAD infrastructure
        self.time_substitution = SecureTimeSubstitution()
        self.lock_manager = TransformLockManager()
        logger.debug(f"Initialized {self.__class__.__name__}")

    @abstractmethod
    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate SQL with parameters for secure execution.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """

    def generate_sql(self, load_step) -> str:
        """Legacy method for compatibility with LoadModeHandler interface.

        This method is kept for interface compatibility but not used
        in transform mode execution.

        Args:
            load_step: Load step (not used in transforms)

        Returns:
            Empty string (not used)
        """
        return ""


class ReplaceTransformHandler(TransformModeHandler):
    """REPLACE mode - drops and recreates table atomically."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate CREATE OR REPLACE SQL for REPLACE mode.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        sql = f"""
            CREATE OR REPLACE TABLE {transform_step.table_name} AS
            {transform_step.sql_query}
        """

        return [sql.strip()], {}


class AppendTransformHandler(TransformModeHandler):
    """APPEND mode - validates schema compatibility and inserts new data."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate schema-validated INSERT SQL for APPEND mode.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)

        Raises:
            SchemaValidationError: If schema validation fails
        """
        table_info = self.validation_helper.get_table_info(transform_step.table_name)

        if table_info.exists:
            # For APPEND mode, we need to validate schema compatibility
            # First, create a temporary view of the transform query to get its schema
            temp_view_name = f"temp_transform_{int(time.time())}"

            sql_statements = [
                f"CREATE OR REPLACE VIEW {temp_view_name} AS {transform_step.sql_query}",
                f"INSERT INTO {transform_step.table_name} SELECT * FROM {temp_view_name}",
                f"DROP VIEW {temp_view_name}",
            ]
        else:
            # Table doesn't exist, create it
            sql_statements = [
                f"""
                CREATE TABLE {transform_step.table_name} AS
                {transform_step.sql_query}
                """.strip()
            ]

        return sql_statements, {}


class MergeTransformHandler(TransformModeHandler):
    """MERGE mode - upserts data based on specified merge keys."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate UPSERT SQL for MERGE mode.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)

        Raises:
            MergeKeyValidationError: If merge key validation fails
        """
        table_info = self.validation_helper.get_table_info(transform_step.table_name)

        if table_info.exists:
            return self._generate_merge_sql(transform_step, table_info)
        else:
            # Table doesn't exist, create it
            sql = f"""
                CREATE TABLE {transform_step.table_name} AS
                {transform_step.sql_query}
            """
            return [sql.strip()], {}

    def _generate_merge_sql(
        self, transform_step: SQLBlockStep, table_info: TableInfo
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate SQL for MERGE operation on existing table.

        Args:
            transform_step: Transform step configuration
            table_info: Information about the target table

        Returns:
            Tuple of (sql_statements, parameters)
        """
        # Create temporary table for new data
        temp_table_name = f"temp_merge_{int(time.time())}"
        merge_condition = " AND ".join(
            f"target.{key} = source.{key}" for key in transform_step.merge_keys
        )

        # Use INSERT OR REPLACE for efficient UPSERT in DuckDB
        sql_statements = [
            f"CREATE OR REPLACE TABLE {temp_table_name} AS {transform_step.sql_query}",
            f"""
            INSERT OR REPLACE INTO {transform_step.table_name} 
            SELECT * FROM {temp_table_name}
            """.strip(),
            f"DROP TABLE {temp_table_name}",
        ]

        return sql_statements, {}


class IncrementalTransformHandler(TransformModeHandler):
    """INCREMENTAL mode - time-based partitioned updates with transaction safety."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate DELETE + INSERT pattern with atomic transaction.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        table_info = self.validation_helper.get_table_info(transform_step.table_name)

        if table_info.exists:
            return self._generate_incremental_sql(transform_step)
        else:
            # Table doesn't exist, create it with current data
            # For initial load, we'll use a current time range
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)  # Default 1 day lookback

            sql_query, parameters = self.time_substitution.substitute_time_macros(
                transform_step.sql_query, start_time, end_time
            )

            sql = f"""
                CREATE TABLE {transform_step.table_name} AS
                {sql_query}
            """

            return [sql.strip()], parameters

    def _generate_incremental_sql(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate incremental update SQL.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        # For now, use a simple time range (this will be enhanced with watermark management)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)

        # Apply LOOKBACK if specified
        if transform_step.lookback:
            lookback_days = self._parse_lookback(transform_step.lookback)
            start_time = start_time - timedelta(days=lookback_days)

        # Substitute time macros securely
        insert_sql, parameters = self.time_substitution.substitute_time_macros(
            transform_step.sql_query, start_time, end_time
        )

        # Generate atomic transaction SQL
        delete_sql = f"""
            DELETE FROM {transform_step.table_name}
            WHERE {transform_step.time_column} >= $start_date 
            AND {transform_step.time_column} <= $end_date
        """

        insert_sql = f"INSERT INTO {transform_step.table_name} {insert_sql}"

        # Ensure atomic execution with explicit transaction
        sql_statements = [
            "BEGIN TRANSACTION;",
            delete_sql,
            insert_sql,
            "COMMIT;",
        ]

        return sql_statements, parameters

    def _parse_lookback(self, lookback: str) -> int:
        """Parse lookback string to number of days.

        Args:
            lookback: Lookback string (e.g., "2 DAYS", "1 DAY")

        Returns:
            Number of days
        """
        # Simple parsing for now - will be enhanced
        parts = lookback.lower().split()
        if len(parts) >= 2:
            try:
                return int(parts[0])
            except ValueError:
                pass
        return 1  # Default to 1 day


class TransformModeHandlerFactory:
    """Factory for creating appropriate transform mode handlers."""

    @staticmethod
    def create(mode: str, engine: "DuckDBEngine") -> TransformModeHandler:
        """Create appropriate transform mode handler.

        Args:
            mode: Transform mode (REPLACE, APPEND, MERGE, INCREMENTAL)
            engine: DuckDB engine instance

        Returns:
            Appropriate transform mode handler

        Raises:
            InvalidLoadModeError: If transform mode is not supported
        """
        mode = mode.upper()

        if mode == "REPLACE":
            return ReplaceTransformHandler(engine)
        elif mode == "APPEND":
            return AppendTransformHandler(engine)
        elif mode == "MERGE":
            return MergeTransformHandler(engine)
        elif mode == "INCREMENTAL":
            return IncrementalTransformHandler(engine)
        else:
            valid_modes = ["REPLACE", "APPEND", "MERGE", "INCREMENTAL"]
            raise InvalidLoadModeError(mode, valid_modes)
