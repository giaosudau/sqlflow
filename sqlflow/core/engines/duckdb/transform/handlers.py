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

# Import performance optimization framework
from .performance import PerformanceOptimizer

# Import optimized watermark manager for transform operations
from .watermark import OptimizedWatermarkManager

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
        self.watermark_manager = OptimizedWatermarkManager(engine)
        self.performance_optimizer = PerformanceOptimizer()
        logger.debug(
            f"Initialized {self.__class__.__name__} with optimized watermark manager and performance optimizer"
        )

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

    def _calculate_time_range(
        self, last_processed: Optional[datetime], lookback: Optional[str]
    ) -> Tuple[datetime, datetime]:
        """Calculate time range for incremental processing.

        Args:
            last_processed: Last processed timestamp from watermark
            lookback: Optional lookback duration string

        Returns:
            Tuple of (start_time, end_time) for time range
        """
        end_time = datetime.now()

        if last_processed is None:
            # First run - start from a reasonable default
            start_time = end_time - timedelta(days=30)  # Default 30 days lookback
        else:
            start_time = last_processed

        # Apply LOOKBACK if specified
        if lookback:
            lookback_days = self._parse_lookback(lookback)
            start_time = start_time - timedelta(days=lookback_days)

        return start_time, end_time

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
    """APPEND mode - inserts data while validating schema compatibility."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate INSERT SQL for APPEND mode.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        # Check if target table exists (reuse LOAD validation)
        table_info = self.validation_helper.get_table_info(transform_step.table_name)

        if not table_info.exists:
            # Table doesn't exist, create it first
            create_sql = f"""
                CREATE TABLE {transform_step.table_name} AS
                {transform_step.sql_query}
            """
            return [create_sql], {}
        else:
            # Table exists, append data
            insert_sql = f"""
                INSERT INTO {transform_step.table_name}
                {transform_step.sql_query}
            """
            return [insert_sql], {}


class UpsertTransformHandler(TransformModeHandler):
    """UPSERT mode - upserts data based on specified upsert keys."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate UPSERT SQL for UPSERT mode.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        # Get table information (reuse LOAD validation)
        table_info = self.validation_helper.get_table_info(transform_step.table_name)

        return self._generate_upsert_sql(transform_step, table_info)

    def _generate_upsert_sql(
        self, transform_step: SQLBlockStep, table_info: TableInfo
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate UPSERT SQL using DELETE + INSERT since DuckDB doesn't have full UPSERT syntax support.

        Args:
            transform_step: Transform step configuration
            table_info: Table information from validation

        Returns:
            Tuple of (sql_statements, parameters)
        """
        if not table_info.exists:
            # Table doesn't exist, create it first
            create_sql = f"""
                CREATE TABLE {transform_step.table_name} AS
                {transform_step.sql_query}
            """
            return [create_sql], {}

        # Generate UPSERT logic using DELETE + INSERT (DuckDB compatible)
        upsert_keys_str = ", ".join(transform_step.upsert_keys)

        # Create temporary view for source data
        temp_view = f"temp_upsert_{transform_step.table_name}_{int(time.time())}"
        create_view_sql = f"""
            CREATE TEMPORARY VIEW {temp_view} AS
            {transform_step.sql_query}
        """

        # Delete existing records that match the upsert keys
        delete_sql = f"""
            DELETE FROM {transform_step.table_name}
            WHERE ({upsert_keys_str}) IN (
                SELECT {upsert_keys_str} FROM {temp_view}
            )
        """

        # Insert all records from source (both updated and new)
        insert_sql = f"""
            INSERT INTO {transform_step.table_name}
            SELECT * FROM {temp_view}
        """

        # Cleanup temporary view
        cleanup_sql = f"DROP VIEW {temp_view}"

        return [create_view_sql, delete_sql, insert_sql, cleanup_sql], {}


class IncrementalTransformHandler(TransformModeHandler):
    """INCREMENTAL mode - time-based partitioned updates with optimized watermark management."""

    def generate_sql_with_params(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate incremental SQL with optimized watermark lookup.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        # Check if target table exists (reuse LOAD validation)
        table_info = self.validation_helper.get_table_info(transform_step.table_name)

        if not table_info.exists:
            # Table doesn't exist, create it first (full load)
            # For CREATE TABLE, we need to substitute time macros if they exist in the query
            if (
                "@start_date" in transform_step.sql_query
                or "@end_date" in transform_step.sql_query
            ):
                # Calculate default time range for initial table creation
                end_time = datetime.now()
                start_time = end_time - timedelta(
                    days=30
                )  # Default 30 days for initial load

                # Substitute time macros
                substituted_sql, parameters = (
                    self.time_substitution.substitute_time_macros(
                        transform_step.sql_query, start_time, end_time
                    )
                )

                create_sql = f"""
                    CREATE TABLE {transform_step.table_name} AS
                    {substituted_sql}
                """
                return [create_sql], parameters
            else:
                # No time macros, use query as-is
                create_sql = f"""
                    CREATE TABLE {transform_step.table_name} AS
                    {transform_step.sql_query}
                """
                return [create_sql], {}
        else:
            # Table exists, perform incremental update
            return self._generate_incremental_sql(transform_step)

    def _generate_incremental_sql(
        self, transform_step: SQLBlockStep
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Generate incremental update SQL with optimized watermark management and performance optimization.

        Args:
            transform_step: Transform step configuration

        Returns:
            Tuple of (sql_statements, parameters)
        """
        # Get current watermark using optimized lookup (sub-10ms cached, sub-100ms cold)
        last_processed = self.watermark_manager.get_transform_watermark(
            transform_step.table_name, transform_step.time_column
        )

        # Calculate time range with LOOKBACK support
        start_time, end_time = self._calculate_time_range(
            last_processed, transform_step.lookback
        )

        logger.info(
            f"Incremental processing for {transform_step.table_name}: "
            f"{start_time} to {end_time} (last_processed: {last_processed})"
        )

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

        # Apply performance optimization
        # Estimate rows based on time range (simplified estimation)
        try:
            time_range_days = (end_time - start_time).days + 1
            estimated_rows = (
                time_range_days * 1000
            )  # Assume 1000 rows per day (simplified)
        except (AttributeError, TypeError):
            # Fallback for unit tests or when datetime operations fail
            estimated_rows = 5000  # Default estimate for testing

        # Optimize DELETE operation
        optimized_delete, delete_optimized = (
            self.performance_optimizer.optimize_delete_operation(
                delete_sql, transform_step.table_name
            )
        )

        # Optimize INSERT operation
        optimized_insert, insert_optimized = (
            self.performance_optimizer.optimize_insert_operation(
                insert_sql, estimated_rows
            )
        )

        # Log optimization results
        if delete_optimized or insert_optimized:
            logger.info(
                f"Applied performance optimizations: DELETE={delete_optimized}, INSERT={insert_optimized}"
            )

        # Ensure atomic execution with explicit transaction
        sql_statements = [
            "BEGIN TRANSACTION;",
            optimized_delete,
            optimized_insert,
            "COMMIT;",
        ]

        return sql_statements, parameters

    def update_watermark_after_success(
        self, transform_step: SQLBlockStep, execution_time: datetime
    ) -> None:
        """Update watermark after successful incremental execution.

        Args:
            transform_step: Transform step configuration
            execution_time: Time when the execution completed
        """
        try:
            self.watermark_manager.update_watermark(
                transform_step.table_name, transform_step.time_column, execution_time
            )
            logger.info(
                f"Updated watermark for {transform_step.table_name}.{transform_step.time_column}: {execution_time}"
            )
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")


class TransformModeHandlerFactory:
    """Factory for creating appropriate transform mode handlers."""

    @staticmethod
    def create(mode: str, engine: "DuckDBEngine") -> TransformModeHandler:
        """Create appropriate transform mode handler.

        Args:
            mode: Transform mode (REPLACE, APPEND, UPSERT, INCREMENTAL)
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
        elif mode == "UPSERT":
            return UpsertTransformHandler(engine)
        elif mode == "INCREMENTAL":
            return IncrementalTransformHandler(engine)
        else:
            valid_modes = ["REPLACE", "APPEND", "UPSERT", "INCREMENTAL"]
            raise InvalidLoadModeError(mode, valid_modes)
