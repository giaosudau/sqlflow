"""Pythonic Error Handling for V2 Executor.

Following Raymond Hettinger's recommendations:
- Use context managers for resource management
- Proper exception hierarchies
- Clean error propagation
- Automatic observability integration

Design principles:
- "Errors should never pass silently"
- "Unless explicitly silenced"
- Context managers for automatic cleanup
"""

import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional, Type

from sqlflow.logging import get_logger

logger = get_logger(__name__)


# Exception hierarchy for clear error handling
class SQLFlowError(Exception):
    """Base exception for all SQLFlow errors."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__.upper()
        self.context = context or {}
        self.timestamp = datetime.utcnow()


class StepExecutionError(SQLFlowError):
    """Error during step execution."""

    def __init__(self, step_id: str, step_type: str, message: str, **kwargs):
        super().__init__(message, **kwargs)
        self.step_id = step_id
        self.step_type = step_type


class ConnectorError(SQLFlowError):
    """Error in connector operations."""


class DatabaseError(SQLFlowError):
    """Error in database operations."""


class VariableSubstitutionError(SQLFlowError):
    """Error in variable substitution."""


def _safe_record_observability(observability_manager, action_name: str, action_func):
    """Safely execute observability recording with error handling."""
    if not observability_manager:
        return

    try:
        action_func()
    except Exception as e:
        logger.warning(f"Observability {action_name} recording failed: {e}")


def _create_execution_context(
    step_id: str, step_type: str, start_time: float
) -> Dict[str, Any]:
    """Create the initial execution context dictionary."""
    return {
        "step_id": step_id,
        "step_type": step_type,
        "start_time": start_time,
        "performance_metrics": {},
    }


def _log_step_start(step_id: str, step_type: str, auto_log: bool) -> None:
    """Log step start if auto_log is enabled."""
    if auto_log:
        logger.info(f"🔄 Starting {step_type} step: {step_id}")


def _handle_step_success(
    execution_context: Dict[str, Any], observability_manager, auto_log: bool
) -> None:
    """Handle successful step completion."""
    step_id = execution_context["step_id"]
    step_type = execution_context["step_type"]
    duration_ms = (time.perf_counter() - execution_context["start_time"]) * 1000
    execution_context["duration_ms"] = duration_ms

    if auto_log:
        logger.info(f"✅ Completed {step_type} step: {step_id} ({duration_ms:.1f}ms)")

    # Record success in observability
    _safe_record_observability(
        observability_manager,
        "success",
        lambda: observability_manager.record_step_success(
            step_id,
            {
                "duration_ms": duration_ms,
                "step_type": step_type,
                **execution_context.get("performance_metrics", {}),
            },
        ),
    )


def _handle_step_failure(
    execution_context: Dict[str, Any],
    exception: Exception,
    observability_manager,
    auto_log: bool,
) -> SQLFlowError:
    """Handle step execution failure and return appropriate SQLFlow error."""
    step_id = execution_context["step_id"]
    step_type = execution_context["step_type"]
    duration_ms = (time.perf_counter() - execution_context["start_time"]) * 1000

    # Convert to SQLFlow error if needed
    if isinstance(exception, SQLFlowError):
        sqlflow_error = exception
    else:
        sqlflow_error = StepExecutionError(
            step_id=step_id,
            step_type=step_type,
            message=f"Step execution failed: {str(exception)}",
            context={"original_error": str(exception), "duration_ms": duration_ms},
        )

    if auto_log:
        logger.error(
            f"❌ Failed {step_type} step: {step_id} ({duration_ms:.1f}ms) - {sqlflow_error.message}"
        )

    # Record failure in observability
    _safe_record_observability(
        observability_manager,
        "failure",
        lambda: observability_manager.record_step_failure(
            step_id, step_type, str(sqlflow_error), duration_ms
        ),
    )

    return sqlflow_error


@contextmanager
def step_execution_context(
    step_id: str, step_type: str, observability_manager=None, auto_log: bool = True
):
    """Context manager for step execution with automatic error handling.

    Args:
        step_id: ID of the step being executed
        step_type: Type of the step
        observability_manager: Optional observability manager
        auto_log: Whether to automatically log start/success/failure

    Yields:
        Dictionary containing execution context

    Example:
        with step_execution_context("load_1", "load", observability) as ctx:
            # Execute step logic
            result = perform_load_operation()
            ctx["rows_processed"] = result.row_count
    """
    start_time = time.perf_counter()
    execution_context = _create_execution_context(step_id, step_type, start_time)

    _log_step_start(step_id, step_type, auto_log)

    # Record step start in observability
    _safe_record_observability(
        observability_manager,
        "start",
        lambda: observability_manager.record_step_start(step_id, step_type),
    )

    try:
        yield execution_context
        _handle_step_success(execution_context, observability_manager, auto_log)
    except Exception as e:
        sqlflow_error = _handle_step_failure(
            execution_context, e, observability_manager, auto_log
        )
        raise sqlflow_error


@contextmanager
def database_operation_context(
    operation_name: str,
    sql_engine,
    auto_commit: bool = True,
    auto_rollback: bool = True,
):
    """Context manager for database operations with automatic transaction management.

    Args:
        operation_name: Name of the database operation
        sql_engine: SQL engine instance
        auto_commit: Whether to automatically commit on success
        auto_rollback: Whether to automatically rollback on failure

    Example:
        with database_operation_context("bulk_insert", engine) as ctx:
            engine.execute_query("INSERT INTO table VALUES ...")
            ctx["rows_affected"] = 1000
    """
    start_time = time.perf_counter()
    operation_context = {"operation": operation_name, "start_time": start_time}

    logger.debug(f"🔧 Starting database operation: {operation_name}")

    try:
        yield operation_context

        # Success path
        duration_ms = (time.perf_counter() - start_time) * 1000

        if auto_commit and hasattr(sql_engine, "commit"):
            try:
                sql_engine.commit()
                logger.debug(f"✅ Committed {operation_name} ({duration_ms:.1f}ms)")
            except Exception as e:
                logger.warning(f"Commit failed for {operation_name}: {e}")

    except Exception as e:
        # Error path
        duration_ms = (time.perf_counter() - start_time) * 1000

        if auto_rollback and hasattr(sql_engine, "rollback"):
            try:
                sql_engine.rollback()
                logger.debug(f"🔄 Rolled back {operation_name} after error")
            except Exception as rollback_error:
                logger.error(f"Rollback failed for {operation_name}: {rollback_error}")

        # Convert to appropriate SQLFlow error
        if not isinstance(e, SQLFlowError):
            e = DatabaseError(
                message=f"Database operation '{operation_name}' failed: {str(e)}",
                context={"operation": operation_name, "duration_ms": duration_ms},
            )

        logger.error(
            f"❌ Database operation failed: {operation_name} ({duration_ms:.1f}ms) - {e.message}"
        )
        raise


@contextmanager
def connector_operation_context(
    connector_type: str, operation: str, source_name: Optional[str] = None
):
    """Context manager for connector operations.

    Args:
        connector_type: Type of connector (csv, postgres, etc.)
        operation: Operation being performed (read, write, test)
        source_name: Optional source name for logging

    Example:
        with connector_operation_context("csv", "read", "customers.csv") as ctx:
            data = connector.read()
            ctx["rows_read"] = len(data)
    """
    start_time = time.perf_counter()
    context_name = f"{connector_type}:{operation}"
    if source_name:
        context_name += f":{source_name}"

    operation_context = {
        "connector_type": connector_type,
        "operation": operation,
        "source_name": source_name,
        "start_time": start_time,
    }

    logger.debug(f"🔌 Starting connector operation: {context_name}")

    try:
        yield operation_context

        # Success path
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.debug(
            f"✅ Connector operation completed: {context_name} ({duration_ms:.1f}ms)"
        )

    except Exception as e:
        # Error path
        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to appropriate SQLFlow error
        if not isinstance(e, SQLFlowError):
            e = ConnectorError(
                message=f"Connector operation '{context_name}' failed: {str(e)}",
                context={
                    "connector_type": connector_type,
                    "operation": operation,
                    "source_name": source_name,
                    "duration_ms": duration_ms,
                },
            )

        logger.error(
            f"❌ Connector operation failed: {context_name} ({duration_ms:.1f}ms) - {e.message}"
        )
        raise


def handle_specific_errors(
    error_mappings: Dict[Type[Exception], Type[SQLFlowError]],
    default_error: Type[SQLFlowError] = SQLFlowError,
):
    """Decorator for mapping specific exceptions to SQLFlow errors.

    Args:
        error_mappings: Dictionary mapping exception types to SQLFlow error types
        default_error: Default SQLFlow error type for unmapped exceptions

    Example:
        @handle_specific_errors({
            psycopg2.Error: DatabaseError,
            FileNotFoundError: ConnectorError
        })
        def risky_operation():
            # Function that might raise various exceptions
            pass
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Find matching error type
                for source_error, target_error in error_mappings.items():
                    if isinstance(e, source_error):
                        raise target_error(
                            message=f"Operation failed: {str(e)}",
                            context={
                                "original_error": str(e),
                                "error_type": type(e).__name__,
                            },
                        ) from e

                # Default mapping
                if not isinstance(e, SQLFlowError):
                    raise default_error(
                        message=f"Unexpected error: {str(e)}",
                        context={
                            "original_error": str(e),
                            "error_type": type(e).__name__,
                        },
                    ) from e

                # Already a SQLFlow error, re-raise
                raise

        return wrapper

    return decorator


# Utility functions for error analysis
def extract_error_context(error: Exception) -> Dict[str, Any]:
    """Extract structured context from any exception."""
    context: Dict[str, Any] = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Add SQLFlow-specific context if available
    if isinstance(error, SQLFlowError):
        context["error_code"] = error.error_code
        context["sqlflow_context"] = error.context

        # Check for step-specific attributes
        if isinstance(error, StepExecutionError):
            context["step_id"] = error.step_id
            context["step_type"] = error.step_type

    return context


def is_retriable_error(error: Exception) -> bool:
    """Determine if an error is potentially retriable."""
    # Network-related errors are often retriable
    retriable_types = (
        ConnectionError,
        TimeoutError,
        # Add database-specific retriable errors here
    )

    if isinstance(error, retriable_types):
        return True

    # Check error message for common retriable patterns
    error_message = str(error).lower()
    retriable_patterns = [
        "timeout",
        "connection reset",
        "temporary failure",
        "deadlock",
        "lock wait timeout",
    ]

    return any(pattern in error_message for pattern in retriable_patterns)


__all__ = [
    # Exception classes
    "SQLFlowError",
    "StepExecutionError",
    "ConnectorError",
    "DatabaseError",
    "VariableSubstitutionError",
    # Context managers
    "step_execution_context",
    "database_operation_context",
    "connector_operation_context",
    # Decorators and utilities
    "handle_specific_errors",
    "extract_error_context",
    "is_retriable_error",
]
