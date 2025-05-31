"""Query tracing and operation tracing for SQLFlow debugging.

This module provides detailed tracing capabilities for SQL queries and
connector operations, including explain plans and performance analysis.
"""

import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class QueryTracer:
    """Tracer for SQL query execution and performance analysis."""

    def __init__(self, engine: Optional[Any] = None, debug_mode: bool = False):
        """Initialize query tracer.

        Args:
            engine: Database engine for explain plans
            debug_mode: Whether to enable detailed tracing
        """
        self.engine = engine
        self.debug_mode = debug_mode
        self.query_history = []
        self.performance_stats = {}

    def set_debug_mode(self, enabled: bool) -> None:
        """Enable or disable debug mode.

        Args:
            enabled: Whether to enable debug mode
        """
        self.debug_mode = enabled
        logger.info(f"Query tracer debug mode {'enabled' if enabled else 'disabled'}")

    @contextmanager
    def trace_query(self, query: str, query_type: str = "unknown", **context):
        """Trace SQL query execution.

        Args:
            query: SQL query to trace
            query_type: Type of query (SELECT, INSERT, etc.)
            **context: Additional context information
        """
        query_id = f"query_{int(time.time() * 1000)}"
        start_time = time.time()

        # Get explain plan if possible
        explain_plan = self._get_explain_plan(query) if self.debug_mode else None

        trace_info = {
            "id": query_id,
            "query": query,
            "query_type": query_type,
            "start_time": start_time,
            "explain_plan": explain_plan,
            "context": context,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if self.debug_mode:
            logger.debug(
                f"Starting query trace: {query_type} (ID: {query_id})",
                extra={
                    "query_id": query_id,
                    "query": query[:200] + "..." if len(query) > 200 else query,
                    "query_type": query_type,
                    "explain_plan": explain_plan,
                    "context": context,
                },
            )

        try:
            yield query_id
        except Exception as e:
            trace_info["error"] = {"type": type(e).__name__, "message": str(e)}
            logger.error(
                f"Query failed: {query_type} (ID: {query_id})",
                extra={"query_id": query_id, "error": trace_info["error"]},
            )
            raise
        finally:
            # Calculate execution time
            duration = time.time() - start_time
            trace_info["duration"] = duration
            trace_info["end_time"] = time.time()

            # Add to history
            self.query_history.append(trace_info)

            # Update performance stats
            if query_type not in self.performance_stats:
                self.performance_stats[query_type] = {
                    "count": 0,
                    "total_duration": 0,
                    "avg_duration": 0,
                    "min_duration": float("inf"),
                    "max_duration": 0,
                    "error_count": 0,
                }

            stats = self.performance_stats[query_type]
            stats["count"] += 1
            stats["total_duration"] += duration
            stats["avg_duration"] = stats["total_duration"] / stats["count"]
            stats["min_duration"] = min(stats["min_duration"], duration)
            stats["max_duration"] = max(stats["max_duration"], duration)

            if "error" in trace_info:
                stats["error_count"] += 1

            if self.debug_mode:
                logger.debug(
                    f"Completed query trace: {query_type} (ID: {query_id}) in {duration:.3f}s",
                    extra={
                        "query_id": query_id,
                        "duration": duration,
                        "success": "error" not in trace_info,
                    },
                )

    def _get_explain_plan(self, query: str) -> Optional[Dict[str, Any]]:
        """Get explain plan for a query.

        Args:
            query: SQL query to explain

        Returns:
            Explain plan information or None if not available
        """
        if not self.engine or not hasattr(self.engine, "execute_query"):
            return None

        try:
            # Try to get explain plan
            explain_query = f"EXPLAIN {query}"
            result = self.engine.execute_query(explain_query)

            if result:
                rows = result.fetchall()
                if rows:
                    return {
                        "plan": [str(row[0]) for row in rows],
                        "query": explain_query,
                    }
        except Exception as e:
            logger.debug(f"Failed to get explain plan: {e}")

        return None

    def get_query_history(
        self, limit: Optional[int] = None, query_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get query execution history.

        Args:
            limit: Maximum number of queries to return
            query_type: Filter by query type

        Returns:
            List of query trace information
        """
        history = self.query_history

        if query_type:
            history = [q for q in history if q.get("query_type") == query_type]

        if limit:
            history = history[-limit:]

        return history

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get query performance statistics.

        Returns:
            Performance statistics by query type
        """
        return self.performance_stats.copy()

    def clear_history(self) -> None:
        """Clear query history and reset stats."""
        self.query_history.clear()
        self.performance_stats.clear()
        if self.debug_mode:
            logger.debug("Query history and performance stats cleared")


class OperationTracer:
    """Tracer for connector and pipeline operations."""

    def __init__(self, debug_mode: bool = False):
        """Initialize operation tracer.

        Args:
            debug_mode: Whether to enable detailed tracing
        """
        self.debug_mode = debug_mode
        self.operation_history = []
        self.active_operations = {}

    def set_debug_mode(self, enabled: bool) -> None:
        """Enable or disable debug mode.

        Args:
            enabled: Whether to enable debug mode
        """
        self.debug_mode = enabled
        logger.info(
            f"Operation tracer debug mode {'enabled' if enabled else 'disabled'}"
        )

    @contextmanager
    def trace_operation(self, operation_type: str, operation_name: str, **context):
        """Trace a connector or pipeline operation.

        Args:
            operation_type: Type of operation (load, transform, export, etc.)
            operation_name: Name of the operation
            **context: Additional context information
        """
        operation_id = f"{operation_type}_{int(time.time() * 1000)}"
        start_time = time.time()

        trace_info = {
            "id": operation_id,
            "type": operation_type,
            "name": operation_name,
            "start_time": start_time,
            "context": context,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Add to active operations
        self.active_operations[operation_id] = trace_info

        if self.debug_mode:
            logger.debug(
                f"Starting operation trace: {operation_type} - {operation_name} (ID: {operation_id})",
                extra={
                    "operation_id": operation_id,
                    "operation_type": operation_type,
                    "operation_name": operation_name,
                    "context": context,
                },
            )

        try:
            yield operation_id
        except Exception as e:
            trace_info["error"] = {"type": type(e).__name__, "message": str(e)}
            logger.error(
                f"Operation failed: {operation_type} - {operation_name} (ID: {operation_id})",
                extra={"operation_id": operation_id, "error": trace_info["error"]},
            )
            raise
        finally:
            # Calculate execution time
            duration = time.time() - start_time
            trace_info["duration"] = duration
            trace_info["end_time"] = time.time()

            # Remove from active operations
            self.active_operations.pop(operation_id, None)

            # Add to history
            self.operation_history.append(trace_info)

            if self.debug_mode:
                logger.debug(
                    f"Completed operation trace: {operation_type} - {operation_name} (ID: {operation_id}) in {duration:.3f}s",
                    extra={
                        "operation_id": operation_id,
                        "duration": duration,
                        "success": "error" not in trace_info,
                    },
                )

    def trace_data_flow(
        self, source: str, target: str, rows_processed: int, **metrics
    ) -> None:
        """Trace data flow between source and target.

        Args:
            source: Source name
            target: Target name
            rows_processed: Number of rows processed
            **metrics: Additional data flow metrics
        """
        flow_info = {
            "source": source,
            "target": target,
            "rows_processed": rows_processed,
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if self.debug_mode:
            logger.debug(
                f"Data flow: {source} -> {target} ({rows_processed} rows)",
                extra=flow_info,
            )

    def get_active_operations(self) -> Dict[str, Dict[str, Any]]:
        """Get currently active operations.

        Returns:
            Dictionary of active operations
        """
        return self.active_operations.copy()

    def get_operation_history(
        self, limit: Optional[int] = None, operation_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get operation execution history.

        Args:
            limit: Maximum number of operations to return
            operation_type: Filter by operation type

        Returns:
            List of operation trace information
        """
        history = self.operation_history

        if operation_type:
            history = [op for op in history if op.get("type") == operation_type]

        if limit:
            history = history[-limit:]

        return history

    def clear_history(self) -> None:
        """Clear operation history."""
        self.operation_history.clear()
        if self.debug_mode:
            logger.debug("Operation history cleared")
