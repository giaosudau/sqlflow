"""Execution context factory for V2 Orchestrator.

This module handles the creation of execution contexts that contain
all the dependencies needed for step execution.

Following Martin Fowler's Factory Pattern and Dependency Injection principles.
"""

from typing import Any, Dict, Optional

from sqlflow.logging import get_logger

from .database_session import DatabaseSessionManager

logger = get_logger(__name__)


class ExecutionContextFactory:
    """
    Execution context factory using established SQLFlow patterns.

    Integrates with existing architecture:
    - Uses existing DuckDBEngine implementation
    - Follows established state management patterns
    - Maintains compatibility with existing watermark management
    - Leverages existing connector registry

    Following Martin Fowler's Factory Pattern:
    Encapsulates complex object creation and dependency wiring.
    """

    @staticmethod
    def create_context(
        db_session: DatabaseSessionManager,
        observability: Any,  # ObservabilityManager
        variables: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Create execution context with advanced engine integration."""
        from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
        from sqlflow.core.executors.v2.context import ExecutionContext
        from sqlflow.core.variables.manager import VariableConfig, VariableManager

        # Get the advanced SQL engine with full capabilities
        sql_engine = db_session.get_sql_engine()

        # Create variable manager with provided variables
        if variables:
            variable_config = VariableConfig(cli_variables=variables)
            variable_manager = VariableManager(variable_config)
        else:
            variable_manager = VariableManager()

        # State management using existing patterns
        watermark_manager = ExecutionContextFactory._create_watermark_manager(
            sql_engine
        )

        # Create ExecutionContext following existing patterns
        if sql_engine is None:
            raise ValueError("SQL engine is required for execution context")

        return ExecutionContext(
            sql_engine=sql_engine,
            variable_manager=variable_manager,
            observability_manager=observability,
            connector_registry=enhanced_registry,
            watermark_manager=watermark_manager,
            run_id=observability.run_id,
            config=kwargs,
        )

    @staticmethod
    def _create_watermark_manager(sql_engine):
        """Create watermark manager with proper fallback handling.

        Following Kent Beck's simple design principle:
        Extract method to reduce complexity and improve testability.
        """
        from sqlflow.core.state.backends import DuckDBStateBackend
        from sqlflow.core.state.watermark_manager import WatermarkManager

        if sql_engine and sql_engine.connection:
            state_backend = DuckDBStateBackend(sql_engine.connection)
            watermark_manager = WatermarkManager(state_backend)
            logger.debug("Using standard watermark manager with DuckDB backend")
        else:
            # Fallback to in-memory state if no connection available
            state_backend = DuckDBStateBackend()  # Creates its own connection
            watermark_manager = WatermarkManager(state_backend)
            logger.debug("Using fallback watermark manager")

        return watermark_manager
