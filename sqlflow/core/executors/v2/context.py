"""Execution Context for V2 Executor.

This module provides the ExecutionContext class, which serves as an immutable
container for shared services required during pipeline execution. It acts as
a dependency injection mechanism following the Dependency Inversion Principle.
"""

import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlflow.connectors.registry.enhanced_registry import EnhancedConnectorRegistry
from sqlflow.core.engines.base import SQLEngine
from sqlflow.core.executors.v2.observability import ObservabilityManager
from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.core.variables.manager import VariableManager


@dataclass(frozen=True)
class ExecutionContext:
    """
    An immutable container for shared services required during pipeline execution.

    This object is created once per pipeline run and passed to each StepHandler,
    acting as a dependency injection mechanism that promotes loose coupling and
    high testability.

    Attributes:
        sql_engine: The SQL execution engine (e.g., DuckDB)
        connector_registry: Registry for creating source/destination connectors
        variable_manager: Manages variable substitution in queries and configs
        watermark_manager: Handles incremental loading state persistence
        observability_manager: Tracks performance metrics and generates insights
        run_id: Unique identifier for this execution run
        variables: Optional runtime variables for this execution
        config: Optional configuration overrides for this execution
    """

    sql_engine: SQLEngine
    connector_registry: EnhancedConnectorRegistry
    variable_manager: VariableManager
    watermark_manager: WatermarkManager
    observability_manager: ObservabilityManager
    run_id: str
    variables: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None

    @classmethod
    def create(
        cls,
        sql_engine: SQLEngine,
        connector_registry: EnhancedConnectorRegistry,
        variable_manager: VariableManager,
        watermark_manager: WatermarkManager,
        observability_manager: ObservabilityManager,
        variables: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> "ExecutionContext":
        """Create a new ExecutionContext with generated run_id if not provided."""
        if run_id is None:
            run_id = f"run_{uuid.uuid4().hex[:8]}"

        return cls(
            sql_engine=sql_engine,
            connector_registry=connector_registry,
            variable_manager=variable_manager,
            watermark_manager=watermark_manager,
            observability_manager=observability_manager,
            run_id=run_id,
            variables=variables or {},
            config=config or {},
        )

    def with_variables(self, variables: Dict[str, Any]) -> "ExecutionContext":
        """Create a new context with updated variables (immutable update)."""
        merged_variables = {**(self.variables or {}), **variables}
        return ExecutionContext(
            sql_engine=self.sql_engine,
            connector_registry=self.connector_registry,
            variable_manager=self.variable_manager,
            watermark_manager=self.watermark_manager,
            observability_manager=self.observability_manager,
            run_id=self.run_id,
            variables=merged_variables,
            config=self.config,
        )

    def with_config(self, config: Dict[str, Any]) -> "ExecutionContext":
        """Create a new context with updated config (immutable update)."""
        merged_config = {**(self.config or {}), **config}
        return ExecutionContext(
            sql_engine=self.sql_engine,
            connector_registry=self.connector_registry,
            variable_manager=self.variable_manager,
            watermark_manager=self.watermark_manager,
            observability_manager=self.observability_manager,
            run_id=self.run_id,
            variables=self.variables,
            config=merged_config,
        )
