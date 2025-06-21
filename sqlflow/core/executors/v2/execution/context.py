"""Immutable execution context management.

Following functional programming principles:
- Immutable data structures
- Clear separation of concerns
- Type safety with protocols
- Context manager for resource lifecycle
"""

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ..protocols import DatabaseEngine


@dataclass(frozen=True)
class ExecutionContext:
    """Immutable execution context.

    This is the core context that flows through the entire pipeline execution.
    Being immutable ensures thread safety and prevents accidental state mutations.
    """

    # Core identifiers
    execution_id: str
    started_at: float

    # Configuration
    profile: Dict[str, Any] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)

    # Resources (these are references, the objects themselves may be mutable)
    session: Any = None
    engine: Optional[DatabaseEngine] = None

    # Execution state
    source_definitions: Dict[str, Any] = field(default_factory=dict)

    def with_variables(self, new_variables: Dict[str, Any]) -> "ExecutionContext":
        """Create new context with updated variables."""
        merged_variables = {**self.variables, **new_variables}
        return ExecutionContext(
            execution_id=self.execution_id,
            started_at=self.started_at,
            profile=self.profile,
            variables=merged_variables,
            session=self.session,
            engine=self.engine,
            source_definitions=self.source_definitions,
        )

    def with_source_definition(
        self, name: str, definition: Dict[str, Any]
    ) -> "ExecutionContext":
        """Create new context with additional source definition."""
        new_definitions = {**self.source_definitions, name: definition}
        return ExecutionContext(
            execution_id=self.execution_id,
            started_at=self.started_at,
            profile=self.profile,
            variables=self.variables,
            session=self.session,
            engine=self.engine,
            source_definitions=new_definitions,
        )

    def with_engine(self, engine: DatabaseEngine) -> "ExecutionContext":
        """Create new context with database engine."""
        return ExecutionContext(
            execution_id=self.execution_id,
            started_at=self.started_at,
            profile=self.profile,
            variables=self.variables,
            session=self.session,
            engine=engine,
            source_definitions=self.source_definitions,
        )

    @property
    def execution_time(self) -> float:
        """Current execution time in seconds."""
        return time.time() - self.started_at


class ExecutionContextFactory:
    """Factory for creating execution contexts.

    Follows the factory pattern for clean object creation.
    """

    @staticmethod
    def create(
        profile: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        execution_id: Optional[str] = None,
    ) -> ExecutionContext:
        """Create new execution context.

        Args:
            profile: Profile configuration
            variables: Execution variables
            execution_id: Unique execution identifier

        Returns:
            New immutable execution context
        """
        return ExecutionContext(
            execution_id=execution_id or str(uuid.uuid4()),
            started_at=time.time(),
            profile=profile or {},
            variables=variables or {},
        )

    @staticmethod
    def from_legacy_orchestrator(legacy_orchestrator: Any) -> ExecutionContext:
        """Create context from legacy orchestrator for compatibility.

        Args:
            legacy_orchestrator: V1/legacy orchestrator instance

        Returns:
            New execution context with legacy state
        """
        # Extract state from legacy orchestrator
        profile = getattr(legacy_orchestrator, "_profile", {})
        variables = getattr(legacy_orchestrator, "_variables", {})
        engine = getattr(legacy_orchestrator, "_engine", None)
        source_definitions = getattr(legacy_orchestrator, "source_definitions", {})

        context = ExecutionContextFactory.create(profile, variables)

        if engine:
            context = context.with_engine(engine)

        if source_definitions:
            new_context = context
            for name, definition in source_definitions.items():
                new_context = new_context.with_source_definition(name, definition)
            context = new_context

        return context


@contextmanager
def execution_session(
    profile: Dict[str, Any],
    variables: Optional[Dict[str, Any]] = None,
    engine_factory=None,
):
    """Context manager for pipeline execution session.

    This provides proper resource management for database connections
    and other resources used during pipeline execution.

    Args:
        profile: Profile configuration
        variables: Execution variables
        engine_factory: Factory function for creating database engine

    Yields:
        ExecutionContext: Configured execution context

    Example:
        >>> with execution_session(profile, variables) as context:
        ...     result = execute_pipeline(steps, context)
    """
    context = ExecutionContextFactory.create(profile, variables)

    # Initialize database engine if factory provided
    engine = None
    if engine_factory and profile:
        try:
            engine_config = profile.get("engine", {})
            engine = engine_factory(**engine_config)
            context = context.with_engine(engine)
        except Exception as e:
            # Log error but continue - some operations may not need database
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to initialize database engine: {e}")

    try:
        yield context
    finally:
        # Cleanup resources
        if hasattr(engine, "close"):
            try:
                engine.close()
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(f"Error closing database engine: {e}")


@dataclass(frozen=True)
class StepExecutionResult:
    """Immutable step execution result."""

    step_id: str
    status: str  # "success", "error", "skipped"
    message: Optional[str] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    data: Optional[Dict[str, Any]] = None

    @classmethod
    def success(
        cls,
        step_id: str,
        message: str = None,
        execution_time: float = None,
        data: Dict[str, Any] = None,
    ) -> "StepExecutionResult":
        """Create successful result."""
        return cls(
            step_id=step_id,
            status="success",
            message=message,
            execution_time=execution_time,
            data=data,
        )

    @classmethod
    def with_error(
        cls, step_id: str, error_msg: str, execution_time: float = None
    ) -> "StepExecutionResult":
        """Create error result."""
        return cls(
            step_id=step_id,
            status="error",
            error=error_msg,
            execution_time=execution_time,
        )

    @classmethod
    def skipped(cls, step_id: str, reason: str = None) -> "StepExecutionResult":
        """Create skipped result."""
        return cls(step_id=step_id, status="skipped", message=reason)
