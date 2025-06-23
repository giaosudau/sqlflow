"""Execution context using simple Python patterns.

Following Raymond Hettinger's guidance:
- Simple dataclass instead of complex builders
- Real immutability using frozen=True
- Factory functions instead of builder patterns
- "Simple is better than complex"
"""

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from sqlflow.connectors.registry import EnhancedConnectorRegistry
from sqlflow.core.executors.v2.execution.engines import create_engine_adapter
from sqlflow.project import Project


@dataclass(frozen=True)
class ExecutionContext:
    """Simple, truly immutable execution context.

    Following the Parameter Object pattern correctly -
    this object is actually immutable and carries only what's needed.
    """

    # Core identifiers
    execution_id: str
    started_at: float

    # Required dependencies
    engine: Any

    # Optional dependencies with sensible defaults
    variables: Dict[str, Any] = field(default_factory=dict)
    observability: Optional[Any] = None
    project: Optional[Project] = None
    connector_registry: Optional[EnhancedConnectorRegistry] = None
    engine_adapter: Optional[Any] = None
    artifact_manager: Optional[Any] = None
    state_backend: Optional[Any] = None

    # For source definitions - separate concern
    _source_definitions: Dict[str, Any] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self):
        """Initialize missing components with sensible defaults."""
        # Only create what's actually missing - no automatic magic
        if self.connector_registry is None:
            registry = _create_default_registry()
            object.__setattr__(self, "connector_registry", registry)

        if self.engine_adapter is None and self.engine is not None:
            adapter = create_engine_adapter("duckdb")
            object.__setattr__(self, "engine_adapter", adapter)

    @property
    def source_definitions(self) -> Dict[str, Any]:
        """Access to source definitions - read-only view."""
        return self._source_definitions.copy()

    def add_source_definition(self, name: str, definition: Dict[str, Any]) -> None:
        """Add source definition - controlled mutation."""
        self._source_definitions[name] = definition

    @property
    def execution_time(self) -> float:
        """Current execution time in seconds."""
        return time.time() - self.started_at


# Simple factory functions - the Pythonic way
def create_execution_context(
    engine: Any = None,
    variables: Optional[Dict[str, Any]] = None,
    observability: Optional[Any] = None,
    project: Optional[Project] = None,
    execution_id: Optional[str] = None,
    artifact_manager: Optional[Any] = None,
    state_backend: Optional[Any] = None,
) -> ExecutionContext:
    """Create execution context with sensible defaults.

    This is the Pythonic way - simple function with keyword arguments.
    No builder pattern needed!
    """
    return ExecutionContext(
        execution_id=execution_id or str(uuid.uuid4()),
        started_at=time.time(),
        engine=engine,
        variables=variables or {},
        observability=observability,
        project=project,
        artifact_manager=artifact_manager,
        state_backend=state_backend,
    )


def create_test_context(
    engine: Any, variables: Optional[Dict[str, Any]] = None, **kwargs
) -> ExecutionContext:
    """Create test context - optimized for testing."""
    return ExecutionContext(
        execution_id="test-" + str(uuid.uuid4())[:8],
        started_at=time.time(),
        engine=engine,
        variables=variables or {},
        observability=None,  # Tests usually don't need observability
        **kwargs,
    )


def _create_default_registry() -> EnhancedConnectorRegistry:
    """Create default connector registry."""
    registry = EnhancedConnectorRegistry()

    # Register standard connectors - simple and explicit
    _register_standard_connectors(registry)
    return registry


def _register_standard_connectors(registry: EnhancedConnectorRegistry) -> None:
    """Register standard connectors - functional approach."""
    connectors_to_register = [
        ("csv", "sqlflow.connectors.csv", "CSVSource", "CSVDestination"),
        (
            "parquet",
            "sqlflow.connectors.parquet",
            "ParquetSource",
            "ParquetDestination",
        ),
    ]

    for conn_type, module_name, source_class, dest_class in connectors_to_register:
        try:
            module = __import__(module_name, fromlist=[source_class, dest_class])
            source = getattr(module, source_class)
            destination = getattr(module, dest_class)

            registry.register_source(
                conn_type, source, description=f"{conn_type.upper()} file connector"
            )
            registry.register_destination(
                conn_type,
                destination,
                description=f"{conn_type.upper()} file connector",
            )
        except ImportError:
            # Silently skip unavailable connectors
            pass


# Context manipulation functions - functional style
def with_variables(
    context: ExecutionContext, new_variables: Dict[str, Any]
) -> ExecutionContext:
    """Create new context with updated variables - functional approach."""
    merged_variables = {**context.variables, **new_variables}

    # Create new context with updated variables
    new_context = ExecutionContext(
        execution_id=context.execution_id,
        started_at=context.started_at,
        engine=context.engine,
        variables=merged_variables,
        observability=context.observability,
        project=context.project,
        connector_registry=context.connector_registry,
        engine_adapter=context.engine_adapter,
        artifact_manager=context.artifact_manager,
        state_backend=context.state_backend,
    )

    # Copy source definitions
    for name, definition in context.source_definitions.items():
        new_context.add_source_definition(name, definition)

    return new_context


def with_engine(context: ExecutionContext, engine: Any) -> ExecutionContext:
    """Create new context with different engine."""
    new_context = ExecutionContext(
        execution_id=context.execution_id,
        started_at=context.started_at,
        engine=engine,
        variables=context.variables,
        observability=context.observability,
        project=context.project,
        connector_registry=context.connector_registry,
        engine_adapter=context.engine_adapter,
        artifact_manager=context.artifact_manager,
        state_backend=context.state_backend,
    )

    # Copy source definitions
    for name, definition in context.source_definitions.items():
        new_context.add_source_definition(name, definition)

    return new_context
