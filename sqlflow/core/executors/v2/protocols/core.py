"""Core protocols for SQLFlow V2 clean architecture.

Following the Zen of Python and Raymond Hettinger's guidance:
- "Simple is better than complex"
- "Explicit is better than implicit"
- "Beautiful is better than ugly"
- "Flat is better than nested"

These protocols define the essential contracts using structural typing
with typing.Protocol - the most Pythonic way to define interfaces.

Design Philosophy:
- Protocols over abstract base classes (more Pythonic)
- Composition over inheritance (following Strategy pattern)
- Immutable data structures where possible
- Clear separation of concerns
"""

from typing import Any, Dict, Iterator, List, Optional, Protocol


# Core Data Protocols
class Step(Protocol):
    """Protocol for all step definitions.

    Using structural typing - any object with these attributes
    can be treated as a Step.
    """

    @property
    def id(self) -> str: ...  # noqa

    @property
    def step_type(self) -> str: ...  # noqa


class StepResult(Protocol):
    """Protocol for step execution results."""

    @property
    def step_id(self) -> str: ...  # noqa

    @property
    def success(self) -> bool: ...  # noqa

    @property
    def duration_ms(self) -> float: ...  # noqa: E704

    @property
    def rows_affected(self) -> int: ...  # noqa

    @property
    def error_message(self) -> Optional[str]: ...  # noqa


# Engine and Data Access Protocols
class DatabaseEngine(Protocol):
    """Protocol for database engines.

    Clean interface following the Interface Segregation Principle.
    """

    def execute_query(self, sql: str) -> Any:
        """Execute SQL query and return result."""
        ...

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        ...

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get table schema as column -> type mapping."""
        ...


class DataConnector(Protocol):
    """Protocol for data connectors."""

    def read(self) -> Iterator[Any]:
        """Read data from source."""
        ...

    def write(self, data: Any, destination: str) -> int:
        """Write data to destination, return rows written."""
        ...


# Execution Context Protocol
class ExecutionContext(Protocol):
    """Protocol for execution context.

    Immutable context following the Parameter Object pattern.
    Carries all dependencies needed for execution.
    """

    @property
    def engine(self) -> Optional[DatabaseEngine]:
        """Database engine instance (optional for testing)."""
        ...

    @property
    def variables(self) -> Dict[str, Any]:
        """Execution variables (immutable view)."""
        ...

    @property
    def observability(self) -> Optional["ObservabilityManager"]:
        """Observability manager (optional)."""
        ...

    @property
    def source_definitions(self) -> Dict[str, Any]:
        """Source definitions (immutable view)."""
        ...

    def add_source_definition(self, name: str, definition: Dict[str, Any]) -> None:
        """Add source definition - controlled mutation."""
        ...


# Observability Protocol
class ObservabilityManager(Protocol):
    """Protocol for observability and monitoring.

    Simple interface for performance tracking and metrics.
    """

    def start_step(self, step_id: str) -> None:
        """Start monitoring a step."""
        ...

    def end_step(
        self, step_id: str, success: bool, error: Optional[str] = None
    ) -> None:
        """End monitoring a step."""
        ...

    def record_rows_affected(self, step_id: str, rows: int) -> None:
        """Record number of rows affected by a step."""
        ...

    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics."""
        ...


# Execution Protocols
class StepExecutor(Protocol):
    """Protocol for step executors.

    Following the Strategy Pattern - each executor implements
    a specific algorithm for handling a step type.
    """

    def can_execute(self, step: Step) -> bool:
        """Check if this executor can handle the step."""
        ...

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute the step with given context."""
        ...


class ExecutionStrategy(Protocol):
    """Protocol for execution strategies.

    Different strategies for executing multiple steps:
    - Sequential: one after another
    - Parallel: concurrent execution where safe
    - Streaming: continuous processing
    """

    def execute(self, steps: List[Step], context: ExecutionContext) -> List[StepResult]:
        """Execute steps using this strategy."""
        ...


# Utility Protocols
class VariableSubstitution(Protocol):
    """Protocol for variable substitution in SQL and configs."""

    def substitute(self, text: str, variables: Dict[str, Any]) -> str:
        """Substitute variables in text using {{variable}} syntax."""
        ...


class StepRegistry(Protocol):
    """Protocol for step executor registry.

    Type-safe registry for finding the right executor for each step.
    """

    def register(self, executor: StepExecutor) -> None:
        """Register a step executor."""
        ...

    def find_executor(self, step: Step) -> StepExecutor:
        """Find executor for the given step."""
        ...
