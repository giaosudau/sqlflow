"""Core protocols for clean architecture.

Following Python protocol best practices:
- Define clear interfaces using typing.Protocol
- Enable structural subtyping (duck typing with type safety)
- Support dependency inversion principle
- Allow for easy testing with mock implementations
"""

from abc import abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Protocol

from ..validation import ValidationResult


class Step(Protocol):
    """Protocol for pipeline steps."""

    type: str
    id: str


class StepResult(Protocol):
    """Protocol for step execution results."""

    step_id: str
    status: str
    message: Optional[str] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None


class ExecutionContext(Protocol):
    """Protocol for execution context."""

    @property
    def session(self) -> Any:
        """Database session."""
        ...

    @property
    def variables(self) -> Dict[str, Any]:
        """Execution variables."""
        ...

    @property
    def profile(self) -> Dict[str, Any]:
        """Profile configuration."""
        ...


class DatabaseEngine(Protocol):
    """Protocol for database engines."""

    @abstractmethod
    def execute_query(self, sql: str) -> Any:
        """Execute SQL query and return results."""
        ...

    @abstractmethod
    def create_table_from_dataframe(self, df: Any, table_name: str, **kwargs) -> None:
        """Create table from pandas DataFrame."""
        ...

    @abstractmethod
    def read_table(self, table_name: str) -> Any:
        """Read table into DataFrame."""
        ...


class StepExecutor(Protocol):
    """Protocol for step executors."""

    @abstractmethod
    def execute(self, step: Dict[str, Any], context: ExecutionContext) -> StepResult:
        """Execute a pipeline step."""
        ...

    @abstractmethod
    def can_execute(self, step: Dict[str, Any]) -> bool:
        """Check if this executor can handle the step."""
        ...


class ConnectorProtocol(Protocol):
    """Protocol for data connectors."""

    @abstractmethod
    def read(self) -> Iterator[Any]:
        """Read data from source."""
        ...

    @abstractmethod
    def write(self, data: Iterator[Any]) -> int:
        """Write data to destination."""
        ...


class VariableSubstitutor(Protocol):
    """Protocol for variable substitution."""

    @abstractmethod
    def substitute(self, text: str, variables: Dict[str, Any]) -> str:
        """Substitute variables in text."""
        ...

    @abstractmethod
    def substitute_in_dict(
        self, data: Dict[str, Any], variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Substitute variables in dictionary."""
        ...


class Validator(Protocol):
    """Protocol for validation."""

    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """Validate data and return result."""
        ...


class ProfileManager(Protocol):
    """Protocol for profile management."""

    @abstractmethod
    def load_profile(self, name: str) -> Dict[str, Any]:
        """Load profile configuration."""
        ...

    @abstractmethod
    def get_engine_config(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract engine configuration from profile."""
        ...


class UDFRegistry(Protocol):
    """Protocol for UDF registry."""

    @abstractmethod
    def register_udf(self, name: str, func: Any) -> None:
        """Register a user-defined function."""
        ...

    @abstractmethod
    def get_udf(self, name: str) -> Optional[Any]:
        """Get registered UDF."""
        ...

    @abstractmethod
    def list_udfs(self) -> List[str]:
        """List all registered UDFs."""
        ...


class MetricsCollector(Protocol):
    """Protocol for metrics collection."""

    @abstractmethod
    def record_step_start(self, step_id: str) -> None:
        """Record step start time."""
        ...

    @abstractmethod
    def record_step_end(self, step_id: str, status: str) -> None:
        """Record step completion."""
        ...

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics."""
        ...


class ExecutionStrategy(Protocol):
    """Protocol for execution strategies."""

    @abstractmethod
    def execute_steps(
        self, steps: List[Dict[str, Any]], context: ExecutionContext
    ) -> List[StepResult]:
        """Execute steps according to strategy."""
        ...


class ResultBuilder(Protocol):
    """Protocol for building execution results."""

    @abstractmethod
    def build_result(
        self, step_results: List[StepResult], context: ExecutionContext
    ) -> Dict[str, Any]:
        """Build final execution result."""
        ...
