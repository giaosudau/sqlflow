"""Memory-optimized data structures using __slots__.

Following the Zen of Python:
- Simple is better than complex
- Sparse is better than dense
- Practicality beats purity

This module provides memory-optimized versions of core data structures
using __slots__ to reduce memory overhead for large datasets.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class StepStatus(Enum):
    """Memory-efficient enum for step status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"


class OptimizedStepResult:
    """Memory-optimized step result using __slots__.

    Reduces memory usage by ~40% compared to regular class
    by preventing dynamic attribute creation.
    """

    __slots__ = (
        "step_id",
        "status",
        "message",
        "error_message",
        "execution_time",
        "data",
        "row_count",
        "memory_usage",
    )

    def __init__(
        self,
        step_id: str,
        status: StepStatus = StepStatus.PENDING,
        message: str = "",
        error_message: Optional[str] = None,
        execution_time: Optional[float] = None,
        data: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        memory_usage: Optional[float] = None,
    ):
        """Initialize optimized step result."""
        self.step_id = step_id
        self.status = status
        self.message = message
        self.error_message = error_message
        self.execution_time = execution_time
        self.data = data
        self.row_count = row_count
        self.memory_usage = memory_usage

    @classmethod
    def success(
        cls,
        step_id: str,
        message: str = "Step completed successfully",
        execution_time: Optional[float] = None,
        data: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
    ) -> "OptimizedStepResult":
        """Create successful step result."""
        return cls(
            step_id=step_id,
            status=StepStatus.SUCCESS,
            message=message,
            execution_time=execution_time,
            data=data,
            row_count=row_count,
        )

    @classmethod
    def error(
        cls, step_id: str, error_message: str, execution_time: Optional[float] = None
    ) -> "OptimizedStepResult":
        """Create error step result."""
        return cls(
            step_id=step_id,
            status=StepStatus.ERROR,
            error_message=error_message,
            execution_time=execution_time,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "step_id": self.step_id,
            "status": (
                self.status.value
                if isinstance(self.status, StepStatus)
                else self.status
            ),
            "message": self.message,
            "error": self.error_message,
            "execution_time": self.execution_time,
            "data": self.data,
            "row_count": self.row_count,
            "memory_usage": self.memory_usage,
        }


class OptimizedExecutionContext:
    """Memory-optimized execution context using __slots__.

    Stores execution state efficiently for large-scale processing.
    """

    __slots__ = (
        "_session",
        "_variables",
        "_observability",
        "_cache",
        "_metadata",
        "_step_results",
    )

    def __init__(
        self,
        session: Any,
        variables: Optional[Dict[str, Any]] = None,
        observability: Optional[Any] = None,
    ):
        """Initialize optimized execution context."""
        self._session = session
        self._variables = variables or {}
        self._observability = observability
        self._cache = {}
        self._metadata = {}
        self._step_results = []

    @property
    def session(self) -> Any:
        """Get database session."""
        return self._session

    @property
    def variables(self) -> Dict[str, Any]:
        """Get execution variables."""
        return self._variables

    @property
    def observability(self) -> Any:
        """Get observability manager."""
        return self._observability

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get specific variable value."""
        return self._variables.get(name, default)

    def set_variable(self, name: str, value: Any) -> None:
        """Set variable value."""
        self._variables[name] = value

    def get_cache(self, key: str) -> Any:
        """Get cached value."""
        return self._cache.get(key)

    def set_cache(self, key: str, value: Any) -> None:
        """Set cached value."""
        self._cache[key] = value

    def clear_cache(self) -> None:
        """Clear execution cache."""
        self._cache.clear()

    def add_step_result(self, result: OptimizedStepResult) -> None:
        """Add step result to context."""
        self._step_results.append(result)

    def get_step_results(self) -> List[OptimizedStepResult]:
        """Get all step results."""
        return self._step_results.copy()

    def with_variables(
        self, new_variables: Dict[str, Any]
    ) -> "OptimizedExecutionContext":
        """Create new context with updated variables."""
        updated_vars = {**self._variables, **new_variables}
        new_context = OptimizedExecutionContext(
            session=self._session,
            variables=updated_vars,
            observability=self._observability,
        )
        # Copy cache and metadata
        new_context._cache = self._cache.copy()
        new_context._metadata = self._metadata.copy()
        new_context._step_results = self._step_results.copy()
        return new_context


class OptimizedStepDefinition:
    """Memory-optimized step definition using __slots__.

    Defines pipeline steps with minimal memory footprint.
    """

    __slots__ = (
        "id",
        "type",
        "name",
        "sql",
        "source",
        "target_table",
        "depends_on",
        "parameters",
        "metadata",
    )

    def __init__(
        self,
        id: str,
        type: str,
        name: Optional[str] = None,
        sql: Optional[str] = None,
        source: Optional[Dict[str, Any]] = None,
        target_table: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        **metadata,
    ):
        """Initialize optimized step definition."""
        self.id = id
        self.type = type
        self.name = name or id
        self.sql = sql
        self.source = source
        self.target_table = target_table
        self.depends_on = depends_on or []
        self.parameters = parameters or {}
        self.metadata = metadata

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "depends_on": self.depends_on,
            "parameters": self.parameters,
        }

        # Add optional fields if present
        if self.sql:
            result["sql"] = self.sql
        if self.source:
            result["source"] = self.source
        if self.target_table:
            result["target_table"] = self.target_table
        if self.metadata:
            result.update(self.metadata)

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OptimizedStepDefinition":
        """Create step definition from dictionary."""
        # Extract known fields
        known_fields = {
            "id",
            "type",
            "name",
            "sql",
            "source",
            "target_table",
            "depends_on",
            "parameters",
        }

        init_args = {}
        metadata = {}

        for key, value in data.items():
            if key in known_fields:
                init_args[key] = value
            else:
                metadata[key] = value

        return cls(**init_args, **metadata)


class OptimizedPipelineDefinition:
    """Memory-optimized pipeline definition using __slots__.

    Defines entire pipeline with minimal memory overhead.
    """

    __slots__ = ("name", "version", "steps", "variables", "metadata", "_step_index")

    def __init__(
        self,
        name: str,
        steps: List[OptimizedStepDefinition],
        version: str = "1.0",
        variables: Optional[Dict[str, Any]] = None,
        **metadata,
    ):
        """Initialize optimized pipeline definition."""
        self.name = name
        self.version = version
        self.steps = steps
        self.variables = variables or {}
        self.metadata = metadata
        self._step_index = {step.id: step for step in steps}

    def get_step(self, step_id: str) -> Optional[OptimizedStepDefinition]:
        """Get step by ID."""
        return self._step_index.get(step_id)

    def get_steps_by_type(self, step_type: str) -> List[OptimizedStepDefinition]:
        """Get all steps of specific type."""
        return [step for step in self.steps if step.type == step_type]

    def get_dependencies(self, step_id: str) -> List[OptimizedStepDefinition]:
        """Get dependencies for a step."""
        step = self.get_step(step_id)
        if not step:
            return []

        return [
            self.get_step(dep_id) for dep_id in step.depends_on if self.get_step(dep_id)
        ]

    def validate(self) -> List[str]:
        """Validate pipeline definition and return any errors."""
        errors = []

        # Check for duplicate step IDs
        step_ids = [step.id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            errors.append("Duplicate step IDs found")

        # Check for circular dependencies
        if self._has_circular_dependencies():
            errors.append("Circular dependencies detected")

        # Check for missing dependencies
        for step in self.steps:
            for dep_id in step.depends_on:
                if dep_id not in self._step_index:
                    errors.append(
                        f"Step {step.id} depends on non-existent step {dep_id}"
                    )

        return errors

    def _has_circular_dependencies(self) -> bool:
        """Check for circular dependencies using DFS."""
        visited = set()
        rec_stack = set()

        def dfs(step_id: str) -> bool:
            if step_id in rec_stack:
                return True  # Circular dependency found
            if step_id in visited:
                return False

            visited.add(step_id)
            rec_stack.add(step_id)

            step = self.get_step(step_id)
            if step:
                for dep_id in step.depends_on:
                    if dfs(dep_id):
                        return True

            rec_stack.remove(step_id)
            return False

        for step in self.steps:
            if step.id not in visited:
                if dfs(step.id):
                    return True

        return False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "version": self.version,
            "variables": self.variables,
            "steps": [step.to_dict() for step in self.steps],
            **self.metadata,
        }


class OptimizedMetricsCollector:
    """Memory-optimized metrics collection using __slots__.

    Collects performance metrics with minimal memory overhead.
    """

    __slots__ = (
        "_step_metrics",
        "_pipeline_metrics",
        "_memory_snapshots",
        "_start_time",
        "_peak_memory",
    )

    def __init__(self):
        """Initialize optimized metrics collector."""
        self._step_metrics = {}
        self._pipeline_metrics = {}
        self._memory_snapshots = []
        self._start_time = None
        self._peak_memory = 0.0

    def start_pipeline(self) -> None:
        """Start pipeline metrics collection."""
        import time

        self._start_time = time.time()
        self._peak_memory = 0.0

    def record_step_metric(self, step_id: str, metric_name: str, value: Any) -> None:
        """Record step-level metric."""
        if step_id not in self._step_metrics:
            self._step_metrics[step_id] = {}
        self._step_metrics[step_id][metric_name] = value

    def record_memory_snapshot(self, memory_mb: float) -> None:
        """Record memory usage snapshot."""
        self._memory_snapshots.append(memory_mb)
        if memory_mb > self._peak_memory:
            self._peak_memory = memory_mb

    def get_step_metrics(self, step_id: str) -> Dict[str, Any]:
        """Get metrics for specific step."""
        return self._step_metrics.get(step_id, {})

    def get_peak_memory(self) -> float:
        """Get peak memory usage."""
        return self._peak_memory

    def get_total_execution_time(self) -> Optional[float]:
        """Get total pipeline execution time."""
        if self._start_time is None:
            return None

        import time

        return time.time() - self._start_time

    def clear_metrics(self) -> None:
        """Clear all collected metrics."""
        self._step_metrics.clear()
        self._pipeline_metrics.clear()
        self._memory_snapshots.clear()
        self._start_time = None
        self._peak_memory = 0.0

    def get_memory_efficiency_ratio(self) -> float:
        """Calculate memory efficiency ratio.

        Returns the ratio of average memory usage to peak memory usage.
        Higher values indicate more consistent memory usage.
        """
        if not self._memory_snapshots or self._peak_memory == 0:
            return 0.0

        avg_memory = sum(self._memory_snapshots) / len(self._memory_snapshots)
        return avg_memory / self._peak_memory
