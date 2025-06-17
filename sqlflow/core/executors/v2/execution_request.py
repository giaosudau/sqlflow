"""Execution request parameter objects following Martin Fowler's Parameter Object pattern.

This module implements parameter objects to replace long parameter lists
and provide better encapsulation of execution configuration.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class OrchestrationRequest:
    """Parameter object for orchestration requests.

    Follows Martin Fowler's Parameter Object refactoring pattern
    to replace long parameter lists with a cohesive data structure.
    """

    plan: List[Dict[str, Any]]
    variables: Optional[Dict[str, Any]] = None
    profile: Optional[Dict[str, Any]] = None
    execution_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate the request after initialization."""
        if not isinstance(self.plan, list):
            raise ValueError("Plan must be a list of step dictionaries")

        if self.variables is not None and not isinstance(self.variables, dict):
            raise ValueError("Variables must be a dictionary")

        if self.profile is not None and not isinstance(self.profile, dict):
            raise ValueError("Profile must be a dictionary")


@dataclass(frozen=True)
class ExecutionSummary:
    """Parameter object for execution result data.

    Consolidates all the parameters needed to build execution results,
    following the Parameter Object pattern to reduce method complexity.
    """

    run_id: str
    results: List[Any]  # StepExecutionResult objects
    observability: Any  # ObservabilityManager
    total_time: float
    start_time: datetime
    engine_stats: Optional[Dict[str, Any]] = None
    performance_report: Optional[Dict[str, Any]] = None

    @property
    def end_time(self) -> datetime:
        """Calculate end time from start time and duration."""
        from datetime import timedelta

        return self.start_time + timedelta(seconds=self.total_time)


@dataclass(frozen=True)
class ExecutionEnvironment:
    """Environment configuration for pipeline execution.

    Encapsulates all the environmental setup needed for execution,
    making dependency injection easier for testing.
    """

    run_id: str
    profile: Optional[Dict[str, Any]] = None
    variables: Optional[Dict[str, Any]] = None
    execution_options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_request(
        cls, request: OrchestrationRequest, run_id: str
    ) -> "ExecutionEnvironment":
        """Create execution environment from orchestration request."""
        return cls(
            run_id=run_id,
            profile=request.profile,
            variables=request.variables,
            execution_options=request.execution_options,
        )
