"""Interfaces for planner components.

Following Zen of Python: Explicit is better than implicit.
Clear interfaces make the system more testable and extensible.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from sqlflow.parser.ast import Pipeline


class IDependencyAnalyzer(ABC):
    """Interface for dependency analysis components."""

    @abstractmethod
    def analyze(
        self, pipeline: Pipeline, step_id_map: Dict[int, str]
    ) -> Dict[str, List[str]]:
        """Analyze dependencies in a pipeline.

        Args:
            pipeline: The pipeline to analyze
            step_id_map: Mapping from step object IDs to step IDs

        Returns:
            Dict mapping step IDs to their dependency step IDs
        """

    @abstractmethod
    def get_undefined_table_references(self) -> List[tuple]:
        """Get undefined table references found during analysis.

        Returns:
            List of tuples (table_name, step, line_number) for undefined tables
        """


class IExecutionOrderResolver(ABC):
    """Interface for execution order resolution components."""

    @abstractmethod
    def resolve(
        self, step_dependencies: Dict[str, List[str]], all_step_ids: List[str]
    ) -> List[str]:
        """Resolve execution order from dependencies.

        Args:
            step_dependencies: Dict mapping step IDs to their dependency step IDs
            all_step_ids: List of all step IDs in the pipeline

        Returns:
            List of step IDs in execution order
        """


class IStepBuilder(ABC):
    """Interface for step building components."""

    @abstractmethod
    def build_steps(
        self,
        pipeline: Pipeline,
        execution_order: List[str],
        step_id_map: Dict[int, str],
        step_dependencies: Dict[str, List[str]],
    ) -> List[Dict[str, Any]]:
        """Build execution steps from pipeline and execution order.

        Args:
            pipeline: The source pipeline
            execution_order: List of step IDs in execution order
            step_id_map: Mapping from step object IDs to step IDs
            step_dependencies: Dict mapping step IDs to their dependency step IDs

        Returns:
            List of execution step dictionaries
        """
