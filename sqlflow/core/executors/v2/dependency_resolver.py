"""Dependency Resolution Engine for V2 Executor.

Following the Zen of Python:
- Simple is better than complex
- Explicit is better than implicit
- Readability counts

This module provides clean, testable dependency analysis for step execution.
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Set

from sqlflow.core.executors.v2.steps import BaseStep
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class DependencyGraph:
    """
    Immutable dependency graph for pipeline steps.

    Following Kent Beck's simple design principle:
    Data structures should be obvious and predictable.
    """

    steps: Dict[str, BaseStep] = field(default_factory=dict)
    dependencies: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    dependents: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    def __post_init__(self):
        """Ensure immutability after construction."""
        # Convert defaultdicts to regular dicts for immutability
        self.dependencies = dict(self.dependencies)
        self.dependents = dict(self.dependents)

    @classmethod
    def from_steps(cls, steps: List[BaseStep]) -> "DependencyGraph":
        """
        Create dependency graph from steps.

        Following Martin Fowler's factory pattern:
        Encapsulate complex construction logic.
        """
        step_map = {step.id: step for step in steps}
        dependencies = defaultdict(set)
        dependents = defaultdict(set)

        for step in steps:
            step_id = step.id
            for dep_id in step.depends_on:
                dependencies[step_id].add(dep_id)
                dependents[dep_id].add(step_id)

        return cls(
            steps=step_map, dependencies=dict(dependencies), dependents=dict(dependents)
        )

    def get_ready_steps(self, completed: Set[str]) -> List[str]:
        """
        Get steps ready for execution.

        A step is ready when all its dependencies are completed.
        """
        ready = []
        for step_id, step in self.steps.items():
            if step_id in completed:
                continue

            step_deps = self.dependencies.get(step_id, set())
            if step_deps.issubset(completed):
                ready.append(step_id)

        return ready

    def validate_acyclic(self) -> None:
        """
        Validate that the graph has no cycles.

        Uses Kahn's algorithm for topological sorting.
        Raises ValueError if cycles are detected.
        """
        # Calculate in-degrees
        in_degree = defaultdict(int)
        for step_id in self.steps:
            in_degree[step_id] = len(self.dependencies.get(step_id, set()))

        # Initialize queue with zero in-degree nodes
        queue = deque([step_id for step_id, degree in in_degree.items() if degree == 0])
        processed = 0

        while queue:
            current = queue.popleft()
            processed += 1

            # Reduce in-degree for dependents
            for dependent in self.dependents.get(current, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if processed != len(self.steps):
            # Find cycle participants
            remaining = [step_id for step_id, degree in in_degree.items() if degree > 0]
            raise ValueError(f"Circular dependency detected involving: {remaining}")


class ConcurrencyAnalyzer:
    """
    Analyzes steps for safe concurrent execution.

    Following the Single Responsibility Principle:
    This class only determines what can run in parallel.
    """

    def __init__(self, dependency_graph: DependencyGraph):
        self.graph = dependency_graph

    def get_execution_levels(self) -> List[List[str]]:
        """
        Group steps into execution levels for maximum parallelism.

        Returns list where each inner list contains steps that can
        execute concurrently.
        """
        levels = []
        completed = set()
        remaining = set(self.graph.steps.keys())

        while remaining:
            # Find steps ready at this level
            current_level = []
            for step_id in remaining:
                step_deps = self.graph.dependencies.get(step_id, set())
                if step_deps.issubset(completed):
                    current_level.append(step_id)

            if not current_level:
                # This shouldn't happen if graph is acyclic
                raise ValueError("Unable to find executable steps - possible cycle")

            levels.append(current_level)
            completed.update(current_level)
            remaining -= set(current_level)

        return levels

    def estimate_parallelism(self) -> Dict[str, float]:
        """
        Estimate parallelism potential for performance planning.

        Returns metrics about the execution plan.
        """
        levels = self.get_execution_levels()

        return {
            "total_steps": float(len(self.graph.steps)),
            "execution_levels": float(len(levels)),
            "max_parallel_steps": float(
                max(len(level) for level in levels) if levels else 0
            ),
            "sequential_bottlenecks": float(
                len([level for level in levels if len(level) == 1])
            ),
            "parallelism_factor": (
                sum(len(level) for level in levels) / len(levels) if levels else 0.0
            ),
        }


def analyze_dependencies(steps: List[BaseStep]) -> DependencyGraph:
    """
    Create and validate dependency graph from steps.

    Convenience function that combines graph creation and validation.
    """
    graph = DependencyGraph.from_steps(steps)
    graph.validate_acyclic()

    logger.info(
        f"Analyzed {len(steps)} steps with {sum(len(deps) for deps in graph.dependencies.values())} dependencies"
    )

    return graph


def plan_concurrent_execution(steps: List[BaseStep]) -> List[List[str]]:
    """
    Plan concurrent execution levels for optimal parallelism.

    Following the principle of least surprise:
    Returns simple list of execution levels.
    """
    graph = analyze_dependencies(steps)
    analyzer = ConcurrencyAnalyzer(graph)
    levels = analyzer.get_execution_levels()

    parallelism = analyzer.estimate_parallelism()
    logger.info(
        f"Planned {parallelism['execution_levels']} execution levels with max {parallelism['max_parallel_steps']} parallel steps"
    )

    return levels
