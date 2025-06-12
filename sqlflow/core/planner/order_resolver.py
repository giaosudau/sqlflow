"""Execution order resolution for SQLFlow pipeline steps.

This module extracts the execution order resolution logic from ExecutionPlanBuilder
to follow the Single Responsibility Principle (Zen of Python).

Following Zen of Python:
- Simple is better than complex: Clear order resolution logic
- Explicit is better than implicit: Clear dependency-based ordering
- Practicality beats purity: Compatible with existing DependencyResolver
"""

from typing import Dict, List

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.errors import PlanningError
from sqlflow.logging import get_logger

from .interfaces import IExecutionOrderResolver

logger = get_logger(__name__)


class ExecutionOrderResolver(IExecutionOrderResolver):
    """Resolves execution order for pipeline steps based on dependencies.

    Following Zen of Python: Simple is better than complex.
    Focuses solely on order resolution logic.
    """

    def __init__(self):
        """Initialize the execution order resolver."""
        logger.debug("ExecutionOrderResolver initialized")

    def resolve(self, step_dependencies: Dict[str, List[str]]) -> List[str]:
        """Resolve execution order based on step dependencies.

        Args:
            step_dependencies: Dictionary mapping step IDs to their dependencies

        Returns:
            List of step IDs in execution order

        Raises:
            PlanningError: If circular dependencies are detected

        Following Zen of Python: Explicit is better than implicit.
        Clear dependency-based order resolution.
        """
        # Create dependency resolver
        resolver = self._create_dependency_resolver(step_dependencies)

        # Check for cycles in the dependency graph
        cycles = self._detect_cycles(resolver)
        if cycles:
            error_msg = self._format_cycle_error(cycles)
            logger.debug(f"Dependency cycle detected: {error_msg}")
            raise PlanningError(error_msg)

        # Find entry points (steps with no dependencies)
        all_step_ids = list(step_dependencies.keys())
        entry_points = self._find_entry_points(resolver, all_step_ids)
        logger.debug(f"Found {len(entry_points)} entry points: {entry_points}")

        # Build execution order
        execution_order = self._build_execution_order(resolver, entry_points)

        # Ensure all steps are included
        self._ensure_all_steps_included(execution_order, all_step_ids)

        logger.debug(f"Resolved execution order with {len(execution_order)} steps")
        return execution_order

    def _create_dependency_resolver(
        self, step_dependencies: Dict[str, List[str]]
    ) -> DependencyResolver:
        """Create a dependency resolver from step dependencies.

        Following Zen of Python: Simple is better than complex.
        Clear dependency resolver creation.
        """
        resolver = DependencyResolver()

        # Add dependencies as edges (nodes are added implicitly)
        for step_id, dependencies in step_dependencies.items():
            for dependency_id in dependencies:
                resolver.add_dependency(step_id, dependency_id)

        return resolver

    def _detect_cycles(self, resolver: DependencyResolver) -> List[List[str]]:
        """Detect cycles in the dependency graph.

        Following Zen of Python: Explicit is better than implicit.
        Clear cycle detection using depth-first search.
        """
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node):
            if node in rec_stack:
                # Found a cycle, but we need to extract it
                # For now, just return the problematic node
                return [node]
            if node in visited:
                return []

            visited.add(node)
            rec_stack.add(node)

            for neighbor in resolver.dependencies.get(node, []):
                cycle = dfs(neighbor)
                if cycle:
                    if node not in cycle:
                        cycle.append(node)
                    cycles.append(cycle)

            rec_stack.remove(node)
            return []

        # Get all nodes from the dependencies dict
        all_nodes = set(resolver.dependencies.keys())
        for deps in resolver.dependencies.values():
            all_nodes.update(deps)

        for node in all_nodes:
            if node not in visited:
                dfs(node)

        return cycles

    def _format_cycle_error(self, cycles: List[List[str]]) -> str:
        """Format cycle detection error message.

        Following Zen of Python: Simple is better than complex.
        Clear error message formatting.
        """
        if not cycles:
            return "Circular dependency detected"

        error_lines = ["Circular dependencies detected in the pipeline:"]

        for i, cycle in enumerate(cycles[:3]):  # Show up to 3 cycles
            cycle_str = " -> ".join(cycle + [cycle[0]])  # Complete the cycle
            error_lines.append(f"  Cycle {i+1}: {cycle_str}")

        if len(cycles) > 3:
            error_lines.append(f"  ... and {len(cycles) - 3} more cycles")

        error_lines.extend(
            [
                "",
                "Please review your pipeline structure to remove circular dependencies.",
                "Each step should only depend on steps that come before it.",
            ]
        )

        return "\n".join(error_lines)

    def _find_entry_points(
        self, resolver: DependencyResolver, all_step_ids: List[str]
    ) -> List[str]:
        """Find entry points (steps with no dependencies).

        Following Zen of Python: Simple is better than complex.
        Clear identification of steps with no dependencies.
        """
        entry_points = []
        for step_id in all_step_ids:
            dependencies = resolver.dependencies.get(step_id, [])
            if not dependencies:
                entry_points.append(step_id)
        return entry_points

    def _build_execution_order(
        self, resolver: DependencyResolver, entry_points: List[str]
    ) -> List[str]:
        """Build execution order from entry points.

        Following Zen of Python: Explicit is better than implicit.
        Clear execution order building from dependency resolution.
        """
        execution_order = []

        for entry_point in entry_points:
            if entry_point in execution_order:
                continue

            # Get topological order starting from this entry point
            step_order = resolver.resolve_dependencies(entry_point)

            # Add steps to execution order if not already present
            for step_id in step_order:
                if step_id not in execution_order:
                    execution_order.append(step_id)

        return execution_order

    def _ensure_all_steps_included(
        self, execution_order: List[str], all_step_ids: List[str]
    ) -> None:
        """Ensure all steps are included in the execution order.

        Following Zen of Python: Explicit is better than implicit.
        Clear verification that all steps are included.
        """
        for step_id in all_step_ids:
            if step_id not in execution_order:
                logger.debug(f"Adding missing step to execution order: {step_id}")
                execution_order.append(step_id)
