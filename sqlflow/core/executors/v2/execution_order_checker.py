"""Execution Order Checker for V1 Compatibility.

This module extracts the complex execution order checking logic from the orchestrator,
following the Single Responsibility Principle.
"""

from typing import Any, Dict, List

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ExecutionOrderChecker:
    """Checks execution order against dependency resolution order."""

    def check_order_mismatch(self, plan: List[Dict[str, Any]], resolver) -> None:
        """Check if execution order matches dependency order and warn if not."""
        if not plan or not resolver:
            return

        actual_order = self._extract_step_ids(plan)
        if not actual_order:
            return

        try:
            expected_order = self._find_optimal_dependency_order(actual_order, resolver)
            if expected_order:
                self._compare_and_warn_if_different(actual_order, expected_order)
        except Exception as e:
            logger.debug(f"Could not check execution order: {e}")

    def _extract_step_ids(self, plan: List[Dict[str, Any]]) -> List[str]:
        """Extract step IDs from the plan."""
        return [step.get("id", "") for step in plan if step.get("id")]

    def _find_optimal_dependency_order(
        self, actual_order: List[str], resolver
    ) -> List[str]:
        """Find the optimal dependency order by finding the step with the most dependencies."""
        max_deps = 0
        expected_order = []

        for step_id in actual_order:
            try:
                deps = resolver.resolve_dependencies(step_id)
                if len(deps) > max_deps:
                    max_deps = len(deps)
                    expected_order = deps
            except Exception:
                continue

        return expected_order

    def _compare_and_warn_if_different(
        self, actual_order: List[str], expected_order: List[str]
    ) -> None:
        """Compare orders and warn if they're different."""
        common_steps = set(actual_order) & set(expected_order)
        if len(common_steps) <= 1:
            return  # No point checking order for 0 or 1 steps

        actual_filtered = [step for step in actual_order if step in common_steps]
        expected_filtered = [step for step in expected_order if step in common_steps]

        if actual_filtered != expected_filtered:
            self._log_order_mismatch_warning(actual_filtered, expected_filtered)

    def _log_order_mismatch_warning(
        self, actual_filtered: List[str], expected_filtered: List[str]
    ) -> None:
        """Log warning about order mismatch."""
        # Import logger here to match the test's mock target
        from sqlflow.core.executors.local_executor import logger

        logger.warning(
            "Execution order mismatch detected. Actual: %s, Expected: %s",
            actual_filtered,
            expected_filtered,
        )
