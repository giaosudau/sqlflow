"""Local executor for SQLFlow pipelines."""

import logging
from typing import Any, Dict, List, Optional, Set

from sqlflow.sqlflow.core.dependencies import DependencyResolver
from sqlflow.sqlflow.core.executors.base_executor import BaseExecutor

logger = logging.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Executes pipelines sequentially in a single process."""

    def __init__(self):
        """Initialize a LocalExecutor."""
        self.executed_steps: Set[str] = set()
        self.failed_step: Optional[Dict[str, Any]] = None
        self.results: Dict[str, Any] = {}
        self.dependency_resolver: Optional[DependencyResolver] = None

    def execute(
        self,
        plan: List[Dict[str, Any]],
        dependency_resolver: Optional[DependencyResolver] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline plan.

        Args:
            plan: List of operations to execute
            dependency_resolver: Optional DependencyResolver to cross-check execution order

        Returns:
            Dict containing execution results
        """
        self.results = {}
        self.dependency_resolver = dependency_resolver

        if (
            dependency_resolver is not None
            and dependency_resolver.last_resolved_order is not None
        ):
            plan_ids = [step["id"] for step in plan]

            if plan_ids != dependency_resolver.last_resolved_order:
                logger.warning(
                    "Execution order mismatch detected. Plan order: %s, Resolved order: %s",
                    plan_ids,
                    dependency_resolver.last_resolved_order,
                )

        for step in plan:
            try:
                step_result = self.execute_step(step)
                self.results[step["id"]] = step_result
                self.executed_steps.add(step["id"])
            except Exception as e:
                self.failed_step = step
                self.results["error"] = str(e)
                self.results["failed_step"] = step["id"]
                break

        return self.results

    def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step in the pipeline.

        Args:
            step: Operation to execute

        Returns:
            Dict containing execution results
        """
        return {"status": "success"}

    def can_resume(self) -> bool:
        """Check if the executor supports resuming from failure.

        Returns:
            True if the executor supports resuming, False otherwise
        """
        return self.failed_step is not None

    def resume(self) -> Dict[str, Any]:
        """Resume execution from the last failure.

        Returns:
            Dict containing execution results
        """
        if not self.can_resume():
            return {"status": "nothing_to_resume"}

        failed_step = self.failed_step
        self.failed_step = None

        if failed_step is None:
            return {"status": "nothing_to_resume"}

        try:
            step_result = self.execute_step(failed_step)
            self.results[failed_step["id"]] = step_result
            self.executed_steps.add(failed_step["id"])
        except Exception as e:
            self.failed_step = failed_step
            self.results["error"] = str(e)
            self.results["failed_step"] = failed_step["id"]
            return self.results

        return self.results
