"""Thread-pool executor for SQLFlow pipelines."""

import json
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor as ConcurrentThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.executors.base_executor import BaseExecutor
from sqlflow.core.executors.task_status import TaskState, TaskStatus

logger = logging.getLogger(__name__)


class ThreadPoolTaskExecutor(BaseExecutor):
    """Executes pipeline steps concurrently using a thread pool."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
        state_backend: Optional[Any] = None,
        run_id: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        project_dir: Optional[str] = None,
    ):
        """Initialize a ThreadPoolTaskExecutor.

        Args:
        ----
            max_workers: Maximum number of worker threads to use.
                         Defaults to os.cpu_count().
            state_backend: Optional state backend for persistence.
            run_id: Unique identifier for the execution run.
                    Defaults to a generated UUID.
            max_retries: Maximum number of retries for failed tasks.
            retry_delay: Delay in seconds between retries.
            project_dir: Project directory for UDF discovery.

        """
        # Initialize base executor (UDF manager)
        super().__init__()

        self.max_workers = max_workers or os.cpu_count()
        self.task_statuses: Dict[str, TaskStatus] = {}
        self.results: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.dependency_resolver: Optional[DependencyResolver] = None
        self.failed_step: Optional[Dict[str, Any]] = None
        self.executed_steps: Set[str] = set()
        self.state_backend = state_backend
        self.run_id = run_id or str(uuid.uuid4())
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.original_plan: List[Dict[str, Any]] = []

        # Discover UDFs if project_dir is provided
        if project_dir:
            self.discover_udfs(project_dir)

    def execute(
        self,
        plan: List[Dict[str, Any]],
        dependency_resolver: Optional[DependencyResolver] = None,
        resume: bool = False,
        project_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline plan concurrently.

        Args:
        ----
            plan: List of operations to execute
            dependency_resolver: Optional DependencyResolver to cross-check execution order
            resume: Whether to resume from a previous execution
            project_dir: Project directory for UDF discovery

        Returns:
        -------
            Dict containing execution results

        """
        self.results = {}
        self.dependency_resolver = dependency_resolver
        self.original_plan = plan.copy()

        # Ensure UDFs are discovered
        if project_dir:
            self.discover_udfs(project_dir)
        elif not self.discovered_udfs:
            self.discover_udfs()

        self._check_execution_order(plan, dependency_resolver)
        self._initialize_execution_state(plan, resume)

        with ConcurrentThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return self._execute_with_thread_pool(executor, plan)

    def _check_execution_order(
        self,
        plan: List[Dict[str, Any]],
        dependency_resolver: Optional[DependencyResolver],
    ) -> None:
        """Check if the execution order matches the resolved order.

        Args:
        ----
            plan: List of operations to execute
            dependency_resolver: Optional DependencyResolver to cross-check execution order

        """
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

    def _initialize_execution_state(
        self, plan: List[Dict[str, Any]], resume: bool
    ) -> None:
        """Initialize the execution state.

        Args:
        ----
            plan: List of operations to execute
            resume: Whether to resume from a previous execution

        """
        if self.state_backend is not None:
            if not resume:
                self.state_backend.create_run(self.run_id)
                self.state_backend.save_plan(self.run_id, plan)
                self.task_statuses = {}
            else:
                self.task_statuses = self.state_backend.load_task_statuses(self.run_id)
                if not self.task_statuses:
                    logger.warning("No saved state found for run_id %s", self.run_id)
                    self.task_statuses = {}

        if not self.task_statuses:
            self._initialize_task_statuses(plan)

        if self.state_backend is not None:
            self.state_backend.update_run_status(self.run_id, "RUNNING")

    def _execute_with_thread_pool(
        self, executor: ConcurrentThreadPoolExecutor, plan: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute the plan with a thread pool.

        Args:
        ----
            executor: ThreadPoolExecutor to use
            plan: List of operations to execute

        Returns:
        -------
            Dict containing execution results

        """
        futures = {}
        completed_tasks = set()

        for task_id, status in self.task_statuses.items():
            if status.state == TaskState.SUCCESS:
                completed_tasks.add(task_id)
                self.executed_steps.add(task_id)

        while len(completed_tasks) < len(plan) and not any(
            status.state == TaskState.FAILED for status in self.task_statuses.values()
        ):
            self._submit_eligible_tasks(executor, plan, futures)
            result = self._process_completed_futures(plan, futures, completed_tasks)

            if result is not None:
                return result

            if self._check_for_deadlock(plan, futures, completed_tasks):
                return self.results

            if futures and len(completed_tasks) < len(plan):
                time.sleep(0.1)

        if self.state_backend is not None:
            self.state_backend.update_run_status(self.run_id, "SUCCESS")

        return self.results

    def _submit_eligible_tasks(
        self,
        executor: ConcurrentThreadPoolExecutor,
        plan: List[Dict[str, Any]],
        futures: Dict[str, Any],
    ) -> None:
        """Submit eligible tasks to the executor.

        Args:
        ----
            executor: ThreadPoolExecutor to use
            plan: List of operations to execute
            futures: Dict of futures to update

        """
        eligible_tasks = self._get_eligible_tasks()

        for task_id in eligible_tasks:
            if task_id not in futures:
                task = next(step for step in plan if step["id"] == task_id)
                self._update_task_state(task_id, TaskState.RUNNING)
                futures[task_id] = executor.submit(self.execute_step, task)

    def _process_completed_futures(
        self,
        plan: List[Dict[str, Any]],
        futures: Dict[str, Any],
        completed_tasks: Set[str],
    ) -> Optional[Dict[str, Any]]:
        """Process completed futures.

        Args:
        ----
            plan: List of operations to execute
            futures: Dict of futures to process
            completed_tasks: Set of completed tasks to update

        Returns:
        -------
            Dict containing execution results if execution should stop, None otherwise

        """
        for task_id, future in list(futures.items()):
            if future.done():
                try:
                    result = future.result()
                    self.results[task_id] = result
                    self._update_task_state(task_id, TaskState.SUCCESS)
                    self.executed_steps.add(task_id)
                    completed_tasks.add(task_id)

                    self._update_dependent_tasks(task_id)

                    # Persist task status
                    if self.state_backend is not None:
                        self.state_backend.save_task_status(
                            self.run_id, self.task_statuses[task_id]
                        )
                except Exception as e:
                    result = self._handle_task_failure(task_id, plan, str(e))
                    if result is not None:
                        return result

                futures.pop(task_id)

        return None

    def _handle_task_failure(
        self, task_id: str, plan: List[Dict[str, Any]], error_msg: str
    ) -> Optional[Dict[str, Any]]:
        """Handle a task failure.

        Args:
        ----
            task_id: ID of the failed task
            plan: List of operations to execute
            error_msg: Error message

        Returns:
        -------
            Dict containing execution results if execution should stop, None otherwise

        """
        self.results["error"] = error_msg
        self.results["failed_step"] = task_id

        status = self.task_statuses[task_id]
        if status.attempts < self.max_retries:
            logger.info(
                "Task %s failed, retrying (%d/%d) after %.1f seconds",
                task_id,
                status.attempts,
                self.max_retries,
                self.retry_delay,
            )
            time.sleep(self.retry_delay)
            self._update_task_state(task_id, TaskState.ELIGIBLE)
            return None
        else:
            self._update_task_state(task_id, TaskState.FAILED, error_msg)
            self.failed_step = next(step for step in plan if step["id"] == task_id)
            logger.info(f"Setting failed_step to {self.failed_step}")

            # Persist failed task status
            if self.state_backend is not None:
                self.state_backend.save_task_status(
                    self.run_id, self.task_statuses[task_id]
                )
                self.state_backend.update_run_status(self.run_id, "FAILED")

            return self.results

    def _check_for_deadlock(
        self,
        plan: List[Dict[str, Any]],
        futures: Dict[str, Any],
        completed_tasks: Set[str],
    ) -> bool:
        """Check for deadlock in the execution plan.

        Args:
        ----
            plan: List of operations to execute
            futures: Dict of futures
            completed_tasks: Set of completed tasks

        Returns:
        -------
            True if deadlock detected, False otherwise

        """
        if (
            not self._get_eligible_tasks()
            and not futures
            and len(completed_tasks) < len(plan)
        ):
            logger.error(
                "Deadlock detected: No eligible tasks and no running tasks, but incomplete tasks remain"
            )
            for task_id, status in self.task_statuses.items():
                if status.state != TaskState.SUCCESS:
                    logger.error(
                        "Incomplete task %s: state=%s, unmet_dependencies=%d",
                        task_id,
                        status.state,
                        status.unmet_dependencies,
                    )

            self.results["error"] = "Deadlock detected in execution plan"
            if self.state_backend is not None:
                self.state_backend.update_run_status(self.run_id, "FAILED")
            return True

        return False

    def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step in the pipeline.

        Args:
        ----
            step: Operation to execute

        Returns:
        -------
            Dict containing execution results

        """
        step_id = step.get("id", "unknown")
        step_type = step.get("type", "unknown")

        logger.info(f"Executing step {step_id} of type {step_type}")

        # For SQL-based steps, check if they reference UDFs
        if step_type in ["transform", "query"] and "query" in step:
            # Get the SQL query
            sql_query = step["query"]
            if isinstance(sql_query, str):
                # Extract UDFs referenced in the query
                udfs = self.get_udfs_for_query(sql_query)
                if udfs:
                    logger.info(f"Found {len(udfs)} UDF references in step {step_id}")

        # In practice, this executor would implement different step types
        # For now, we just return success to indicate that the step was "executed"
        # In a real implementation, you would delegate to specific handlers for each step type

        # Sample implementation for special INCLUDE steps that affect UDFs
        if step_type == "INCLUDE" and step.get("file_path", "").endswith(".py"):
            try:
                # For Python files, trigger rediscovery of UDFs
                logger.info(f"Including Python file for UDFs: {step['file_path']}")
                self.discover_udfs()
                logger.info(f"Now have {len(self.discovered_udfs)} UDFs available")
                return {"status": "success"}
            except Exception as e:
                logger.error(f"Error including Python file: {e}")
                return {"status": "failed", "error": str(e)}

        return {"status": "success"}

    def can_resume(self) -> bool:
        """Check if the executor supports resuming from failure.

        Returns
        -------
            True if the executor supports resuming, False otherwise

        """
        return self.failed_step is not None

    def _resume_from_state_backend(self) -> Dict[str, Any]:
        """Resume execution from state backend.

        Returns
        -------
            Dict containing execution results

        """
        if self.state_backend is None:
            logger.warning("No state backend configured")
            return {"status": "nothing_to_resume"}

        plan = self.state_backend.load_plan(self.run_id)
        if plan is None:
            logger.warning("No saved plan found for run_id %s", self.run_id)
            return {"status": "nothing_to_resume"}

        return self.execute(plan, resume=True)

    def _get_plan_for_resume(self) -> List[Dict[str, Any]]:
        """Get the plan for resuming execution.

        Returns
        -------
            List of operations to execute

        """
        if self.original_plan:
            return self.original_plan

        logger.warning("Reconstructing plan from task statuses - this is suboptimal")
        plan = []
        for task_id in self.task_statuses:
            for step in self.original_plan:
                if step["id"] == task_id:
                    plan.append(step)
                    break
        return plan

    def _execute_failed_step(self, failed_step: Dict[str, Any]) -> None:
        """Execute a failed step.

        Args:
        ----
            failed_step: Failed step to execute

        """
        logger.info(f"Resuming execution of failed step: {failed_step['id']}")
        step_result = self.execute_step(failed_step)
        self.results[failed_step["id"]] = step_result
        self.executed_steps.add(failed_step["id"])
        self._update_task_state(failed_step["id"], TaskState.SUCCESS)
        self._update_dependent_tasks(failed_step["id"])

    def resume(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Resume execution from the last failure.

        Args:
        ----
            run_id: Optional run ID to resume from. If not provided,
                   uses the current run_id.

        Returns:
        -------
            Dict containing execution results

        """
        if run_id is not None:
            self.run_id = run_id

        if self.state_backend is not None:
            return self._resume_from_state_backend()

        logger.info(
            f"Checking can_resume: {self.can_resume()}, failed_step: {self.failed_step}"
        )
        if not self.can_resume():
            return {"status": "nothing_to_resume"}

        failed_step = self.failed_step
        if failed_step is None:
            return {"status": "nothing_to_resume"}

        self.failed_step = None
        plan = self._get_plan_for_resume()

        try:
            self._execute_failed_step(failed_step)

            with ConcurrentThreadPoolExecutor(max_workers=self.max_workers) as executor:
                result = self._execute_with_thread_pool(executor, plan)
                if result is not None:
                    self.results.update(result)
        except Exception as e:
            self.failed_step = failed_step
            self.results["error"] = str(e)
            self.results["failed_step"] = failed_step["id"]
            self._update_task_state(failed_step["id"], TaskState.FAILED, str(e))
            return self.results

        return self.results

    def _initialize_task_statuses(self, plan: List[Dict[str, Any]]) -> None:
        """Initialize task statuses for all steps in the plan.

        Args:
        ----
            plan: List of operations to execute

        """
        dependency_map = {}
        for step in plan:
            step_id = step["id"]
            dependencies = step.get("depends_on", [])
            dependency_map[step_id] = dependencies

        for step in plan:
            step_id = step["id"]
            dependencies = dependency_map.get(step_id, [])
            unmet_dependencies = len(dependencies)

            state = TaskState.PENDING
            if unmet_dependencies == 0:
                state = TaskState.ELIGIBLE

            self.task_statuses[step_id] = TaskStatus(
                id=step_id,
                state=state,
                unmet_dependencies=unmet_dependencies,
                dependencies=dependencies,
            )

            self._log_state_transition(step_id, None, state)

    def _get_eligible_tasks(self) -> List[str]:
        """Get tasks that are eligible for execution.

        Returns
        -------
            List of task IDs that are eligible for execution

        """
        return [
            task_id
            for task_id, status in self.task_statuses.items()
            if status.state == TaskState.ELIGIBLE
        ]

    def _update_task_state(
        self, task_id: str, state: TaskState, error: Optional[str] = None
    ) -> None:
        """Update the state of a task.

        Args:
        ----
            task_id: ID of the task to update
            state: New state of the task
            error: Error message if the task failed

        """
        with self.lock:
            if task_id not in self.task_statuses:
                return

            old_state = self.task_statuses[task_id].state
            self.task_statuses[task_id].state = state

            if state == TaskState.RUNNING:
                self.task_statuses[task_id].start_time = time.time()
                self.task_statuses[task_id].attempts += 1
            elif state in (TaskState.SUCCESS, TaskState.FAILED):
                self.task_statuses[task_id].end_time = time.time()

            if state == TaskState.FAILED:
                self.task_statuses[task_id].error = error

            self._log_state_transition(task_id, old_state, state)

    def _update_dependent_tasks(self, completed_task_id: str) -> None:
        """Update tasks that depend on a completed task.

        Args:
        ----
            completed_task_id: ID of the completed task

        """
        eligible_tasks = []

        with self.lock:
            for task_id, status in self.task_statuses.items():
                if (
                    completed_task_id in status.dependencies
                    and status.state == TaskState.PENDING
                ):
                    status.unmet_dependencies -= 1
                    if status.unmet_dependencies == 0:
                        eligible_tasks.append(task_id)

        for task_id in eligible_tasks:
            self._update_task_state(task_id, TaskState.ELIGIBLE)

    def _log_state_transition(
        self, task_id: str, old_state: Optional[TaskState], new_state: TaskState
    ) -> None:
        """Log a task state transition.

        Args:
        ----
            task_id: ID of the task
            old_state: Previous state of the task
            new_state: New state of the task

        """
        status = self.task_statuses.get(task_id)
        if status:
            log_data = {
                "event": "task_state_transition",
                "task_id": task_id,
                "old_state": old_state,
                "new_state": new_state,
                "timestamp": time.time(),
                "attempts": status.attempts,
            }

            if status.start_time:
                log_data["start_time"] = status.start_time

            if status.end_time:
                log_data["end_time"] = status.end_time
                log_data["duration"] = status.end_time - (status.start_time or 0)

            if status.error:
                log_data["error"] = status.error

            logger.info(json.dumps(log_data))
