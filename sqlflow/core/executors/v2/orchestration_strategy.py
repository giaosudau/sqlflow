"""Orchestration strategies for V2 Executor.

This module implements Martin Fowler's Strategy Pattern for different
execution approaches while maintaining a consistent interface.

Future extensibility: Easy to add ParallelOrchestrationStrategy,
DistributedOrchestrationStrategy, etc.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from sqlflow.logging import get_logger

from .database_session import DatabaseSessionManager

logger = get_logger(__name__)


class PipelineExecutionError(Exception):
    """Pipeline execution failed."""


class OrchestrationStrategy(ABC):
    """
    Abstract strategy for pipeline orchestration.

    Following Martin Fowler's Strategy Pattern:
    Defines the algorithm family interface that can be used interchangeably.
    """

    @abstractmethod
    def execute_pipeline(
        self,
        plan: List[Dict[str, Any]],
        context: Any,  # ExecutionContext
        db_session: DatabaseSessionManager,
    ) -> List[Any]:  # List[StepExecutionResult]
        """Execute pipeline according to strategy's approach."""


class SequentialOrchestrationStrategy(OrchestrationStrategy):
    """
    Sequential execution strategy.

    Following Kent Beck's simple design principle:
    Start with the simplest thing that could possibly work.
    """

    def execute_pipeline(
        self,
        plan: List[Dict[str, Any]],
        context: Any,
        db_session: DatabaseSessionManager,
    ) -> List[Any]:
        """Execute steps sequentially with monitoring and error handling."""
        results = []

        for i, step_dict in enumerate(plan):
            result = self._execute_single_step(step_dict, i, plan, context)
            results.append(result)

            if not result.is_successful():
                logger.error(
                    "❌ Step %s failed: %s", result.step_id, result.error_message
                )
                raise PipelineExecutionError(f"Step {result.step_id} failed")

            # Commit changes using existing engine patterns
            db_session.commit_changes()
            logger.info("✅ Step %s completed", result.step_id)

        return results

    def _execute_single_step(
        self,
        step_dict: Dict[str, Any],
        index: int,
        plan: List[Dict[str, Any]],
        context: Any,
    ) -> Any:
        """Execute one step using existing patterns.

        Following Andy Hunt & Dave Thomas' DRY principle:
        Single method for step execution logic.
        """
        from sqlflow.core.executors.v2.handlers.factory import StepHandlerFactory
        from sqlflow.core.executors.v2.steps import create_step_from_dict

        step_id = step_dict.get("id", f"step_{index}")
        step_type = step_dict.get("type", "unknown")

        logger.info(
            "🔄 Executing step %d/%d: %s (%s)", index + 1, len(plan), step_id, step_type
        )

        try:
            step = create_step_from_dict(step_dict)
            handler = StepHandlerFactory.get_handler(step.type)
            result = handler.execute(step, context)
            return result

        except Exception as e:
            logger.error("Step execution failed: %s", e)
            raise PipelineExecutionError(str(e)) from e


class VariableSubstitutionMixin:
    """
    Mixin for variable substitution functionality.

    Following Andy Hunt & Dave Thomas' orthogonality principle:
    Variable substitution is a separate concern that can be mixed in.
    """

    def substitute_variables(
        self, plan: List[Dict[str, Any]], variable_manager: Any
    ) -> List[Dict[str, Any]]:
        """Apply variable substitution to the entire plan.

        Following Kent Beck's simple design:
        Clear, single-purpose method.
        """
        return variable_manager.substitute(plan)


# Future Strategy implementations can be added here:
#
# class ParallelOrchestrationStrategy(OrchestrationStrategy):
#     """Parallel execution strategy for independent steps."""
#
#     def execute_pipeline(self, plan, context, db_session):
#         # Build dependency graph
#         # Execute independent steps in parallel
#         # Handle synchronization points
#         pass
#
# class DistributedOrchestrationStrategy(OrchestrationStrategy):
#     """Distributed execution strategy using Celery/Dask."""
#
#     def execute_pipeline(self, plan, context, db_session):
#         # Serialize context for remote execution
#         # Submit tasks to distributed workers
#         # Collect and aggregate results
#         pass
