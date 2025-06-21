"""Main orchestration coordinator - Clean Architecture Implementation.

This module embodies the principles from the refactoring plan:
- Single Responsibility: Coordinates execution, doesn't implement details
- Dependency Inversion: Depends on abstractions (protocols)
- Open/Closed: Easy to extend with new step types and strategies
- Composition over Inheritance: Uses dependency injection

Less than 200 lines as mandated by Raymond Hettinger.
"""

import logging
import time
from typing import Any, Dict, List, Optional

from ..execution import ExecutionContext, StepExecutionResult
from ..protocols import ExecutionStrategy, StepExecutor
from ..validation import validate_pipeline, validate_variables
from ..variables import substitute_in_step

logger = logging.getLogger(__name__)


class PipelineCoordinator:
    """Clean pipeline coordinator with dependency injection.

    This class implements the Coordinator pattern:
    - Orchestrates the execution flow
    - Delegates actual work to specialized components
    - Manages the execution context
    - Handles error propagation and cleanup

    Following Clean Architecture principles:
    - Dependencies point inward (to abstractions)
    - Core business logic is isolated
    - Easy to test with mock implementations
    """

    def __init__(
        self,
        strategy: ExecutionStrategy,
        step_executors: List[StepExecutor],
        enable_validation: bool = True,
    ):
        """Initialize coordinator with dependencies.

        Args:
            strategy: Strategy for executing steps
            step_executors: List of step executors
            enable_validation: Whether to validate inputs
        """
        self._strategy = strategy
        self._step_executors = step_executors
        self._enable_validation = enable_validation

        logger.info(
            f"PipelineCoordinator initialized with {len(step_executors)} executors"
        )

    def execute_pipeline(
        self,
        steps: List[Dict[str, Any]],
        context: ExecutionContext,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute pipeline with clean error handling.

        Args:
            steps: Pipeline steps to execute
            context: Execution context
            variables: Additional variables for substitution

        Returns:
            Execution result with status and details
        """
        start_time = time.time()
        execution_id = context.execution_id

        logger.info(
            f"Starting pipeline execution {execution_id} with {len(steps)} steps"
        )

        try:
            # Validation phase
            if self._enable_validation:
                validation_result = self._validate_inputs(steps, variables)
                if not validation_result["is_valid"]:
                    return self._build_validation_error_result(
                        validation_result, start_time
                    )

            # Variable substitution phase
            processed_context = self._prepare_context(context, variables)
            processed_steps = self._substitute_variables(
                steps, processed_context.variables
            )

            # Execution phase
            step_results = self._strategy.execute_steps(
                processed_steps, processed_context
            )

            # Result building phase
            return self._build_success_result(
                step_results, start_time, processed_context
            )

        except Exception as e:
            logger.error(f"Pipeline execution {execution_id} failed: {e}")
            return self._build_error_result(str(e), start_time, execution_id)

    def _validate_inputs(
        self, steps: List[Dict[str, Any]], variables: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate pipeline inputs."""
        # Validate pipeline structure
        pipeline_validation = validate_pipeline(steps)

        # Validate variables if provided
        variables_validation = None
        if variables is not None:
            variables_validation = validate_variables(variables)

        errors = []
        warnings = []

        if not pipeline_validation.is_valid:
            errors.extend(
                [f"{e.field}: {e.message}" for e in pipeline_validation.errors]
            )
        warnings.extend(pipeline_validation.warnings)

        if variables_validation and not variables_validation.is_valid:
            errors.extend(
                [f"{e.field}: {e.message}" for e in variables_validation.errors]
            )
        if variables_validation:
            warnings.extend(variables_validation.warnings)

        return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def _prepare_context(
        self, context: ExecutionContext, variables: Optional[Dict[str, Any]]
    ) -> ExecutionContext:
        """Prepare execution context with variables."""
        if variables:
            return context.with_variables(variables)
        return context

    def _substitute_variables(
        self, steps: List[Dict[str, Any]], variables: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Substitute variables in all steps."""
        if not variables:
            return steps

        return [substitute_in_step(step, variables) for step in steps]

    def _build_validation_error_result(
        self, validation_result: Dict[str, Any], start_time: float
    ) -> Dict[str, Any]:
        """Build result for validation errors."""
        return {
            "status": "validation_error",
            "errors": validation_result["errors"],
            "warnings": validation_result["warnings"],
            "executed_steps": [],
            "step_results": [],
            "total_steps": 0,
            "execution_time": time.time() - start_time,
        }

    def _build_success_result(
        self,
        step_results: List[StepExecutionResult],
        start_time: float,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        """Build successful execution result."""
        failed_steps = [r for r in step_results if r.status == "error"]
        overall_status = "failed" if failed_steps else "success"

        return {
            "status": overall_status,
            "executed_steps": [r.step_id for r in step_results],
            "step_results": [
                {
                    "step_id": r.step_id,
                    "status": r.status,
                    "message": r.message,
                    "error": r.error,
                    "execution_time": r.execution_time,
                    "data": r.data,
                }
                for r in step_results
            ],
            "total_steps": len(step_results),
            "execution_time": time.time() - start_time,
            "execution_id": context.execution_id,
            "source_definitions": dict(context.source_definitions),
        }

    def _build_error_result(
        self, error_message: str, start_time: float, execution_id: str
    ) -> Dict[str, Any]:
        """Build error result for unexpected failures."""
        return {
            "status": "error",
            "error": error_message,
            "executed_steps": [],
            "step_results": [],
            "total_steps": 0,
            "execution_time": time.time() - start_time,
            "execution_id": execution_id,
        }
