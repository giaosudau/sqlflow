"""Execution-related exceptions for SQLFlow V2 Executor.

These exceptions handle errors during pipeline and step execution.
"""

from typing import Any, Dict, List, Optional

from .base import SQLFlowError


class StepExecutionError(SQLFlowError):
    """Error during individual step execution."""

    def __init__(
        self,
        step_id: str,
        step_type: str,
        message: str,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        suggested_actions: Optional[List[str]] = None,
        original_error: Optional[Exception] = None,
        recoverable: bool = True,
    ):
        # Default suggested actions for step execution errors
        default_actions = [
            f"Check {step_type} step configuration for '{step_id}'",
            "Verify input data and dependencies",
            "Review error logs for details",
        ]

        actions = suggested_actions or default_actions
        step_context = {"step_id": step_id, "step_type": step_type}

        if context:
            step_context.update(context)

        if original_error:
            step_context["original_error"] = str(original_error)
            step_context["original_error_type"] = type(original_error).__name__

        super().__init__(
            message=f"Step '{step_id}' ({step_type}) failed: {message}",
            error_code=error_code,
            context=step_context,
            suggested_actions=actions,
            recoverable=recoverable,
        )

        self.step_id = step_id
        self.step_type = step_type
        self.original_error = original_error


class PipelineExecutionError(SQLFlowError):
    """Error during pipeline execution."""

    def __init__(
        self,
        pipeline_id: str,
        message: str,
        failed_steps: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
        suggested_actions: Optional[List[str]] = None,
        recoverable: bool = False,
    ):
        default_actions = [
            "Review failed steps and their dependencies",
            "Check pipeline configuration",
            "Verify data sources and destinations",
            "Consider running steps individually for debugging",
        ]

        actions = suggested_actions or default_actions
        pipeline_context = {
            "pipeline_id": pipeline_id,
            "failed_steps": failed_steps or [],
        }

        if context:
            pipeline_context.update(context)

        super().__init__(
            message=f"Pipeline '{pipeline_id}' execution failed: {message}",
            context=pipeline_context,
            suggested_actions=actions,
            recoverable=recoverable,
        )

        self.pipeline_id = pipeline_id
        self.failed_steps = failed_steps or []


class DependencyResolutionError(SQLFlowError):
    """Error resolving step dependencies."""

    def __init__(
        self,
        step_id: str,
        missing_dependencies: Optional[List[str]] = None,
        circular_dependencies: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        if missing_dependencies:
            message = f"Missing dependencies for step '{step_id}': {', '.join(missing_dependencies)}"
            actions = [
                "Ensure all dependent steps are included in pipeline",
                "Check step names for typos",
                "Verify step order and execution plan",
            ]
        elif circular_dependencies:
            message = f"Circular dependency detected involving step '{step_id}': {' -> '.join(circular_dependencies)}"
            actions = [
                "Review step dependencies to remove circular references",
                "Reorganize pipeline logic to avoid cycles",
                "Consider breaking complex dependencies into smaller steps",
            ]
        else:
            message = f"Dependency resolution failed for step '{step_id}'"
            actions = [
                "Check step dependencies configuration",
                "Verify all referenced steps exist",
            ]

        dep_context = {
            "step_id": step_id,
            "missing_dependencies": missing_dependencies or [],
            "circular_dependencies": circular_dependencies or [],
        }

        if context:
            dep_context.update(context)

        super().__init__(
            message=message,
            context=dep_context,
            suggested_actions=actions,
            recoverable=False,  # Dependency issues usually require configuration changes
        )

        self.step_id = step_id
        self.missing_dependencies = missing_dependencies or []
        self.circular_dependencies = circular_dependencies or []


class StepTimeoutError(SQLFlowError):
    """Error when step execution exceeds timeout."""

    def __init__(
        self,
        step_id: str,
        step_type: str,
        timeout_seconds: float,
        actual_duration_seconds: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        duration_info = ""
        if actual_duration_seconds is not None:
            duration_info = f" (ran for {actual_duration_seconds:.1f}s)"

        message = (
            f"Step '{step_id}' timed out after {timeout_seconds:.1f}s{duration_info}"
        )

        actions = [
            f"Increase timeout for {step_type} steps",
            "Optimize step performance to reduce execution time",
            "Check for resource constraints or blocking operations",
            "Consider breaking large operations into smaller chunks",
        ]

        timeout_context = {
            "step_id": step_id,
            "step_type": step_type,
            "timeout_seconds": timeout_seconds,
            "actual_duration_seconds": actual_duration_seconds,
        }

        if context:
            timeout_context.update(context)

        super().__init__(
            message=message,
            context=timeout_context,
            suggested_actions=actions,
            recoverable=True,  # Can often be resolved by increasing timeout
        )

        self.step_id = step_id
        self.step_type = step_type
        self.timeout_seconds = timeout_seconds
        self.actual_duration_seconds = actual_duration_seconds
