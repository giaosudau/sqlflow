"""Compatibility bridge for seamless V1/V2 executor switching.

Following the Zen of Python:
- Explicit is better than implicit
- Simple is better than complex
- Errors should never pass silently

This module provides a seamless bridge between V1 and V2 executors,
enabling gradual rollout and automatic fallback capabilities.
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlflow.core.executors.base_executor import BaseExecutor
from sqlflow.logging import get_logger

from .feature_flags import FeatureFlag, is_v2_enabled, should_use_v2_executor

# Import executors at module level for test compatibility
try:
    from sqlflow.core.executors.local_executor import LocalExecutor
except ImportError:
    LocalExecutor = None

try:
    from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator
except ImportError:
    LocalOrchestrator = None

logger = get_logger(__name__)


class ExecutorCompatibilityError(Exception):
    """Error in executor compatibility bridge."""


class V1V2CompatibilityBridge(BaseExecutor):
    """
    Compatibility bridge providing seamless V1/V2 switching.

    Following the Single Responsibility Principle:
    This class only handles executor selection and result compatibility.
    """

    def __init__(self, enable_fallback: bool = True, **kwargs):
        """Initialize the compatibility bridge."""
        super().__init__()
        self._v1_executor = None
        self._v2_executor = None

        # Filter options to avoid passing bridge-specific params to executors
        execution_options = {
            k: v for k, v in kwargs.items() if k not in ["enable_fallback"]
        }
        self._execution_options = execution_options
        self._fallback_enabled = enable_fallback

        # Initialize metrics tracking
        self._execution_metrics = {
            "total_executions": 0,
            "v1_executions": 0,
            "v2_executions": 0,
            "fallback_count": 0,
            "v2_success_rate": 0.0,
            "last_execution": None,
        }

        logger.info("V1/V2 Compatibility Bridge initialized")

    def _get_v1_executor(self):
        """Lazy initialization of V1 executor."""
        if self._v1_executor is None:
            from sqlflow.core.executors.local_executor import LocalExecutor

            self._v1_executor = LocalExecutor(**self._execution_options)
            logger.debug("V1 LocalExecutor initialized")
        return self._v1_executor

    def _get_v2_executor(self):
        """Lazy initialization of V2 executor."""
        if self._v2_executor is None:
            try:
                from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator

                self._v2_executor = LocalOrchestrator(**self._execution_options)
                logger.debug("V2 LocalOrchestrator initialized")
            except ImportError as e:
                logger.warning(f"V2 orchestrator not available: {e}")
                if not self._fallback_enabled:
                    raise ExecutorCompatibilityError(
                        f"V2 executor requested but not available: {e}"
                    )
                return None
        return self._v2_executor

    def execute(
        self,
        plan: List[Dict[str, Any]],
        variables: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Execute pipeline with automatic V1/V2 selection.

        Following 'Explicit is better than implicit':
        Clear decision logic for executor selection.
        """
        # Generate execution ID for consistent rollout behavior
        execution_id = self._generate_execution_id(plan)

        # Determine which executor to use
        use_v2 = should_use_v2_executor(execution_id)

        if use_v2:
            logger.info(f"🚀 Using V2 executor for execution {execution_id}")
            return self._execute_with_v2(plan, variables, execution_id, **kwargs)
        else:
            logger.info(f"🔄 Using V1 executor for execution {execution_id}")
            return self._execute_with_v1(plan, variables, execution_id, **kwargs)

    def _execute_with_v2(
        self,
        plan: List[Dict[str, Any]],
        variables: Optional[Dict[str, Any]],
        execution_id: str,
        **kwargs,
    ) -> Dict[str, Any]:
        """Execute with V2 orchestrator, with fallback to V1 if enabled."""
        v2_executor = self._get_v2_executor()

        if v2_executor is None:
            if self._fallback_enabled:
                logger.warning(
                    f"V2 executor unavailable, falling back to V1 for {execution_id}"
                )
                return self._execute_with_v1(plan, variables, execution_id, **kwargs)
            else:
                raise ExecutorCompatibilityError(
                    "V2 executor requested but unavailable"
                )

        try:
            start_time = time.time()
            # Handle both single arg and keyword arg patterns for compatibility
            result = (
                v2_executor.execute(plan, **kwargs)
                if variables is None
                else v2_executor.execute(plan, variables, **kwargs)
            )
            execution_time = time.time() - start_time

            # Enhance result with compatibility metadata
            enhanced_result = self._enhance_v2_result(
                result, execution_id, execution_time
            )

            # Track successful V2 execution
            self._track_execution_metrics("v2", success=True)

            logger.info(
                f"✅ V2 execution completed in {execution_time:.2f}s for {execution_id}"
            )
            return enhanced_result

        except Exception as e:
            if self._fallback_enabled:
                # Log the fallback for monitoring
                logger.error(
                    f"V2 execution failed, falling back to V1 for {execution_id}: {e}"
                )
                # Also log at warning level for monitoring tools
                logger.warning(f"V2 execution failed, falling back to V1: {e}")
                # Track the fallback attempt
                self._track_execution_metrics("v2", success=False, is_fallback=True)
                try:
                    # Execute V1 without tracking metrics again (fallback is already tracked)
                    result = self._execute_with_v1_internal(
                        plan, variables, execution_id, track_metrics=False, **kwargs
                    )
                    return result
                except Exception as v1_error:
                    logger.error(
                        f"V1 execution also failed for {execution_id}: {v1_error}"
                    )
                    raise ExecutorCompatibilityError(
                        f"Both V1 and V2 executors failed: V2: {e}, V1: {v1_error}"
                    )
            else:
                logger.error(f"V2 execution failed for {execution_id}: {e}")
                raise

    def _execute_with_v1(
        self,
        plan: List[Dict[str, Any]],
        variables: Optional[Dict[str, Any]],
        execution_id: str,
        **kwargs,
    ) -> Dict[str, Any]:
        """Execute with V1 executor."""
        return self._execute_with_v1_internal(
            plan, variables, execution_id, track_metrics=True, **kwargs
        )

    def _execute_with_v1_internal(
        self,
        plan: List[Dict[str, Any]],
        variables: Optional[Dict[str, Any]],
        execution_id: str,
        track_metrics: bool = True,
        **kwargs,
    ) -> Dict[str, Any]:
        """Execute with V1 executor with optional metrics tracking."""
        try:
            v1_executor = self._get_v1_executor()
        except Exception as e:
            logger.error(f"V1 executor creation failed for {execution_id}: {e}")
            raise ExecutorCompatibilityError(
                f"Both V1 and V2 executors failed: V1 creation failed: {e}"
            )

        try:
            start_time = time.time()
            # Handle both single arg and keyword arg patterns for compatibility
            result = (
                v1_executor.execute(plan, **kwargs)
                if variables is None
                else v1_executor.execute(plan, variables, **kwargs)
            )
            execution_time = time.time() - start_time

            # Enhance result with compatibility metadata
            enhanced_result = self._enhance_v1_result(
                result, execution_id, execution_time
            )

            # Track V1 execution only if requested
            if track_metrics:
                self._track_execution_metrics("v1", success=True)

            logger.info(
                f"✅ V1 execution completed in {execution_time:.2f}s for {execution_id}"
            )
            return enhanced_result

        except Exception as e:
            logger.error(f"V1 execution failed for {execution_id}: {e}")
            raise

    def _generate_execution_id(self, plan: List[Dict[str, Any]]) -> str:
        """Generate consistent execution ID from plan."""
        import hashlib

        # Create hash from plan structure for consistency
        plan_str = str(
            sorted([step.get("id", step.get("type", "unknown")) for step in plan])
        )
        timestamp = int(time.time())

        hash_input = f"{plan_str}_{timestamp}"
        execution_hash = hashlib.md5(hash_input.encode()).hexdigest()[:8]

        return f"exec_{execution_hash}"

    def _enhance_v2_result(
        self, result: Dict[str, Any], execution_id: str, execution_time: float
    ) -> Dict[str, Any]:
        """Enhance V2 result with compatibility metadata."""
        enhanced_result = result.copy()

        # Add compatibility metadata
        enhanced_result.update(
            {
                "executor_version": "v2",
                "execution_id": execution_id,
                "total_execution_time": execution_time,
                "compatibility_bridge": True,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

        # Ensure V1-compatible structure
        if "step_results" in result:
            # Convert step_results to execution_results
            enhanced_result["execution_results"] = {
                step["step_id"]: step for step in result["step_results"]
            }
            enhanced_result["executed_steps"] = [
                step["step_id"] for step in result["step_results"]
            ]
            enhanced_result["total_steps"] = len(result["step_results"])
        elif "steps" in result:
            # Convert steps to execution_results
            enhanced_result["execution_results"] = {
                step["id"]: step for step in result["steps"]
            }
            enhanced_result["executed_steps"] = [step["id"] for step in result["steps"]]
            enhanced_result["total_steps"] = len(result["steps"])
        elif "execution_results" in result:
            enhanced_result["executed_steps"] = list(result["execution_results"].keys())
            enhanced_result["total_steps"] = len(result["execution_results"])

        return enhanced_result

    def _enhance_v1_result(
        self, result: Dict[str, Any], execution_id: str, execution_time: float
    ) -> Dict[str, Any]:
        """Enhance V1 result with compatibility metadata."""
        enhanced_result = result.copy()

        # Add compatibility metadata
        enhanced_result.update(
            {
                "executor_version": "v1",
                "execution_id": execution_id,
                "total_execution_time": execution_time,
                "compatibility_bridge": True,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

        return enhanced_result

    # BaseExecutor compatibility methods
    def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute single step using appropriate executor."""
        return self.execute([step])

    def can_resume(self) -> bool:
        """Check if resume is supported by current executor."""
        if should_use_v2_executor():
            v2_executor = self._get_v2_executor()
            return v2_executor.can_resume() if v2_executor else False
        else:
            v1_executor = self._get_v1_executor()
            return v1_executor.can_resume()

    def resume(self) -> Dict[str, Any]:
        """Resume execution using appropriate executor."""
        if should_use_v2_executor():
            v2_executor = self._get_v2_executor()
            if v2_executor:
                return v2_executor.resume()
            elif self._fallback_enabled:
                return self._get_v1_executor().resume()
            else:
                raise ExecutorCompatibilityError("V2 executor unavailable for resume")
        else:
            return self._get_v1_executor().resume()

    def get_execution_state(self) -> Dict[str, Any]:
        """Get execution state from current executor."""
        state: Dict[str, Any] = {
            "compatibility_bridge": True,
            "v2_enabled": should_use_v2_executor(),
            "fallback_enabled": self._fallback_enabled,
        }

        # Add executor-specific state
        if should_use_v2_executor():
            v2_executor = self._get_v2_executor()
            if v2_executor:
                v2_state = v2_executor.get_execution_state()
                if isinstance(v2_state, dict):
                    state.update(v2_state)
            else:
                state["executor"] = "v1_fallback"
        else:
            state["executor"] = "v1"
            state["phase"] = "V1 LocalExecutor"

        return state

    def _track_execution_metrics(
        self, executor_type: str, success: bool = True, is_fallback: bool = False
    ):
        """Track execution metrics for monitoring."""
        self._execution_metrics["total_executions"] += 1

        if executor_type == "v1":
            self._execution_metrics["v1_executions"] += 1
        elif executor_type == "v2" and not is_fallback:
            # Only count as V2 execution if it was successful (no fallback)
            self._execution_metrics["v2_executions"] += 1

        if is_fallback:
            self._execution_metrics["fallback_count"] += 1

        # Update success rate
        total_v2 = self._execution_metrics["v2_executions"]
        if total_v2 > 0:
            successful_v2 = total_v2 - self._execution_metrics["fallback_count"]
            self._execution_metrics["v2_success_rate"] = (
                successful_v2 / total_v2
            ) * 100.0

        self._execution_metrics["last_execution"] = datetime.utcnow().isoformat()

    def get_execution_metrics(self) -> Dict[str, Any]:
        """Get execution metrics for monitoring and analysis."""
        # Return actual metrics from tracking
        metrics = self._execution_metrics.copy()
        metrics.update(
            {
                "compatibility_bridge_active": True,
                "v2_enabled": should_use_v2_executor(),
            }
        )
        return metrics


# Factory function for executor creation
def create_executor(
    force_v1: bool = False, force_v2: bool = False, **kwargs
) -> BaseExecutor:
    """
    Factory function to create the appropriate executor.

    Following 'There should be one obvious way to do it'.
    This is the main entry point for executor creation.
    """
    # Check for conflicting flags
    if force_v1 and force_v2:
        raise ExecutorCompatibilityError(
            "Cannot force both V1 and V2 executors simultaneously"
        )

    # Handle explicit force flags
    if force_v2:
        logger.debug("Creating V2 executor (explicitly requested)")
        return create_v2_executor(**kwargs)
    elif force_v1:
        logger.debug("Creating V1 executor (explicitly requested)")
        return create_v1_executor(**kwargs)

    # Check if bridge is needed
    if is_v2_enabled(FeatureFlag.V2_ORCHESTRATOR_ENABLED):
        logger.debug("Creating V1/V2 compatibility bridge")
        return V1V2CompatibilityBridge(**kwargs)
    else:
        logger.debug("Creating V1 executor directly")
        return create_v1_executor(**kwargs)


# Convenience functions for direct executor access
def create_v1_executor(**kwargs) -> BaseExecutor:
    """Create V1 executor directly (for testing and comparison)."""
    if LocalExecutor is None:
        from sqlflow.core.executors.local_executor import (
            LocalExecutor as _LocalExecutor,
        )

        return _LocalExecutor(**kwargs)
    return LocalExecutor(**kwargs)


def create_v2_executor(**kwargs) -> BaseExecutor:
    """Create V2 executor directly (for testing and comparison)."""
    if LocalOrchestrator is None:
        from sqlflow.core.executors.v2.orchestrator import (
            LocalOrchestrator as _LocalOrchestrator,
        )

        return _LocalOrchestrator(**kwargs)
    return LocalOrchestrator(**kwargs)
