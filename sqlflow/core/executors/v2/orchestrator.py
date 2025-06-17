"""V2 Orchestrator: Clean, maintainable pipeline execution.

Refactored following expert recommendations:
- Martin Fowler: Parameter Objects, Strategy Pattern, Factory Pattern
- Kent Beck: Simple Design, Small Methods, Test-Driven Development
- Robert Martin: SOLID Principles, Clean Code, Clean Architecture
- Andy Hunt & Dave Thomas: DRY, Orthogonality, Pragmatic Programming
- Jon Bentley: Performance Awareness, Algorithm Efficiency

Simple is better than complex.
Explicit is better than implicit.
Readability counts.

Now leveraging the sophisticated DuckDB engine infrastructure!
"""

import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlflow.core.executors.base_executor import BaseExecutor
from sqlflow.logging import get_logger

# Import refactored modules
from .database_session import DatabaseSessionManager
from .execution_context import ExecutionContextFactory
from .execution_request import (
    ExecutionEnvironment,
    ExecutionSummary,
    OrchestrationRequest,
)
from .orchestration_strategy import (
    OrchestrationStrategy,
    SequentialOrchestrationStrategy,
    VariableSubstitutionMixin,
)
from .result_builder import ExecutionResultBuilder

# Initialize logger first (Zen: Simple is better than complex)
logger = get_logger(__name__)

# Ensure handlers are registered (separated from main logic)
from .handlers_registration import ensure_handlers_registered

ensure_handlers_registered()


class LocalOrchestrator(BaseExecutor, VariableSubstitutionMixin):
    """
    V2 Orchestrator implementing expert design recommendations.

    Following Robert Martin's Clean Architecture:
    - Core business logic separated from framework details
    - Dependencies point inward toward business rules
    - Testable through dependency injection

    Following Martin Fowler's patterns:
    - Strategy Pattern for different execution approaches
    - Parameter Objects for complex method signatures
    - Factory Pattern for object creation

    Following Kent Beck's principles:
    - Small, focused methods with clear names
    - Test-friendly design with dependency injection
    - Simple design that can evolve
    """

    def __init__(
        self,
        session_factory: type = DatabaseSessionManager,
        context_factory: type = ExecutionContextFactory,
        result_builder: type = ExecutionResultBuilder,
        strategy: Optional[OrchestrationStrategy] = None,
        **kwargs,
    ):
        """Initialize orchestrator with injectable dependencies.

        Following Kent Beck's testing principle:
        Dependencies should be injectable for easy testing.
        """
        super().__init__()
        self._session_factory = session_factory
        self._context_factory = context_factory
        self._result_builder = result_builder
        self._strategy = strategy or SequentialOrchestrationStrategy()
        self._config = kwargs

        logger.info("V2 Orchestrator initialized with clean architecture")

    def execute(
        self,
        plan: List[Dict[str, Any]],
        variables: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Execute pipeline using clean architecture patterns.

        Following Martin Fowler's Parameter Object pattern:
        Complex parameters are encapsulated in request objects.

        Following Robert Martin's Template Method pattern:
        High-level algorithm with delegated implementation details.
        """
        # Create request object (Fowler's Parameter Object)
        request = OrchestrationRequest(
            plan=plan,
            variables=variables,
            profile=kwargs.get("profile", {}),
            execution_options=kwargs,
        )

        # Handle empty pipeline case early
        if not request.plan:
            return self._result_builder.build_empty_result()

        # Execute pipeline with proper resource management
        return self._execute_pipeline_safely(request)

    def _execute_pipeline_safely(self, request: OrchestrationRequest) -> Dict[str, Any]:
        """Execute pipeline with comprehensive error handling and resource cleanup.

        Following Andy Hunt & Dave Thomas' exception handling:
        Clean separation between happy path and error handling.
        """
        run_id = f"v2_run_{uuid.uuid4().hex[:8]}"
        start_time = time.time()
        pipeline_start_time = datetime.utcnow()

        logger.info("🚀 Starting pipeline execution - Run ID: %s", run_id)
        logger.info("📋 Pipeline has %d steps", len(request.plan))

        # Create execution environment
        environment = ExecutionEnvironment.from_request(request, run_id)

        # Initialize observability
        from sqlflow.core.executors.v2.observability import ObservabilityManager

        observability = ObservabilityManager(run_id=run_id)

        # Create database session with proper cleanup
        db_session = self._session_factory(run_id, environment.profile)

        try:
            return self._execute_with_observability(
                request,
                environment,
                observability,
                db_session,
                start_time,
                pipeline_start_time,
            )
        except Exception as e:
            return self._result_builder.build_failure_result(run_id, e, observability)
        finally:
            # Always clean up resources (Andy Hunt & Dave Thomas)
            db_session.close()

    def _execute_with_observability(
        self,
        request: OrchestrationRequest,
        environment: ExecutionEnvironment,
        observability: Any,
        db_session: DatabaseSessionManager,
        start_time: float,
        pipeline_start_time: datetime,
    ) -> Dict[str, Any]:
        """Execute pipeline with full observability and metrics collection.

        Following Kent Beck's simple design:
        One method, one responsibility - orchestrated execution.
        """
        # Create execution context
        context = self._context_factory.create_context(
            db_session=db_session,
            observability=observability,
            variables=environment.variables,
            **environment.execution_options,
        )

        # Apply variable substitution
        substituted_plan = self.substitute_variables(
            request.plan, context.variable_manager
        )

        # Execute steps using strategy pattern
        results = self._strategy.execute_pipeline(substituted_plan, context, db_session)

        # Build comprehensive result
        execution_summary = ExecutionSummary(
            run_id=environment.run_id,
            results=results,
            observability=observability,
            total_time=time.time() - start_time,
            start_time=pipeline_start_time,
            engine_stats=db_session.get_engine_stats(),
            performance_report={},  # Future: Add performance analytics
        )

        return self._result_builder.build_success_result(execution_summary)

    # Required BaseExecutor methods (maintain backward compatibility)
    def execute_step(self, step):
        """Execute single step (delegate to main execute)."""
        return self.execute([step])

    def can_resume(self) -> bool:
        """Resume not supported in Phase 3."""
        return False

    def resume(self) -> Dict[str, Any]:
        """Resume execution (not supported in Phase 3)."""
        return {
            "status": "failed",
            "error": "Resume functionality not supported in Phase 3",
            "executed_steps": [],
            "total_steps": 0,
        }

    def get_execution_state(self):
        """Get execution state."""
        return {"phase": "Phase 3 - V2 Orchestrator with Expert Design Patterns"}
