"""Source Step Executor following Single Responsibility Principle.

Handles source definition steps that register data sources with the execution context.
This executor creates and validates source connectors, making them available for load steps.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict

from sqlflow.logging import get_logger

from ..protocols.core import ExecutionContext, Step
from ..results.models import StepResult
from .base import BaseStepExecutor

logger = get_logger(__name__)


@dataclass
class SourceStepExecutor(BaseStepExecutor):
    """Executes source definition steps - SINGLE responsibility.

    Handles only source definition operations:
    - Source connector creation and validation
    - Source registration with execution context
    - Connection testing and schema discovery
    """

    def can_execute(self, step: Any) -> bool:
        """Check if this executor can handle the step."""
        return hasattr(step, "step_type") and step.step_type in (
            "source",
            "source_definition",
        )

    def execute(self, step: Step, context: ExecutionContext) -> StepResult:
        """Execute source definition step with focused implementation."""
        start_time = time.time()
        step_id = getattr(step, "id", "unknown")

        try:
            logger.info(f"Executing source definition step: {step_id}")

            # Extract source details
            source_name, connector_type, configuration = self._extract_source_details(
                step
            )

            # Create and validate connector
            with self._observability_scope(context, "source_definition"):
                connector = self._create_and_validate_connector(
                    source_name, connector_type, configuration, context
                )

                # Register source with context if supported
                self._register_source_with_context(
                    source_name, connector_type, configuration, context
                )

            execution_time = time.time() - start_time

            return self._create_result(
                step_id=step_id,
                status="success",
                message=f"Successfully defined source '{source_name}' with connector type '{connector_type}'",
                execution_time=execution_time,
                metadata={
                    "source_name": source_name,
                    "connector_type": connector_type,
                    "configuration_keys": (
                        list(configuration.keys()) if configuration else []
                    ),
                },
            )

        except Exception as error:
            execution_time = time.time() - start_time
            logger.error(f"Source definition step {step_id} failed: {error}")
            return self._handle_execution_error(step_id, error, execution_time)

    def _extract_source_details(self, step: Step) -> tuple[str, str, Dict[str, Any]]:
        """Extract source definition details with validation."""
        if hasattr(step, "name"):
            # New dataclass step
            source_name = getattr(step, "name", "")
            connector_type = getattr(step, "connector_type", "")
            configuration = getattr(step, "configuration", {}) or {}
        else:
            raise ValueError(f"Invalid step format: {type(step)}")

        # Validation
        if not source_name:
            raise ValueError("Source definition must have a name")
        if not connector_type:
            raise ValueError("Source definition must specify a connector_type")

        return source_name, connector_type, configuration

    def _create_and_validate_connector(
        self,
        source_name: str,
        connector_type: str,
        configuration: Dict[str, Any],
        context: ExecutionContext,
    ):
        """Create and validate the source connector."""
        # Get connector registry from context
        if not hasattr(context, "connector_registry"):
            raise RuntimeError("Context missing connector_registry")

        try:
            # Debug logging
            logger.debug(
                f"Creating {connector_type} connector for source '{source_name}' with config: {configuration}"
            )

            # Create connector using enhanced registry if available
            connector_registry = getattr(context, "connector_registry")
            if hasattr(connector_registry, "create_source_connector"):
                connector = connector_registry.create_source_connector(
                    connector_type, configuration
                )
            else:
                # Fallback to basic registry
                connector_class = connector_registry.get(connector_type)
                connector = connector_class()
                if hasattr(connector, "configure"):
                    connector.configure(configuration)

            # Test connection if supported
            if hasattr(connector, "test_connection"):
                connection_result = connector.test_connection()
                if not connection_result.success:
                    logger.warning(
                        f"Connection test failed for source '{source_name}': {connection_result.message}"
                    )
                else:
                    logger.debug(f"Connection test passed for source '{source_name}'")

            return connector

        except Exception as e:
            raise RuntimeError(
                f"Failed to create connector for source '{source_name}' "
                f"with type '{connector_type}': {str(e)}"
            ) from e

    def _register_source_with_context(
        self,
        source_name: str,
        connector_type: str,
        configuration: Dict[str, Any],
        context: ExecutionContext,
    ) -> None:
        """Register source definition with execution context if supported."""
        # Store source definition in context using the proper method
        if hasattr(context, "add_source_definition"):
            source_definition = {
                "name": source_name,
                "connector_type": connector_type,
                "configuration": configuration,
            }
            context.add_source_definition(source_name, source_definition)
            logger.debug(f"Registered source definition '{source_name}' with context")

        # Register with connector engine if available (legacy support)
        connector_engine = getattr(context, "_connector_engine", None)
        if connector_engine:
            connector_engine.register_connector(
                source_name, connector_type, configuration
            )
            logger.debug(f"Registered source '{source_name}' with connector engine")

    def _observability_scope(self, context: ExecutionContext, scope_name: str):
        """Create observability scope for measurements."""
        observability = getattr(context, "observability", None)
        if observability is not None and hasattr(observability, "measure_scope"):
            return observability.measure_scope(scope_name)
        else:
            # Fallback no-op context manager
            from contextlib import nullcontext

            return nullcontext()
