"""Incremental Source Handler - V2 Architecture.

Following expert recommendations:
- Robert Martin: Single Responsibility Principle
- Kent Beck: Test-Driven Development
- Raymond Hettinger: Python excellence with dataclasses and typing
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class IncrementalSourceStep:
    """Typed representation of an incremental source step (Raymond Hettinger: Type Safety)."""

    id: str
    name: str
    connector_type: str
    cursor_field: str
    sync_mode: str = "incremental"
    params: Dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)


class IncrementalSourceHandler:
    """Handler for incremental source execution (Robert Martin: Single Responsibility)."""

    def execute(
        self, step: IncrementalSourceStep, context: ExecutionContext
    ) -> Dict[str, Any]:
        """Execute incremental source step with watermark management."""
        logger.info(f"Executing incremental source step: {step.id}")

        try:
            # Validate required fields (Kent Beck: Fail Fast)
            self._validate_step(step)

            # Get watermark value
            previous_watermark = self._get_previous_watermark(step, context)

            # Create incremental connector
            connector = self._create_incremental_connector(step, context)

            # Read incremental data
            object_name = self._extract_object_name(step)
            if hasattr(connector, "read_incremental"):
                data_chunks = list(
                    connector.read_incremental(
                        object_name=object_name,
                        cursor_field=step.cursor_field,
                        cursor_value=previous_watermark,
                        batch_size=10000,
                    )
                )
            else:
                # Fallback to regular read for connectors without incremental support
                data_chunks = list(connector.read(object_name=object_name))

            # Process data and get new watermark
            rows_processed = sum(len(chunk.arrow_table) for chunk in data_chunks)
            new_watermark = self._get_new_watermark(connector, data_chunks)

            # Update watermark
            if new_watermark:
                self._update_watermark(step, context, new_watermark)

            return {
                "status": "success",
                "step_id": step.id,
                "sync_mode": "incremental",
                "source_name": step.name,
                "rows_processed": rows_processed,
                "previous_watermark": previous_watermark,
                "new_watermark": new_watermark,
            }

        except Exception as e:
            logger.error(f"Incremental source step {step.id} failed: {e}")
            return {
                "status": "error",
                "step_id": step.id,
                "error_message": str(e),
            }

    def _validate_step(self, step: IncrementalSourceStep) -> None:
        """Validate incremental source step requirements (Robert Martin: Explicit Error Handling)."""
        if not step.cursor_field:
            raise ValueError("Incremental source requires cursor_field parameter")

        if step.sync_mode != "incremental":
            raise ValueError(
                f"Expected sync_mode='incremental', got '{step.sync_mode}'"
            )

    def _get_previous_watermark(
        self, step: IncrementalSourceStep, context: ExecutionContext
    ) -> Optional[str]:
        """Retrieve previous watermark value."""
        if not hasattr(context, "watermark_manager") or not context.watermark_manager:
            return None

        # Get pipeline name from context or use default
        pipeline_name = getattr(context, "pipeline_name", "test_pipeline")

        return context.watermark_manager.get_source_watermark(
            pipeline=pipeline_name, source=step.name, cursor_field=step.cursor_field
        )

    def _create_incremental_connector(
        self, step: IncrementalSourceStep, context: ExecutionContext
    ):
        """Create connector instance for incremental reading."""
        if not hasattr(context, "connector_registry"):
            raise RuntimeError("Context missing connector_registry")

        connector = context.connector_registry.create_source_connector(
            step.connector_type, step.params
        )

        if (
            not hasattr(connector, "supports_incremental")
            or not connector.supports_incremental()
        ):
            raise ValueError(
                f"Connector {step.connector_type} does not support incremental reads"
            )

        return connector

    def _extract_object_name(self, step: IncrementalSourceStep) -> str:
        """Extract object name from step parameters (Raymond Hettinger: Simple is better than complex)."""
        # Try different parameter names for object identification
        params = step.params

        # CSV connector
        if step.connector_type.lower() == "csv":
            return params.get("path", step.name)

        # PostgreSQL connector
        if step.connector_type.lower() in ["postgres", "postgresql"]:
            table = params.get("table", "")
            schema = params.get("schema", "")
            if schema and table:
                return f"{schema}.{table}"
            return table or step.name

        # Fallback to common parameters
        return (
            params.get("object_name")
            or params.get("table")
            or params.get("path")
            or step.name
        )

    def _get_new_watermark(
        self, connector, data_chunks: list[DataChunk]
    ) -> Optional[str]:
        """Get new watermark value from processed data."""
        if not data_chunks:
            return None

        # Use connector's cursor value method if available
        if hasattr(connector, "get_cursor_value"):
            return connector.get_cursor_value()

        return None

    def _update_watermark(
        self, step: IncrementalSourceStep, context: ExecutionContext, new_watermark: str
    ) -> None:
        """Update watermark value after successful processing."""
        if not hasattr(context, "watermark_manager") or not context.watermark_manager:
            return

        pipeline_name = getattr(context, "pipeline_name", "default_pipeline")

        context.watermark_manager.update_source_watermark(
            pipeline=pipeline_name,
            source=step.name,
            cursor_field=step.cursor_field,
            value=new_watermark,
        )
