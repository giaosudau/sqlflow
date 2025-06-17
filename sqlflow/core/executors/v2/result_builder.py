"""Result building for V2 Orchestrator execution.

This module handles the construction of execution results, including
success and failure scenarios, performance metrics, and data lineage.

Following Robert Martin's Clean Code principle:
Functions should do one thing and do it well.
"""

from dataclasses import asdict
from typing import Any, Dict, List

from sqlflow.logging import get_logger

from .execution_request import ExecutionSummary

logger = get_logger(__name__)


class ExecutionResultBuilder:
    """
    Builds execution results with comprehensive metrics and lineage.

    Following Martin Fowler's Builder Pattern:
    Constructs complex result objects step by step.

    Following Robert Martin's Single Responsibility Principle:
    This class has one job - building execution results.
    """

    @staticmethod
    def build_success_result(summary: ExecutionSummary) -> Dict[str, Any]:
        """Create success result with comprehensive metrics.

        Uses Martin Fowler's Parameter Object pattern to simplify
        the interface and reduce coupling.
        """
        # Calculate total rows processed from step results
        total_rows = sum(
            result.rows_affected
            for result in summary.results
            if result.rows_affected is not None
        )

        # Create pipeline summary using the correct constructor
        from sqlflow.core.executors.v2.results import PipelineExecutionSummary

        pipeline_summary = PipelineExecutionSummary.from_step_results(
            run_id=summary.run_id,
            pipeline_name="advanced_pipeline",  # Could be extracted from config
            start_time=summary.start_time,
            step_results=summary.results,
        )

        # Convert dataclass to dict using dataclass fields
        result = {
            "status": "success",
            "summary": asdict(pipeline_summary),
            "executed_steps": [r.step_id for r in summary.results],
            "step_results": [asdict(r) for r in summary.results],
            "total_steps": len(summary.results),
            "failed_steps": [],
            "execution_time_seconds": summary.total_time,
            "performance_summary": summary.observability.get_performance_summary(),
            "data_lineage": ExecutionResultBuilder._build_lineage(summary.results),
        }

        # Add advanced engine statistics if available
        if summary.engine_stats:
            result["engine_stats"] = summary.engine_stats

        if summary.performance_report:
            result["performance_report"] = summary.performance_report

        logger.info("🎉 Pipeline completed successfully in %.2fs", summary.total_time)
        return result

    @staticmethod
    def build_failure_result(
        run_id: str, error: Exception, observability: Any
    ) -> Dict[str, Any]:
        """Create failure result with observability data.

        Following Kent Beck's simple design principle:
        Clear, readable error handling.
        """
        logger.error("💥 Pipeline execution failed: %s", str(error))

        return {
            "status": "failed",
            "error": str(error),
            "error_type": type(error).__name__,
            "run_id": run_id,
            "performance_summary": observability.get_performance_summary(),
            "executed_steps": [],
            "total_steps": 0,
        }

    @staticmethod
    def build_empty_result() -> Dict[str, Any]:
        """Result for empty pipeline.

        Following the principle of least surprise:
        Empty input should produce predictable output.
        """
        logger.warning("Empty execution plan provided")
        return {"status": "success", "executed_steps": [], "total_steps": 0}

    @staticmethod
    def _build_lineage(results: List[Any]) -> Dict[str, Any]:
        """Build data lineage from results.

        Following Jon Bentley's efficiency principle:
        Build lineage incrementally to avoid memory issues.
        """
        return {
            "steps": [result.to_lineage_event() for result in results],
            "tables_created": [
                result.data_lineage.get("target_table")
                for result in results
                if result.data_lineage and result.data_lineage.get("target_table")
            ],
            "data_flows": [
                {
                    "from": source,
                    "to": result.data_lineage.get("target_table"),
                    "step_id": result.step_id,
                    "transformation": result.step_type,
                }
                for result in results
                if result.input_schemas and result.data_lineage
                for source in result.input_schemas.keys()
            ],
        }
