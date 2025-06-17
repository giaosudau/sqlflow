"""Step Execution Results for V2 Executor.

This module defines the StepExecutionResult class and related types that provide
rich observability data from step execution. These results enable comprehensive
performance monitoring, failure analysis, and data lineage tracking.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional


@dataclass(frozen=True)
class StepExecutionResult:
    """
    A rich object returned from step execution that enables comprehensive observability.

    This class captures not just success/failure status, but detailed performance metrics,
    schema information, resource usage, and other observability data that allows the
    V2 Executor to provide deep insights into pipeline execution.

    Attributes:
        status: Execution status (SUCCESS, FAILURE, SKIPPED, CANCELLED)
        step_id: ID of the step that was executed
        step_type: Type of step that was executed
        start_time: When execution started (UTC timestamp)
        end_time: When execution ended (UTC timestamp)
        execution_duration_ms: Total execution time in milliseconds
        rows_affected: Number of rows processed/affected by this step
        input_schemas: Schemas of input tables/sources used by this step
        output_schema: Schema of output table/result produced by this step
        performance_metrics: Detailed performance metrics (throughput, latency, etc.)
        resource_usage: Resource consumption data (memory, CPU, I/O)
        error_message: Error message if status is FAILURE
        warnings: List of warning messages generated during execution
        data_lineage: Data lineage information for this step
        observability_data: Additional observability data for monitoring systems
    """

    status: Literal["SUCCESS", "FAILURE", "SKIPPED", "CANCELLED"]
    step_id: str
    step_type: str
    start_time: datetime
    end_time: datetime
    execution_duration_ms: float

    # Data metrics
    rows_affected: Optional[int] = None
    bytes_processed: Optional[int] = None

    # Schema information for data lineage
    input_schemas: Dict[str, List[str]] = field(default_factory=dict)
    output_schema: Optional[Dict[str, str]] = None

    # Performance metrics
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    resource_usage: Dict[str, Any] = field(default_factory=dict)

    # Error handling
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    # Data lineage and observability
    data_lineage: Dict[str, Any] = field(default_factory=dict)
    observability_data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def success(
        cls,
        step_id: str,
        step_type: str,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        rows_affected: Optional[int] = None,
        **kwargs,
    ) -> "StepExecutionResult":
        """
        Create a successful execution result.

        Args:
            step_id: ID of the executed step
            step_type: Type of the executed step
            start_time: When execution started
            end_time: When execution ended (defaults to now)
            rows_affected: Number of rows processed
            **kwargs: Additional result data

        Returns:
            StepExecutionResult with SUCCESS status
        """
        if end_time is None:
            end_time = datetime.utcnow()

        duration_ms = (end_time - start_time).total_seconds() * 1000

        return cls(
            status="SUCCESS",
            step_id=step_id,
            step_type=step_type,
            start_time=start_time,
            end_time=end_time,
            execution_duration_ms=duration_ms,
            rows_affected=rows_affected,
            **kwargs,
        )

    @classmethod
    def failure(
        cls,
        step_id: str,
        step_type: str,
        start_time: datetime,
        error_message: str,
        end_time: Optional[datetime] = None,
        error_code: Optional[str] = None,
        **kwargs,
    ) -> "StepExecutionResult":
        """
        Create a failed execution result.

        Args:
            step_id: ID of the executed step
            step_type: Type of the executed step
            start_time: When execution started
            error_message: Description of the error
            end_time: When execution ended (defaults to now)
            error_code: Optional error code for categorization
            **kwargs: Additional result data

        Returns:
            StepExecutionResult with FAILURE status
        """
        if end_time is None:
            end_time = datetime.utcnow()

        duration_ms = (end_time - start_time).total_seconds() * 1000

        return cls(
            status="FAILURE",
            step_id=step_id,
            step_type=step_type,
            start_time=start_time,
            end_time=end_time,
            execution_duration_ms=duration_ms,
            error_message=error_message,
            error_code=error_code,
            **kwargs,
        )

    @classmethod
    def skipped(
        cls, step_id: str, step_type: str, reason: str, **kwargs
    ) -> "StepExecutionResult":
        """
        Create a skipped execution result.

        Args:
            step_id: ID of the skipped step
            step_type: Type of the skipped step
            reason: Reason why the step was skipped
            **kwargs: Additional result data

        Returns:
            StepExecutionResult with SKIPPED status
        """
        now = datetime.utcnow()

        return cls(
            status="SKIPPED",
            step_id=step_id,
            step_type=step_type,
            start_time=now,
            end_time=now,
            execution_duration_ms=0.0,
            observability_data={"skip_reason": reason},
            **kwargs,
        )

    def to_observability_event(self) -> Dict[str, Any]:
        """
        Convert this result to an observability event for monitoring systems.

        This method creates a structured event that can be sent to monitoring
        systems like Prometheus, DataDog, or custom dashboards.

        Returns:
            Dictionary containing structured observability data
        """
        event = {
            "timestamp": self.end_time.isoformat(),
            "step_id": self.step_id,
            "step_type": self.step_type,
            "status": self.status,
            "duration_ms": self.execution_duration_ms,
            "rows_affected": self.rows_affected,
            "bytes_processed": self.bytes_processed,
        }

        # Add performance metrics
        if self.performance_metrics:
            event["performance"] = self.performance_metrics

        # Add resource usage
        if self.resource_usage:
            event["resources"] = self.resource_usage

        # Add error information
        if self.status == "FAILURE":
            event["error"] = {"message": self.error_message, "code": self.error_code}

        # Add warnings
        if self.warnings:
            event["warnings"] = self.warnings

        # Add custom observability data
        if self.observability_data:
            event.update(self.observability_data)

        return event

    def to_lineage_event(self) -> Dict[str, Any]:
        """
        Convert this result to a data lineage event.

        This method creates a structured event that describes the data transformation
        performed by this step, including input/output schemas and relationships.

        Returns:
            Dictionary containing data lineage information
        """
        lineage_event = {
            "step_id": self.step_id,
            "step_type": self.step_type,
            "timestamp": self.end_time.isoformat(),
            "status": self.status,
            "input_schemas": self.input_schemas,
            "output_schema": self.output_schema,
            "rows_affected": self.rows_affected,
        }

        # Add custom lineage data
        if self.data_lineage:
            lineage_event["lineage"] = self.data_lineage

        return lineage_event

    def add_performance_metric(self, name: str, value: Any) -> None:
        """
        Add a performance metric to this result.

        Note: This method modifies the performance_metrics dict in place.
        While the StepExecutionResult is frozen, the internal dicts are mutable.

        Args:
            name: Name of the metric
            value: Value of the metric
        """
        self.performance_metrics[name] = value

    def add_resource_usage(self, resource: str, value: Any) -> None:
        """
        Add resource usage information to this result.

        Args:
            resource: Name of the resource (memory, cpu, disk_io, etc.)
            value: Usage value
        """
        self.resource_usage[resource] = value

    def add_warning(self, warning: str) -> None:
        """
        Add a warning message to this result.

        Args:
            warning: Warning message to add
        """
        self.warnings.append(warning)

    def is_successful(self) -> bool:
        """Check if the step execution was successful."""
        return self.status == "SUCCESS"

    def is_failed(self) -> bool:
        """Check if the step execution failed."""
        return self.status == "FAILURE"

    def is_skipped(self) -> bool:
        """Check if the step execution was skipped."""
        return self.status == "SKIPPED"

    def get_throughput_rows_per_second(self) -> Optional[float]:
        """
        Calculate data throughput in rows per second.

        Returns:
            Throughput in rows/second, or None if not calculable
        """
        if (
            self.rows_affected is None
            or self.execution_duration_ms <= 0
            or self.rows_affected <= 0
        ):
            return None

        duration_seconds = self.execution_duration_ms / 1000
        return self.rows_affected / duration_seconds

    def get_throughput_bytes_per_second(self) -> Optional[float]:
        """
        Calculate data throughput in bytes per second.

        Returns:
            Throughput in bytes/second, or None if not calculable
        """
        if (
            self.bytes_processed is None
            or self.execution_duration_ms <= 0
            or self.bytes_processed <= 0
        ):
            return None

        duration_seconds = self.execution_duration_ms / 1000
        return self.bytes_processed / duration_seconds


@dataclass(frozen=True)
class PipelineExecutionSummary:
    """
    Summary of an entire pipeline execution.

    This class aggregates results from all steps in a pipeline to provide
    high-level observability and performance metrics.

    Attributes:
        run_id: Unique identifier for this pipeline run
        pipeline_name: Name of the executed pipeline
        start_time: When pipeline execution started
        end_time: When pipeline execution ended
        total_duration_ms: Total execution time in milliseconds
        step_results: Results from all executed steps
        status: Overall pipeline status
        total_rows_processed: Total rows processed across all steps
        performance_summary: Aggregated performance metrics
        error_summary: Summary of any errors that occurred
    """

    run_id: str
    pipeline_name: str
    start_time: datetime
    end_time: datetime
    total_duration_ms: float
    step_results: List[StepExecutionResult]
    status: Literal["SUCCESS", "FAILURE", "PARTIAL_SUCCESS"]
    total_rows_processed: int = 0
    performance_summary: Dict[str, Any] = field(default_factory=dict)
    error_summary: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_step_results(
        cls,
        run_id: str,
        pipeline_name: str,
        start_time: datetime,
        step_results: List[StepExecutionResult],
    ) -> "PipelineExecutionSummary":
        """
        Create a pipeline summary from step results.

        Args:
            run_id: Unique identifier for this pipeline run
            pipeline_name: Name of the executed pipeline
            start_time: When pipeline execution started
            step_results: Results from all executed steps

        Returns:
            PipelineExecutionSummary with aggregated metrics
        """
        if not step_results:
            end_time = start_time
            status = "SUCCESS"
        else:
            end_time = max(result.end_time for result in step_results)

            # Determine overall status
            failed_steps = [r for r in step_results if r.is_failed()]
            successful_steps = [r for r in step_results if r.is_successful()]

            if failed_steps:
                status = "FAILURE" if not successful_steps else "PARTIAL_SUCCESS"
            else:
                status = "SUCCESS"

        total_duration_ms = (end_time - start_time).total_seconds() * 1000
        total_rows_processed = sum(
            r.rows_affected for r in step_results if r.rows_affected is not None
        )

        return cls(
            run_id=run_id,
            pipeline_name=pipeline_name,
            start_time=start_time,
            end_time=end_time,
            total_duration_ms=total_duration_ms,
            step_results=step_results,
            status=status,
            total_rows_processed=total_rows_processed,
        )
