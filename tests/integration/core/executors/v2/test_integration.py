"""Integration tests for V2 Executor components working together."""

from datetime import datetime
from unittest.mock import Mock

import pytest

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.observability import AlertSeverity, ObservabilityManager
from sqlflow.core.executors.v2.results import (
    PipelineExecutionSummary,
    StepExecutionResult,
)
from sqlflow.core.executors.v2.steps import (
    ExportStep,
    LoadStep,
    TransformStep,
    create_step_from_dict,
)


class TestV2Integration:
    """Test V2 components working together."""

    def test_end_to_end_observability_flow(self):
        """Test the complete observability flow from step creation to performance summary."""

        # Create observability manager
        obs_manager = ObservabilityManager("test_run_123")

        # Create a load step
        load_step = LoadStep(
            id="load_customers",
            source="customers.csv",
            target_table="customers",
            expected_duration_ms=5000.0,
        )

        # Simulate step execution
        start_time = datetime.utcnow()
        obs_manager.record_step_start(load_step.id, load_step.type)

        # Simulate successful execution
        execution_result = StepExecutionResult.success(
            step_id=load_step.id,
            step_type=load_step.type,
            start_time=start_time,
            rows_affected=1000,
            performance_metrics={"throughput": 200.0},
            resource_usage={"memory_mb": 150.0},
        )

        obs_manager.record_step_success(
            load_step.id, execution_result.to_observability_event()
        )

        # Get performance summary
        summary = obs_manager.get_performance_summary()

        # Assertions
        assert summary["run_id"] == "test_run_123"
        assert summary["total_steps"] == 1
        assert summary["total_failures"] == 0
        assert summary["failure_rate"] == 0.0
        assert summary["total_rows_processed"] == 1000
        assert "load" in summary["step_details"]

        step_details = summary["step_details"]["load"]
        assert step_details["calls"] == 1
        assert step_details["failures"] == 0
        assert step_details["total_rows"] == 1000
        assert step_details["throughput_rows_per_second"] > 0

    def test_error_handling_and_alerts(self):
        """Test error handling and alert generation."""

        obs_manager = ObservabilityManager("test_run_456")

        # Create a transform step
        transform_step = TransformStep(
            id="slow_transform",
            sql="SELECT * FROM large_table WHERE complex_calculation()",
            target_table="result",
        )

        # Simulate step start
        obs_manager.record_step_start(transform_step.id, transform_step.type)

        # Simulate failure
        obs_manager.record_step_failure(
            transform_step.id,
            transform_step.type,
            "Connection timeout after 30s",
            duration_ms=30000.0,
        )

        # Check alerts were generated
        alerts = obs_manager.get_alerts()
        assert len(alerts) > 0

        failure_alerts = obs_manager.get_alerts(severity=AlertSeverity.ERROR)
        assert len(failure_alerts) >= 1

        failure_alert = failure_alerts[0]
        assert failure_alert.component == transform_step.id
        assert failure_alert.alert_type == "step_failure"
        assert "Connection timeout" in failure_alert.message
        assert len(failure_alert.suggested_actions) > 0

    def test_step_dictionary_migration_compatibility(self):
        """Test that dictionary-based steps can be converted to V2 step objects."""

        # Legacy dictionary format
        legacy_steps = [
            {
                "type": "load",
                "source": "orders.csv",
                "target_table": "orders",
                "load_mode": "append",
            },
            {
                "id": "transform_orders",
                "type": "transform",
                "sql": "SELECT * FROM orders WHERE amount > 100",
                "target_table": "high_value_orders",
            },
            {
                "type": "export",
                "source_table": "high_value_orders",
                "target": "output/high_value_orders.parquet",
                "export_format": "parquet",
            },
        ]

        # Convert to V2 step objects
        v2_steps = [create_step_from_dict(step_dict) for step_dict in legacy_steps]

        # Verify conversion
        assert len(v2_steps) == 3

        load_step, transform_step, export_step = v2_steps

        # Check load step
        assert isinstance(load_step, LoadStep)
        assert load_step.source == "orders.csv"
        assert load_step.target_table == "orders"
        assert load_step.load_mode == "append"
        assert load_step.id.startswith("load_")  # Auto-generated ID

        # Check transform step
        assert isinstance(transform_step, TransformStep)
        assert transform_step.id == "transform_orders"
        assert "orders WHERE amount > 100" in transform_step.sql
        assert transform_step.target_table == "high_value_orders"

        # Check export step
        assert isinstance(export_step, ExportStep)
        assert export_step.source_table == "high_value_orders"
        assert export_step.target == "output/high_value_orders.parquet"
        assert export_step.export_format == "parquet"

    def test_pipeline_execution_summary(self):
        """Test pipeline execution summary aggregation."""

        start_time = datetime.utcnow()

        # Create multiple step results
        step_results = [
            StepExecutionResult.success(
                step_id="load_customers",
                step_type="load",
                start_time=start_time,
                rows_affected=5000,
                performance_metrics={"throughput": 1000.0},
            ),
            StepExecutionResult.success(
                step_id="transform_customers",
                step_type="transform",
                start_time=start_time,
                rows_affected=5000,
                performance_metrics={"throughput": 800.0},
            ),
            StepExecutionResult.failure(
                step_id="export_customers",
                step_type="export",
                start_time=start_time,
                error_message="S3 connection failed",
            ),
        ]

        # Create pipeline summary
        summary = PipelineExecutionSummary.from_step_results(
            run_id="pipeline_123",
            pipeline_name="customer_etl",
            start_time=start_time,
            step_results=step_results,
        )

        # Verify aggregation
        assert summary.run_id == "pipeline_123"
        assert summary.pipeline_name == "customer_etl"
        assert summary.status == "PARTIAL_SUCCESS"  # Mixed success/failure
        assert summary.total_rows_processed == 10000  # 5000 + 5000 + 0
        assert len(summary.step_results) == 3

        # Check individual step results
        successful_steps = [r for r in summary.step_results if r.is_successful()]
        failed_steps = [r for r in summary.step_results if r.is_failed()]

        assert len(successful_steps) == 2
        assert len(failed_steps) == 1
        assert failed_steps[0].error_message == "S3 connection failed"

    def test_execution_context_immutability(self):
        """Test ExecutionContext immutability and helper methods."""

        # Create mock dependencies
        sql_engine = Mock()
        connector_registry = Mock()
        variable_manager = Mock()
        watermark_manager = Mock()
        obs_manager = ObservabilityManager("test_context")

        # Create context
        context = ExecutionContext.create(
            sql_engine=sql_engine,
            connector_registry=connector_registry,
            variable_manager=variable_manager,
            watermark_manager=watermark_manager,
            observability_manager=obs_manager,
            variables={"env": "test"},
        )

        # Test immutability
        original_run_id = context.run_id
        original_variables = context.variables

        # Test variable update (creates new context)
        updated_context = context.with_variables({"new_var": "value"})

        # Original context unchanged
        assert context.run_id == original_run_id
        assert context.variables == original_variables

        # New context has updates
        assert updated_context.run_id == original_run_id  # Same run
        assert updated_context.variables == {"env": "test", "new_var": "value"}

        # Test config update
        config_context = context.with_config({"debug": True})
        assert config_context.config == {"debug": True}
        assert context.config == {}  # Original unchanged


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
