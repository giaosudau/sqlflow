"""V2 Connector Integration Tests.

Migrated from V1 patterns to V2 patterns using:
- ExecutionCoordinator instead of get_executor()
- Step-based execution instead of direct engine calls
- StepExecutionResult validation
- Enhanced observability and error handling

Following Kent Beck TDD, Robert Martin Clean Architecture, and Raymond Hettinger Pythonic Design.
"""

import os
import tempfile

import pandas as pd

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TestV2ConnectorDataLoading:
    """Test V2 connector data loading with step-based execution."""

    def test_data_loading_failed_connector_v2_pattern(self, v2_pipeline_runner):
        """Test V2 data loading error when connector initialization fails.

        V2 Pattern: Uses coordinator with source definition steps instead of
        direct engine method calls.
        """
        source_step = {
            "type": "source_definition",
            "id": "invalid_source",
            "name": "failed_connector",
            "connector_type": "postgres",
            "params": {
                "host": "nonexistent-host-12345",
                "port": 99999,
                "database": "nonexistent_db",
                "username": "invalid_user",
                "password": "invalid_password",
            },
        }

        coordinator = v2_pipeline_runner([source_step])
        result = coordinator.result

        assert result.success is False
        assert len(result.step_results) == 1
        step_result = result.step_results[0]
        assert step_result.step_id == "invalid_source"
        assert step_result.success is False

    def test_csv_connector_success_v2_pattern(self, v2_pipeline_runner):
        """Test successful CSV connector using V2 patterns."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n")
            csv_path = f.name

        try:
            source_step = {
                "type": "source_definition",
                "id": "csv_source",
                "name": "test_csv",
                "connector_type": "csv",
                "params": {"path": csv_path, "has_header": True, "delimiter": ","},
            }

            coordinator = v2_pipeline_runner([source_step])
            result = coordinator.result

            assert result.success is True
            assert len(result.step_results) == 1
            step_result = result.step_results[0]
            assert step_result.step_id == "csv_source"
            assert step_result.success is True
        finally:
            os.unlink(csv_path)

    def test_connector_not_found_v2_pattern(self, v2_pipeline_runner):
        """Test V2 handling when connector type is not supported."""
        source_step = {
            "type": "source_definition",
            "id": "unsupported_source",
            "name": "test_unsupported",
            "connector_type": "unsupported_connector_type",
            "params": {},
        }
        coordinator = v2_pipeline_runner([source_step])
        result = coordinator.result

        assert result.success is False
        assert len(result.step_results) == 1
        step_result = result.step_results[0]
        assert step_result.step_id == "unsupported_source"
        assert step_result.success is False


class TestV2ConnectorExport:
    """Test V2 connector export functionality with step-based execution."""

    def test_export_no_data_handling_v2_pattern(self, v2_pipeline_runner):
        """Test V2 export behavior when no data is provided.

        V2 Pattern: Uses complete pipeline execution to maintain table context.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "empty_export.csv")
            pipeline = [
                {
                    "type": "transform",
                    "id": "create_empty_table",
                    "target_table": "empty_table",
                    "sql": "SELECT 1 as id, 'test' as value WHERE FALSE",
                },
                {
                    "type": "export",
                    "id": "export_empty",
                    "source_table": "empty_table",
                    "destination": destination,
                    "format": "csv",
                },
            ]
            coordinator = v2_pipeline_runner(pipeline)
            result = coordinator.result

            assert result.success, f"Export should succeed, got: {result}"
            assert len(result.step_results) == 2, "Should have 2 step results"
            assert os.path.exists(destination)

    def test_csv_export_success_v2_pattern(self, v2_pipeline_runner):
        """Test successful CSV export using V2 patterns.

        V2 Pattern: Uses complete pipeline execution instead of separate method calls.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "test_export.csv")
            pipeline = [
                {
                    "type": "transform",
                    "id": "create_test_data",
                    "target_table": "test_export_data",
                    "sql": "SELECT 1 as id, 'Alice' as name, 100.0 as value UNION ALL SELECT 2, 'Bob', 200.0 UNION ALL SELECT 3, 'Charlie', 300.0",
                },
                {
                    "type": "export",
                    "id": "export_test_data",
                    "source_table": "test_export_data",
                    "destination": destination,
                    "format": "csv",
                },
            ]
            coordinator = v2_pipeline_runner(pipeline)
            result = coordinator.result

            assert result.success
            assert os.path.exists(destination)
            exported_df = pd.read_csv(destination)
            assert len(exported_df) == 3
            assert list(exported_df.columns) == ["id", "name", "value"]


class TestV2DataFormatConversion:
    """Test V2 data format conversion patterns.

    Migrated from direct engine format testing to step-based pipeline testing.
    """

    def test_format_pipeline_csv_v2_pattern(self, v2_pipeline_runner):
        """Test CSV format through complete V2 pipeline."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,category,amount\n1,groceries,50.25\n2,fuel,75.00\n")
            csv_path = f.name
        try:
            pipeline_plan = [
                {
                    "type": "source_definition",
                    "id": "csv_source",
                    "name": "expenses",
                    "connector_type": "csv",
                    "params": {"path": csv_path, "has_header": True},
                },
                {
                    "type": "load",
                    "id": "load_expenses",
                    "source_name": "expenses",
                    "target_table": "raw_expenses",
                    "load_mode": "replace",
                },
                {
                    "type": "transform",
                    "id": "calculate_total",
                    "target_table": "expense_summary",
                    "sql": "SELECT category, SUM(amount) as total FROM raw_expenses GROUP BY category",
                },
            ]
            coordinator = v2_pipeline_runner(pipeline_plan)
            result = coordinator.result
            assert result.success
            engine = coordinator.context.engine
            summary_df = engine.execute_query("SELECT * FROM expense_summary").df()
            assert len(summary_df) == 2
        finally:
            os.unlink(csv_path)

    def test_format_pipeline_json_style_v2_pattern(self, v2_pipeline_runner):
        """Test JSON-style data processing through V2 pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "structured_export.csv")
            pipeline_plan = [
                {
                    "type": "transform",
                    "id": "create_json_style_data",
                    "target_table": "structured_data",
                    "sql": 'SELECT 1 as id, \'user_profile\' as type, \'{"name": "Alice", "age": 30}\' as data_json',
                },
                {
                    "type": "export",
                    "id": "export_json_style",
                    "source_table": "structured_data",
                    "destination": destination,
                    "connector_type": "csv",
                },
            ]
            coordinator = v2_pipeline_runner(pipeline_plan)
            result = coordinator.result
            assert result.success
            assert os.path.exists(destination)

    def test_format_pipeline_default_fallback_v2_pattern(self, v2_pipeline_runner):
        """Test default format fallback through V2 pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "mixed_export.csv")
            pipeline_plan = [
                {
                    "type": "transform",
                    "id": "create_mixed_data",
                    "target_table": "mixed_data",
                    "sql": "SELECT 1 as int_col, 3.14 as float_col, 'val' as text_col",
                },
                {
                    "type": "export",
                    "id": "export_mixed",
                    "source_table": "mixed_data",
                    "destination": destination,
                    "connector_type": "csv",
                },
            ]
            coordinator = v2_pipeline_runner(pipeline_plan)
            result = coordinator.result
            assert result.success
            assert os.path.exists(destination)


class TestV2ConnectorErrorPropagation:
    """Test V2 error propagation patterns.

    Migrated from direct connector testing to step-based error handling.
    """

    def test_connection_error_propagation_v2_pattern(self, v2_pipeline_runner):
        """Test V2 connection error propagation through step execution.

        V2 Pattern: Errors should be captured in StepResult structure.
        """
        source_step = {
            "type": "source_definition",
            "id": "invalid_connection",
            "name": "test_invalid_connection",
            "connector_type": "postgres",
            "params": {"host": "nonexistent-host-99999", "port": 5432},
        }
        coordinator = v2_pipeline_runner([source_step])
        result = coordinator.result
        assert not result.success
        assert (
            "unknown source connector type"
            in result.step_results[0].error_message.lower()
        )

    def test_parameter_mismatch_error_handling_v2_pattern(self, v2_pipeline_runner):
        """Test V2 parameter validation error handling."""
        source_step = {
            "type": "source_definition",
            "id": "missing_params",
            "name": "test_missing_params",
            "connector_type": "csv",
            "params": {"has_header": True},
        }
        coordinator = v2_pipeline_runner([source_step])
        result = coordinator.result
        assert not result.success
        assert (
            "'path' parameter is required"
            in result.step_results[0].error_message.lower()
        )


class TestV2ConnectorObservability:
    """Test V2 connector observability and monitoring features."""

    def test_connector_performance_tracking_v2_pattern(self, v2_pipeline_runner):
        """Test V2 automatic performance tracking for connector operations."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n" + "\n".join([f"{i},user_{i}" for i in range(1000)]))
            csv_path = f.name
        try:
            pipeline_plan = [
                {
                    "type": "load",
                    "id": "perf_load",
                    "source": csv_path,
                    "target_table": "perf_data",
                },
                {
                    "type": "transform",
                    "id": "perf_transform",
                    "target_table": "perf_summary",
                    "sql": "SELECT COUNT(*) FROM perf_data",
                },
            ]
            coordinator = v2_pipeline_runner(pipeline_plan)
            result = coordinator.result
            assert result.success
            assert result.step_results[0].duration_ms > 0
            assert result.step_results[1].duration_ms > 0
        finally:
            os.unlink(csv_path)

    def test_connector_error_monitoring_v2_pattern(self, v2_pipeline_runner):
        """Test V2 error monitoring and alerting for connector issues."""
        pipeline_plan = [
            {
                "type": "load",
                "id": "error_load",
                "source": "/nonexistent/file.csv",
                "target_table": "error_data",
            },
        ]
        coordinator = v2_pipeline_runner(pipeline_plan)
        result = coordinator.result
        assert not result.success
        assert result.step_results[0].success is False
        assert "No such file" in result.step_results[0].error_message
