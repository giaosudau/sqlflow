"""Integration tests for V2 step executors with real data.

Following Kent Beck's TDD principles:
- Test real behavior, not mocks
- Focus on user scenarios
- Clear test names that describe the scenario
- Test the actual step executor implementations

This file tests the step executors with real data and proper V2 architecture.
"""

import tempfile
from pathlib import Path

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2 import ExecutionCoordinator
from sqlflow.core.executors.v2.execution.context import create_test_context


class TestLoadStepExecutorIntegration:
    """Test LoadStepExecutor with real CSV files."""

    def test_load_csv_file_creates_table(self, coordinator, execution_context):
        """Test that load step creates a table from CSV file."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
            csv_path = f.name

        try:

            load_step = {
                "id": "load_test_data",
                "type": "load",
                "source": csv_path,
                "target_table": "test_data",
            }

            # Act
            result = coordinator.execute([load_step], execution_context)

            # Assert
            assert result.success, "Load step should succeed"
            assert len(result.step_results) == 1, "Should have one step result"
            step_result = result.step_results[0]
            assert step_result.success, "Step should succeed"
            assert step_result.rows_affected == 3, "Should load 3 rows"

        finally:
            Path(csv_path).unlink(missing_ok=True)

    def test_load_step_with_invalid_file_fails_gracefully(self):
        """Test that load step fails gracefully with invalid file."""
        # Arrange
        coordinator = ExecutionCoordinator()
        context = create_test_context(engine=DuckDBEngine(":memory:"))

        load_step = {
            "id": "load_invalid",
            "type": "load",
            "source": "/nonexistent/file.csv",
            "target_table": "invalid_data",
        }

        # Act
        result = coordinator.execute([load_step], context)

        # Assert
        assert not result.success, "Load step should fail with invalid file"
        assert len(result.step_results) == 1, "Should have one step result"
        step_result = result.step_results[0]
        assert not step_result.success, "Step should fail"
        assert step_result.error_message is not None, "Should have error message"


class TestTransformStepExecutorIntegration:
    """Test TransformStepExecutor with real SQL operations."""

    def test_transform_step_executes_sql_query(self, coordinator, execution_context):
        """Test that transform step executes SQL query on loaded data."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
            csv_path = f.name

        try:

            # First load the data
            load_step = {
                "id": "load_data",
                "type": "load",
                "source": csv_path,
                "target_table": "source_data",
            }

            # Then transform it
            transform_step = {
                "id": "transform_data",
                "type": "transform",
                "sql": "SELECT name, value * 2 as doubled_value FROM source_data WHERE value > 150",
                "target_table": "transformed_data",
            }

            # Act
            result = coordinator.execute([load_step, transform_step], execution_context)

            # Assert
            assert result.success, "Pipeline should succeed"
            assert len(result.step_results) == 2, "Should have two step results"

            # Check load step
            load_result = result.step_results[0]
            assert load_result.success, "Load step should succeed"
            assert load_result.rows_affected == 3, "Should load 3 rows"

            # Check transform step
            transform_result = result.step_results[1]
            assert transform_result.success, "Transform step should succeed"
            # Should have 2 rows (Bob and Charlie have value > 150)
            assert transform_result.rows_affected == 2, "Should transform 2 rows"

        finally:
            Path(csv_path).unlink(missing_ok=True)

    def test_transform_step_with_invalid_sql_fails(self):
        """Test that transform step fails with invalid SQL."""
        # Arrange
        coordinator = ExecutionCoordinator()
        context = create_test_context(engine=DuckDBEngine(":memory:"))

        transform_step = {
            "id": "invalid_transform",
            "type": "transform",
            "sql": "SELECT * FROM nonexistent_table",
            "target_table": "invalid_result",
        }

        # Act
        result = coordinator.execute([transform_step], context)

        # Assert
        assert not result.success, "Transform step should fail with invalid SQL"
        assert len(result.step_results) == 1, "Should have one step result"
        step_result = result.step_results[0]
        assert not step_result.success, "Step should fail"
        assert step_result.error_message is not None, "Should have error message"


class TestExportStepExecutorIntegration:
    """Test ExportStepExecutor with real file outputs."""

    def test_export_step_writes_to_csv_file(self, coordinator, execution_context):
        """Test that export step writes data to CSV file."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
            csv_path = f.name

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as output_file:
            output_path = output_file.name

        try:

            # Load data
            load_step = {
                "id": "load_data",
                "type": "load",
                "source": csv_path,
                "target_table": "source_data",
            }

            # Export data
            export_step = {
                "id": "export_data",
                "type": "export",
                "source_table": "source_data",
                "destination": output_path,
                "format": "csv",
            }

            # Act
            result = coordinator.execute([load_step, export_step], execution_context)

            # Assert
            assert result.success, "Pipeline should succeed"
            assert len(result.step_results) == 2, "Should have two step results"

            # Check export step
            export_result = result.step_results[1]
            assert export_result.success, "Export step should succeed"

            # Verify file was created
            assert Path(output_path).exists(), "Output file should be created"

            # Verify file content
            with open(output_path, "r") as f:
                content = f.read()
                assert "id,name,value" in content, "Should have header"
                assert "1,Alice,100" in content, "Should have data"
                assert "2,Bob,200" in content, "Should have data"
                assert "3,Charlie,300" in content, "Should have data"

        finally:
            Path(csv_path).unlink(missing_ok=True)
            Path(output_path).unlink(missing_ok=True)


class TestCompletePipelineIntegration:
    """Test complete load → transform → export pipeline."""

    def test_complete_pipeline_load_transform_export(
        self, coordinator, execution_context
    ):
        """Test complete pipeline: load CSV → transform with SQL → export to file."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(
                "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n4,David,400\n"
            )
            csv_path = f.name

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as output_file:
            output_path = output_file.name

        try:

            pipeline = [
                {
                    "id": "load_data",
                    "type": "load",
                    "source": csv_path,
                    "target_table": "raw_data",
                },
                {
                    "id": "transform_data",
                    "type": "transform",
                    "sql": "SELECT name, value * 1.5 as adjusted_value FROM raw_data WHERE value >= 200",
                    "target_table": "processed_data",
                },
                {
                    "id": "export_results",
                    "type": "export",
                    "source_table": "processed_data",
                    "destination": output_path,
                    "format": "csv",
                },
            ]

            # Act
            result = coordinator.execute(pipeline, execution_context)

            # Assert
            assert result.success, "Complete pipeline should succeed"
            assert len(result.step_results) == 3, "Should have three step results"

            # Check load step
            load_result = result.step_results[0]
            assert load_result.success, "Load step should succeed"
            assert load_result.rows_affected == 4, "Should load 4 rows"

            # Check transform step
            transform_result = result.step_results[1]
            assert transform_result.success, "Transform step should succeed"
            assert (
                transform_result.rows_affected == 3
            ), "Should transform 3 rows (Bob, Charlie, David)"

            # Check export step
            export_result = result.step_results[2]
            assert export_result.success, "Export step should succeed"

            # Verify output file
            assert Path(output_path).exists(), "Output file should be created"

            # Verify file content has transformed data
            with open(output_path, "r") as f:
                content = f.read()
                assert (
                    "name,adjusted_value" in content
                ), "Should have transformed header"
                assert "Bob,300.0" in content, "Should have Bob's adjusted value"
                assert (
                    "Charlie,450.0" in content
                ), "Should have Charlie's adjusted value"
                assert "David,600.0" in content, "Should have David's adjusted value"
                assert (
                    "Alice" not in content
                ), "Alice should be filtered out (value < 200)"

        finally:
            Path(csv_path).unlink(missing_ok=True)
            Path(output_path).unlink(missing_ok=True)

    def test_pipeline_fail_fast_on_step_failure(self):
        """Test that pipeline stops on first step failure."""
        # Arrange
        coordinator = ExecutionCoordinator()
        context = create_test_context(engine=DuckDBEngine(":memory:"))

        pipeline = [
            {
                "id": "load_invalid",
                "type": "load",
                "source": "/nonexistent/file.csv",  # This will fail
                "target_table": "data",
            },
            {
                "id": "transform_step",
                "type": "transform",
                "sql": "SELECT * FROM data",
                "target_table": "result",
            },
            {
                "id": "export_step",
                "type": "export",
                "source_table": "result",
                "destination": "/tmp/output.csv",
                "format": "csv",
            },
        ]

        # Act
        result = coordinator.execute(pipeline, context)

        # Assert
        assert not result.success, "Pipeline should fail"
        assert len(result.step_results) == 1, "Should stop after first failure"

        # Only the first step should have been executed
        step_result = result.step_results[0]
        assert not step_result.success, "First step should fail"
        assert step_result.error_message is not None, "Should have error message"
