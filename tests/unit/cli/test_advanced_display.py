"""Unit tests for advanced Rich display features.

Tests for the advanced display functionality implemented in Task 3.1,
ensuring that Rich progress display and interactive selection work correctly
with proper error handling and fallback behavior.
"""

import json
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from sqlflow.cli.advanced_display import (
    display_execution_summary_with_metrics,
    display_interactive_pipeline_selection,
    display_pipeline_execution_progress,
    display_pipeline_selection_with_preview,
)
from sqlflow.cli.advanced_integration import (
    check_advanced_features_dependencies,
    get_advanced_features_status,
    get_feature_recommendations,
    run_pipeline_with_advanced_progress,
)


class TestAdvancedDisplay:
    """Test suite for advanced Rich display features."""

    def test_display_pipeline_execution_progress_success(self):
        """Test successful pipeline execution with progress display."""
        # Mock operations
        operations = [
            {"id": "load_data", "type": "load", "name": "customer_data"},
            {"id": "transform_data", "type": "transform", "name": "clean_data"},
            {"id": "export_data", "type": "export", "name": "output_csv"},
        ]
        
        # Mock executor callback that returns success
        def mock_executor(operation: Dict[str, Any]) -> Dict[str, Any]:
            time.sleep(0.01)  # Simulate execution time
            return {"status": "success", "rows_processed": 100}
        
        # Execute with progress display
        result = display_pipeline_execution_progress(
            operations=operations,
            executor_callback=mock_executor,
            pipeline_name="test_pipeline",
        )
        
        # Verify results
        assert result["status"] == "success"
        assert result["total_steps"] == 3
        assert len(result["executed_steps"]) == 3
        assert "duration" in result
        assert result["duration"] > 0
        
        # Verify all steps were executed
        expected_steps = ["load_data", "transform_data", "export_data"]
        assert result["executed_steps"] == expected_steps

    def test_display_pipeline_execution_progress_failure(self):
        """Test pipeline execution failure with progress display."""
        operations = [
            {"id": "load_data", "type": "load", "name": "customer_data"},
            {"id": "failing_step", "type": "transform", "name": "bad_transform"},
        ]
        
        def mock_executor(operation: Dict[str, Any]) -> Dict[str, Any]:
            if operation["id"] == "failing_step":
                return {"status": "error", "message": "Transform failed"}
            return {"status": "success"}
        
        result = display_pipeline_execution_progress(
            operations=operations,
            executor_callback=mock_executor,
            pipeline_name="test_pipeline",
        )
        
        # Verify failure results
        assert result["status"] == "failed"
        assert result["error"] == "Transform failed"
        assert result["failed_step"] == "failing_step"
        assert result["failed_step_type"] == "transform"
        assert result["executed_steps"] == ["load_data"]
        assert result["failed_at_step"] == 2

    def test_display_pipeline_execution_progress_exception(self):
        """Test pipeline execution with exception handling."""
        operations = [
            {"id": "error_step", "type": "load", "name": "error_data"},
        ]
        
        def mock_executor(operation: Dict[str, Any]) -> Dict[str, Any]:
            raise ValueError("Mock exception for testing")
        
        result = display_pipeline_execution_progress(
            operations=operations,
            executor_callback=mock_executor,
            pipeline_name="test_pipeline",
        )
        
        # Verify exception handling
        assert result["status"] == "failed"
        assert "Exception in load" in result["error"]
        assert result["failed_step"] == "error_step"
        assert "exception" in result
        assert result["executed_steps"] == []

    @patch("sqlflow.cli.advanced_display.list_pipelines_operation")
    @patch("typer.prompt")
    def test_display_interactive_pipeline_selection_success(self, mock_prompt, mock_list):
        """Test successful interactive pipeline selection."""
        # Mock available pipelines
        mock_pipelines = [
            {"name": "pipeline1", "size": "1024", "modified": "1640995200"},
            {"name": "pipeline2", "size": "2048", "modified": "1640995300"},
        ]
        mock_list.return_value = mock_pipelines
        
        # Mock user input - select first pipeline
        mock_prompt.return_value = "1"
        
        result = display_interactive_pipeline_selection(profile_name="test")
        
        assert result == "pipeline1"
        mock_list.assert_called_once_with("test")
        mock_prompt.assert_called_once()

    @patch("sqlflow.cli.advanced_display.list_pipelines_operation")
    @patch("typer.prompt")
    def test_display_interactive_pipeline_selection_invalid_input(self, mock_prompt, mock_list):
        """Test interactive selection with invalid input followed by valid input."""
        mock_pipelines = [
            {"name": "pipeline1", "size": "1024", "modified": "1640995200"},
        ]
        mock_list.return_value = mock_pipelines
        
        # Mock invalid input followed by valid input
        mock_prompt.side_effect = ["invalid", "1"]
        
        result = display_interactive_pipeline_selection(profile_name="test")
        
        assert result == "pipeline1"
        assert mock_prompt.call_count == 2

    @patch("sqlflow.cli.advanced_display.list_pipelines_operation")
    def test_display_interactive_pipeline_selection_no_pipelines(self, mock_list):
        """Test interactive selection when no pipelines are available."""
        mock_list.return_value = []
        
        from click.exceptions import Exit
        with pytest.raises(Exit):
            display_interactive_pipeline_selection(profile_name="test")

    def test_display_execution_summary_success(self, capsys):
        """Test execution summary display for successful execution."""
        results = {
            "status": "success",
            "total_steps": 5,
            "executed_steps": ["step1", "step2", "step3", "step4", "step5"],
            "duration": 12.34,
        }
        
        display_execution_summary_with_metrics(
            results=results,
            pipeline_name="test_pipeline",
            show_detailed_metrics=True,
        )
        
        # The function prints to console, so we can't easily assert the output
        # But we can verify it doesn't raise exceptions
        captured = capsys.readouterr()
        assert "test_pipeline" in captured.out
        assert "successfully" in captured.out

    def test_display_execution_summary_failure(self, capsys):
        """Test execution summary display for failed execution."""
        results = {
            "status": "failed",
            "error": "Pipeline validation failed",
            "failed_step": "transform_step",
            "failed_at_step": 3,
            "total_steps": 5,
            "executed_steps": ["step1", "step2"],
            "duration": 5.67,
        }
        
        display_execution_summary_with_metrics(
            results=results,
            pipeline_name="test_pipeline",
            show_detailed_metrics=True,
        )
        
        captured = capsys.readouterr()
        assert "test_pipeline" in captured.out
        assert "failed" in captured.out


class TestAdvancedIntegration:
    """Test suite for advanced feature integration."""

    def test_get_advanced_features_status(self):
        """Test getting status of advanced features."""
        status = get_advanced_features_status()
        
        # Verify expected keys are present
        expected_features = [
            "rich_progress",
            "interactive_selection", 
            "enhanced_display",
            "pipeline_preview",
            "execution_metrics",
        ]
        
        for feature in expected_features:
            assert feature in status
            assert isinstance(status[feature], bool)

    def test_check_advanced_features_dependencies(self):
        """Test checking if all advanced features are available."""
        # This should return True in our test environment since Rich and Typer are installed
        result = check_advanced_features_dependencies()
        assert isinstance(result, bool)

    def test_get_feature_recommendations(self):
        """Test getting feature recommendations."""
        recommendations = get_feature_recommendations()
        assert isinstance(recommendations, list)
        # Should have at least one recommendation (even if just "All features available")
        assert len(recommendations) >= 1

    @patch("sqlflow.cli.advanced_integration.compile_pipeline_operation")
    @patch("sqlflow.cli.advanced_integration.create_executor_for_command")
    def test_run_pipeline_with_advanced_progress_fallback(self, mock_executor, mock_compile):
        """Test advanced progress with fallback to basic execution."""
        # Mock compilation
        mock_operations = [{"id": "test", "type": "load", "name": "test"}]
        mock_compile.return_value = (mock_operations, "/tmp/output")
        
        # Mock basic execution result
        basic_result = {"status": "success", "executed_steps": ["test"]}
        
        # Mock the run_pipeline_operation to return basic result
        with patch("sqlflow.cli.advanced_integration.run_pipeline_operation") as mock_run:
            mock_run.return_value = basic_result
            
            result = run_pipeline_with_advanced_progress(
                pipeline_name="test_pipeline",
                use_advanced_progress=False,  # Use fallback
            )
        
        assert result == basic_result
        mock_run.assert_called_once()

    @patch("sqlflow.cli.advanced_integration.compile_pipeline_operation")
    @patch("sqlflow.cli.advanced_integration.create_executor_for_command")
    def test_run_pipeline_with_advanced_progress_enabled(self, mock_executor, mock_compile):
        """Test advanced progress when enabled."""
        # Mock compilation
        mock_operations = [{"id": "test", "type": "load", "name": "test"}]
        mock_compile.return_value = (mock_operations, "/tmp/output")
        
        # Mock executor
        mock_executor_instance = MagicMock()
        mock_executor_instance._execute_step.return_value = {"status": "success"}
        mock_executor.return_value = mock_executor_instance
        
        result = run_pipeline_with_advanced_progress(
            pipeline_name="test_pipeline",
            use_advanced_progress=True,
        )
        
        # Verify advanced execution was used
        assert result["status"] == "success"
        assert "executed_steps" in result
        assert "duration" in result
        mock_compile.assert_called_once()
        mock_executor.assert_called_once()


class TestRichIntegrationEdgeCases:
    """Test edge cases and error conditions for Rich integration."""

    def test_empty_operations_list(self):
        """Test handling of empty operations list."""
        def mock_executor(operation: Dict[str, Any]) -> Dict[str, Any]:
            return {"status": "success"}
        
        result = display_pipeline_execution_progress(
            operations=[],
            executor_callback=mock_executor,
            pipeline_name="empty_pipeline",
        )
        
        assert result["status"] == "success"
        assert result["total_steps"] == 0
        assert result["executed_steps"] == []

    def test_malformed_operation_data(self):
        """Test handling of malformed operation data."""
        operations = [
            {},  # Empty operation
            {"type": "load"},  # Missing id and name
            {"id": "test", "name": "test"},  # Missing type
        ]
        
        def mock_executor(operation: Dict[str, Any]) -> Dict[str, Any]:
            return {"status": "success"}
        
        result = display_pipeline_execution_progress(
            operations=operations,
            executor_callback=mock_executor,
            pipeline_name="malformed_pipeline",
        )
        
        # Should handle malformed data gracefully
        assert result["status"] == "success"
        assert result["total_steps"] == 3

    @patch("sqlflow.cli.advanced_display.list_pipelines_operation")
    def test_pipeline_list_with_malformed_data(self, mock_list):
        """Test interactive selection with malformed pipeline data."""
        # Mock pipelines with missing or malformed data
        mock_pipelines = [
            {"name": "good_pipeline", "size": "1024", "modified": "1640995200"},
            {"name": "bad_size_pipeline", "size": "invalid", "modified": "1640995200"},
            {"name": "bad_time_pipeline", "size": "1024", "modified": "invalid"},
            {"name": "minimal_pipeline"},  # Missing size and modified
        ]
        mock_list.return_value = mock_pipelines
        
        with patch("typer.prompt", return_value="1"):
            result = display_interactive_pipeline_selection(profile_name="test")
            assert result == "good_pipeline" 