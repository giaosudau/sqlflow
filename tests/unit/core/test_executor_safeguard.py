"""Tests for the executor safeguard."""

from unittest.mock import patch

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.executors.local_executor import LocalExecutor


class TestExecutorSafeguard:
    """Test cases for the executor safeguard."""

    def test_execution_order_match(self):
        """Test that no warning is logged when execution order matches."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_c", "pipeline_a")
        resolver.add_dependency("pipeline_c", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_c")
        assert order == ["pipeline_a", "pipeline_b", "pipeline_c"]

        plan = [
            {"id": "pipeline_a", "type": "test"},
            {"id": "pipeline_b", "type": "test"},
            {"id": "pipeline_c", "type": "test"},
        ]

        executor = LocalExecutor()

        with patch("sqlflow.core.executors.local_executor.logger") as mock_logger:
            executor.execute(plan, resolver)
            mock_logger.warning.assert_not_called()

    def test_execution_order_mismatch(self):
        """Test that a warning is logged when execution order doesn't match."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_c", "pipeline_a")
        resolver.add_dependency("pipeline_c", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_c")
        assert order == ["pipeline_a", "pipeline_b", "pipeline_c"]

        plan = [
            {"id": "pipeline_a", "type": "test"},
            {"id": "pipeline_c", "type": "test"},  # pipeline_c before pipeline_b
            {"id": "pipeline_b", "type": "test"},
        ]

        executor = LocalExecutor()

        with patch("sqlflow.core.executors.local_executor.logger") as mock_logger:
            executor.execute(plan, resolver)
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args[0]
            assert "Execution order mismatch detected" in call_args[0]
            assert "pipeline_a" in call_args[1]
            assert "pipeline_c" in call_args[1]
            assert "pipeline_b" in call_args[1]
            assert "pipeline_a" in call_args[2]
            assert "pipeline_b" in call_args[2]
            assert "pipeline_c" in call_args[2]

    def test_no_dependency_resolver(self):
        """Test that no warning is logged when no dependency resolver is provided."""
        plan = [
            {"id": "pipeline_a", "type": "test"},
            {"id": "pipeline_b", "type": "test"},
            {"id": "pipeline_c", "type": "test"},
        ]

        executor = LocalExecutor()

        with patch("sqlflow.core.executors.local_executor.logger") as mock_logger:
            executor.execute(plan)
            mock_logger.warning.assert_not_called()
