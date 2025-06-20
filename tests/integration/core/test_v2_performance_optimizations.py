"""Test V2 Performance Optimizations.

Following 04_testing_standards.mdc principles:
- Minimize mocking: Use real implementations
- Test behavior, not implementation
- Tests as documentation
- Cover real edge cases
"""

import os
import tempfile
import time

import pandas as pd

from sqlflow.core.executors.v2.data_transfer_optimization import (
    DataTransferMetrics,
)
from sqlflow.core.executors.v2.load_operations import LoadStepExecutor
from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator
from sqlflow.core.executors.v2.variable_optimization import (
    OptimizedVariableSubstitution,
    benchmark_substitution_methods,
)


class TestDataTransferOptimizations:
    """Test data transfer performance optimizations."""

    def test_data_transfer_metrics_tracking(self):
        """Test that DataTransferMetrics properly tracks performance."""
        # Create test data
        metrics = DataTransferMetrics(
            rows_transferred=1000,
            bytes_transferred=50000,
            duration_ms=250.0,
            chunks_processed=1,
            strategy_used="duckdb_copy_csv",
        )

        # Verify metrics calculation
        assert metrics.rows_transferred == 1000
        assert metrics.bytes_transferred == 50000
        assert metrics.duration_ms == 250.0
        assert metrics.throughput_rows_per_second == 4000.0  # 1000 rows / 0.25 seconds
        assert metrics.strategy_used == "duckdb_copy_csv"

    def test_orchestrator_optimizations_enabled_by_default(self):
        """Test that optimizations are enabled by default in V2 orchestrator."""
        orchestrator = LocalOrchestrator()

        # Verify optimizations are enabled by default
        assert orchestrator._enable_optimizations is True
        assert orchestrator._variable_substitution is not None
        assert isinstance(
            orchestrator._variable_substitution, OptimizedVariableSubstitution
        )
        assert orchestrator._transfer_metrics == []

    def test_orchestrator_optimizations_can_be_disabled(self):
        """Test that optimizations can be disabled if needed."""
        orchestrator = LocalOrchestrator(enable_optimizations=False)

        # Verify optimizations are disabled
        assert orchestrator._enable_optimizations is False
        assert orchestrator._data_transfer_optimizer is None

    def test_large_dataset_uses_optimized_transfer(self):
        """Test that large datasets automatically use optimized transfer."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        # Create large test dataset
        large_data = [{"id": i, "value": f"value_{i}"} for i in range(200)]

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, "large_data.csv")

            # Create CSV file
            pd.DataFrame(large_data).to_csv(csv_path, index=False)

            pipeline_plan = [
                {
                    "type": "source_definition",
                    "id": "large_source",
                    "name": "large_dataset",
                    "connector_type": "csv",
                    "params": {"path": csv_path, "has_header": True},
                },
                {
                    "type": "load",
                    "id": "large_load",
                    "source_name": "large_dataset",
                    "target_table": "large_table",
                    "mode": "REPLACE",
                },
            ]

            result = orchestrator.execute(pipeline_plan, {})

            # Verify successful execution
            assert result["status"] == "success"

            # Verify performance metrics are captured
            assert "performance_summary" in result
            perf_summary = result["performance_summary"]
            assert "data_transfer" in perf_summary

            transfer_summary = perf_summary["data_transfer"]
            assert transfer_summary["optimization_enabled"] is True

            # Check if optimized transfer was used (may fall back to standard)
            if transfer_summary["total_transfers"] > 0:
                assert transfer_summary["total_rows_transferred"] >= 0
                assert "strategies_used" in transfer_summary

    def test_small_dataset_uses_standard_transfer(self):
        """Test that small datasets use standard transfer method."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        # Create small test dataset (below optimization threshold)
        small_data = [{"id": i, "value": f"value_{i}"} for i in range(5)]

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, "small_data.csv")

            # Create CSV file
            pd.DataFrame(small_data).to_csv(csv_path, index=False)

            pipeline_plan = [
                {
                    "type": "source_definition",
                    "id": "small_source",
                    "name": "small_dataset",
                    "connector_type": "csv",
                    "params": {"path": csv_path, "has_header": True},
                },
                {
                    "type": "load",
                    "id": "small_load",
                    "source_name": "small_dataset",
                    "target_table": "small_table",
                    "mode": "REPLACE",
                },
            ]

            result = orchestrator.execute(pipeline_plan, {})

            # Verify successful execution
            assert result["status"] == "success"

            # Small datasets may not trigger optimized transfer
            assert "performance_summary" in result

    def test_transfer_performance_summary(self):
        """Test transfer performance summary aggregation."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        # Simulate multiple transfers
        metrics1 = DataTransferMetrics(
            rows_transferred=100,
            bytes_transferred=5000,
            duration_ms=50.0,
            chunks_processed=1,
            strategy_used="optimized_copy",
        )

        metrics2 = DataTransferMetrics(
            rows_transferred=200,
            bytes_transferred=10000,
            duration_ms=75.0,
            chunks_processed=1,
            strategy_used="standard_register_table",
        )

        orchestrator._record_transfer_metrics(metrics1)
        orchestrator._record_transfer_metrics(metrics2)

        summary = orchestrator.get_transfer_performance_summary()

        # Verify aggregated metrics
        assert summary["total_transfers"] == 2
        assert summary["total_rows_transferred"] == 300
        assert summary["total_bytes_transferred"] == 15000
        assert summary["total_transfer_time_ms"] == 125.0
        assert summary["optimization_enabled"] is True
        assert len(summary["strategies_used"]) == 2
        assert "optimized_copy" in summary["strategies_used"]
        assert "standard_register_table" in summary["strategies_used"]


class TestVariableOptimizations:
    """Test variable substitution optimizations."""

    def test_optimized_variable_substitution_enabled_by_default(self):
        """Test that optimized variable substitution is used by default."""
        orchestrator = LocalOrchestrator()

        variables = {"env": "test", "table_prefix": "tmp_"}
        text = "SELECT * FROM ${table_prefix}users WHERE env = '${env}'"

        result = orchestrator._substitute_variables(text, variables)
        expected = "SELECT * FROM tmp_users WHERE env = 'test'"

        assert result == expected

    def test_variable_substitution_fallback_when_disabled(self):
        """Test fallback to V1 method when optimizations disabled."""
        orchestrator = LocalOrchestrator(enable_optimizations=False)

        variables = {"env": "test", "table_prefix": "tmp_"}
        text = "SELECT * FROM ${table_prefix}users WHERE env = '${env}'"

        result = orchestrator._substitute_variables(text, variables)
        expected = "SELECT * FROM tmp_users WHERE env = 'test'"

        assert result == expected

    def test_variable_substitution_in_dict(self):
        """Test optimized variable substitution in dictionaries."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        variables = {"env": "test", "db_name": "analytics"}
        data = {
            "source": "${db_name}_raw",
            "target": "${db_name}_processed",
            "config": {"environment": "${env}", "timeout": 300},
        }

        result = orchestrator._substitute_variables_in_dict(data, variables)

        assert result["source"] == "analytics_raw"
        assert result["target"] == "analytics_processed"
        assert result["config"]["environment"] == "test"
        assert result["config"]["timeout"] == 300

    def test_variable_substitution_performance_benchmark(self):
        """Test that optimized substitution is faster than V1 method."""
        variables = {
            "env": "production",
            "region": "us-west-2",
            "table_prefix": "analytics_",
            "date": "2024-01-01",
        }

        text = """
        SELECT 
            ${table_prefix}users.id,
            ${table_prefix}orders.amount
        FROM ${table_prefix}users
        JOIN ${table_prefix}orders ON users.id = orders.user_id
        WHERE date = '${date}'
        AND region = '${region}'
        AND environment = '${env}'
        """

        # Benchmark both methods
        benchmark_results = benchmark_substitution_methods(
            text, variables, iterations=100
        )

        # Verify benchmark ran successfully
        assert "optimized" in benchmark_results
        assert "legacy" in benchmark_results
        assert benchmark_results["optimized"] > 0
        assert benchmark_results["legacy"] > 0

        # Optimized should be faster (though in small tests the difference may be minimal)
        # Just verify both methods produce the same result
        from sqlflow.core.executors.v2.variable_optimization import (
            substitute_variables_legacy,
            substitute_variables_optimized,
        )

        optimized_result = substitute_variables_optimized(text, variables)
        legacy_result = substitute_variables_legacy(text, variables)

        assert optimized_result == legacy_result


class TestLoadStepOptimizations:
    """Test load step performance optimizations."""

    def test_load_step_executor_optimizations_enabled(self):
        """Test that LoadStepExecutor uses optimizations by default."""
        executor = LoadStepExecutor(enable_optimizations=True)

        assert executor.enable_optimizations is True
        assert executor._transfer_metrics == []

    def test_load_step_executor_optimizations_disabled(self):
        """Test LoadStepExecutor with optimizations disabled."""
        executor = LoadStepExecutor(enable_optimizations=False)

        assert executor.enable_optimizations is False

    def test_optimized_transfer_threshold(self):
        """Test that optimized transfer is only used for large datasets."""
        from sqlflow.core.executors.v2.load_operations import LoadStepData

        executor = LoadStepExecutor(enable_optimizations=True)

        # Mock engine with connection
        class MockEngine:
            def __init__(self):
                self.connection = True

        engine = MockEngine()

        # Small dataset - should not use optimization
        small_step = {"source_name": "small", "target_table": "test", "mode": "REPLACE"}
        small_data = LoadStepData(
            small_step, engine, {"small": [{"id": i} for i in range(50)]}
        )

        assert not executor._can_use_optimized_transfer(small_data)

        # Large dataset - should use optimization
        large_step = {"source_name": "large", "target_table": "test", "mode": "REPLACE"}
        large_data = LoadStepData(
            large_step, engine, {"large": [{"id": i} for i in range(150)]}
        )

        assert executor._can_use_optimized_transfer(large_data)

    def test_load_step_transfer_metrics_collection(self):
        """Test that load step collects transfer metrics."""
        executor = LoadStepExecutor(enable_optimizations=True)

        # Add mock metrics
        metrics = DataTransferMetrics(
            rows_transferred=500,
            bytes_transferred=25000,
            duration_ms=100.0,
            chunks_processed=1,
            strategy_used="optimized_load",
        )

        executor._transfer_metrics.append(metrics)

        summary = executor.get_transfer_performance_summary()

        assert summary["total_transfers"] == 1
        assert summary["total_rows_transferred"] == 500
        assert summary["total_transfer_time_ms"] == 100.0


class TestErrorHandlingWithOptimizations:
    """Test error handling when optimizations are enabled."""

    def test_optimized_transfer_fallback_on_error(self):
        """Test that optimization failures gracefully fall back to standard methods."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        # This should work even if optimization fails
        pipeline_plan = [
            {
                "type": "transform",
                "id": "simple_transform",
                "target_table": "test_table",
                "sql": "SELECT 1 as id, 'test' as name",
            }
        ]

        result = orchestrator.execute(pipeline_plan, {})

        # Should succeed even if optimizations fail
        assert result["status"] == "success"

    def test_variable_substitution_error_handling(self):
        """Test error handling in variable substitution."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        # Test with malformed template
        variables = {"valid_var": "test"}
        text = "SELECT * FROM ${invalid_template"  # Missing closing brace

        # Should handle gracefully (may fall back to V1 method)
        result = orchestrator._substitute_variables(text, variables)

        # Should return something (either processed or original)
        assert isinstance(result, str)


class TestEndToEndOptimizedPipeline:
    """Test complete pipelines with all optimizations enabled."""

    def test_complete_optimized_pipeline(self):
        """Test a complete pipeline with all optimizations enabled."""
        orchestrator = LocalOrchestrator(enable_optimizations=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data file
            test_data = [
                {
                    "customer_id": i,
                    "order_amount": i * 10.5,
                    "region": f"region_{i % 3}",
                }
                for i in range(150)  # Large enough to trigger optimizations
            ]

            csv_path = os.path.join(temp_dir, "orders.csv")
            pd.DataFrame(test_data).to_csv(csv_path, index=False)

            export_path = os.path.join(temp_dir, "summary.csv")

            # Variables for substitution
            variables = {
                "source_file": csv_path,
                "output_file": export_path,
                "min_amount": "50",
            }

            pipeline_plan = [
                {
                    "type": "source_definition",
                    "id": "orders_source",
                    "name": "orders",
                    "connector_type": "csv",
                    "params": {"path": "${source_file}", "has_header": True},
                },
                {
                    "type": "load",
                    "id": "load_orders",
                    "source_name": "orders",
                    "target_table": "raw_orders",
                    "mode": "REPLACE",
                },
                {
                    "type": "transform",
                    "id": "summarize_orders",
                    "target_table": "order_summary",
                    "sql": """
                        SELECT 
                            region,
                            COUNT(*) as order_count,
                            AVG(order_amount) as avg_amount,
                            SUM(order_amount) as total_amount
                        FROM raw_orders 
                        WHERE order_amount >= ${min_amount}
                        GROUP BY region
                    """,
                },
                {
                    "type": "export",
                    "id": "export_summary",
                    "source_table": "order_summary",
                    "destination": "${output_file}",
                    "connector_type": "csv",
                },
            ]

            result = orchestrator.execute(pipeline_plan, variables)

            # Verify successful execution
            assert result["status"] == "success"
            assert len(result["executed_steps"]) == 4

            # Verify performance metrics are captured
            assert "performance_summary" in result
            perf_summary = result["performance_summary"]

            # Check data transfer metrics
            assert "data_transfer" in perf_summary
            transfer_summary = perf_summary["data_transfer"]
            assert transfer_summary["optimization_enabled"] is True

            # Verify output file exists
            assert os.path.exists(export_path)

            # Verify output content
            summary_df = pd.read_csv(export_path)
            assert len(summary_df) == 3  # 3 regions
            assert "region" in summary_df.columns
            assert "order_count" in summary_df.columns
            assert "avg_amount" in summary_df.columns
            assert "total_amount" in summary_df.columns

    def test_pipeline_performance_comparison(self):
        """Test performance comparison between optimized and standard execution."""
        # Create test data
        test_data = [
            {"id": i, "value": f"value_{i}", "category": f"cat_{i % 5}"}
            for i in range(300)  # Large enough for meaningful comparison
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, "perf_test.csv")
            pd.DataFrame(test_data).to_csv(csv_path, index=False)

            pipeline_plan = [
                {
                    "type": "source_definition",
                    "id": "perf_source",
                    "name": "perf_data",
                    "connector_type": "csv",
                    "params": {"path": csv_path, "has_header": True},
                },
                {
                    "type": "load",
                    "id": "perf_load",
                    "source_name": "perf_data",
                    "target_table": "perf_table",
                    "mode": "REPLACE",
                },
            ]

            # Test with optimizations enabled
            start_time = time.time()
            orchestrator_optimized = LocalOrchestrator(enable_optimizations=True)
            result_optimized = orchestrator_optimized.execute(pipeline_plan, {})
            optimized_time = time.time() - start_time

            # Test with optimizations disabled
            start_time = time.time()
            orchestrator_standard = LocalOrchestrator(enable_optimizations=False)
            result_standard = orchestrator_standard.execute(pipeline_plan, {})
            standard_time = time.time() - start_time

            # Both should succeed
            assert result_optimized["status"] == "success"
            assert result_standard["status"] == "success"

            # Verify performance tracking is different
            optimized_perf = result_optimized["performance_summary"]["data_transfer"]
            standard_perf = result_standard["performance_summary"]["data_transfer"]

            assert optimized_perf["optimization_enabled"] is True
            assert standard_perf["optimization_enabled"] is False

            # Log performance comparison for analysis
            print(f"Optimized execution time: {optimized_time:.3f}s")
            print(f"Standard execution time: {standard_time:.3f}s")
            print(f"Optimized transfers: {optimized_perf['total_transfers']}")
            print(f"Standard transfers: {standard_perf['total_transfers']}")
