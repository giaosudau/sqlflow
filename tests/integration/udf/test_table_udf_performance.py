"""Table UDF Performance Tests.

This module contains focused performance tests for table UDFs,
including benchmarks, optimization validation, and memory efficiency tests.
"""

import time
from typing import Any, Dict, cast

import numpy as np
import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.core.engines.duckdb.udf.performance import ArrowPerformanceOptimizer
from sqlflow.logging import get_logger
from sqlflow.udfs.decorators import python_table_udf

logger = get_logger(__name__)


class TestTableUDFPerformance:
    """Performance-focused tests for table UDFs."""

    # Performance benchmarking targets for realistic testing
    PERFORMANCE_TARGETS = {
        "small_dataset": {"rows": 1000, "max_time_ms": 500},
        "medium_dataset": {"rows": 10000, "max_time_ms": 5000},
        "large_dataset": {"rows": 100000, "max_time_ms": 10000},
    }

    def test_performance_benchmarks_small_dataset(self, duckdb_engine: DuckDBEngine):
        """Test performance benchmarks with small dataset."""
        
        @python_table_udf(
            output_schema={"id": "INTEGER", "value": "DOUBLE", "category": "VARCHAR"}
        )
        def benchmark_udf(df: pd.DataFrame) -> pd.DataFrame:
            """Simple benchmark UDF for performance testing."""
            result = df.copy()
            result["value"] = result["id"] * 2.5 + np.random.random(len(df))
            result["category"] = result["id"].apply(
                lambda x: "high" if x > 500 else "low"
            )
            return result[["id", "value", "category"]]

        # Create test dataset
        test_data = pd.DataFrame({
            "id": range(1, self.PERFORMANCE_TARGETS["small_dataset"]["rows"] + 1),
            "original_value": np.random.random(self.PERFORMANCE_TARGETS["small_dataset"]["rows"]),
        })

        duckdb_engine.register_table("test_data", test_data)
        duckdb_engine.register_python_udf("benchmark_udf", benchmark_udf)

        # Execute and measure performance
        start_time = time.time()
        result = duckdb_engine.execute_query(
            "SELECT * FROM benchmark_udf(SELECT * FROM test_data)"
        ).fetchdf()
        execution_time = (time.time() - start_time) * 1000

        # Validate performance
        max_time = self.PERFORMANCE_TARGETS["small_dataset"]["max_time_ms"]
        assert execution_time < max_time, f"Execution took {execution_time:.2f}ms, expected < {max_time}ms"
        
        # Validate results
        assert len(result) == self.PERFORMANCE_TARGETS["small_dataset"]["rows"]
        assert "value" in result.columns
        assert "category" in result.columns

    def test_performance_benchmarks_medium_dataset(self, duckdb_engine: DuckDBEngine):
        """Test performance benchmarks with medium dataset."""
        
        @python_table_udf(
            output_schema={
                "id": "INTEGER",
                "processed_value": "DOUBLE",
                "bucket": "VARCHAR",
                "rank": "INTEGER",
            }
        )
        def medium_benchmark_udf(df: pd.DataFrame) -> pd.DataFrame:
            """Medium complexity UDF for performance testing."""
            result = df.copy()
            
            # More complex processing
            result["processed_value"] = (
                result["value"] * 2 + np.sin(result["id"] / 100) * 10
            )
            result["bucket"] = pd.cut(
                result["processed_value"], 
                bins=5, 
                labels=["very_low", "low", "medium", "high", "very_high"]
            ).astype(str)
            result["rank"] = result["processed_value"].rank(method="dense").astype(int)
            
            return result[["id", "processed_value", "bucket", "rank"]]

        # Create medium test dataset
        rows = self.PERFORMANCE_TARGETS["medium_dataset"]["rows"]
        test_data = pd.DataFrame({
            "id": range(1, rows + 1),
            "value": np.random.normal(100, 25, rows),
        })

        duckdb_engine.register_table("medium_test_data", test_data)
        duckdb_engine.register_python_udf("medium_benchmark_udf", medium_benchmark_udf)

        # Execute and measure performance
        start_time = time.time()
        result = duckdb_engine.execute_query(
            "SELECT * FROM medium_benchmark_udf(SELECT * FROM medium_test_data)"
        ).fetchdf()
        execution_time = (time.time() - start_time) * 1000

        # Validate performance
        max_time = self.PERFORMANCE_TARGETS["medium_dataset"]["max_time_ms"]
        assert execution_time < max_time, f"Execution took {execution_time:.2f}ms, expected < {max_time}ms"
        
        # Validate results structure
        assert len(result) == rows
        assert set(result.columns) == {"id", "processed_value", "bucket", "rank"}

    @pytest.mark.skipif(
        True, reason="psutil not available for memory testing"
    )
    def test_memory_efficiency_validation(self, duckdb_engine: DuckDBEngine):
        """Test memory efficiency of table UDFs."""
        try:
            import psutil
            import os
        except ImportError:
            pytest.skip("psutil not available for memory testing")

        @python_table_udf(
            output_schema={"id": "INTEGER", "result": "DOUBLE"}
        )
        def memory_efficient_udf(df: pd.DataFrame) -> pd.DataFrame:
            """Memory-efficient UDF implementation."""
            # Process in chunks to manage memory
            chunk_size = 1000
            results = []
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk_result = chunk.copy()
                chunk_result["result"] = chunk_result["id"] * 1.5
                results.append(chunk_result[["id", "result"]])
            
            return pd.concat(results, ignore_index=True)

        # Monitor memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Create large dataset
        large_data = pd.DataFrame({
            "id": range(1, 50001),  # 50K rows
            "value": np.random.random(50000),
        })

        duckdb_engine.register_table("large_data", large_data)
        duckdb_engine.register_python_udf("memory_efficient_udf", memory_efficient_udf)

        # Execute UDF
        result = duckdb_engine.execute_query(
            "SELECT * FROM memory_efficient_udf(SELECT * FROM large_data)"
        ).fetchdf()

        # Check memory usage
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Validate memory efficiency (should not exceed 100MB increase)
        max_memory_increase = 100 * 1024 * 1024  # 100MB
        assert memory_increase < max_memory_increase, f"Memory increased by {memory_increase / 1024 / 1024:.2f}MB"
        
        # Validate results
        assert len(result) == 50000
        assert "result" in result.columns

    def test_arrow_performance_optimization(self, duckdb_engine: DuckDBEngine):
        """Test Arrow-based performance optimization."""
        optimizer = ArrowPerformanceOptimizer()

        @python_table_udf(
            output_schema={"id": "INTEGER", "optimized_value": "DOUBLE"}
        )
        def arrow_optimized_udf(df: pd.DataFrame) -> pd.DataFrame:
            """UDF that can benefit from Arrow optimization."""
            result = df.copy()
            
            # Vectorized operations that can be optimized
            result["optimized_value"] = (
                result["value"] * 2.0 + 
                np.sqrt(result["value"]) - 
                np.log1p(result["value"])
            )
            
            return result[["id", "optimized_value"]]

        # Create test data
        test_data = pd.DataFrame({
            "id": range(1, 5001),
            "value": np.random.uniform(1, 100, 5000),
        })

        duckdb_engine.register_table("arrow_test_data", test_data)
        duckdb_engine.register_python_udf("arrow_optimized_udf", arrow_optimized_udf)

        # Test optimization detection
        optimization_opportunities = optimizer.identify_optimization_opportunities(
            arrow_optimized_udf
        )
        assert optimization_opportunities is not None

        # Execute and validate
        result = duckdb_engine.execute_query(
            "SELECT * FROM arrow_optimized_udf(SELECT * FROM arrow_test_data)"
        ).fetchdf()

        assert len(result) == 5000
        assert "optimized_value" in result.columns
        assert not result["optimized_value"].isna().any()

    def test_performance_regression_prevention(self, duckdb_engine: DuckDBEngine):
        """Test performance regression prevention mechanisms."""
        
        @python_table_udf(
            output_schema={"id": "INTEGER", "baseline_result": "DOUBLE"}
        )
        def baseline_performance_udf(df: pd.DataFrame) -> pd.DataFrame:
            """Baseline UDF for regression testing."""
            result = df.copy()
            result["baseline_result"] = result["value"] * 1.5 + result["id"] * 0.1
            return result[["id", "baseline_result"]]

        # Create baseline test data
        test_data = pd.DataFrame({
            "id": range(1, 10001),
            "value": np.random.random(10000),
        })

        duckdb_engine.register_table("regression_test_data", test_data)
        duckdb_engine.register_python_udf("baseline_performance_udf", baseline_performance_udf)

        # Measure baseline performance
        execution_times = []
        for _ in range(3):  # Run multiple times for stability
            start_time = time.time()
            result = duckdb_engine.execute_query(
                "SELECT * FROM baseline_performance_udf(SELECT * FROM regression_test_data)"
            ).fetchdf()
            execution_times.append((time.time() - start_time) * 1000)

        avg_execution_time = sum(execution_times) / len(execution_times)
        
        # Performance should be consistent and within acceptable range
        baseline_threshold = 2000  # 2 seconds for 10K rows
        assert avg_execution_time < baseline_threshold, (
            f"Average execution time {avg_execution_time:.2f}ms exceeds threshold {baseline_threshold}ms"
        )
        
        # Validate results consistency
        assert len(result) == 10000
        assert "baseline_result" in result.columns 