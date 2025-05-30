"""Comprehensive UDF performance and optimization tests.

This module consolidates all UDF performance testing:
- Performance benchmarking across dataset sizes
- Memory efficiency validation
- Batch processing and scalability tests
- Optimization validation (vectorized vs non-vectorized)
- Arrow-based performance improvements
- Performance regression detection

Tests follow naming convention: test_{performance_aspect}_{scenario}
Each test represents a real performance scenario users encounter.
"""

import gc
import math
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Generator, Tuple

import numpy as np
import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)

# Performance targets for different dataset sizes
PERFORMANCE_TARGETS = {
    "micro": {"rows": 100, "max_time_ms": 50, "max_memory_mb": 5},
    "small": {"rows": 1000, "max_time_ms": 200, "max_memory_mb": 10},
    "medium": {"rows": 10000, "max_time_ms": 1000, "max_memory_mb": 50},
    "large": {"rows": 100000, "max_time_ms": 5000, "max_memory_mb": 200},
}


@pytest.fixture
def performance_test_env() -> Generator[Dict[str, Any], None, None]:
    """Create test environment with performance-optimized UDF functions."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create performance UDF file
        performance_udf_file = create_performance_udf_file(udf_dir)

        # Create optimization UDF file
        optimization_udf_file = create_optimization_udf_file(udf_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "performance_udf_file": performance_udf_file,
            "optimization_udf_file": optimization_udf_file,
        }


def create_performance_udf_file(udf_dir: str) -> Path:
    """Create UDF file with performance-focused functions."""
    udf_file = Path(udf_dir) / "performance_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Performance-focused UDF functions for benchmarking."""

import math
import pandas as pd
import numpy as np
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def simple_add(value: float) -> float:
    """Simple addition for baseline performance."""
    return value + 10.0


@python_scalar_udf
def complex_math(value: float) -> float:
    """Complex mathematical operations for performance testing."""
    if value <= 0:
        return 0.0
    return math.log(value) * math.sin(value) + math.sqrt(abs(value))


@python_scalar_udf
def string_processing(text: str) -> str:
    """String processing for text performance testing."""
    if not text:
        return ""
    return text.upper().strip() + "_PROCESSED"


@python_table_udf(output_schema={
    "id": "INTEGER",
    "value": "DOUBLE",
    "processed": "DOUBLE",
    "category": "VARCHAR"
})
def basic_table_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Basic table processing for performance baseline."""
    result = df.copy()
    
    # Safe numeric operations
    value_col = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0)
    result["processed"] = value_col * 2.5 + 10
    result["category"] = result["processed"].apply(
        lambda x: "high" if x > 100 else "medium" if x > 50 else "low"
    )
    
    return result[["id", "value", "processed", "category"]]


@python_table_udf(output_schema={
    "id": "INTEGER",
    "value": "DOUBLE", 
    "complex_result": "DOUBLE",
    "rank": "INTEGER"
})
def complex_table_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Complex table processing for performance testing."""
    result = df.copy()
    
    # Complex vectorized operations
    value_col = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0)
    
    # Mathematical transformations
    result["complex_result"] = (
        np.log1p(value_col) * np.sqrt(np.abs(value_col)) + 
        np.sin(value_col * np.pi / 180)
    )
    
    # Ranking operation
    result["rank"] = result["complex_result"].rank(method="dense").astype(int)
    
    return result[["id", "value", "complex_result", "rank"]]


@python_table_udf(output_schema={
    "batch_id": "INTEGER",
    "row_count": "INTEGER",
    "avg_value": "DOUBLE",
    "processed_count": "INTEGER"
})
def batch_aggregation(df: pd.DataFrame, *, batch_size: int = 1000) -> pd.DataFrame:
    """Batch processing with aggregation for scalability testing."""
    results = []
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        
        # Safe aggregation
        value_col = pd.to_numeric(batch.get("value", 0), errors="coerce").fillna(0)
        
        batch_result = {
            "batch_id": i // batch_size,
            "row_count": len(batch),
            "avg_value": value_col.mean(),
            "processed_count": len(batch[value_col > 0])
        }
        results.append(batch_result)
    
    return pd.DataFrame(results)
'''
        )
    return udf_file


def create_optimization_udf_file(udf_dir: str) -> Path:
    """Create UDF file with optimized vs non-optimized functions."""
    udf_file = Path(udf_dir) / "optimization_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Optimization comparison UDF functions."""

import pandas as pd
import numpy as np
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_table_udf(output_schema={
    "id": "INTEGER",
    "input_value": "DOUBLE",
    "processed_value": "DOUBLE"
})
def row_by_row_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Non-optimized row-by-row processing."""
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["input_value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    
    # Inefficient row-by-row processing
    processed_values = []
    for _, row in df.iterrows():
        value = row.get("value", 0)
        if pd.isna(value):
            processed_values.append(0.0)
        else:
            # Complex calculation per row
            processed = value ** 2 + np.sin(value) + np.log1p(abs(value))
            processed_values.append(processed)
    
    result["processed_value"] = processed_values
    return result


@python_table_udf(output_schema={
    "id": "INTEGER",
    "input_value": "DOUBLE", 
    "processed_value": "DOUBLE"
})
def vectorized_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Optimized vectorized processing."""
    result = df.copy()
    
    # Efficient vectorized operations
    value_col = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0)
    result["input_value"] = value_col
    result["processed_value"] = (
        value_col ** 2 + np.sin(value_col) + np.log1p(np.abs(value_col))
    )
    
    return result[["id", "input_value", "processed_value"]]


@python_table_udf(output_schema={
    "id": "INTEGER",
    "result": "DOUBLE"
})
def memory_efficient_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Memory-efficient processing with chunking."""
    chunk_size = min(1000, len(df))
    results = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        
        # Process chunk efficiently
        value_col = pd.to_numeric(chunk.get("value", 0), errors="coerce").fillna(0)
        chunk_result = chunk[["id"]].copy()
        chunk_result["result"] = value_col * 2.0
        
        results.append(chunk_result)
    
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame({"id": [], "result": []})
'''
        )
    return udf_file


def time_execution(func, *args, **kwargs) -> Tuple[Any, float]:
    """Time the execution of a function and return result and duration."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    return result, end_time - start_time


def generate_test_data(size_category: str) -> pd.DataFrame:
    """Generate test data for performance testing."""
    size_config = PERFORMANCE_TARGETS[size_category]
    rows = size_config["rows"]

    return pd.DataFrame(
        {
            "id": range(1, rows + 1),
            "value": np.random.uniform(1.0, 100.0, rows),
            "text": [f"text_{i}" for i in range(rows)],
            "group_id": np.random.randint(1, max(10, rows // 100), rows),
        }
    )


class TestPerformanceBenchmarks:
    """Test performance benchmarks across different scenarios."""

    @pytest.mark.parametrize("size", ["micro", "small", "medium"])
    def test_performance_scalar_udf_benchmarks(
        self, performance_test_env: Dict[str, Any], size: str
    ) -> None:
        """User runs scalar UDFs on datasets of varying sizes."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Generate test data
        test_data = generate_test_data(size)
        target = PERFORMANCE_TARGETS[size]

        # Register test data as table
        engine.register_table("test_data", test_data)

        # Test simple scalar UDF performance
        simple_result, simple_time = time_execution(
            engine.execute_query,
            "SELECT id, simple_add(value) as result FROM test_data",
        )
        simple_result_df = simple_result.fetchdf()
        simple_time_ms = simple_time * 1000

        # Test complex scalar UDF performance
        complex_result, complex_time = time_execution(
            engine.execute_query,
            "SELECT id, complex_math(value) as result FROM test_data",
        )
        complex_result_df = complex_result.fetchdf()
        complex_time_ms = complex_time * 1000

        # Validate results
        assert len(simple_result_df) == target["rows"]
        assert len(complex_result_df) == target["rows"]

        # Performance assertions
        assert (
            simple_time_ms < target["max_time_ms"]
        ), f"Simple UDF too slow: {simple_time_ms:.2f}ms > {target['max_time_ms']}ms"

        # Complex UDF gets more lenient time limit
        complex_limit = target["max_time_ms"] * 3
        assert (
            complex_time_ms < complex_limit
        ), f"Complex UDF too slow: {complex_time_ms:.2f}ms > {complex_limit}ms"

        logger.info(
            f"✅ Scalar UDF performance ({size}): simple={simple_time_ms:.2f}ms, complex={complex_time_ms:.2f}ms"
        )

    @pytest.mark.parametrize("size", ["micro", "small", "medium"])
    def test_performance_table_udf_benchmarks(
        self, performance_test_env: Dict[str, Any], size: str
    ) -> None:
        """User runs table UDFs on datasets of varying sizes."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Generate test data
        test_data = generate_test_data(size)
        target = PERFORMANCE_TARGETS[size]

        # Get UDF functions
        basic_func = udfs["python_udfs.performance_udfs.basic_table_processing"]
        complex_func = udfs["python_udfs.performance_udfs.complex_table_processing"]

        # Test basic table UDF performance
        basic_result, basic_time = time_execution(basic_func, test_data)
        basic_time_ms = basic_time * 1000

        # Test complex table UDF performance
        complex_result, complex_time = time_execution(complex_func, test_data)
        complex_time_ms = complex_time * 1000

        # Validate results
        assert len(basic_result) == target["rows"]
        assert len(complex_result) == target["rows"]
        assert "processed" in basic_result.columns
        assert "complex_result" in complex_result.columns

        # Performance assertions
        table_time_limit = target["max_time_ms"] * 2  # Table UDFs get 2x time limit
        assert (
            basic_time_ms < table_time_limit
        ), f"Basic table UDF too slow: {basic_time_ms:.2f}ms > {table_time_limit}ms"

        complex_table_limit = (
            target["max_time_ms"] * 4
        )  # Complex table UDFs get 4x time limit
        assert (
            complex_time_ms < complex_table_limit
        ), f"Complex table UDF too slow: {complex_time_ms:.2f}ms > {complex_table_limit}ms"

        logger.info(
            f"✅ Table UDF performance ({size}): basic={basic_time_ms:.2f}ms, complex={complex_time_ms:.2f}ms"
        )

    def test_performance_throughput_validation(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User validates UDF processing throughput."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Test throughput with medium dataset
        test_data = generate_test_data("medium")
        basic_func = udfs["python_udfs.performance_udfs.basic_table_processing"]

        # Multiple runs for stable measurement
        times = []
        for _ in range(3):
            _, exec_time = time_execution(basic_func, test_data)
            times.append(exec_time)

        avg_time = sum(times) / len(times)
        throughput = len(test_data) / avg_time  # rows per second

        # Throughput should be at least 1000 rows/second
        min_throughput = 1000
        assert (
            throughput > min_throughput
        ), f"Throughput too low: {throughput:.0f} rows/sec < {min_throughput} rows/sec"

        logger.info(f"✅ Throughput validation: {throughput:.0f} rows/sec")


class TestMemoryEfficiency:
    """Test memory efficiency and resource management."""

    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="Memory tests require psutil and can be flaky in CI",
    )
    def test_memory_efficiency_validation(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User validates memory efficiency of UDF processing."""
        try:
            import psutil
        except ImportError:
            pytest.skip("psutil not available for memory testing")

        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        process = psutil.Process(os.getpid())

        # Get memory-efficient UDF
        memory_func = udfs["python_udfs.optimization_udfs.memory_efficient_processing"]

        # Test different data sizes
        for size_category in ["small", "medium"]:
            gc.collect()  # Clean up before test

            test_data = generate_test_data(size_category)
            target = PERFORMANCE_TARGETS[size_category]

            memory_before = process.memory_info().rss / 1024 / 1024  # MB

            # Execute memory-efficient UDF
            result = memory_func(test_data)

            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_used = memory_after - memory_before

            # Validate results
            assert len(result) == target["rows"]

            # Memory efficiency assertion
            assert (
                memory_used < target["max_memory_mb"]
            ), f"Memory usage too high ({size_category}): {memory_used:.2f}MB > {target['max_memory_mb']}MB"

            logger.info(
                f"✅ Memory efficiency ({size_category}): {memory_used:.2f}MB used"
            )

    def test_memory_efficiency_chunked_processing(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User processes large datasets with memory-efficient chunking."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get batch processing UDF
        batch_func = udfs["python_udfs.performance_udfs.batch_aggregation"]

        # Create larger dataset for chunking test
        large_data = pd.DataFrame(
            {"id": range(5000), "value": np.random.uniform(1.0, 100.0, 5000)}
        )

        # Test with different batch sizes
        for batch_size in [500, 1000, 2000]:
            result, exec_time = time_execution(
                batch_func, large_data, batch_size=batch_size
            )

            # Validate chunked results
            expected_batches = math.ceil(len(large_data) / batch_size)
            assert len(result) == expected_batches
            assert "batch_id" in result.columns
            assert "row_count" in result.columns

            # Verify chunk sizes are correct
            total_rows = result["row_count"].sum()
            assert total_rows == len(large_data)

            logger.info(
                f"✅ Chunked processing (batch_size={batch_size}): {len(result)} batches in {exec_time * 1000:.2f}ms"
            )


class TestOptimizationValidation:
    """Test optimization effectiveness and comparisons."""

    def test_optimization_vectorized_vs_iterative(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User compares vectorized vs iterative processing performance."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get optimization comparison UDFs
        iterative_func = udfs["python_udfs.optimization_udfs.row_by_row_processing"]
        vectorized_func = udfs["python_udfs.optimization_udfs.vectorized_processing"]

        # Test with medium dataset where optimization matters
        test_data = generate_test_data("medium")

        # Run multiple times for stable measurement
        iterative_times = []
        vectorized_times = []

        for _ in range(3):
            # Test iterative approach
            _, iter_time = time_execution(iterative_func, test_data)
            iterative_times.append(iter_time)

            # Test vectorized approach
            _, vec_time = time_execution(vectorized_func, test_data)
            vectorized_times.append(vec_time)

        avg_iterative = sum(iterative_times) / len(iterative_times)
        avg_vectorized = sum(vectorized_times) / len(vectorized_times)

        # Vectorized should be faster (at least 10% improvement)
        speedup = avg_iterative / avg_vectorized
        min_speedup = 1.1  # At least 10% faster

        assert (
            speedup > min_speedup
        ), f"Vectorization not effective: {speedup:.2f}x speedup < {min_speedup}x"

        logger.info(
            f"✅ Optimization validation: {speedup:.2f}x speedup from vectorization"
        )

    def test_optimization_performance_regression_detection(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User detects performance regressions in UDF system."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Use simple UDF as baseline
        baseline_func = udfs["python_udfs.performance_udfs.basic_table_processing"]

        # Test with different data sizes
        sizes = ["micro", "small", "medium"]
        execution_times = []

        for size in sizes:
            test_data = generate_test_data(size)

            # Multiple runs for stability
            times = []
            for _ in range(3):
                _, exec_time = time_execution(baseline_func, test_data)
                times.append(exec_time * 1000)  # Convert to ms

            avg_time = sum(times) / len(times)
            execution_times.append(avg_time)

            target = PERFORMANCE_TARGETS[size]

            # Regression detection: should scale reasonably
            expected_max_time = (
                target["max_time_ms"] * 2
            )  # 2x buffer for regression detection
            assert (
                avg_time < expected_max_time
            ), f"Performance regression detected for {size}: {avg_time:.2f}ms > {expected_max_time}ms"

        # Check scaling is reasonable (not exponential)
        if len(execution_times) >= 2:
            scaling_factor = execution_times[-1] / execution_times[0]
            size_factor = (
                PERFORMANCE_TARGETS[sizes[-1]]["rows"]
                / PERFORMANCE_TARGETS[sizes[0]]["rows"]
            )
            scaling_ratio = scaling_factor / size_factor

            # Time should not scale worse than O(n^1.5)
            max_scaling_ratio = 1.5
            assert (
                scaling_ratio < max_scaling_ratio
            ), f"Poor scaling detected: {scaling_ratio:.2f} > {max_scaling_ratio}"

        logger.info(f"✅ Regression detection: execution times {execution_times}")


class TestScalabilityValidation:
    """Test scalability characteristics and limits."""

    def test_scalability_dataset_size_scaling(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User validates UDF scalability across increasing dataset sizes."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get scalability test UDF
        scalability_func = udfs["python_udfs.performance_udfs.complex_table_processing"]

        # Test with increasing dataset sizes
        sizes = [100, 500, 1000, 2500, 5000]
        execution_times = []

        for size in sizes:
            test_data = pd.DataFrame(
                {"id": range(size), "value": np.random.uniform(1.0, 100.0, size)}
            )

            _, exec_time = time_execution(scalability_func, test_data)
            execution_time_ms = exec_time * 1000
            execution_times.append(execution_time_ms)

            logger.info(f"   {size:,} rows: {execution_time_ms:.2f}ms")

        # Analyze scalability characteristics
        # Calculate scaling ratios
        scaling_ratios = []
        for i in range(1, len(sizes)):
            time_ratio = execution_times[i] / execution_times[i - 1]
            size_ratio = sizes[i] / sizes[i - 1]
            scaling_ratio = time_ratio / size_ratio
            scaling_ratios.append(scaling_ratio)

        # Average scaling should be reasonable (close to linear)
        avg_scaling = sum(scaling_ratios) / len(scaling_ratios)
        max_acceptable_scaling = 2.0  # Time growth should not exceed 2x size growth

        assert (
            avg_scaling < max_acceptable_scaling
        ), f"Poor scalability: {avg_scaling:.2f}x > {max_acceptable_scaling}x"

        logger.info(f"✅ Scalability validation: {avg_scaling:.2f}x scaling ratio")

    def test_scalability_concurrent_processing(
        self, performance_test_env: Dict[str, Any]
    ) -> None:
        """User validates UDF behavior under concurrent processing scenarios."""
        manager = PythonUDFManager(project_dir=performance_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get UDF for concurrent testing
        concurrent_func = udfs["python_udfs.performance_udfs.basic_table_processing"]

        # Create multiple datasets for concurrent processing simulation
        datasets = []
        for i in range(3):
            data = pd.DataFrame(
                {
                    "id": range(i * 1000, (i + 1) * 1000),
                    "value": np.random.uniform(1.0, 100.0, 1000),
                }
            )
            datasets.append(data)

        # Process datasets sequentially (simulating concurrent load)
        results = []
        total_start_time = time.perf_counter()

        for i, dataset in enumerate(datasets):
            result, exec_time = time_execution(concurrent_func, dataset)
            results.append(result)
            logger.info(f"   Dataset {i + 1}: {exec_time * 1000:.2f}ms")

        total_time = time.perf_counter() - total_start_time

        # Validate all results
        for i, result in enumerate(results):
            assert len(result) == 1000
            assert "processed" in result.columns

        # Total time should be reasonable for concurrent processing
        max_total_time = 5.0  # 5 seconds for 3 datasets
        assert (
            total_time < max_total_time
        ), f"Concurrent processing too slow: {total_time:.2f}s > {max_total_time}s"

        logger.info(
            f"✅ Concurrent processing: {total_time:.2f}s total for {len(datasets)} datasets"
        )
