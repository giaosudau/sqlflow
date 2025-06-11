"""
Performance tests for Parquet Source connector optimizations.

These tests validate the performance improvements implemented in Task 2.2:
- Predicate pushdown for efficient filtering at storage layer
- Optimized batch sizes based on file characteristics
- Parallel file reading for multiple files

Following our testing standards: "Minimize mocking" - use real implementations.
"""

import os
import tempfile
import unittest

import pandas as pd

from sqlflow.connectors.parquet.source import (
    DEFAULT_BATCH_SIZE,
    LARGE_FILE_THRESHOLD_MB,
    MEMORY_EFFICIENT_BATCH_SIZE,
    ParquetSource,
)


class TestParquetPerformanceOptimizations(unittest.TestCase):
    """Test Parquet performance optimizations following Zen of Python principles."""

    def setUp(self):
        """Set up test data and temporary directory."""
        self.temp_dir = tempfile.mkdtemp()

        # Create test DataFrame with varied data types for realistic testing
        self.test_data = pd.DataFrame(
            {
                "id": range(1000),
                "name": [f"Person_{i}" for i in range(1000)],
                "age": [20 + (i % 60) for i in range(1000)],
                "salary": [30000 + (i * 100) for i in range(1000)],
                "department": [f"Dept_{i % 10}" for i in range(1000)],
                "active": [i % 2 == 0 for i in range(1000)],
            }
        )

        # Create single test file
        self.single_file = os.path.join(self.temp_dir, "test_single.parquet")
        self.test_data.to_parquet(self.single_file)

        # Create multiple test files for parallel testing
        self.multi_files = []
        for i in range(5):
            file_path = os.path.join(self.temp_dir, f"test_multi_{i}.parquet")
            # Create different sized chunks for realistic testing
            chunk_size = 200 + (i * 50)  # Varying sizes: 200, 250, 300, 350, 400
            chunk_data = self.test_data.iloc[i * 200 : (i * 200) + chunk_size].copy()
            chunk_data.to_parquet(file_path)
            self.multi_files.append(file_path)

    def tearDown(self):
        """Clean up test files."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_optimal_batch_size_calculation(self):
        """Test optimal batch size calculation based on file characteristics."""
        connector = ParquetSource()

        # Test with actual file
        optimal_size = connector._get_optimal_batch_size(self.single_file)
        self.assertEqual(
            optimal_size,
            DEFAULT_BATCH_SIZE,
            "Small files should use default batch size",
        )

        # Test user-specified batch size takes precedence
        user_size = 15000
        optimal_size = connector._get_optimal_batch_size(self.single_file, user_size)
        self.assertEqual(
            optimal_size, user_size, "User batch size should take precedence"
        )

    def test_batch_size_for_large_files(self):
        """Test batch size optimization for larger files."""
        # Create a larger file for testing
        large_data = pd.concat([self.test_data] * 100, ignore_index=True)  # 100k rows
        large_file = os.path.join(self.temp_dir, "large_test.parquet")
        large_data.to_parquet(large_file)

        try:
            connector = ParquetSource()

            # Check file size and verify appropriate batch size
            file_size_mb = os.path.getsize(large_file) / (1024 * 1024)
            optimal_size = connector._get_optimal_batch_size(large_file)

            if file_size_mb >= LARGE_FILE_THRESHOLD_MB:
                self.assertEqual(optimal_size, MEMORY_EFFICIENT_BATCH_SIZE)
            elif file_size_mb >= 50:
                self.assertEqual(optimal_size, DEFAULT_BATCH_SIZE // 2)
            else:
                self.assertEqual(optimal_size, DEFAULT_BATCH_SIZE)

        finally:
            os.unlink(large_file)

    def test_predicate_pushdown_filter_building(self):
        """Test predicate pushdown filter expression building."""
        connector = ParquetSource()

        # Test simple equality filter
        filters = {"age": 25}
        filter_expr = connector._build_filter_expression(filters)
        self.assertIsNotNone(filter_expr)

        # Test range filter
        range_filters = {"salary": {">=": 50000, "<": 80000}}
        filter_expr = connector._build_filter_expression(range_filters)
        self.assertIsNotNone(filter_expr)

        # Test multiple conditions
        multi_filters = {"age": {">=": 25}, "department": "Dept_1"}
        filter_expr = connector._build_filter_expression(multi_filters)
        self.assertIsNotNone(filter_expr)

        # Test empty filters
        empty_filter_expr = connector._build_filter_expression({})
        self.assertIsNone(empty_filter_expr)

    def test_predicate_pushdown_functionality(self):
        """Test actual predicate pushdown with real data filtering."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        # Test filtering by age range
        age_filter = {"age": {">=": 50, "<": 60}}
        chunks = list(connector.read(filters=age_filter))

        # Verify filtering worked
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        self.assertGreater(total_rows, 0, "Should have some filtered results")

        # Verify all returned rows meet the filter criteria
        for chunk in chunks:
            df = chunk.pandas_df
            self.assertTrue(all(df["age"] >= 50), "All ages should be >= 50")
            self.assertTrue(all(df["age"] < 60), "All ages should be < 60")

    def test_column_selection_optimization(self):
        """Test column selection for memory efficiency."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        # Read with column selection
        selected_columns = ["id", "name", "age"]
        chunks = list(connector.read(columns=selected_columns))

        # Verify only selected columns are returned
        self.assertGreater(len(chunks), 0, "Should have chunks")
        for chunk in chunks:
            df = chunk.pandas_df
            self.assertEqual(list(df.columns), selected_columns)
            self.assertEqual(len(df.columns), 3)

    def test_parallel_file_reading_behavior(self):
        """Test parallel file reading with multiple files."""
        # Create file pattern for multiple files
        pattern = os.path.join(self.temp_dir, "test_multi_*.parquet")

        connector = ParquetSource()
        connector.configure(
            {"path": pattern, "combine_files": False, "parallel_reading": True}
        )

        chunks = list(connector.read())

        # Verify all data was read
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        expected_rows = sum(len(pd.read_parquet(f)) for f in self.multi_files)
        self.assertEqual(
            total_rows,
            expected_rows,
            "All rows should be read with parallel processing",
        )

    def test_parallel_vs_sequential_reading(self):
        """Test that parallel reading can be enabled/disabled."""
        pattern = os.path.join(self.temp_dir, "test_multi_*.parquet")

        # Test with parallel reading enabled
        connector_parallel = ParquetSource()
        connector_parallel.configure(
            {"path": pattern, "combine_files": False, "parallel_reading": True}
        )

        parallel_chunks = list(connector_parallel.read())
        parallel_total = sum(len(chunk.pandas_df) for chunk in parallel_chunks)

        # Test with parallel reading disabled
        connector_sequential = ParquetSource()
        connector_sequential.configure(
            {"path": pattern, "combine_files": False, "parallel_reading": False}
        )

        sequential_chunks = list(connector_sequential.read())
        sequential_total = sum(len(chunk.pandas_df) for chunk in sequential_chunks)

        # Both should read the same amount of data
        self.assertEqual(
            parallel_total,
            sequential_total,
            "Parallel and sequential should read same data",
        )

    def test_combined_files_optimization(self):
        """Test optimized combined file reading."""
        pattern = os.path.join(self.temp_dir, "test_multi_*.parquet")

        connector = ParquetSource()
        connector.configure({"path": pattern, "combine_files": True})

        chunks = list(connector.read())

        # Verify all data was read and combined
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        expected_rows = sum(len(pd.read_parquet(f)) for f in self.multi_files)
        self.assertEqual(
            total_rows, expected_rows, "Combined reading should get all rows"
        )

    def test_batch_size_optimization_for_datasets(self):
        """Test dataset-level batch size optimization."""
        connector = ParquetSource()

        # Test with small total dataset
        small_total_mb = 10.0
        batch_size = connector._get_optimal_batch_size_for_dataset(small_total_mb)
        self.assertEqual(batch_size, DEFAULT_BATCH_SIZE)

        # Test with medium dataset
        medium_total_mb = 200.0
        batch_size = connector._get_optimal_batch_size_for_dataset(medium_total_mb)
        self.assertEqual(batch_size, DEFAULT_BATCH_SIZE // 2)

        # Test with large dataset
        large_total_mb = 1000.0
        batch_size = connector._get_optimal_batch_size_for_dataset(large_total_mb)
        self.assertEqual(batch_size, MEMORY_EFFICIENT_BATCH_SIZE)

    def test_filters_with_column_selection(self):
        """Test combining predicate pushdown with column selection."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        # Combine filtering and column selection
        filters = {"age": {">=": 30}}
        columns = ["id", "name", "age"]

        chunks = list(connector.read(filters=filters, columns=columns))

        # Verify both optimizations worked
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        self.assertGreater(total_rows, 0, "Should have filtered results")

        for chunk in chunks:
            df = chunk.pandas_df
            # Check column selection
            self.assertEqual(list(df.columns), columns)
            # Check filtering
            self.assertTrue(all(df["age"] >= 30), "All ages should be >= 30")

    def test_optimized_batch_sizes_with_real_files(self):
        """Test that optimized batch sizes work with actual file operations."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        # Test with small batch size
        small_batch_chunks = list(connector.read(batch_size=100))

        # Test with optimized batch size (let connector decide)
        optimized_chunks = list(connector.read())

        # Both should read the same total data
        small_total = sum(len(chunk.pandas_df) for chunk in small_batch_chunks)
        optimized_total = sum(len(chunk.pandas_df) for chunk in optimized_chunks)

        self.assertEqual(
            small_total,
            optimized_total,
            "Should read same data regardless of batch size",
        )

        # Small batch size should create more chunks
        self.assertGreater(
            len(small_batch_chunks),
            len(optimized_chunks),
            "Smaller batch size should create more chunks",
        )


if __name__ == "__main__":
    unittest.main()
