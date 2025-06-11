"""
Performance tests for CSV Source connector optimizations.

These tests validate the performance improvements implemented in Task 2.1:
- Optimal chunk size calculation
- Automatic PyArrow engine selection for large files
- Column selection at read time

Following our testing standards: "Minimize mocking" - use real implementations.
"""

import os
import tempfile
import unittest

import pandas as pd

from sqlflow.connectors.csv.source import (
    DEFAULT_CHUNK_SIZE,
    LARGE_FILE_THRESHOLD_MB,
    MEMORY_EFFICIENT_CHUNK_SIZE,
    CSVSource,
)


class TestCSVPerformanceOptimizations(unittest.TestCase):
    """Test CSV performance optimizations following Zen of Python principles."""

    def setUp(self):
        """Set up test data."""
        self.test_csv_content = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n"

    def test_optimal_chunk_size_calculation(self):
        """Test that optimal chunk size is calculated based on file characteristics."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})

            # Test small file gets default chunk size
            optimal_size = connector._get_optimal_chunk_size(file_path)
            self.assertEqual(optimal_size, DEFAULT_CHUNK_SIZE)

            # Test user-specified chunk size takes precedence
            user_size = 5000
            optimal_size = connector._get_optimal_chunk_size(file_path, user_size)
            self.assertEqual(optimal_size, user_size)

        finally:
            os.unlink(file_path)

    def test_optimal_chunk_size_for_different_file_sizes(self):
        """Test chunk size optimization for different file sizes using real files."""
        # Test with medium file (10MB < size < 100MB range)
        medium_content = "id,name,value,description\n"

        # Create content for medium file (~15MB)
        for i in range(100000):
            medium_content += f"{i},Name{i},{i*10},Description{i}\n"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(medium_content)
            medium_file_path = f.name

        try:
            connector = CSVSource()

            # Verify medium file size
            file_size_mb = os.path.getsize(medium_file_path) / (1024 * 1024)

            if file_size_mb > 100:
                # Large file case - should use memory efficient chunk size
                optimal_size = connector._get_optimal_chunk_size(medium_file_path)
                self.assertEqual(optimal_size, MEMORY_EFFICIENT_CHUNK_SIZE)
            elif file_size_mb > 10:
                # Medium file case - should use larger chunk size
                optimal_size = connector._get_optimal_chunk_size(medium_file_path)
                self.assertEqual(optimal_size, DEFAULT_CHUNK_SIZE * 2)
            else:
                # Small file case - should use default chunk size
                optimal_size = connector._get_optimal_chunk_size(medium_file_path)
                self.assertEqual(optimal_size, DEFAULT_CHUNK_SIZE)

            # Test that user-specified chunk size always takes precedence
            user_size = 7500
            optimal_size = connector._get_optimal_chunk_size(
                medium_file_path, user_size
            )
            self.assertEqual(optimal_size, user_size)

        finally:
            os.unlink(medium_file_path)

    def test_pyarrow_engine_selection(self):
        """Test automatic PyArrow engine selection based on file size."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path, "engine": "auto"})

            # Small file should use pandas
            should_use_pyarrow = connector._should_use_pyarrow(file_path, "auto")
            self.assertFalse(should_use_pyarrow)

            # Test explicit engine selection
            should_use_pyarrow = connector._should_use_pyarrow(file_path, "pyarrow")
            self.assertTrue(should_use_pyarrow)

            should_use_pyarrow = connector._should_use_pyarrow(file_path, "pandas")
            self.assertFalse(should_use_pyarrow)

        finally:
            os.unlink(file_path)

    def test_pyarrow_selection_logic(self):
        """Test PyArrow engine selection logic with real files of different sizes."""
        # Create a file that might be large enough to trigger PyArrow selection
        content = "id,name,value,description,extra_field1,extra_field2\n"

        # Create content for testing engine selection logic
        for i in range(200000):  # Create a substantial file
            content += f"{i},TestName{i},{i*100},Long description field for testing engine selection logic,ExtraData{i},MoreData{i}\n"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(content)
            file_path = f.name

        try:
            connector = CSVSource()

            # Check actual file size and test appropriate behavior
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

            if file_size_mb >= LARGE_FILE_THRESHOLD_MB:
                # File is large enough - should select PyArrow
                should_use_pyarrow = connector._should_use_pyarrow(file_path, "auto")
                self.assertTrue(
                    should_use_pyarrow,
                    f"Files >= {LARGE_FILE_THRESHOLD_MB}MB should use PyArrow",
                )
            else:
                # File is smaller - should use pandas
                should_use_pyarrow = connector._should_use_pyarrow(file_path, "auto")
                self.assertFalse(
                    should_use_pyarrow,
                    f"Files < {LARGE_FILE_THRESHOLD_MB}MB should use pandas",
                )

            # Test explicit engine selection always works
            should_use_pyarrow = connector._should_use_pyarrow(file_path, "pyarrow")
            self.assertTrue(
                should_use_pyarrow, "Explicit 'pyarrow' should always be honored"
            )

            should_use_pyarrow = connector._should_use_pyarrow(file_path, "pandas")
            self.assertFalse(
                should_use_pyarrow, "Explicit 'pandas' should always be honored"
            )

        finally:
            os.unlink(file_path)

    def test_column_selection_optimization(self):
        """Test that column selection happens at read time for memory efficiency."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})

            # Read with column selection
            df = connector.read(columns=["id", "name"])

            # Verify only selected columns are returned
            self.assertEqual(list(df.columns), ["id", "name"])
            self.assertEqual(len(df.columns), 2)

            # Verify data integrity
            self.assertEqual(len(df), 3)
            self.assertEqual(df["id"].tolist(), [1, 2, 3])
            self.assertEqual(df["name"].tolist(), ["Alice", "Bob", "Charlie"])

        finally:
            os.unlink(file_path)

    def test_chunked_reading_with_optimization(self):
        """Test that chunked reading uses optimized chunk sizes."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            # Create larger test data
            large_content = "id,name,value\n"
            for i in range(100):
                large_content += f"{i},Name{i},{i*10}\n"
            f.write(large_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})

            # Test chunked reading with small chunk size
            chunk_size = 20
            chunks = list(connector.read(batch_size=chunk_size, as_iterator=True))

            # Verify chunking worked
            self.assertGreater(len(chunks), 1)

            # Verify total rows
            total_rows = sum(len(chunk) for chunk in chunks)
            self.assertEqual(total_rows, 100)

            # Verify each chunk (except possibly the last) has the expected size
            for i, chunk in enumerate(chunks[:-1]):  # All but last chunk
                self.assertEqual(len(chunk), chunk_size)

        finally:
            os.unlink(file_path)

    def test_engine_behavior_with_pandas_fallback(self):
        """Test that pandas engine works reliably as fallback option."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            # Test with explicit pandas engine (our fallback option)
            connector = CSVSource(config={"path": file_path, "engine": "pandas"})
            df = connector.read()

            # Verify pandas reading works correctly
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(len(df), 3)
            self.assertEqual(list(df.columns), ["id", "name", "value"])

            # Test with auto engine on small file (should choose pandas)
            connector_auto = CSVSource(config={"path": file_path, "engine": "auto"})
            should_use_pyarrow = connector_auto._should_use_pyarrow(file_path, "auto")
            self.assertFalse(should_use_pyarrow, "Small files should use pandas")

            df_auto = connector_auto.read()
            self.assertIsInstance(df_auto, pd.DataFrame)
            self.assertEqual(len(df_auto), 3)

        finally:
            os.unlink(file_path)

    def test_read_incremental_uses_optimizations(self):
        """Test that read_incremental benefits from the same optimizations."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(
                "id,name,timestamp\n1,Alice,2023-01-01\n2,Bob,2023-01-02\n3,Charlie,2023-01-03\n"
            )
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})

            # Test incremental read - should use optimizations internally
            chunks = list(
                connector.read_incremental(
                    object_name=file_path,
                    cursor_field="id",
                    cursor_value=1,
                    batch_size=2,
                )
            )

            # Verify incremental filtering worked
            self.assertGreater(len(chunks), 0)

            # Verify only records after cursor_value=1 are returned
            all_ids = []
            for chunk in chunks:
                all_ids.extend(chunk.pandas_df["id"].tolist())

            # Should only have ids 2 and 3 (greater than 1)
            self.assertEqual(sorted(all_ids), [2, 3])

        finally:
            os.unlink(file_path)


if __name__ == "__main__":
    unittest.main()
