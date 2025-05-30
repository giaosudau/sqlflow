"""Tests for CSV writer functionality."""

import csv
import os
import tempfile

import pandas as pd
import pytest

from sqlflow.core.writers.csv_writer import CSVWriter


class TestCSVWriter:
    """Test CSV writer functionality."""

    @pytest.fixture
    def csv_writer(self):
        """Create a CSV writer instance for testing."""
        return CSVWriter()

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["New York", "London", "Tokyo"],
            }
        )

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir

    def test_write_basic_dataframe(self, csv_writer, sample_dataframe, temp_dir):
        """Test writing a basic DataFrame to CSV."""
        destination = os.path.join(temp_dir, "test_output.csv")

        csv_writer.write(sample_dataframe, destination)

        # Verify file was created
        assert os.path.exists(destination)

        # Verify content
        result_df = pd.read_csv(destination)
        pd.testing.assert_frame_equal(result_df, sample_dataframe)

    def test_write_with_custom_options(self, csv_writer, sample_dataframe, temp_dir):
        """Test writing CSV with custom options."""
        destination = os.path.join(temp_dir, "test_custom.csv")
        options = {
            "include_index": True,
            "include_header": False,
            "delimiter": ";",
            "quoting": csv.QUOTE_ALL,
            "quotechar": "'",
            "encoding": "utf-8",
        }

        csv_writer.write(sample_dataframe, destination, options)

        # Verify file was created with custom options
        assert os.path.exists(destination)

        # Read raw content to verify formatting
        with open(destination, encoding="utf-8") as f:
            content = f.read()

        # Check that semicolon delimiter was used
        assert ";" in content
        # Check that single quotes were used (quotechar)
        assert "'" in content
        # Check line terminator (may be converted by OS)
        assert "\r\n" in content or "\n" in content

    def test_write_without_header(self, csv_writer, sample_dataframe, temp_dir):
        """Test writing CSV without header."""
        destination = os.path.join(temp_dir, "test_no_header.csv")
        options = {"include_header": False}

        csv_writer.write(sample_dataframe, destination, options)

        # Verify file was created
        assert os.path.exists(destination)

        # Read and verify no header
        with open(destination) as f:
            first_line = f.readline().strip()

        # First line should be data, not headers
        assert "1," in first_line
        assert "id" not in first_line

    def test_write_with_index(self, csv_writer, sample_dataframe, temp_dir):
        """Test writing CSV with index included."""
        destination = os.path.join(temp_dir, "test_with_index.csv")
        options = {"include_index": True}

        csv_writer.write(sample_dataframe, destination, options)

        # Verify file was created
        assert os.path.exists(destination)

        # Read and verify index is included
        result_df = pd.read_csv(destination, index_col=0)
        assert len(result_df.columns) == len(sample_dataframe.columns)
        assert result_df.index.tolist() == sample_dataframe.index.tolist()

    def test_write_creates_directories(self, csv_writer, sample_dataframe, temp_dir):
        """Test that writer creates missing directories."""
        nested_path = os.path.join(temp_dir, "nested", "directories", "output.csv")

        csv_writer.write(sample_dataframe, nested_path)

        # Verify nested directories were created
        assert os.path.exists(nested_path)
        assert os.path.exists(os.path.dirname(nested_path))

    def test_write_non_dataframe_data(self, csv_writer, temp_dir):
        """Test writing non-DataFrame data (should be converted)."""
        destination = os.path.join(temp_dir, "test_dict.csv")
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
        ]

        csv_writer.write(data, destination)

        # Verify file was created and data was converted
        assert os.path.exists(destination)
        result_df = pd.read_csv(destination)
        assert len(result_df) == 3
        assert "name" in result_df.columns
        assert "age" in result_df.columns

    def test_write_empty_dataframe(self, csv_writer, temp_dir):
        """Test writing an empty DataFrame."""
        destination = os.path.join(temp_dir, "test_empty.csv")
        empty_df = pd.DataFrame()

        csv_writer.write(empty_df, destination)

        # Verify file was created
        assert os.path.exists(destination)

        # File should exist but be essentially empty
        with open(destination) as f:
            content = f.read().strip()
        assert content == "" or content == "\n"  # Just newline or empty

    def test_write_custom_line_terminator(self, csv_writer, sample_dataframe, temp_dir):
        """Test writing with custom line terminator."""
        destination = os.path.join(temp_dir, "test_line_term.csv")
        options = {"line_terminator": "\r\n"}

        csv_writer.write(sample_dataframe, destination, options)

        # Verify file was created with custom line terminator
        assert os.path.exists(destination)

        with open(destination, "rb") as f:
            content = f.read()
        assert b"\r\n" in content

    def test_write_with_different_encoding(self, csv_writer, temp_dir):
        """Test writing with different encoding."""
        destination = os.path.join(temp_dir, "test_encoding.csv")
        data = pd.DataFrame({"text": ["café", "naïve", "résumé"]})
        options = {"encoding": "latin1"}

        csv_writer.write(data, destination, options)

        # Verify file was created
        assert os.path.exists(destination)

        # Read with same encoding to verify
        result_df = pd.read_csv(destination, encoding="latin1")
        assert len(result_df) == 3

    def test_write_overwrite_existing_file(
        self, csv_writer, sample_dataframe, temp_dir
    ):
        """Test that writer overwrites existing files."""
        destination = os.path.join(temp_dir, "test_overwrite.csv")

        # Write initial data
        initial_data = pd.DataFrame({"col": [1, 2]})
        csv_writer.write(initial_data, destination)

        # Overwrite with different data
        csv_writer.write(sample_dataframe, destination)

        # Verify final content is the new data
        result_df = pd.read_csv(destination)
        pd.testing.assert_frame_equal(result_df, sample_dataframe)

    def test_write_with_none_options(self, csv_writer, sample_dataframe, temp_dir):
        """Test writing with None options (should use defaults)."""
        destination = os.path.join(temp_dir, "test_none_options.csv")

        csv_writer.write(sample_dataframe, destination, None)

        # Should work with default options
        assert os.path.exists(destination)
        result_df = pd.read_csv(destination)
        pd.testing.assert_frame_equal(result_df, sample_dataframe)
