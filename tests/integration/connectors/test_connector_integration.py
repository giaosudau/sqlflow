"""Integration tests for connector framework."""

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow.connectors.csv.destination import CSVDestination
from sqlflow.connectors.csv.source import CSVSource
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.parquet.destination import ParquetDestination


def test_csv_to_parquet_conversion(sample_csv_file, temp_dir):
    """Test converting data from CSV to Parquet via connectors."""
    # Use new architecture - configure after instantiation
    csv_connector = CSVSource()
    csv_connector.configure({"path": sample_csv_file})

    output_path = os.path.join(temp_dir, "output.parquet")
    parquet_connector = ParquetDestination(config={"path": output_path})

    # Read data using new connector interface
    data = csv_connector.read()

    # Write data using destination connector
    parquet_connector.write(data)

    # Verify data integrity
    csv_data = pd.read_csv(sample_csv_file)
    parquet_data = pq.read_table(output_path).to_pandas()

    # Normalize data for comparison (handle type differences)
    csv_data_str = csv_data.astype(str)
    parquet_data_str = parquet_data.astype(str)

    for df in [csv_data_str, parquet_data_str]:
        for col in df.columns:
            df[col] = (
                df[col]
                .str.lower()
                .where(df[col].isin(["true", "false", "True", "False"]), df[col])
            )

    pd.testing.assert_frame_equal(
        csv_data_str.reset_index(drop=True),
        parquet_data_str.reset_index(drop=True),
    )


def test_data_chunk_conversion():
    """Test DataChunk conversion between Arrow and pandas."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    }

    arrow_table = pa.Table.from_pydict(data)
    chunk_from_arrow = DataChunk(arrow_table)

    pandas_df = pd.DataFrame(data)
    chunk_from_pandas = DataChunk(pandas_df)

    assert chunk_from_arrow.arrow_table.equals(chunk_from_pandas.arrow_table)
    pd.testing.assert_frame_equal(
        chunk_from_arrow.pandas_df.reset_index(drop=True),
        chunk_from_pandas.pandas_df.reset_index(drop=True),
    )


def test_csv_source_with_new_interface(sample_csv_file):
    """Test CSV source using new standardized interface."""
    connector = CSVSource()

    # Test configuration
    connector.configure({"path": sample_csv_file, "has_header": True})

    # Test connection
    result = connector.test_connection()
    assert result.success, f"Connection test failed: {result.message}"

    # Test discovery
    discovered = connector.discover()
    assert len(discovered) == 1
    assert discovered[0] == sample_csv_file

    # Test schema retrieval
    schema = connector.get_schema(sample_csv_file)
    assert schema is not None
    assert len(schema.arrow_schema) > 0

    # Test data reading
    data = connector.read()
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 0


def test_csv_destination_integration(temp_dir):
    """Test CSV destination integration."""
    test_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100.0, 200.0, 300.0],
        }
    )

    output_path = os.path.join(temp_dir, "test_output.csv")

    # Write data using destination connector
    destination = CSVDestination(config={"path": output_path})
    destination.write(test_data, options={"index": False})

    # Verify file was created
    assert os.path.exists(output_path)

    # Read back and verify data integrity
    read_data = pd.read_csv(output_path)
    pd.testing.assert_frame_equal(test_data, read_data)
