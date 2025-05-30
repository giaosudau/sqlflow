"""Integration tests for connector framework."""

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlflow.connectors.connector_engine import ConnectorEngine
from sqlflow.connectors.csv_connector import CSVConnector
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.parquet_connector import ParquetConnector


def test_csv_to_parquet_conversion(sample_csv_file, temp_dir):
    """Test reading from CSV and writing to Parquet."""
    csv_connector = CSVConnector()
    csv_connector.configure({"path": sample_csv_file})

    parquet_path = os.path.join(temp_dir, "output.parquet")
    parquet_connector = ParquetConnector()
    parquet_connector.configure({"path": parquet_path})

    chunks = list(csv_connector.read("csv_data"))
    assert len(chunks) > 0

    for chunk in chunks:
        parquet_connector.write("parquet_data", chunk)

    csv_data = pd.read_csv(sample_csv_file)
    parquet_data = pq.read_table(parquet_path).to_pandas()

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


def test_connector_engine_integration(sample_csv_file, sample_parquet_file, temp_dir):
    """Test ConnectorEngine with CSV and Parquet connectors."""
    engine = ConnectorEngine()

    engine.register_connector("csv_source", "CSV", {"path": sample_csv_file})
    engine.register_connector(
        "parquet_source", "PARQUET", {"path": sample_parquet_file}
    )

    csv_chunks = list(engine.load_data("csv_source", "csv_data"))
    assert len(csv_chunks) > 0
    csv_data = csv_chunks[0].pandas_df

    parquet_chunks = list(engine.load_data("parquet_source", "parquet_data"))
    assert len(parquet_chunks) > 0
    parquet_data = parquet_chunks[0].pandas_df

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

    output_path = os.path.join(temp_dir, "engine_output.parquet")
    engine.export_data(csv_data, output_path, "PARQUET", {"path": output_path})

    exported_data = pq.read_table(output_path).to_pandas()
    csv_data_str = csv_data.astype(str)
    exported_data_str = exported_data.astype(str)

    for df in [csv_data_str, exported_data_str]:
        for col in df.columns:
            df[col] = (
                df[col]
                .str.lower()
                .where(df[col].isin(["true", "false", "True", "False"]), df[col])
            )

    pd.testing.assert_frame_equal(
        csv_data_str.reset_index(drop=True),
        exported_data_str.reset_index(drop=True),
    )


def test_filtered_data_loading(sample_parquet_file):
    """Test loading filtered data from Parquet."""
    engine = ConnectorEngine()

    engine.register_connector(
        "parquet_source", "PARQUET", {"path": sample_parquet_file}
    )

    chunks = list(
        engine.load_data("parquet_source", "parquet_data", columns=["id", "name"])
    )
    assert len(chunks) > 0
    data = chunks[0].pandas_df

    assert list(data.columns) == ["id", "name"]
    assert "value" not in data.columns
    assert "active" not in data.columns

    chunks = list(
        engine.load_data("parquet_source", "parquet_data", filters={"id": {"<": 3}})
    )
    assert len(chunks) > 0
    data = chunks[0].pandas_df

    assert len(data) < 5  # Should be filtered
    assert all(id < 3 for id in data["id"])


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
