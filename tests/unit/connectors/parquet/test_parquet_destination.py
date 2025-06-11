import os

import pandas as pd
import pyarrow.parquet as pq
import pytest

from sqlflow.connectors.parquet.destination import ParquetDestination


def test_write_success(tmp_path):
    """Test successful write to a Parquet file."""
    file_path = tmp_path / "test_output.parquet"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    connector = ParquetDestination(config={"path": str(file_path)})
    connector.write(df)

    # Verify the file was created and has the correct data
    assert os.path.exists(file_path)
    written_df = pq.read_table(file_path).to_pandas()
    pd.testing.assert_frame_equal(df, written_df)


def test_missing_path_config():
    """Test that an error is raised if 'path' is not in config."""
    with pytest.raises(ValueError, match="path"):
        ParquetDestination(config={})
