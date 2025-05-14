"""Test script to verify SQLFlow connector framework installation."""

from sqlflow.sqlflow.connectors.data_chunk import DataChunk
from sqlflow.sqlflow.connectors.csv_connector import CSVConnector
from sqlflow.sqlflow.connectors.parquet_connector import ParquetConnector
from sqlflow.sqlflow.connectors.registry import get_connector_class

print("Import test successful!")
print(f"Available connectors: {get_connector_class('CSV')}, {get_connector_class('PARQUET')}")
