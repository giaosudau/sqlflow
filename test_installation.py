"""Test script to verify SQLFlow connector framework installation."""

from sqlflow.sqlflow.connectors.registry import get_connector_class

print("Import test successful!")
print(
    f"Available connectors: {get_connector_class('CSV')}, {get_connector_class('PARQUET')}"
)
