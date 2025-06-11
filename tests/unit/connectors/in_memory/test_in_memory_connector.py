import unittest

import pandas as pd

from sqlflow.connectors.in_memory.in_memory_connector import (
    IN_MEMORY_DATA_STORE,
    InMemoryDestination,
    InMemorySource,
)


class TestInMemoryConnector(unittest.TestCase):
    def setUp(self):
        IN_MEMORY_DATA_STORE.clear()

    def test_write_and_read(self):
        """Test writing to and reading from the in-memory store."""
        table_name = "test_table"
        df_to_write = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        # Write data
        dest_connector = InMemoryDestination(config={"table_name": table_name})
        dest_connector.write(df_to_write)

        # Read data
        source_connector = InMemorySource(config={"table_name": table_name})
        df_read = pd.concat(chunk.pandas_df for chunk in source_connector.read())

        self.assertTrue(df_to_write.equals(df_read))

    def test_missing_table_name_source(self):
        """Test that an error is raised if 'table_name' is not in source config."""
        with self.assertRaises(ValueError):
            InMemorySource(config={})

    def test_missing_table_name_destination(self):
        """Test that an error is raised if 'table_name' is not in destination config."""
        with self.assertRaises(ValueError):
            InMemoryDestination(config={})

    def test_read_nonexistent_table(self):
        """Test that an error is raised when reading a non-existent table."""
        source_connector = InMemorySource(config={"table_name": "nonexistent"})
        with self.assertRaises(ValueError):
            # Consume the generator to trigger the error
            _ = list(source_connector.read())

    def test_write_replace(self):
        """Test that 'replace' mode overwrites existing data."""
        table_name = "test_table"
        df_to_write = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        # Write data
        dest_connector = InMemoryDestination(config={"table_name": table_name})
        dest_connector.write(df_to_write)

        # Read data
        source_connector = InMemorySource(config={"table_name": table_name})
        df_read = pd.concat(chunk.pandas_df for chunk in source_connector.read())

        self.assertTrue(df_to_write.equals(df_read))


if __name__ == "__main__":
    unittest.main()
