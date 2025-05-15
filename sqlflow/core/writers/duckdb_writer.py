"""DuckDB writer for SQLFlow."""

import logging
from typing import Any, Dict, Optional

import duckdb

from sqlflow.core.protocols import WriterProtocol


class DuckDBWriter(WriterProtocol):
    """Writes data to DuckDB."""

    def __init__(self, connection: Optional[duckdb.DuckDBPyConnection] = None):
        """Initialize a DuckDBWriter.

        Args:
            connection: DuckDB connection, or None to create a new one
        """
        self.connection = connection or duckdb.connect()

    def write(
        self, data: Any, destination: str, options: Optional[Dict[str, Any]] = None
    ) -> None:
        """Write data to a DuckDB table.

        Args:
            data: Data to write
            destination: Table name to write to
            options: Options for the writer
        """
        options = options or {}
        logger = logging.getLogger(__name__)
        logger.info(f"Writing data to DuckDB table: {destination}")

        try:
            # Register the data with a temporary name
            self.connection.register("temp_data", data)

            # Create table if needed and it doesn't exist yet
            if options.get("create_table", True) and not self._table_exists(
                destination
            ):
                self._create_table_if_needed(destination, data)

            # Write the data
            if options.get("mode") == "append":
                self._append_data(destination)
            else:
                self._overwrite_data(destination)

            # Persist changes
            self._execute_checkpoint()

            logger.info(f"Successfully wrote data to table {destination}")
        except Exception as e:
            logger.error(f"Error in DuckDBWriter.write: {e}")
            raise

    def _create_table_if_needed(self, destination: str, data: Any) -> None:
        """Create the table if it doesn't exist with the schema from the data.

        Args:
            destination: Table name to create
            data: Data to derive schema from
        """
        logger = logging.getLogger(__name__)

        try:
            # Check if table exists
            table_exists = self._table_exists(destination)

            # If table exists, check if schema matches
            if table_exists and self._table_schema_matches(destination, data):
                logger.debug(f"Table {destination} already exists with matching schema")
                return

            # Drop table if it exists but schema doesn't match
            if table_exists:
                logger.debug(
                    f"Dropping table {destination} to recreate with correct schema"
                )
                self.connection.execute(f"DROP TABLE IF EXISTS {destination}")

            # Create table with schema from data
            logger.debug(f"Creating table {destination} with schema from data")
            self.connection.execute(
                f"CREATE TABLE {destination} AS SELECT * FROM temp_data WHERE 1=0"
            )
            logger.debug(f"Created table {destination}")
        except Exception as e:
            logger.error(f"Error creating table {destination}: {e}")
            raise RuntimeError(f"Failed to create table {destination}: {e}")

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table to check

        Returns:
            True if the table exists, False otherwise
        """
        try:
            self.connection.execute(f"SELECT * FROM {table_name} WHERE 1=0")
            return True
        except Exception:
            return False

    def _table_schema_matches(self, table_name: str, data: Any) -> bool:
        """Check if a table's schema matches the data.

        Args:
            table_name: Name of the table to check
            data: Data to compare schema with

        Returns:
            True if the table's schema matches the data, False otherwise
        """
        try:
            # Get column names from the table
            table_columns = set(
                self.connection.execute(f"SELECT * FROM {table_name} WHERE 1=0").columns
            )

            # Get column names from the data (already registered as temp_data)
            data_columns = set(
                self.connection.execute("SELECT * FROM temp_data WHERE 1=0").columns
            )

            # Compare column names
            return table_columns == data_columns
        except Exception:
            return False

    def _append_data(self, destination: str) -> None:
        """Append data to the table.

        Args:
            destination: Table name to append to
        """
        logger = logging.getLogger(__name__)
        self.connection.execute(f"INSERT INTO {destination} SELECT * FROM temp_data")
        logger.debug(f"Appended data to table {destination}")

    def _overwrite_data(self, destination: str) -> None:
        """Overwrite data in the table.

        Args:
            destination: Table name to overwrite
        """
        logger = logging.getLogger(__name__)
        self.connection.execute(f"DELETE FROM {destination}")
        self.connection.execute(f"INSERT INTO {destination} SELECT * FROM temp_data")
        logger.debug(f"Overwrote data in table {destination}")

    def _execute_checkpoint(self) -> None:
        """Execute a checkpoint to persist data."""
        logger = logging.getLogger(__name__)
        try:
            self.connection.execute("CHECKPOINT")
            logger.debug("Executed checkpoint to persist data")
        except Exception as e:
            logger.warning(f"Could not execute checkpoint: {e}")
