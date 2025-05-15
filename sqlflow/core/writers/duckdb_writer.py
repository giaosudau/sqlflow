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
            self._create_table_if_needed(destination, options)
            self._write_data_to_table(data, destination, options)
            logger.info(f"Successfully wrote data to table {destination}")
        except Exception as e:
            logger.error(f"Error in DuckDBWriter.write: {e}")
            raise

    def _create_table_if_needed(
        self, destination: str, options: Dict[str, Any]
    ) -> None:
        """Create the table if it doesn't exist and create_table option is True.

        Args:
            destination: Table name to create
            options: Writer options
        """
        logger = logging.getLogger(__name__)
        if not options.get("create_table", True):
            return

        logger.debug(f"Creating table {destination} if it doesn't exist")
        try:
            self.connection.execute(
                f"CREATE TABLE IF NOT EXISTS {destination} AS SELECT * FROM data WHERE 1=0"
            )
            logger.debug(f"Created table {destination}")
        except Exception as e:
            logger.error(f"Error creating table {destination}: {e}")
            raise RuntimeError(f"Failed to create table {destination}: {e}")

    def _write_data_to_table(
        self, data: Any, destination: str, options: Dict[str, Any]
    ) -> None:
        """Write data to the table.

        Args:
            data: Data to write
            destination: Table name to write to
            options: Writer options
        """
        logger = logging.getLogger(__name__)
        logger.debug(f"Writing data to table {destination}")

        try:
            self.connection.register("temp_data", data)

            if options.get("mode", "overwrite") == "append":
                self._append_data(destination)
            else:
                self._overwrite_data(destination)

            self._execute_checkpoint()
        except Exception as e:
            logger.error(f"Error writing data to table {destination}: {e}")
            raise RuntimeError(f"Failed to write data to table {destination}: {e}")

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
