"""DuckDB writer for SQLFlow."""

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

        if options.get("create_table", True):
            pass
