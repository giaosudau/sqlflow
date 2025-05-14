"""Parquet connector for SQLFlow."""

import os
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow.sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    ExportConnector,
    Schema,
)
from sqlflow.sqlflow.connectors.data_chunk import DataChunk
from sqlflow.sqlflow.connectors.registry import (
    register_connector,
    register_export_connector,
)
from sqlflow.sqlflow.core.errors import ConnectorError


@register_connector("PARQUET")
@register_export_connector("PARQUET")
class ParquetConnector(Connector, ExportConnector):
    """Connector for Parquet files."""

    def __init__(self):
        """Initialize a ParquetConnector."""
        Connector.__init__(self)
        self.path: Optional[str] = None
        self.use_memory_map: bool = True

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
            params: Configuration parameters

        Raises:
            ConnectorError: If configuration fails
        """
        try:
            self.path = params.get("path")
            if not self.path:
                raise ValueError("Path is required")

            self.use_memory_map = params.get("use_memory_map", True)

            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "PARQUET", f"Configuration failed: {str(e)}"
            )

    def _check_existing_file(self) -> ConnectionTestResult:
        """Check if the file exists and is readable.

        Returns:
            ConnectionTestResult: Result of the check
        """
        try:
            pq.ParquetFile(self.path)
            self.state = ConnectorState.READY
            return ConnectionTestResult(True)
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(
                False, f"File exists but is not readable: {str(e)}"
            )

    def _check_directory(self) -> ConnectionTestResult:
        """Check if the directory exists and is writable.

        Returns:
            ConnectionTestResult: Result of the check
        """
        if self.path is None:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, "Path not configured")

        directory = os.path.dirname(os.path.abspath(str(self.path)))
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                self.state = ConnectorState.ERROR
                return ConnectionTestResult(False, f"Cannot create directory: {str(e)}")

        if not os.access(directory, os.W_OK):
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, f"Directory not writable: {directory}")

        self.state = ConnectorState.READY
        return ConnectionTestResult(True)

    def test_connection(self) -> ConnectionTestResult:
        """Test if the Parquet file exists and is readable, or if the directory is writable.

        Returns:
            Result of the connection test
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                return ConnectionTestResult(False, "Path not configured")

            if os.path.exists(self.path):
                return self._check_existing_file()

            return self._check_directory()
        except Exception as e:
            self.state = ConnectorState.ERROR
            return ConnectionTestResult(False, str(e))

    def discover(self) -> List[str]:
        """Discover available objects in the data source.

        For Parquet, this returns a single object representing the file.

        Returns:
            List with a single object name

        Raises:
            ConnectorError: If discovery fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                raise ValueError("Path not configured")

            if not os.path.exists(self.path):
                raise ValueError(f"File not found: {self.path}")

            base_name = os.path.basename(self.path)
            name, _ = os.path.splitext(base_name)

            return [name]
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "PARQUET", f"Discovery failed: {str(e)}")

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for the Parquet file.

        Args:
            object_name: Name of the object (ignored for Parquet)

        Returns:
            Schema for the Parquet file

        Raises:
            ConnectorError: If schema retrieval fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                raise ValueError("Path not configured")

            if not os.path.exists(self.path):
                raise ValueError(f"File not found: {self.path}")

            parquet_file = pq.ParquetFile(self.path)

            return Schema(parquet_file.schema_arrow)
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(
                self.name or "PARQUET", f"Schema retrieval failed: {str(e)}"
            )

    def _convert_filters(
        self, filters: Dict[str, Any]
    ) -> List[List[Tuple[str, str, Any]]]:
        """Convert SQLFlow filter format to PyArrow filter format.

        Args:
            filters: SQLFlow filters

        Returns:
            PyArrow filters in the format expected by pyarrow.parquet.read_table
        """
        parquet_filters = []

        for column, value in filters.items():
            if isinstance(value, dict):
                for op, val in value.items():
                    if op == ">":
                        parquet_filters.append([(column, ">", val)])
                    elif op == "<":
                        parquet_filters.append([(column, "<", val)])
                    elif op == ">=":
                        parquet_filters.append([(column, ">=", val)])
                    elif op == "<=":
                        parquet_filters.append([(column, "<=", val)])
                    else:
                        parquet_filters.append([(column, op, val)])
            else:
                parquet_filters.append([(column, "==", value)])

        return parquet_filters

    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        """Read data from the Parquet file in chunks.

        Args:
            object_name: Name of the object (ignored for Parquet)
            columns: Optional list of columns to read
            filters: Optional filters to apply
            batch_size: Number of rows per batch

        Yields:
            DataChunk objects

        Raises:
            ConnectorError: If reading fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            if not self.path:
                raise ValueError("Path not configured")

            if not os.path.exists(self.path):
                raise ValueError(f"File not found: {self.path}")

            pq.ParquetFile(self.path)

            if filters:
                parquet_filters = self._convert_filters(filters)
                table = pq.read_table(
                    self.path,
                    columns=columns,
                    filters=parquet_filters,
                    use_threads=True,
                    memory_map=self.use_memory_map,
                )
                for i in range(0, len(table), batch_size):
                    batch = table.slice(i, batch_size)
                    yield DataChunk(batch)
            else:
                table = pq.read_table(
                    self.path,
                    columns=columns,
                    use_threads=True,
                    memory_map=self.use_memory_map,
                )
                for i in range(0, len(table), batch_size):
                    batch = table.slice(i, batch_size)
                    yield DataChunk(batch)

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "PARQUET", f"Reading failed: {str(e)}")

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to a Parquet file.

        Args:
            object_name: Name of the object (used to create filename if path not set)
            data_chunk: Data to write
            mode: Write mode (append or overwrite)

        Raises:
            ConnectorError: If writing fails
        """
        self.validate_state(ConnectorState.CONFIGURED)

        try:
            write_path = self.path
            if not write_path:
                write_path = f"{object_name}.parquet"

            os.makedirs(os.path.dirname(os.path.abspath(write_path)), exist_ok=True)

            table = data_chunk.arrow_table

            if (
                mode == "append"
                and os.path.exists(write_path)
                and os.path.getsize(write_path) > 0
            ):
                try:
                    existing_table = pq.read_table(write_path)

                    if not existing_table.schema.equals(table.schema):
                        try:
                            table = table.cast(existing_table.schema)
                        except pa.ArrowInvalid:
                            raise ValueError(
                                "Schema mismatch between existing file and new data. "
                                "Cannot append with incompatible schemas."
                            )

                    table = pa.concat_tables([existing_table, table])
                except Exception as e:
                    import logging

                    logging.warning(
                        f"Could not read existing Parquet file for append: {str(e)}"
                    )

            pq.write_table(table, write_path, compression="snappy")

            self.state = ConnectorState.READY
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "PARQUET", f"Writing failed: {str(e)}")
