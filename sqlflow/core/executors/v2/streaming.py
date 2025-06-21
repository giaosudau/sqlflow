"""Streaming data processing with generators for memory efficiency.

Following the Zen of Python:
- Simple is better than complex
- Sparse is better than dense
- If the implementation is hard to explain, it's a bad idea

This module provides generator-based streaming for large datasets
to minimize memory usage and improve performance.
"""

import csv
from typing import Any, Dict, Generator, List, Optional, Protocol

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class StreamingDataSource(Protocol):
    """Protocol for streaming data sources."""

    def stream_chunks(
        self, chunk_size: int = 1000
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Stream data in chunks."""
        ...


class StreamingDataProcessor:
    """Memory-efficient data processor using generators.

    Processes data in chunks to avoid loading large datasets into memory.
    Uses Python generators for lazy evaluation and memory efficiency.
    """

    def __init__(self, chunk_size: int = 1000):
        """Initialize with chunk size for streaming."""
        self.chunk_size = chunk_size

    def stream_csv_data(
        self, file_path: str
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Stream CSV data in chunks using generators."""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                chunk = []

                for row in reader:
                    chunk.append(row)

                    if len(chunk) >= self.chunk_size:
                        yield chunk
                        chunk = []

                # Yield remaining rows
                if chunk:
                    yield chunk

        except Exception as e:
            logger.error(f"Error streaming CSV data from {file_path}: {e}")
            raise

    def stream_sql_results(
        self, cursor: Any
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Stream SQL query results in chunks."""
        try:
            chunk = []

            # Get column names
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

            for row in cursor:
                # Convert row to dictionary
                row_dict = dict(zip(columns, row)) if columns else {}
                chunk.append(row_dict)

                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []

            # Yield remaining rows
            if chunk:
                yield chunk

        except Exception as e:
            logger.error(f"Error streaming SQL results: {e}")
            raise

    def transform_stream(
        self,
        data_stream: Generator[List[Dict[str, Any]], None, None],
        transform_func: callable,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Apply transformation function to streaming data."""
        for chunk in data_stream:
            try:
                transformed_chunk = [transform_func(row) for row in chunk]
                yield transformed_chunk
            except Exception as e:
                logger.error(f"Error transforming chunk: {e}")
                raise

    def filter_stream(
        self,
        data_stream: Generator[List[Dict[str, Any]], None, None],
        filter_func: callable,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Filter streaming data using predicate function."""
        for chunk in data_stream:
            try:
                filtered_chunk = [row for row in chunk if filter_func(row)]
                if filtered_chunk:  # Only yield non-empty chunks
                    yield filtered_chunk
            except Exception as e:
                logger.error(f"Error filtering chunk: {e}")
                raise

    def aggregate_stream(
        self,
        data_stream: Generator[List[Dict[str, Any]], None, None],
        key_func: callable,
        agg_func: callable,
        initial_value: Any = None,
    ) -> Dict[Any, Any]:
        """Aggregate streaming data by key."""
        aggregations = {}

        for chunk in data_stream:
            try:
                for row in chunk:
                    key = key_func(row)

                    if key not in aggregations:
                        aggregations[key] = initial_value

                    aggregations[key] = agg_func(aggregations[key], row)

            except Exception as e:
                logger.error(f"Error aggregating chunk: {e}")
                raise

        return aggregations

    def batch_process(
        self, data_stream: Generator[List[Dict[str, Any]], None, None], batch_size: int
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Re-batch streaming data into different chunk sizes."""
        buffer = []

        for chunk in data_stream:
            buffer.extend(chunk)

            while len(buffer) >= batch_size:
                yield buffer[:batch_size]
                buffer = buffer[batch_size:]

        # Yield remaining data
        if buffer:
            yield buffer

    def count_stream(
        self, data_stream: Generator[List[Dict[str, Any]], None, None]
    ) -> int:
        """Count total rows in streaming data without loading into memory."""
        total_count = 0

        for chunk in data_stream:
            total_count += len(chunk)

        return total_count


class StreamingLoadExecutor:
    """Load step executor optimized for streaming data.

    Uses generators to process large datasets efficiently
    without loading everything into memory.
    """

    def __init__(self, chunk_size: int = 1000):
        """Initialize with chunk size."""
        self.chunk_size = chunk_size
        self.processor = StreamingDataProcessor(chunk_size)

    def can_execute(self, step: Dict[str, Any]) -> bool:
        """Check if this executor can handle the step."""
        return step.get("type") == "load" and step.get("streaming", False)

    def execute_streaming_load(
        self, source_config: Dict[str, Any], target_table: str, engine: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute streaming load operation."""
        source_type = source_config.get("type", "csv")

        if source_type == "csv":
            yield from self._load_csv_streaming(source_config, target_table, engine)
        elif source_type == "sql":
            yield from self._load_sql_streaming(source_config, target_table, engine)
        else:
            raise ValueError(f"Unsupported streaming source type: {source_type}")

    def _load_csv_streaming(
        self, source_config: Dict[str, Any], target_table: str, engine: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Load CSV data using streaming approach."""
        file_path = source_config.get("path")
        if not file_path:
            raise ValueError("CSV source requires 'path' parameter")

        total_rows = 0
        chunk_count = 0

        try:
            # Stream CSV data in chunks
            for chunk in self.processor.stream_csv_data(file_path):
                chunk_count += 1
                rows_in_chunk = len(chunk)
                total_rows += rows_in_chunk

                # Load chunk into database
                self._load_chunk_to_table(chunk, target_table, engine)

                # Yield progress information
                yield {
                    "chunk": chunk_count,
                    "rows_in_chunk": rows_in_chunk,
                    "total_rows": total_rows,
                    "status": "processing",
                }

        except Exception as e:
            logger.error(f"Error in streaming CSV load: {e}")
            yield {"error": str(e), "status": "error", "total_rows": total_rows}
            raise

        # Final summary
        yield {
            "total_chunks": chunk_count,
            "total_rows": total_rows,
            "status": "completed",
        }

    def _load_sql_streaming(
        self, source_config: Dict[str, Any], target_table: str, engine: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Load SQL query results using streaming approach."""
        sql_query = source_config.get("sql")
        if not sql_query:
            raise ValueError("SQL source requires 'sql' parameter")

        total_rows = 0
        chunk_count = 0

        try:
            # Execute query and get cursor
            cursor = engine.execute_streaming(sql_query)

            # Stream results in chunks
            for chunk in self.processor.stream_sql_results(cursor):
                chunk_count += 1
                rows_in_chunk = len(chunk)
                total_rows += rows_in_chunk

                # Load chunk into database
                self._load_chunk_to_table(chunk, target_table, engine)

                # Yield progress information
                yield {
                    "chunk": chunk_count,
                    "rows_in_chunk": rows_in_chunk,
                    "total_rows": total_rows,
                    "status": "processing",
                }

        except Exception as e:
            logger.error(f"Error in streaming SQL load: {e}")
            yield {"error": str(e), "status": "error", "total_rows": total_rows}
            raise

        # Final summary
        yield {
            "total_chunks": chunk_count,
            "total_rows": total_rows,
            "status": "completed",
        }

    def _load_chunk_to_table(
        self, chunk: List[Dict[str, Any]], target_table: str, engine: Any
    ) -> None:
        """Load a chunk of data into the target table."""
        if not chunk:
            return

        try:
            # Convert chunk to appropriate format for engine
            if hasattr(engine, "load_data_chunk"):
                engine.load_data_chunk(chunk, target_table)
            else:
                # Fallback to batch insert
                engine.insert_batch(target_table, chunk)

        except Exception as e:
            logger.error(f"Error loading chunk to table {target_table}: {e}")
            raise


class StreamingTransformExecutor:
    """Transform step executor optimized for streaming data."""

    def __init__(self, chunk_size: int = 1000):
        """Initialize with chunk size."""
        self.chunk_size = chunk_size
        self.processor = StreamingDataProcessor(chunk_size)

    def can_execute(self, step: Dict[str, Any]) -> bool:
        """Check if this executor can handle the step."""
        return step.get("type") == "transform" and step.get("streaming", False)

    def execute_streaming_transform(
        self, sql: str, engine: Any, target_table: Optional[str] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute streaming transform operation."""
        total_rows = 0
        chunk_count = 0

        try:
            # Execute transform SQL and get streaming cursor
            cursor = engine.execute_streaming(sql)

            # Stream results and optionally write to target table
            for chunk in self.processor.stream_sql_results(cursor):
                chunk_count += 1
                rows_in_chunk = len(chunk)
                total_rows += rows_in_chunk

                # Optionally load results to target table
                if target_table:
                    self._write_chunk_to_table(chunk, target_table, engine)

                # Yield progress information
                yield {
                    "chunk": chunk_count,
                    "rows_in_chunk": rows_in_chunk,
                    "total_rows": total_rows,
                    "status": "processing",
                    "data": (
                        chunk if not target_table else None
                    ),  # Return data if not persisting
                }

        except Exception as e:
            logger.error(f"Error in streaming transform: {e}")
            yield {"error": str(e), "status": "error", "total_rows": total_rows}
            raise

        # Final summary
        yield {
            "total_chunks": chunk_count,
            "total_rows": total_rows,
            "target_table": target_table,
            "status": "completed",
        }

    def _write_chunk_to_table(
        self, chunk: List[Dict[str, Any]], target_table: str, engine: Any
    ) -> None:
        """Write transform results chunk to target table."""
        if not chunk:
            return

        try:
            if hasattr(engine, "load_data_chunk"):
                engine.load_data_chunk(chunk, target_table)
            else:
                engine.insert_batch(target_table, chunk)

        except Exception as e:
            logger.error(f"Error writing chunk to table {target_table}: {e}")
            raise


class StreamingPipelineCoordinator:
    """Coordinates streaming pipeline execution.

    Manages the flow of streaming data through multiple pipeline steps
    while maintaining memory efficiency.
    """

    def __init__(self, chunk_size: int = 1000):
        """Initialize with chunk size."""
        self.chunk_size = chunk_size
        self.load_executor = StreamingLoadExecutor(chunk_size)
        self.transform_executor = StreamingTransformExecutor(chunk_size)

    def execute_streaming_pipeline(
        self, steps: List[Dict[str, Any]], engine: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute entire pipeline with streaming data processing."""
        for step_index, step in enumerate(steps):
            step_id = step.get("id", f"step_{step_index}")
            step_type = step.get("type")

            logger.info(f"Executing streaming step {step_id} (type: {step_type})")

            try:
                if step_type == "load" and step.get("streaming", False):
                    yield from self._execute_streaming_load_step(step, engine)
                elif step_type == "transform" and step.get("streaming", False):
                    yield from self._execute_streaming_transform_step(step, engine)
                else:
                    # Non-streaming step - execute normally
                    yield {
                        "step_id": step_id,
                        "status": "skipped",
                        "message": "Step not configured for streaming",
                    }

            except Exception as e:
                logger.error(f"Error in streaming step {step_id}: {e}")
                yield {"step_id": step_id, "status": "error", "error": str(e)}
                break  # Fail fast

    def _execute_streaming_load_step(
        self, step: Dict[str, Any], engine: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute streaming load step."""
        source_config = step.get("source", {})
        target_table = step.get("target_table")
        step_id = step.get("id", "unknown")

        for result in self.load_executor.execute_streaming_load(
            source_config, target_table, engine
        ):
            # Add step context to result
            result["step_id"] = step_id
            result["step_type"] = "load"
            yield result

    def _execute_streaming_transform_step(
        self, step: Dict[str, Any], engine: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute streaming transform step."""
        sql = step.get("sql")
        target_table = step.get("target_table")
        step_id = step.get("id", "unknown")

        for result in self.transform_executor.execute_streaming_transform(
            sql, engine, target_table
        ):
            # Add step context to result
            result["step_id"] = step_id
            result["step_type"] = "transform"
            yield result
