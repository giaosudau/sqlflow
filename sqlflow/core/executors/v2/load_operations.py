"""Load Operations for V2 Executor.

This module handles the complex load operations extracted from LocalOrchestrator,
following Raymond Hettinger's principle: "Flat is better than nested".

Key principles applied:
- Single Responsibility: Each class/function has one clear purpose
- Simple is better than complex: Break down complex logic into readable parts
- Explicit is better than implicit: Clear method names and interfaces
"""

import time
from typing import Any, Dict, List, Optional

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

# Import performance optimizations
from .data_transfer_optimization import DataTransferMetrics, DuckDBOptimizedTransfer

logger = get_logger(__name__)


class LoadStepData:
    """Parameter object for load step data (Martin Fowler: Introduce Parameter Object)."""

    def __init__(self, load_step, engine, table_data):
        self.load_step = load_step
        self.engine = engine
        self.table_data = table_data

    @property
    def source_name(self) -> str:
        """Get source name from load step."""
        if hasattr(self.load_step, "source_name"):
            return self.load_step.source_name
        elif hasattr(self.load_step, "source"):
            return self.load_step.source
        elif isinstance(self.load_step, dict):
            return self.load_step.get("source_name", self.load_step.get("source", ""))
        return ""

    @property
    def table_name(self) -> str:
        """Get target table name from load step."""
        if hasattr(self.load_step, "table_name"):
            return self.load_step.table_name
        elif hasattr(self.load_step, "target_table"):
            return self.load_step.target_table
        elif isinstance(self.load_step, dict):
            # Check for both 'table_name' and 'target_table' keys
            return self.load_step.get(
                "table_name", self.load_step.get("target_table", "")
            )
        return ""

    @property
    def mode(self) -> str:
        """Get load mode from load step."""
        if hasattr(self.load_step, "mode"):
            return self.load_step.mode.upper()
        elif hasattr(self.load_step, "load_mode"):
            return self.load_step.load_mode.upper()
        elif isinstance(self.load_step, dict):
            return self.load_step.get(
                "mode", self.load_step.get("load_mode", "REPLACE")
            ).upper()
        return "REPLACE"

    @property
    def upsert_keys(self) -> List[str]:
        """Get upsert keys from load step."""
        # Check in incremental_config first (this is the primary location)
        if (
            hasattr(self.load_step, "incremental_config")
            and self.load_step.incremental_config
        ):
            upsert_keys = self.load_step.incremental_config.get("upsert_keys", [])
            if upsert_keys:
                return upsert_keys

        # Check direct attribute as fallback
        if hasattr(self.load_step, "upsert_keys") and self.load_step.upsert_keys:
            return self.load_step.upsert_keys

        # Check if it's a dict step
        if isinstance(self.load_step, dict):
            # Check in incremental_config first
            incremental_config = self.load_step.get("incremental_config", {})
            if incremental_config and "upsert_keys" in incremental_config:
                return incremental_config["upsert_keys"]
            # Check direct upsert_keys field as fallback
            if "upsert_keys" in self.load_step:
                return self.load_step["upsert_keys"]

        return []


class LoadStepValidator:
    """Validates load step preconditions (Single Responsibility Principle)."""

    @staticmethod
    def validate_source_availability(load_data: LoadStepData) -> Dict[str, Any]:
        """Check if source data is available in table_data or engine."""
        try:
            # Check if source is in table_data (handle Mock objects)
            source_in_table_data = (
                hasattr(load_data.table_data, "__contains__")
                and load_data.source_name in load_data.table_data
            )
        except (TypeError, AttributeError):
            # Handle Mock objects or other objects that don't support 'in' operator
            source_in_table_data = False

        source_available = source_in_table_data or (
            load_data.engine
            and hasattr(load_data.engine, "table_exists")
            and load_data.engine.table_exists(load_data.source_name)
        )

        if not source_available:
            try:
                available_sources = (
                    list(load_data.table_data.keys())
                    if hasattr(load_data.table_data, "keys")
                    else []
                )
            except (TypeError, AttributeError):
                available_sources = []

            if load_data.engine and hasattr(load_data.engine, "get_all_tables"):
                try:
                    engine_tables = load_data.engine.get_all_tables() or []
                    available_sources.extend(engine_tables)
                except Exception:
                    pass

            return {
                "valid": False,
                "error": f"Source '{load_data.source_name}' not found. Available sources: {available_sources}",
            }

        return {"valid": True}


class LoadStepProcessor:
    """Processes load steps with engine operations (Single Responsibility)."""

    @staticmethod
    def register_source_data(load_data: LoadStepData) -> None:
        """Register source data with engine if needed."""
        try:
            source_in_table_data = (
                hasattr(load_data.table_data, "__contains__")
                and load_data.source_name in load_data.table_data
            )
        except (TypeError, AttributeError):
            source_in_table_data = False

        if source_in_table_data:
            source_data = load_data.table_data[load_data.source_name]

            if hasattr(source_data, "arrow_table") and hasattr(
                load_data.engine, "register_arrow"
            ):
                load_data.engine.register_arrow(
                    load_data.source_name, source_data.arrow_table
                )
            elif hasattr(source_data, "pandas_df") and hasattr(
                load_data.engine, "register_table"
            ):
                load_data.engine.register_table(
                    load_data.source_name, source_data.pandas_df
                )

    @staticmethod
    def validate_upsert_mode(load_data: LoadStepData) -> None:
        """Validate upsert keys if in UPSERT mode."""
        if load_data.mode.upper() == "UPSERT" and hasattr(
            load_data.engine, "validate_upsert_keys"
        ):
            load_data.engine.validate_upsert_keys(
                load_data.table_name,
                load_data.source_name,
                load_data.upsert_keys,
            )

    @staticmethod
    def execute_load_sql(load_data: LoadStepData) -> Any:
        """Execute the load SQL using engine."""
        sql = load_data.engine.generate_load_sql(load_data.load_step)
        return load_data.engine.execute_query(sql), sql


class LoadStepMetrics:
    """Handles metrics calculation for load steps (Single Responsibility)."""

    @staticmethod
    def calculate_rows_loaded(load_data: LoadStepData) -> int:
        """Calculate how many rows were loaded."""
        try:
            source_in_table_data = (
                hasattr(load_data.table_data, "__contains__")
                and load_data.source_name in load_data.table_data
            )
        except (TypeError, AttributeError):
            source_in_table_data = False

        if source_in_table_data:
            source_data = load_data.table_data[load_data.source_name]
            if hasattr(source_data, "pandas_df"):
                return len(source_data.pandas_df)
            elif hasattr(source_data, "arrow_table"):
                return len(source_data.arrow_table)
        else:
            # Get row count from engine table if source was pre-registered
            try:
                count_result = load_data.engine.execute_query(
                    f"SELECT COUNT(*) FROM {load_data.source_name}"
                )
                if count_result:
                    return count_result.fetchone()[0]
            except Exception:
                pass
        return 0


class LoadStepExecutor:
    """Main orchestrator for load operations (Facade Pattern)."""

    def __init__(self, enable_optimizations: bool = True):
        """Initialize with optimization settings."""
        self.enable_optimizations = enable_optimizations
        self._transfer_metrics: List[DataTransferMetrics] = []

    def execute_load_step(
        self, step, table_data: Dict[str, Any], engine
    ) -> Dict[str, Any]:
        """Execute load step with comprehensive error handling and performance tracking."""
        start_time = time.time()

        try:
            # Normalize step format for consistent processing
            normalized_step = self._normalize_load_step(step)
            load_data = LoadStepData(normalized_step, engine, table_data)

            # Check if optimized transfer is possible
            if self.enable_optimizations and self._can_use_optimized_transfer(
                load_data
            ):
                return self._execute_optimized_transfer(load_data, start_time)
            elif self._is_direct_file_loading(load_data):
                return self._execute_direct_file_loading(load_data, start_time)
            else:
                return self._execute_with_engine(load_data, start_time)

        except Exception as e:
            logger.error(f"Load step execution failed: {e}")
            return self._create_error_result(
                LoadStepData(step, engine, table_data), str(e), start_time
            )

    def _can_use_optimized_transfer(self, load_data: LoadStepData) -> bool:
        """Check if optimized transfer can be used."""
        # Use optimized transfer for file-based sources with large datasets
        if not hasattr(load_data.engine, "connection"):
            return False

        source_data = load_data.table_data.get(load_data.source_name, [])
        return (
            isinstance(source_data, list)
            and len(source_data) > 100  # Threshold for optimization
            and load_data.mode in ["REPLACE", "APPEND"]  # Supported modes
        )

    def _execute_optimized_transfer(
        self, load_data: LoadStepData, start_time: float
    ) -> Dict[str, Any]:
        """Execute load using optimized data transfer."""
        try:
            optimizer = DuckDBOptimizedTransfer(load_data.engine)
            source_data = load_data.table_data.get(load_data.source_name, [])

            # Convert to temporary file for bulk loading
            import tempfile

            import pandas as pd

            # Convert data to DataFrame
            if isinstance(source_data, list) and len(source_data) > 0:
                if isinstance(source_data[0], dict):
                    df = pd.DataFrame(source_data)
                else:
                    df = pd.DataFrame(source_data)
            else:
                df = pd.DataFrame()

            if df.empty:
                return self._create_success_result(
                    load_data, 0, start_time, "optimized_empty"
                )

            # Create temporary CSV for bulk loading
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False
            ) as temp_file:
                df.to_csv(temp_file.name, index=False)

                try:
                    # Use DuckDB's native COPY command
                    metrics = optimizer.bulk_insert_from_file(
                        temp_file.name, load_data.table_name, file_format="csv"
                    )

                    # Record metrics
                    self._transfer_metrics.append(metrics)

                    # Log performance improvement
                    logger.info(
                        f"Optimized load: {metrics.rows_transferred} rows in "
                        f"{metrics.duration_ms:.1f}ms ({metrics.throughput_rows_per_second:.0f} rows/sec)"
                    )

                    return self._create_success_result(
                        load_data,
                        metrics.rows_transferred,
                        start_time,
                        f"optimized_{metrics.strategy_used}",
                        transfer_metrics=metrics,
                    )

                finally:
                    import os

                    os.unlink(temp_file.name)

        except Exception as e:
            logger.debug(
                f"Optimized transfer failed, falling back to standard method: {e}"
            )
            # Fall back to standard method
            return self._execute_with_engine(load_data, start_time)

    def _create_success_result(
        self,
        load_data: LoadStepData,
        rows_loaded: int,
        start_time: float,
        strategy: str = "standard",
        transfer_metrics: Optional[DataTransferMetrics] = None,
    ) -> Dict[str, Any]:
        """Create success result with performance metrics."""
        execution_time = time.time() - start_time

        # Properly extract step ID from load step
        step_id = "load_step"  # fallback
        if hasattr(load_data.load_step, "id"):
            step_id = load_data.load_step.id
        elif isinstance(load_data.load_step, dict):
            step_id = load_data.load_step.get("id", "load_step")

        result = {
            "status": "success",
            "step_id": step_id,
            "message": f"Loaded {rows_loaded} rows using {strategy}",
            "rows_loaded": rows_loaded,
            "target_table": load_data.table_name,
            "table": load_data.table_name,  # V1 compatibility
            "mode": load_data.mode,
            "execution_time": execution_time,
            "load_strategy": strategy,
        }

        # Add transfer metrics if available
        if transfer_metrics:
            result["transfer_metrics"] = {
                "rows_transferred": transfer_metrics.rows_transferred,
                "bytes_transferred": transfer_metrics.bytes_transferred,
                "duration_ms": transfer_metrics.duration_ms,
                "throughput_rows_per_sec": transfer_metrics.throughput_rows_per_second,
                "strategy_used": transfer_metrics.strategy_used,
            }

        return result

    def _create_error_result(
        self,
        load_data: LoadStepData,
        error_message: str,
        start_time: float,
    ) -> Dict[str, Any]:
        """Create error result with proper error information."""
        execution_time = time.time() - start_time

        # Properly extract step ID from load step
        step_id = "load_step"  # fallback
        if hasattr(load_data.load_step, "id"):
            step_id = load_data.load_step.id
        elif isinstance(load_data.load_step, dict):
            step_id = load_data.load_step.get("id", "load_step")

        return {
            "status": "error",
            "step_id": step_id,
            "error": error_message,
            "target_table": load_data.table_name,
            "table": load_data.table_name,  # V1 compatibility
            "mode": load_data.mode,
            "execution_time": execution_time,
            "rows_loaded": 0,
        }

    def get_transfer_performance_summary(self) -> Dict[str, Any]:
        """Get summary of transfer performance metrics."""
        if not self._transfer_metrics:
            return {"total_transfers": 0}

        total_rows = sum(m.rows_transferred for m in self._transfer_metrics)
        total_time_ms = sum(m.duration_ms for m in self._transfer_metrics)

        return {
            "total_transfers": len(self._transfer_metrics),
            "total_rows_transferred": total_rows,
            "total_transfer_time_ms": total_time_ms,
            "average_throughput_rows_per_sec": (
                total_rows / (total_time_ms / 1000) if total_time_ms > 0 else 0
            ),
            "strategies_used": list(
                set(m.strategy_used for m in self._transfer_metrics)
            ),
        }

    def _normalize_load_step(self, step):
        """Convert various step formats to LoadStep object."""
        if hasattr(step, "table_name"):
            return step
        else:
            from sqlflow.parser.ast import LoadStep

            # Handle both source_name and source (direct file path)
            source_name = step.get("source_name", "")
            if not source_name and step.get("source"):
                source_name = step.get("source")

            # Extract upsert_keys from incremental_config if present
            upsert_keys = step.get("upsert_keys", [])
            if not upsert_keys:
                incremental_config = step.get("incremental_config", {})
                if incremental_config:
                    upsert_keys = incremental_config.get("upsert_keys", [])

            return LoadStep(
                table_name=step.get("table_name", step.get("target_table", "")),
                source_name=source_name,
                mode=step.get("mode", step.get("load_mode", "REPLACE")),
                upsert_keys=upsert_keys,
                line_number=step.get("line_number", 1),
                id=step.get("id", None),  # Preserve step ID
            )

    def _execute_with_engine(
        self, load_data: LoadStepData, start_time: float
    ) -> Dict[str, Any]:
        """Execute load step using engine capabilities."""
        try:
            # Validate source availability
            validation_result = LoadStepValidator.validate_source_availability(
                load_data
            )
            if not validation_result["valid"]:
                return self._create_error_result(
                    load_data, validation_result["error"], start_time
                )

            # Register source data with engine
            LoadStepProcessor.register_source_data(load_data)

            # Validate upsert mode
            LoadStepProcessor.validate_upsert_mode(load_data)

            # Execute SQL
            result, sql = LoadStepProcessor.execute_load_sql(load_data)

            # Calculate metrics
            rows_loaded = LoadStepMetrics.calculate_rows_loaded(load_data)

            return self._create_success_result(
                load_data, rows_loaded, start_time, "engine_direct"
            )

        except Exception as e:
            return self._create_error_result(load_data, str(e), start_time)

    def _is_direct_file_loading(self, load_data: LoadStepData) -> bool:
        """Check if this is direct file loading (source is a file path)."""
        from pathlib import Path

        source_path = load_data.source_name
        return bool(
            source_path and Path(source_path).exists() and Path(source_path).is_file()
        )

    def _execute_direct_file_loading(
        self, load_data: LoadStepData, start_time: float
    ) -> Dict[str, Any]:
        """Execute direct file loading using CSV connector."""
        try:
            csv_connector = self._create_csv_connector(load_data)
            data_chunks = self._read_data_chunks(csv_connector)
            if not data_chunks:
                return self._create_error_result(
                    load_data, "No data found in file", start_time
                )

            combined_data, total_rows = self._combine_data_chunks(data_chunks)
            if load_data.engine and combined_data is not None:
                return self._handle_table_creation(load_data, combined_data, start_time)
            else:
                return self._create_error_result(
                    load_data, "No engine or data to load", start_time
                )
        except Exception as e:
            logger.error(f"Direct file loading failed: {e}")
            return self._create_error_result(load_data, str(e), start_time)

    def _create_csv_connector(self, load_data: LoadStepData):
        """Create a CSV connector for the file."""
        # Use local import since CSVConnectorFactory is defined below
        return CSVConnectorFactory.create_csv_connector(
            {"path": load_data.source_name, "has_header": True}
        )

    def _read_data_chunks(self, csv_connector):
        """Read data chunks from the CSV connector."""
        return csv_connector.read()

    def _combine_data_chunks(self, data_chunks):
        """Combine data chunks into a single DataFrame."""
        import pandas as pd

        combined_data = None
        total_rows = 0
        for chunk in data_chunks:
            if hasattr(chunk, "pandas_df"):
                if combined_data is None:
                    combined_data = chunk.pandas_df
                else:
                    combined_data = pd.concat(
                        [combined_data, chunk.pandas_df], ignore_index=True
                    )
                total_rows += len(chunk.pandas_df)
        return combined_data, total_rows

    def _sanitize_table_names(self, table_name):
        """Sanitize table names to avoid SQL injection and syntax errors."""
        import re

        safe_table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
        import time

        temp_table_name = f"temp_{safe_table_name}_{int(time.time() * 1000)}"
        return safe_table_name, temp_table_name

    def _handle_table_creation(self, load_data, combined_data, start_time):
        """Handle table creation or data insertion based on load mode."""
        import logging

        logger = logging.getLogger(__name__)
        table_name = load_data.table_name.strip()
        if not table_name:
            return self._create_error_result(
                load_data, "Empty table name provided", start_time
            )
        safe_table_name, temp_table_name = self._sanitize_table_names(table_name)
        if hasattr(load_data.engine, "register_table"):
            load_data.engine.register_table(temp_table_name, combined_data)
        logger.info(
            f"Load mode detected: '{load_data.mode.upper()}' for step: {load_data.load_step}"
        )
        create_sql = self._get_create_sql(
            load_data, safe_table_name, temp_table_name, start_time
        )
        logger.info(f"About to execute SQL: {create_sql}")
        logger.info(f"Temp table name: {temp_table_name}")
        logger.info(f"Target table name: {safe_table_name}")
        if load_data.mode.upper() == "UPSERT" and ";" in create_sql:
            for sql_statement in create_sql.split(";"):
                sql_statement = sql_statement.strip()
                if sql_statement:
                    load_data.engine.execute_query(sql_statement)
        else:
            load_data.engine.execute_query(create_sql)
        rows_loaded = len(combined_data) if combined_data is not None else 0
        return self._create_success_result(
            load_data, rows_loaded, start_time, load_data.mode.lower()
        )

    def _get_create_sql(self, load_data, safe_table_name, temp_table_name, start_time):
        """Get the SQL statement for table creation or data insertion."""
        mode = load_data.mode.upper()
        if mode == "REPLACE":
            return self._get_replace_sql(safe_table_name, temp_table_name)
        elif mode == "APPEND":
            return self._get_append_sql(load_data, safe_table_name, temp_table_name)
        elif mode == "UPSERT":
            return self._get_upsert_sql(load_data, safe_table_name, temp_table_name)
        else:
            return self._get_replace_sql(safe_table_name, temp_table_name)

    def _get_replace_sql(self, safe_table_name, temp_table_name):
        """Generate SQL for REPLACE mode."""
        return f"CREATE OR REPLACE TABLE {safe_table_name} AS SELECT * FROM {temp_table_name}"

    def _get_append_sql(self, load_data, safe_table_name, temp_table_name):
        """Generate SQL for APPEND mode."""
        table_exists = self._check_table_exists(load_data, safe_table_name)
        if table_exists:
            return f"INSERT INTO {safe_table_name} SELECT * FROM {temp_table_name}"
        else:
            return f"CREATE TABLE {safe_table_name} AS SELECT * FROM {temp_table_name}"

    def _get_upsert_sql(self, load_data, safe_table_name, temp_table_name):
        """Generate SQL for UPSERT mode."""
        upsert_keys = load_data.upsert_keys
        if not upsert_keys:
            raise Exception("UPSERT mode requires upsert_keys")

        if hasattr(load_data.engine, "table_exists") and load_data.engine.table_exists(
            safe_table_name
        ):
            key_conditions = " AND ".join(
                [f"target.{key} = source.{key}" for key in upsert_keys]
            )
            return f"""
            DELETE FROM {safe_table_name} 
            WHERE EXISTS (
                SELECT 1 FROM {temp_table_name} 
                WHERE {key_conditions.replace('target.', f'{safe_table_name}.').replace('source.', f'{temp_table_name}.')}
            );
            INSERT INTO {safe_table_name} SELECT * FROM {temp_table_name}
            """
        else:
            return f"CREATE TABLE {safe_table_name} AS SELECT * FROM {temp_table_name}"

    def _check_table_exists(self, load_data, safe_table_name):
        """Check if a table exists using multiple methods."""
        table_exists = False
        if hasattr(load_data.engine, "table_exists"):
            table_exists = load_data.engine.table_exists(safe_table_name)
        if not table_exists and hasattr(load_data.engine, "connection"):
            try:
                test_sql = f"SELECT COUNT(*) FROM {safe_table_name} LIMIT 1"
                load_data.engine.execute_query(test_sql)
                table_exists = True
            except Exception:
                table_exists = False
        return table_exists


class CSVConnectorFactory:
    """Factory for creating CSV connectors (Martin Fowler: Factory Pattern)."""

    @staticmethod
    def create_csv_connector(params: Dict[str, Any]):
        """Create CSV connector with proper configuration."""
        path = params.get("path")
        if not path:
            raise ValueError("CSV connector requires 'path' parameter")

        return SimpleCSVConnector(
            path=path,
            has_header=params.get("has_header", True),
            delimiter=params.get("delimiter", ","),
        )


class SimpleCSVConnector:
    """Simplified CSV connector with clear responsibilities."""

    def __init__(self, path: str, has_header: bool = True, delimiter: str = ","):
        self.path = path
        self.has_header = has_header
        self.delimiter = delimiter

    def read(self, object_name: Optional[str] = None) -> List[DataChunk]:
        """Read CSV data and return as DataChunk."""
        import pandas as pd

        try:
            df = pd.read_csv(self.path, delimiter=self.delimiter)
            return [DataChunk(df)]
        except Exception as e:
            logger.error(f"Failed to read CSV file {self.path}: {e}")
            return [DataChunk(pd.DataFrame())]

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental loading."""
        return True

    def read_incremental(
        self,
        object_name: Optional[str] = None,
        cursor_field: Optional[str] = None,
        cursor_value: Optional[str] = None,
        batch_size: int = 10000,
    ) -> List[DataChunk]:
        """Read incremental data with cursor filtering."""
        import pandas as pd

        try:
            df = pd.read_csv(self.path, delimiter=self.delimiter)

            if cursor_field and cursor_value and cursor_field in df.columns:
                df = self._filter_by_cursor(df, cursor_field, cursor_value)

            return [DataChunk(df)]
        except Exception as e:
            logger.error(f"Failed to read incremental CSV file {self.path}: {e}")
            return [DataChunk(pd.DataFrame())]

    def _filter_by_cursor(self, df, cursor_field: str, cursor_value: str):
        """Filter DataFrame by cursor field value."""
        import pandas as pd

        # Convert cursor field to datetime for comparison if it looks like a timestamp
        if cursor_field.endswith("_at") or "date" in cursor_field.lower():
            df[cursor_field] = pd.to_datetime(df[cursor_field])
            cursor_value_dt = pd.to_datetime(cursor_value)
            return df[df[cursor_field] > cursor_value_dt]
        else:
            return df[df[cursor_field] > cursor_value]

    def get_cursor_value(self) -> Optional[str]:
        """Get the latest cursor value from the data."""
        try:
            import pandas as pd

            df = pd.read_csv(self.path, delimiter=self.delimiter)

            # Look for common cursor field patterns
            cursor_candidates = [
                col
                for col in df.columns
                if col.endswith("_at")
                or "date" in col.lower()
                or col.endswith("_time")
                or col == "updated_at"
            ]

            if cursor_candidates and len(df) > 0:
                cursor_field = cursor_candidates[0]
                max_value = df[cursor_field].max()
                return str(max_value)
            return None
        except Exception as e:
            logger.error(f"Failed to get cursor value from {self.path}: {e}")
            return None
