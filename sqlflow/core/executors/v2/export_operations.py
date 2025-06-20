"""Export Operations for V2 Executor.

This module handles data export operations with proper separation of concerns,
following Raymond Hettinger's principle: "Simple is better than complex".

Key refactoring patterns:
- Strategy Pattern: Different export strategies for different destinations
- Command Pattern: Encapsulate export requests
- Builder Pattern: Construct export results incrementally
- Optimized Variable Substitution: Using string.Template for performance
- Pythonic Error Handling: Context managers and proper exception hierarchy
"""

import time
from pathlib import Path
from typing import Any, Dict, Optional

from sqlflow.core.executors.v2.error_handling import (
    ConnectorError,
    SQLFlowError,
    connector_operation_context,
    step_execution_context,
)
from sqlflow.core.executors.v2.variable_optimization import (
    OptimizedVariableSubstitution,
)
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ExportContext:
    """Export context parameter object (Martin Fowler: Parameter Object)."""

    def __init__(self, step: Dict[str, Any], variables: Dict[str, Any]):
        self.step = step
        self.variables = variables
        self.source_table = self._extract_source()
        self.destination = self._extract_destination()
        self.query = step.get("query")

    def _extract_source(self) -> Optional[str]:
        """Extract source table or query."""
        return self.step.get("source_table") or self.step.get("from")

    def _extract_destination(self) -> Optional[str]:
        """Extract destination with fallback chain."""
        return (
            self.step.get("destination")
            or self.step.get("target")
            or (
                self.step.get("query", {}).get("destination_uri")
                if isinstance(self.step.get("query"), dict)
                else None
            )
            or self.step.get("destination_uri")
        )


class QueryDataExtractor:
    """Extracts data for export from queries or tables (Single Responsibility)."""

    def __init__(self, engine):
        self.engine = engine

    def extract_data_for_export(self, context: ExportContext):
        """Extract data from source table or query."""
        with connector_operation_context("internal", "data_extraction") as ctx:
            if context.query:
                result = self._extract_from_query(context.query)
                ctx["extraction_method"] = "query"
                return result
            elif context.source_table:
                result = self._extract_from_table(context.source_table)
                ctx["extraction_method"] = "table"
                return result
            else:
                return None

    def _extract_from_query(self, query):
        """Extract data by executing query."""
        if isinstance(query, dict):
            sql_query = query.get("sql_query", "")
        else:
            sql_query = str(query)

        if not sql_query:
            raise ConnectorError("Export step with query must provide 'sql_query'")

        if self.engine and hasattr(self.engine, "execute_query"):
            return self.engine.execute_query(sql_query)
        else:
            logger.warning("No engine available for query execution")
            return None

    def _extract_from_table(self, source_table: str):
        """Extract data from source table."""
        if self.engine and hasattr(self.engine, "execute_query"):
            try:
                return self.engine.execute_query(f"SELECT * FROM {source_table}")
            except Exception as e:
                logger.warning(f"Could not fetch data from table {source_table}: {e}")
                return None
        return None


class CSVExportStrategy:
    """Strategy for CSV export (Strategy Pattern)."""

    def __init__(self, engine):
        self.engine = engine

    def can_handle(self, destination: str) -> bool:
        """Check if this strategy can handle the destination."""
        return destination.endswith(".csv")

    def export(
        self, export_data, destination: str, options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Export data to CSV format."""
        try:
            dest_path = Path(destination)

            # Ensure directory exists
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            if export_data:
                df = self._convert_to_dataframe(export_data, destination)
                if df is not None:
                    df.to_csv(destination, index=False, **options)
                    return {"status": "success", "rows_exported": len(df)}

            # Create empty file if no data
            self._create_empty_csv(destination, options)
            return {"status": "success", "rows_exported": 0}

        except Exception as e:
            return {"status": "error", "error": str(e), "rows_exported": 0}

    def _convert_to_dataframe(self, export_data, source_table: Optional[str] = None):
        """Convert export data to DataFrame using various methods."""

        # Try engine-specific methods first
        if hasattr(self.engine, "get_table_as_df") and source_table:
            try:
                return self.engine.get_table_as_df(source_table)
            except Exception:
                pass

        # Try cursor-like conversion
        if hasattr(export_data, "fetchall"):
            return self._convert_cursor_to_dataframe(export_data)

        # Try pandas-compatible conversion
        if hasattr(export_data, "to_pandas"):
            return export_data.to_pandas()

        # Try DuckDB-specific methods
        if hasattr(export_data, "df"):
            return export_data.df()

        return None

    def _convert_cursor_to_dataframe(self, result):
        """Convert cursor result to DataFrame."""
        import pandas as pd

        try:
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame()

            # Get column names
            columns = self._extract_column_names(result)
            if columns:
                # Use pandas Index for proper type compatibility
                column_names = pd.Index([str(col) for col in columns])
                return pd.DataFrame(rows, columns=column_names)
            else:
                # For cases without column info, let pandas infer or return empty DataFrame
                return pd.DataFrame(rows) if rows else pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to convert cursor to DataFrame: {e}")
            return pd.DataFrame()

    def _extract_column_names(self, result):
        """Extract column names from result description."""
        if hasattr(result, "description") and result.description:
            return [desc[0] for desc in result.description]
        return None  # Return None instead of empty list to avoid type issues

    def _create_empty_csv(self, destination: str, options: Dict[str, Any]):
        """Create empty CSV file."""
        import pandas as pd

        pd.DataFrame().to_csv(destination, index=False, **options)
        logger.info(f"Created empty CSV file at {destination}")


class DuckDBDirectExportStrategy:
    """Strategy for DuckDB's native COPY command (Strategy Pattern)."""

    def __init__(self, engine):
        self.engine = engine

    def can_handle(self, destination: str) -> bool:
        """Check if this strategy can handle the destination."""
        return (
            hasattr(self.engine, "connection")
            and self.engine.connection
            and destination.endswith(".csv")
        )

    def export(
        self,
        source_table: str,
        destination: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Export using DuckDB's COPY command."""
        if options is None:
            options = {}

        try:
            export_sql = f"COPY (SELECT * FROM {source_table}) TO '{destination}' (HEADER, DELIMITER ',')"
            self.engine.execute_query(export_sql)

            # Count rows for reporting
            count_result = self.engine.execute_query(
                f"SELECT COUNT(*) FROM {source_table}"
            )
            rows_exported = (
                count_result.fetchone()[0] if hasattr(count_result, "fetchone") else 0
            )

            return {"status": "success", "rows_exported": rows_exported}

        except Exception as e:
            logger.error(f"DuckDB direct export failed: {e}")

            # Handle missing table gracefully - create empty file and return success
            if "does not exist" in str(e) or "Table with name" in str(e):
                try:
                    # Ensure directory exists
                    dest_path = Path(destination)
                    dest_path.parent.mkdir(parents=True, exist_ok=True)

                    # Create empty CSV file
                    with open(destination, "w") as f:
                        f.write("")  # Create empty file
                    logger.info(
                        f"Created empty CSV file at {destination} (source table not found)"
                    )
                    return {"status": "success", "rows_exported": 0}
                except Exception as create_error:
                    logger.warning(f"Could not create empty file: {create_error}")

            raise


class ExportStrategySelector:
    """Selects appropriate export strategy (Strategy Pattern + Factory)."""

    def __init__(self, engine):
        self.engine = engine
        self.strategies = {
            "csv": CSVExportStrategy(engine),
            "duckdb_direct": DuckDBDirectExportStrategy(engine),
        }

    def get_strategy(self, destination: str, source_table: Optional[str] = None):
        """Select the best strategy for the export."""
        # Try DuckDB direct export first for CSV files
        if (
            source_table
            and destination.endswith(".csv")
            and self.strategies["duckdb_direct"].can_handle(destination)
        ):
            return self.strategies["duckdb_direct"], "duckdb_direct"

        # Default to CSV strategy
        return self.strategies["csv"], "csv"


class ExportResultBuilder:
    """Builds export results (Builder Pattern)."""

    def __init__(self, step_id: str, start_time: float):
        self.step_id = step_id
        self.start_time = start_time
        self.result = {
            "step_id": step_id,
            "execution_time": 0.0,
        }

    def with_success(
        self,
        source_table: str,
        destination: str,
        connector_type: str,
        rows_exported: int,
    ) -> Dict[str, Any]:
        """Build success result."""
        self.result.update(
            {
                "status": "success",
                "source_table": source_table,
                "destination": destination,
                "connector_type": connector_type,
                "rows_exported": rows_exported,
                "execution_time": time.time() - self.start_time,
            }
        )
        return self.result

    def with_error(self, error: str) -> Dict[str, Any]:
        """Build error result."""
        self.result.update(
            {
                "status": "error",
                "error": error,
                "message": error,  # V1 compatibility
                "execution_time": time.time() - self.start_time,
            }
        )
        return self.result


class ExportOrchestrator:
    """Main orchestrator for export operations (Facade Pattern)."""

    def __init__(self, engine):
        self.engine = engine
        self.data_extractor = QueryDataExtractor(engine)
        self.strategy_selector = ExportStrategySelector(engine)
        self.variable_substitution = OptimizedVariableSubstitution(safe_mode=True)

    def execute_export_step(
        self, step: Dict[str, Any], variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute export step with proper error handling and strategy selection."""
        start_time = time.time()
        step_id = step.get("id", "export")

        with step_execution_context(step_id, "export") as exec_ctx:
            try:
                # Create export context
                context = ExportContext(step, variables)

                # Validate configuration
                self._validate_export_context(context)

                # Apply variable substitution with null checks
                destination = context.destination
                if destination:
                    destination = self.variable_substitution.substitute_in_text(
                        destination, variables
                    )
                else:
                    raise SQLFlowError("No destination specified")

                source_table = context.source_table
                if source_table:
                    source_table = self.variable_substitution.substitute_in_text(
                        source_table, variables
                    )

                # Extract data for export
                export_data = self.data_extractor.extract_data_for_export(context)

                # Select and execute export strategy
                strategy, strategy_type = self.strategy_selector.get_strategy(
                    destination, source_table
                )
                options = step.get("options", {})

                if strategy_type == "duckdb_direct" and source_table:
                    # DuckDB direct export - pass options as third parameter for consistency
                    # Type safety: source_table is checked to be non-None above
                    export_result = strategy.export(source_table, destination, options)
                else:
                    # CSV strategy expects (export_data, destination, options)
                    export_result = strategy.export(export_data, destination, options)

                # Record performance metrics
                exec_ctx["performance_metrics"]["rows_exported"] = export_result.get(
                    "rows_exported", 0
                )
                exec_ctx["performance_metrics"]["strategy_used"] = strategy_type

                if export_result["status"] == "success":
                    result_builder = ExportResultBuilder(step_id, start_time)
                    return result_builder.with_success(
                        source_table or "query",
                        destination,
                        "csv",  # For now, assume CSV
                        export_result["rows_exported"],
                    )
                else:
                    raise SQLFlowError(export_result.get("error", "Export failed"))

            except Exception:
                # Error handling is managed by the context manager
                raise

    def _validate_export_context(self, context: ExportContext):
        """Validate export context configuration."""
        if not context.source_table and not context.query:
            raise SQLFlowError("Export step missing 'source_table', 'from', or 'query'")

        if not context.destination:
            available_keys = list(context.step.keys())
            query_keys = (
                list(context.step.get("query", {}).keys())
                if isinstance(context.step.get("query"), dict)
                else []
            )
            raise SQLFlowError(
                f"Export step missing 'destination' or 'target'. "
                f"Available step keys: {available_keys}, query keys: {query_keys}"
            )
