"""Database engine abstractions."""

from typing import Any


class DatabaseEngineAdapter:
    """Adapter for existing database engines to match our protocol.

    This adapter allows us to use existing DuckDB engines while maintaining
    clean interfaces defined by our protocols.
    """

    def __init__(self, engine: Any):
        """Initialize with existing engine."""
        self._engine = engine

    def execute_query(self, sql: str) -> Any:
        """Execute SQL query and return results."""
        return self._engine.execute(sql)

    def create_table_from_dataframe(self, df: Any, table_name: str, **kwargs) -> None:
        """Create table from pandas DataFrame."""
        if hasattr(self._engine, "create_table_from_dataframe"):
            return self._engine.create_table_from_dataframe(df, table_name, **kwargs)
        else:
            # Fallback for engines without this method
            self._engine.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    def read_table(self, table_name: str) -> Any:
        """Read table into DataFrame."""
        if hasattr(self._engine, "read_table"):
            return self._engine.read_table(table_name)
        else:
            # Fallback using SQL
            return self._engine.execute(f"SELECT * FROM {table_name}").df()

    def register_udf(self, name: str, func: Any) -> None:
        """Register UDF if supported."""
        if hasattr(self._engine, "register_udf"):
            self._engine.register_udf(name, func)

    def close(self) -> None:
        """Close engine if supported."""
        if hasattr(self._engine, "close"):
            self._engine.close()

    @property
    def native_engine(self) -> Any:
        """Access to underlying engine for advanced operations."""
        return self._engine


def create_engine_adapter(
    engine_type: str = "duckdb", **kwargs
) -> DatabaseEngineAdapter:
    """Create database engine adapter.

    Args:
        engine_type: Type of engine to create
        **kwargs: Engine configuration

    Returns:
        DatabaseEngineAdapter wrapping the native engine
    """
    if engine_type == "duckdb":
        from sqlflow.core.engines.duckdb import DuckDBEngine

        native_engine = DuckDBEngine(**kwargs)
        return DatabaseEngineAdapter(native_engine)
    else:
        raise ValueError(f"Unsupported engine type: {engine_type}")
