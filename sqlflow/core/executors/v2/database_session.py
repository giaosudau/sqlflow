"""Database session management for V2 Orchestrator.

This module handles database session lifecycle, configuration,
and integration with existing SQLFlow patterns.

Following Andy Hunt & Dave Thomas' principle of orthogonality:
Database concerns are separated from execution orchestration.
"""

from typing import Any, Dict, Optional

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class DatabaseSessionManager:
    """
    Database session manager using established SQLFlow patterns.

    Integrates with existing architecture:
    - Uses LocalExecutor's DatabaseConfig patterns
    - Leverages existing DuckDBEngine implementation
    - Follows established profile configuration patterns
    - Maintains compatibility with existing state management

    Following Robert Martin's Single Responsibility Principle:
    This class has one reason to change - database session management.
    """

    def __init__(self, run_id: str, profile: Optional[Dict[str, Any]] = None):
        """Initialize database session using existing LocalExecutor patterns."""
        self.run_id = run_id
        self.profile = profile or {}

        # Create database config using existing LocalExecutor patterns
        self.db_config = self._create_database_config()

        # Initialize DuckDB engine using existing patterns
        self._sql_engine = self._create_duckdb_engine()

        logger.info(
            f"Database session initialized: mode={self.db_config.mode}, persistent={self._sql_engine.is_persistent}"
        )

    def _create_database_config(self):
        """Create database configuration using existing LocalExecutor patterns."""
        from sqlflow.core.executors.local_executor import DatabaseConfig

        if not self.profile:
            return DatabaseConfig()

        try:
            return DatabaseConfig.from_profile(self.profile)
        except (AttributeError, KeyError) as e:
            logger.debug(f"Could not extract database config from profile: {e}")
            return DatabaseConfig()

    def _create_duckdb_engine(self):
        """Create DuckDB engine using existing LocalExecutor patterns."""
        from sqlflow.core.engines.duckdb.engine import DuckDBEngine

        try:
            engine = DuckDBEngine(database_path=self.db_config.database_path)
        except TypeError:
            # Fallback for test mocks that expect positional arguments
            engine = DuckDBEngine(self.db_config.database_path)

        engine.is_persistent = self.db_config.is_persistent

        # Configure engine with profile settings if available
        if self.profile:
            try:
                engine_config = self.profile.get("engines", {}).get("duckdb", {})
                profile_variables = self.profile.get("variables", {})
                engine.configure(engine_config, profile_variables)

                # Set memory limit if specified (following LocalExecutor pattern)
                memory_limit = engine_config.get("memory_limit")
                if memory_limit and engine.connection:
                    engine.connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
                    logger.debug(f"Set DuckDB memory limit to {memory_limit}")
            except Exception as e:
                logger.warning(f"Failed to configure engine from profile: {e}")

        logger.debug(f"DuckDB engine initialized in {self.db_config.mode} mode")
        return engine

    def get_sql_engine(self):
        """Get the SQL engine instance."""
        return self._sql_engine

    def commit_changes(self):
        """Commit changes using existing engine patterns."""
        if self._sql_engine:
            try:
                # Use the engine's built-in commit method
                self._sql_engine.commit()
                logger.debug("Database changes committed")
            except Exception as e:
                logger.debug(f"Database commit operation: {e}")

    def get_engine_stats(self) -> Dict[str, Any]:
        """Get engine statistics using existing patterns."""
        if self._sql_engine:
            return self._sql_engine.get_stats()
        return {}

    def close(self):
        """Close the database session using existing cleanup patterns."""
        if self._sql_engine:
            try:
                # Use the engine's existing close method
                self._sql_engine.close()
                logger.debug("Database session closed")
            except Exception as e:
                logger.debug(f"Database close operation: {e}")
            finally:
                self._sql_engine = None
