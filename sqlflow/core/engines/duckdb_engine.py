"""DuckDB engine for SQLFlow."""

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    import pyarrow as pa

import duckdb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class DuckDBEngine:
    """Primary execution engine using DuckDB."""

    def __init__(self, database_path: Optional[str] = None):
        """Initialize a DuckDBEngine.

        Args:
            database_path: Path to the DuckDB database file, or None for in-memory
        """
        self.database_path = self._setup_database_path(database_path)
        self._ensure_directory_exists()
        self.connection = self._establish_connection()
        self._configure_persistence()
        self.variables: Dict[str, Any] = {}
        self._verify_connection()

    def _setup_database_path(self, database_path: Optional[str] = None) -> str:
        """Set up the database path based on input.

        Args:
            database_path: Path to the DuckDB database file, or None

        Returns:
            The resolved database path
        """
        # Use in-memory database if explicitly requested
        if database_path == ":memory:":
            logger.debug("Using true in-memory database")
            return ":memory:"
        # Force a file-based database if none is provided
        elif not database_path:
            default_path = "target/default.db"
            logger.debug(f"No database path provided, using default: {default_path}")
            return default_path
        else:
            logger.debug(f"DuckDB engine initializing with path: {database_path}")
            return database_path

    def _ensure_directory_exists(self) -> None:
        """Ensure directory exists for file-based databases."""
        if self.database_path != ":memory:":
            dir_path = os.path.dirname(self.database_path)
            if dir_path:  # Skip if db file is in current directory (empty dir_path)
                logger.debug(f"Creating directory for DuckDB file: {dir_path}")
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    logger.debug(f"Directory created/verified: {dir_path}")
                except Exception as e:
                    logger.error(f"Error creating directory: {e}")
                    # Don't fall back to memory - throw an error instead
                    raise RuntimeError(
                        f"Failed to create directory for DuckDB database: {e}"
                    )

    def _establish_connection(self):
        """Establish a connection to the DuckDB database.

        Returns:
            DuckDB connection object

        Raises:
            RuntimeError: If connection fails
        """
        logger.debug(f"Connecting to DuckDB database at: {self.database_path}")
        try:
            connection = duckdb.connect(self.database_path)
            logger.debug("DuckDB connection established successfully")
            return connection
        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {e}")
            # Don't fall back to memory - throw an error instead
            raise RuntimeError(
                f"Failed to connect to DuckDB database at {self.database_path}: {e}"
            )

    def _configure_persistence(self) -> None:
        """Configure persistence settings for the database."""
        if self.database_path != ":memory:":
            try:
                # Try to get DuckDB version
                version_result = self.connection.execute("SELECT version()").fetchone()
                duckdb_version = version_result[0] if version_result else "unknown"
                logger.debug(f"DuckDB version: {duckdb_version}")

                # Apply settings based on what's likely to be supported
                try:
                    self.connection.execute("PRAGMA memory_limit='2GB'")
                    logger.debug("Set memory limit to 2GB")
                except Exception as e:
                    logger.debug(f"Could not set memory limit: {e}")

                # Force a checkpoint to ensure data is committed
                self.connection.execute("CHECKPOINT")
                logger.debug("Initial checkpoint executed successfully")

                logger.debug("DuckDB persistence settings applied.")
            except Exception as e:
                logger.warning(f"Could not apply all DuckDB settings: {e}")
                # Don't fail on pragma errors - these are likely version differences

    def _verify_connection(self) -> None:
        """Verify the connection is working.

        Raises:
            RuntimeError: If test query fails
        """
        try:
            self.connection.execute("SELECT 1").fetchone()
            logger.debug("DuckDB connection verified with test query")
        except Exception as e:
            logger.error(f"DuckDB test query failed: {e}")
            raise RuntimeError(f"DuckDB connection test failed: {e}")

    def execute_query(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            DuckDB relation object with the query results
        """
        logger.debug(f"Executing DuckDB query: {query[:100]}...")

        # Verify connection is still alive
        try:
            self.connection.execute("SELECT 1").fetchone()
            logger.debug("DuckDB connection verified before query")
        except Exception as e:
            logger.warning(f"DuckDB connection lost, reconnecting: {e}")
            self.__init__(self.database_path)

        result = self.connection.execute(query)
        logger.debug("Query executed successfully")

        # Force checkpoint to ensure data is written to disk
        if self.database_path != ":memory:":
            try:
                self.connection.execute("CHECKPOINT")
                logger.debug("Checkpoint executed to persist data")
            except Exception as e:
                logger.warning(f"Error performing checkpoint: {e}")
                # Don't raise an error here - checkpoint may not be supported

        return result

    def register_table(self, name: str, data: Any):
        """Register a table in DuckDB.

        Args:
            name: Name of the table
            data: Data to register (pandas DataFrame or similar)
        """
        logger.debug(f"Registering table {name}")
        logger.info(f"Registering table {name} with schema: {data.dtypes}")
        try:
            self.connection.register(name, data)
            logger.debug(f"Table {name} registered successfully")

            # Also create a persistent table if using file-based storage
            if self.database_path != ":memory:":
                try:
                    # Create a persistent copy of the registered table
                    self.connection.execute(
                        f"CREATE TABLE IF NOT EXISTS persistent_{name} AS SELECT * FROM {name}"
                    )
                    logger.debug(f"Created persistent copy of table {name}")
                    # Checkpoint to ensure it's written to disk
                    try:
                        self.connection.execute("CHECKPOINT")
                    except Exception as e:
                        logger.warning(
                            f"Error during checkpoint after table creation: {e}"
                        )
                except Exception as e:
                    logger.warning(f"Could not create persistent table: {e}")

            logger.info(f"Table {name} registered successfully")
        except Exception as e:
            logger.error(f"Error registering table {name}: {e}")
            raise

    def get_table_schema(self, name: str) -> Dict[str, str]:
        """Get the schema of a table.

        Args:
            name: Name of the table

        Returns:
            Dict mapping column names to their types
        """
        logger.debug(f"Getting schema for table {name}")
        try:
            # Try PRAGMA approach first (newer DuckDB)
            try:
                result = self.connection.sql(f"PRAGMA table_info({name})")
                schema = {
                    row["name"]: row["type"]
                    for row in result.fetchdf().to_dict("records")
                }
            except Exception:
                # Fall back to DESCRIBE for older versions
                result = self.connection.sql(f"DESCRIBE {name}")
                schema = {
                    row["column_name"]: row["column_type"]
                    for row in result.fetchdf().to_dict("records")
                }

            logger.debug(f"Schema for table {name}: {schema}")
            logger.info(f"Schema for table {name}: {schema}")
            return schema
        except Exception as e:
            logger.error(f"Error getting schema for table {name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table to check

        Returns:
            True if the table exists, False otherwise
        """
        logger.debug(f"Checking if table {table_name} exists")
        try:
            # Try both regular and persistent variants of the table name
            try:
                # First try the information_schema approach (standard)
                result1 = self.connection.sql(
                    f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'"
                )
                exists1 = len(result1.fetchdf()) > 0

                result2 = self.connection.sql(
                    f"SELECT * FROM information_schema.tables WHERE table_name = 'persistent_{table_name}'"
                )
                exists2 = len(result2.fetchdf()) > 0
            except Exception:
                # Fall back to direct query
                try:
                    self.connection.sql("SELECT 1 FROM {table_name} LIMIT 0")
                    exists1 = True
                except Exception:
                    exists1 = False

                try:
                    self.connection.sql(
                        f"SELECT 1 FROM persistent_{table_name} LIMIT 0"
                    )
                    exists2 = True
                except Exception:
                    exists2 = False

            exists = exists1 or exists2
            logger.debug(
                f"Table {table_name} exists: {exists} (regular: {exists1}, persistent: {exists2})"
            )
            logger.info(f"Table {table_name} exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False

    def commit(self):
        """Commit any pending changes to the database."""
        logger.debug("Committing changes")
        try:
            # Not all DuckDB versions have explicit commit, so try checkpoint first
            try:
                self.connection.execute("CHECKPOINT")
                logger.debug("Checkpoint executed for commit")
            except Exception:
                # Fall back to commit if available
                try:
                    self.connection.commit()
                    logger.debug("Changes committed successfully")
                except Exception as e:
                    logger.warning(f"Could not explicitly commit changes: {e}")
                    # This is not fatal - DuckDB may auto-commit changes

            logger.info("Changes committed successfully")
        except Exception as e:
            logger.error(f"Error committing changes: {e}")
            # No need to re-raise here - DuckDB may still be working correctly

    def execute_pipeline_file(
        self, file_path: str, compile_only: bool = False
    ) -> Dict[str, Any]:
        """Execute a pipeline file.

        Args:
            file_path: Path to the pipeline file
            compile_only: If True, only compile the pipeline without executing

        Returns:
            Dict containing execution results
        """
        logger.debug(
            f"Executing pipeline file: {file_path}, compile_only: {compile_only}"
        )
        return {}

    def substitute_variables(self, template: str) -> str:
        """Substitute variables in a template.

        Args:
            template: Template string with variables in the form ${var_name}

        Returns:
            Template with variables substituted
        """
        logger.debug(f"Substituting variables in template: {template}")
        result = template

        for name, value in self.variables.items():
            placeholder = f"${{{name}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))
        logger.debug(f"Substitution result: {result}")
        return result

    def register_variable(self, name: str, value: Any) -> None:
        """Register a variable for use in queries.

        Args:
            name: Variable name
            value: Variable value
        """
        self.variables[name] = value

    def get_variable(self, name: str) -> Any:
        """Get the value of a variable.

        Args:
            name: Variable name

        Returns:
            Variable value
        """
        return self.variables.get(name)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.connection:
            # Force checkpoint before closing
            if self.database_path != ":memory:":
                try:
                    self.connection.execute("CHECKPOINT")
                    logger.debug("Final checkpoint executed before closing")
                except Exception as e:
                    logger.warning(f"Error performing final checkpoint: {e}")

            self.connection.close()
            logger.debug("DuckDB connection closed")

    def __del__(self) -> None:
        """Close the connection when the object is deleted."""
        self.close()

    def register_arrow(self, table_name: str, arrow_table: "pa.Table") -> None:
        """Register an Arrow table in DuckDB.

        Args:
            table_name: Name of the table
            arrow_table: Arrow table to register
        """
        logger.debug(f"Registering Arrow table {table_name}")
        try:
            self.connection.register(table_name, arrow_table)
            logger.info(f"Arrow table {table_name} registered successfully")
        except Exception as e:
            logger.error(f"Error registering Arrow table {table_name}: {e}")
            raise

    def supports_feature(self, feature: str) -> bool:
        """Check if a feature is supported by this engine.

        Args:
            feature: Feature to check

        Returns:
            True if the feature is supported, False otherwise
        """
        supported_features = {
            "arrow": True,
            "pandas": True,
            "python_funcs": False,  # To be implemented in post-MVP
            "transactions": True,
            "checkpoints": True,
        }

        is_supported = supported_features.get(feature, False)
        logger.debug(
            f"Feature check: {feature} is {'supported' if is_supported else 'not supported'}"
        )
        return is_supported

    def register_python_func(self, name: str, func: Any) -> None:
        """Register a Python function for use in queries.

        Note: This method is planned for post-MVP implementation per the
        implementation timeline (Task 2.3.1).

        Args:
            name: Function name to register
            func: Python function to register
        """
        raise NotImplementedError(
            "Python function support is planned for post-MVP implementation."
        )
