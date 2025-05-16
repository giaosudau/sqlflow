"""DuckDB engine for SQLFlow."""

import logging
import os
from typing import Any, Dict, Optional

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
        self.connection = None  # Initialize connection to None for safety
        self.variables: Dict[str, Any] = {}

        try:
            self._ensure_directory_exists()
            self.connection = self._establish_connection()
            self._configure_persistence()
            self._verify_connection()
        except Exception as e:
            logger.error(f"Error initializing DuckDB engine: {e}")
            if self.database_path != ":memory:":
                # Fall back to in-memory if file-based connection fails
                logger.warning(f"Falling back to in-memory database due to error: {e}")
                print(f"WARNING: Falling back to in-memory database due to error: {e}")
                self.database_path = ":memory:"
                try:
                    self.connection = duckdb.connect(":memory:")
                    print(
                        "DEBUG: Successfully connected to fallback in-memory database"
                    )
                except Exception as fallback_error:
                    logger.error(
                        f"Error connecting to fallback in-memory database: {fallback_error}"
                    )
                    print(
                        f"ERROR: Could not connect to fallback in-memory database: {fallback_error}"
                    )

    def _setup_database_path(self, database_path: Optional[str] = None) -> str:
        """Set up the database path based on input.

        Args:
            database_path: Path to the DuckDB database file, or None

        Returns:
            The resolved database path
        """
        # Use in-memory database if explicitly requested
        if database_path == ":memory:":
            print("DEBUG: Using true in-memory database")
            return ":memory:"
        # Force a file-based database if none is provided
        elif not database_path:
            default_path = "target/default.db"
            print(f"DEBUG: No database path provided, using default: {default_path}")
            return default_path
        else:
            print(f"DEBUG: DuckDB engine initializing with path: {database_path}")
            return database_path

    def _ensure_directory_exists(self) -> None:
        """Ensure directory exists for file-based databases."""
        if self.database_path != ":memory:":
            dir_path = os.path.dirname(self.database_path)
            if dir_path:  # Skip if db file is in current directory (empty dir_path)
                print(f"DEBUG: Creating directory for DuckDB file: {dir_path}")
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    print(f"DEBUG: Directory created/verified: {dir_path}")
                except Exception as e:
                    print(f"DEBUG: Error creating directory: {e}")
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
        print(f"DEBUG: Connecting to DuckDB database at: {self.database_path}")
        try:
            # Connect with standard parameters
            connection = duckdb.connect(self.database_path, read_only=False)
            print("DEBUG: DuckDB connection established successfully")
            return connection
        except Exception as e:
            print(f"DEBUG: Error connecting to DuckDB: {e}")
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
                print(f"DEBUG: DuckDB version: {duckdb_version}")

                # Apply settings based on what's likely to be supported
                try:
                    self.connection.execute("PRAGMA memory_limit='2GB'")
                    print("DEBUG: Set memory limit to 2GB")
                except Exception as e:
                    print(f"DEBUG: Could not set memory limit: {e}")

                # Force a checkpoint to ensure data is committed
                self.connection.execute("CHECKPOINT")
                print("DEBUG: Initial checkpoint executed successfully")

                print("DEBUG: DuckDB persistence settings applied.")
            except Exception as e:
                print(f"DEBUG: Could not apply all DuckDB settings: {e}")
                # Don't fail on pragma errors - these are likely version differences

    def _verify_connection(self) -> None:
        """Verify the connection is working.

        Raises:
            RuntimeError: If test query fails
        """
        try:
            self.connection.execute("SELECT 1").fetchone()
            print("DEBUG: DuckDB connection verified with test query")
        except Exception as e:
            print(f"DEBUG: DuckDB test query failed: {e}")
            raise RuntimeError(f"DuckDB connection test failed: {e}")

    def execute_query(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            DuckDB relation object with the query results
        """
        print(f"DEBUG: Executing DuckDB query: {query[:100]}...")

        # Verify connection is still alive
        try:
            self.connection.execute("SELECT 1").fetchone()
            print("DEBUG: DuckDB connection verified before query")
        except Exception as e:
            print(f"DEBUG: DuckDB connection lost, reconnecting: {e}")
            self.__init__(self.database_path)

        result = self.connection.execute(query)
        print("DEBUG: Query executed successfully")

        # Force checkpoint to ensure data is written to disk
        if self.database_path != ":memory:":
            try:
                self.connection.execute("CHECKPOINT")
                print("DEBUG: Checkpoint executed to persist data")
            except Exception as e:
                print(f"DEBUG: Error performing checkpoint: {e}")
                # Don't raise an error here - checkpoint may not be supported

        return result

    def register_table(self, name: str, data: Any):
        """Register a table in DuckDB.

        Args:
            name: Name of the table
            data: Data to register (pandas DataFrame or similar)
        """
        print(f"DEBUG: Registering table {name}")
        logger.info(f"Registering table {name} with schema: {data.dtypes}")
        try:
            self.connection.register(name, data)
            print(f"DEBUG: Table {name} registered successfully")

            # Also create a persistent table if using file-based storage
            if self.database_path != ":memory:":
                try:
                    # Create a persistent copy of the registered table
                    self.connection.execute(
                        f"CREATE TABLE IF NOT EXISTS persistent_{name} AS SELECT * FROM {name}"
                    )
                    print(f"DEBUG: Created persistent copy of table {name}")
                    # Checkpoint to ensure it's written to disk
                    try:
                        self.connection.execute("CHECKPOINT")
                    except Exception as e:
                        print(
                            f"DEBUG: Error during checkpoint after table creation: {e}"
                        )
                except Exception as e:
                    print(f"DEBUG: Could not create persistent table: {e}")

            logger.info(f"Table {name} registered successfully")
        except Exception as e:
            print(f"DEBUG: Error registering table {name}: {e}")
            logger.error(f"Error registering table {name}: {e}")
            raise

    def get_table_schema(self, name: str) -> Dict[str, str]:
        """Get the schema of a table.

        Args:
            name: Name of the table

        Returns:
            Dict mapping column names to their types
        """
        print(f"DEBUG: Getting schema for table {name}")
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

            print(f"DEBUG: Schema for table {name}: {schema}")
            logger.info(f"Schema for table {name}: {schema}")
            return schema
        except Exception as e:
            print(f"DEBUG: Error getting schema for table {name}: {e}")
            logger.error(f"Error getting schema for table {name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table to check

        Returns:
            True if the table exists, False otherwise
        """
        print(f"DEBUG: Checking if table {table_name} exists")
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
            print(
                f"DEBUG: Table {table_name} exists: {exists} (regular: {exists1}, persistent: {exists2})"
            )
            logger.info(f"Table {table_name} exists: {exists}")
            return exists
        except Exception as e:
            print(f"DEBUG: Error checking if table {table_name} exists: {e}")
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False

    def commit(self):
        """Commit any pending changes to the database."""
        print("DEBUG: Committing changes")
        try:
            # Not all DuckDB versions have explicit commit, so try checkpoint first
            try:
                self.connection.execute("CHECKPOINT")
                print("DEBUG: Checkpoint executed for commit")
            except Exception:
                # Fall back to commit if available
                try:
                    self.connection.commit()
                    print("DEBUG: Changes committed successfully")
                except Exception as e:
                    print(f"DEBUG: Warning: Could not explicitly commit changes: {e}")
                    # This is not fatal - DuckDB may auto-commit changes

            logger.info("Changes committed successfully")
        except Exception as e:
            print(f"DEBUG: Error committing changes: {e}")
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
        print(
            f"DEBUG: Executing pipeline file: {file_path}, compile_only: {compile_only}"
        )
        return {}

    def substitute_variables(self, template: str) -> str:
        """Substitute variables in a template.

        Args:
            template: Template string with variables in the form ${var_name}

        Returns:
            Template with variables substituted
        """
        print(f"DEBUG: Substituting variables in template: {template}")
        result = template

        for name, value in self.variables.items():
            placeholder = f"${{{name}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))
        print(f"DEBUG: Substitution result: {result}")
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
        if hasattr(self, "connection") and self.connection is not None:
            # Force checkpoint before closing
            if self.database_path != ":memory:":
                try:
                    self.connection.execute("CHECKPOINT")
                    print("DEBUG: Final checkpoint executed before closing")
                except Exception as e:
                    print(f"DEBUG: Error performing final checkpoint: {e}")

            self.connection.close()
            self.connection = None
            print("DEBUG: DuckDB connection closed")

    def __del__(self) -> None:
        """Close the connection when the object is deleted."""
        try:
            self.close()
        except Exception as e:
            # Suppress errors in __del__ to prevent issues during garbage collection
            print(f"DEBUG: Error during DuckDBEngine cleanup: {e}")
