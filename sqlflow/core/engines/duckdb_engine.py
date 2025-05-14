"""DuckDB engine for SQLFlow."""

from typing import Any, Dict, Optional

import duckdb


class DuckDBEngine:
    """Primary execution engine using DuckDB."""

    def __init__(self, database_path: Optional[str] = None):
        """Initialize a DuckDBEngine.

        Args:
            database_path: Path to the DuckDB database file, or None for in-memory
        """
        self.database_path = database_path
        self.connection = duckdb.connect(database_path)
        self.variables: Dict[str, Any] = {}

    def execute_query(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            Query result
        """
        query = self.substitute_variables(query)
        return self.connection.execute(query)

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
        return {}

    def substitute_variables(self, template: str) -> str:
        """Substitute variables in a template.

        Args:
            template: Template string with variables in the form ${var_name}

        Returns:
            Template with variables substituted
        """
        result = template

        for name, value in self.variables.items():
            placeholder = f"${{{name}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))

        return result

    def set_variable(self, name: str, value: Any) -> None:
        """Set a variable.

        Args:
            name: Variable name
            value: Variable value
        """
        self.variables[name] = value

    def get_variable(self, name: str) -> Any:
        """Get a variable value.

        Args:
            name: Variable name

        Returns:
            Variable value

        Raises:
            KeyError: If the variable is not defined
        """
        if name not in self.variables:
            raise KeyError(f"Variable '{name}' is not defined")
        return self.variables[name]

    def close(self) -> None:
        """Close the database connection."""
        self.connection.close()
