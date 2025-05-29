"""Load mode handlers for DuckDB engine."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

from sqlflow.logging import get_logger

from ..constants import DuckDBConstants, SQLTemplates
from .sql_generators import SQLGenerator

if TYPE_CHECKING:
    from ..engine import DuckDBEngine

logger = get_logger(__name__)


class LoadStep:
    """Represents a load step with its configuration."""

    def __init__(
        self,
        table_name: str,
        source_name: str,
        mode: str,
        merge_keys: Optional[List[str]] = None,
    ):
        """Initialize a load step.

        Args:
            table_name: Target table name
            source_name: Source table/view name
            mode: Load mode (REPLACE, APPEND, MERGE)
            merge_keys: Keys for MERGE operations
        """
        self.table_name = table_name
        self.source_name = source_name
        self.mode = mode.upper()
        self.merge_keys = merge_keys or []


class LoadModeHandler(ABC):
    """Abstract base class for load mode handlers."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize the load mode handler.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine
        self.sql_generator = SQLGenerator()

    @abstractmethod
    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for the load operation.

        Args:
            load_step: Load step configuration

        Returns:
            SQL string for the load operation
        """


class ReplaceLoadHandler(LoadModeHandler):
    """Handler for REPLACE load mode."""

    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for REPLACE mode.

        Args:
            load_step: Load step configuration

        Returns:
            SQL string for REPLACE operation
        """
        table_exists = self.engine.table_exists(load_step.table_name)

        if table_exists:
            # Use CREATE OR REPLACE for existing tables
            return SQLTemplates.CREATE_OR_REPLACE_TABLE_AS.format(
                table_name=load_step.table_name, source_name=load_step.source_name
            )
        else:
            # Simple CREATE TABLE for new tables
            return SQLTemplates.CREATE_TABLE_AS.format(
                table_name=load_step.table_name, source_name=load_step.source_name
            )


class AppendLoadHandler(LoadModeHandler):
    """Handler for APPEND load mode."""

    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for APPEND mode.

        Args:
            load_step: Load step configuration

        Returns:
            SQL string for APPEND operation
        """
        table_exists = self.engine.table_exists(load_step.table_name)

        if table_exists:
            # Validate schema compatibility for APPEND mode
            source_schema = self.engine.get_table_schema(load_step.source_name)
            self.engine.validate_schema_compatibility(
                load_step.table_name, source_schema
            )

            # Use INSERT INTO for existing tables
            return SQLTemplates.INSERT_INTO.format(
                table_name=load_step.table_name, source_name=load_step.source_name
            )
        else:
            # Create the table first if it doesn't exist
            return SQLTemplates.CREATE_TABLE_AS.format(
                table_name=load_step.table_name, source_name=load_step.source_name
            )


class MergeLoadHandler(LoadModeHandler):
    """Handler for MERGE load mode."""

    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for MERGE mode.

        Args:
            load_step: Load step configuration

        Returns:
            SQL string for MERGE operation
        """
        if not load_step.merge_keys:
            raise ValueError("MERGE mode requires merge keys to be specified")

        table_exists = self.engine.table_exists(load_step.table_name)

        if table_exists:
            return self._generate_merge_sql(load_step)
        else:
            # Create the table if it doesn't exist
            return SQLTemplates.CREATE_TABLE_AS.format(
                table_name=load_step.table_name, source_name=load_step.source_name
            )

    def _generate_merge_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for MERGE operation on existing table.

        Args:
            load_step: Load step configuration

        Returns:
            Complete MERGE SQL
        """
        # Validate schema compatibility and merge keys
        source_schema = self.engine.get_table_schema(load_step.source_name)
        self.engine.validate_schema_compatibility(load_step.table_name, source_schema)
        self.engine.validate_merge_keys(
            load_step.table_name, load_step.source_name, load_step.merge_keys
        )

        return self.sql_generator.generate_merge_sql(
            load_step.table_name,
            load_step.source_name,
            load_step.merge_keys,
            source_schema,
        )


class LoadModeHandlerFactory:
    """Factory for creating appropriate load mode handlers."""

    @staticmethod
    def create(mode: str, engine: "DuckDBEngine") -> LoadModeHandler:
        """Create appropriate load mode handler.

        Args:
            mode: Load mode (REPLACE, APPEND, MERGE)
            engine: DuckDB engine instance

        Returns:
            Appropriate load mode handler

        Raises:
            ValueError: If load mode is not supported
        """
        mode = mode.upper()

        if mode == DuckDBConstants.LOAD_MODE_REPLACE:
            return ReplaceLoadHandler(engine)
        elif mode == DuckDBConstants.LOAD_MODE_APPEND:
            return AppendLoadHandler(engine)
        elif mode == DuckDBConstants.LOAD_MODE_MERGE:
            return MergeLoadHandler(engine)
        else:
            valid_modes = [
                DuckDBConstants.LOAD_MODE_REPLACE,
                DuckDBConstants.LOAD_MODE_APPEND,
                DuckDBConstants.LOAD_MODE_MERGE,
            ]
            raise ValueError(
                f"Invalid load mode: {mode}. Must be one of: {', '.join(valid_modes)}"
            )
