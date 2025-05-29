"""Load mode handlers for DuckDB engine."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlflow.logging import get_logger

from ..constants import DuckDBConstants, SQLTemplates
from ..exceptions import (
    InvalidLoadModeError,
    MergeKeyValidationError,
    SchemaValidationError,
)
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


class TableInfo:
    """Contains information about a table's existence and schema."""

    def __init__(self, exists: bool, schema: Optional[Dict[str, Any]] = None):
        """Initialize table information.

        Args:
            exists: Whether the table exists
            schema: Table schema if it exists
        """
        self.exists = exists
        self.schema = schema


class ValidationHelper:
    """Helper class for common validation operations."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize validation helper.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine

    def get_table_info(self, table_name: str) -> TableInfo:
        """Get table existence and schema information in a single call.

        Args:
            table_name: Name of the table

        Returns:
            TableInfo object with existence and schema information
        """
        exists = self.engine.table_exists(table_name)
        schema = None
        if exists:
            schema = self.engine.get_table_schema(table_name)
        return TableInfo(exists, schema)

    def validate_schema_and_merge_keys(
        self, load_step: LoadStep, target_table_info: TableInfo
    ) -> Dict[str, Any]:
        """Validate schema compatibility and merge keys for existing tables.

        Args:
            load_step: Load step configuration
            target_table_info: Information about the target table

        Returns:
            Source schema dictionary

        Raises:
            SchemaValidationError: If schema validation fails
            MergeKeyValidationError: If merge key validation fails
        """
        # Get source schema
        source_schema = self.engine.get_table_schema(load_step.source_name)

        # Validate schema compatibility if target table exists
        if target_table_info.exists:
            try:
                self.engine.validate_schema_compatibility(
                    load_step.table_name, source_schema
                )
            except Exception as e:
                raise SchemaValidationError(
                    f"Schema validation failed for table {load_step.table_name}: {str(e)}",
                    source_schema=source_schema,
                    target_schema=target_table_info.schema,
                ) from e

        # Validate merge keys if this is a MERGE operation
        if load_step.mode == DuckDBConstants.LOAD_MODE_MERGE:
            if not load_step.merge_keys:
                raise MergeKeyValidationError(
                    "MERGE mode requires merge keys to be specified",
                    table_name=load_step.table_name,
                )

            if target_table_info.exists:
                try:
                    self.engine.validate_merge_keys(
                        load_step.table_name,
                        load_step.source_name,
                        load_step.merge_keys,
                    )
                except Exception as e:
                    raise MergeKeyValidationError(
                        f"Merge key validation failed for table {load_step.table_name}: {str(e)}",
                        table_name=load_step.table_name,
                        merge_keys=load_step.merge_keys,
                    ) from e

        return source_schema


class LoadModeHandler(ABC):
    """Abstract base class for load mode handlers."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize the load mode handler.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine
        self.sql_generator = SQLGenerator()
        self.validation_helper = ValidationHelper(engine)

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
        table_info = self.validation_helper.get_table_info(load_step.table_name)

        if table_info.exists:
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
        table_info = self.validation_helper.get_table_info(load_step.table_name)

        if table_info.exists:
            # Validate schema compatibility for APPEND mode
            self.validation_helper.validate_schema_and_merge_keys(load_step, table_info)

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
        table_info = self.validation_helper.get_table_info(load_step.table_name)

        if table_info.exists:
            return self._generate_merge_sql(load_step, table_info)
        else:
            # Create the table if it doesn't exist
            return SQLTemplates.CREATE_TABLE_AS.format(
                table_name=load_step.table_name, source_name=load_step.source_name
            )

    def _generate_merge_sql(self, load_step: LoadStep, table_info: TableInfo) -> str:
        """Generate SQL for MERGE operation on existing table.

        Args:
            load_step: Load step configuration
            table_info: Information about the target table

        Returns:
            Complete MERGE SQL
        """
        # Validate schema compatibility and merge keys (this will also get source schema)
        source_schema = self.validation_helper.validate_schema_and_merge_keys(
            load_step, table_info
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
            InvalidLoadModeError: If load mode is not supported
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
            raise InvalidLoadModeError(mode, valid_modes)
