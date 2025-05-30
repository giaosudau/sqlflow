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
        ----
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
        ----
            exists: Whether the table exists
            schema: Table schema if it exists

        """
        self.exists = exists
        self.schema = schema


class SQLGenerationHelper:
    """Helper class for common SQL generation patterns."""

    @staticmethod
    def create_table_sql(
        table_name: str, source_name: str, replace: bool = False
    ) -> str:
        """Generate CREATE TABLE SQL statement.

        Args:
        ----
            table_name: Target table name
            source_name: Source table/view name
            replace: Whether to use CREATE OR REPLACE

        Returns:
        -------
            SQL string for table creation

        """
        if replace:
            return SQLTemplates.CREATE_OR_REPLACE_TABLE_AS.format(
                table_name=table_name, source_name=source_name
            )
        else:
            return SQLTemplates.CREATE_TABLE_AS.format(
                table_name=table_name, source_name=source_name
            )

    @staticmethod
    def insert_into_sql(table_name: str, source_name: str) -> str:
        """Generate INSERT INTO SQL statement.

        Args:
        ----
            table_name: Target table name
            source_name: Source table/view name

        Returns:
        -------
            SQL string for insert operation

        """
        return SQLTemplates.INSERT_INTO.format(
            table_name=table_name, source_name=source_name
        )


class SchemaValidator:
    """Helper class for schema validation operations."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize schema validator.

        Args:
        ----
            engine: DuckDB engine instance

        """
        self.engine = engine

    def validate_schema_compatibility(
        self, table_name: str, source_schema: Dict[str, Any]
    ) -> None:
        """Validate schema compatibility between source and target.

        Args:
        ----
            table_name: Target table name
            source_schema: Source schema to validate

        Raises:
        ------
            SchemaValidationError: If schema validation fails

        """
        try:
            self.engine.validate_schema_compatibility(table_name, source_schema)
        except Exception as e:
            target_schema = self.engine.get_table_schema(table_name)
            raise SchemaValidationError(
                f"Schema validation failed for table {table_name}: {str(e)}",
                source_schema=source_schema,
                target_schema=target_schema,
            ) from e

    def validate_merge_keys(
        self, table_name: str, source_name: str, merge_keys: List[str]
    ) -> None:
        """Validate merge keys for MERGE operations.

        Args:
        ----
            table_name: Target table name
            source_name: Source table name
            merge_keys: Keys to validate

        Raises:
        ------
            MergeKeyValidationError: If merge key validation fails

        """
        try:
            self.engine.validate_merge_keys(table_name, source_name, merge_keys)
        except Exception as e:
            raise MergeKeyValidationError(
                f"Merge key validation failed for table {table_name}: {str(e)}",
                table_name=table_name,
                merge_keys=merge_keys,
            ) from e


class ValidationHelper:
    """Helper class for common validation operations."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize validation helper.

        Args:
        ----
            engine: DuckDB engine instance

        """
        self.engine = engine
        self.schema_validator = SchemaValidator(engine)

    def get_table_info(self, table_name: str) -> TableInfo:
        """Get table existence and schema information in a single call.

        Args:
        ----
            table_name: Name of the table

        Returns:
        -------
            TableInfo object with existence and schema information

        """
        exists = self.engine.table_exists(table_name)
        schema = None
        if exists:
            schema = self.engine.get_table_schema(table_name)
        return TableInfo(exists, schema)

    def validate_for_load_mode(
        self, load_step: LoadStep, target_table_info: TableInfo
    ) -> Dict[str, Any]:
        """Validate load step based on its mode.

        Args:
        ----
            load_step: Load step configuration
            target_table_info: Information about the target table

        Returns:
        -------
            Source schema dictionary

        Raises:
        ------
            SchemaValidationError: If schema validation fails
            MergeKeyValidationError: If merge key validation fails

        """
        # Get source schema
        source_schema = self.engine.get_table_schema(load_step.source_name)

        # Validate schema compatibility if target table exists
        if target_table_info.exists:
            self.schema_validator.validate_schema_compatibility(
                load_step.table_name, source_schema
            )

        # Validate merge keys if this is a MERGE operation
        if load_step.mode == DuckDBConstants.LOAD_MODE_MERGE:
            self._validate_merge_requirements(load_step, target_table_info)

        return source_schema

    def _validate_merge_requirements(
        self, load_step: LoadStep, target_table_info: TableInfo
    ) -> None:
        """Validate MERGE operation requirements.

        Args:
        ----
            load_step: Load step configuration
            target_table_info: Information about the target table

        Raises:
        ------
            MergeKeyValidationError: If merge requirements are not met

        """
        if not load_step.merge_keys:
            raise MergeKeyValidationError(
                "MERGE mode requires merge keys to be specified",
                table_name=load_step.table_name,
            )

        if target_table_info.exists:
            self.schema_validator.validate_merge_keys(
                load_step.table_name, load_step.source_name, load_step.merge_keys
            )

    # Deprecated method for backward compatibility
    def validate_schema_and_merge_keys(
        self, load_step: LoadStep, target_table_info: TableInfo
    ) -> Dict[str, Any]:
        """Validate schema compatibility and merge keys for existing tables.

        Deprecated: Use validate_for_load_mode instead.
        """
        return self.validate_for_load_mode(load_step, target_table_info)


class LoadModeHandler(ABC):
    """Abstract base class for load mode handlers."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize the load mode handler.

        Args:
        ----
            engine: DuckDB engine instance

        """
        self.engine = engine
        self.sql_generator = SQLGenerator()
        self.validation_helper = ValidationHelper(engine)
        self.sql_helper = SQLGenerationHelper()

    @abstractmethod
    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for the load operation.

        Args:
        ----
            load_step: Load step configuration

        Returns:
        -------
            SQL string for the load operation

        """

    def _generate_create_table_sql(
        self, load_step: LoadStep, replace: bool = False
    ) -> str:
        """Generate CREATE TABLE SQL using the helper.

        Args:
        ----
            load_step: Load step configuration
            replace: Whether to use CREATE OR REPLACE

        Returns:
        -------
            SQL string for table creation

        """
        return self.sql_helper.create_table_sql(
            load_step.table_name, load_step.source_name, replace
        )


class ReplaceLoadHandler(LoadModeHandler):
    """Handler for REPLACE load mode."""

    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for REPLACE mode.

        Args:
        ----
            load_step: Load step configuration

        Returns:
        -------
            SQL string for REPLACE operation

        """
        table_info = self.validation_helper.get_table_info(load_step.table_name)

        if table_info.exists:
            return self._generate_create_table_sql(load_step, replace=True)
        else:
            return self._generate_create_table_sql(load_step, replace=False)


class AppendLoadHandler(LoadModeHandler):
    """Handler for APPEND load mode."""

    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for APPEND mode.

        Args:
        ----
            load_step: Load step configuration

        Returns:
        -------
            SQL string for APPEND operation

        """
        table_info = self.validation_helper.get_table_info(load_step.table_name)

        if table_info.exists:
            # Validate schema compatibility for APPEND mode
            self.validation_helper.validate_for_load_mode(load_step, table_info)

            return self.sql_helper.insert_into_sql(
                load_step.table_name, load_step.source_name
            )
        else:
            return self._generate_create_table_sql(load_step)


class MergeLoadHandler(LoadModeHandler):
    """Handler for MERGE load mode."""

    def generate_sql(self, load_step: LoadStep) -> str:
        """Generate SQL for MERGE mode.

        Args:
        ----
            load_step: Load step configuration

        Returns:
        -------
            SQL string for MERGE operation

        """
        table_info = self.validation_helper.get_table_info(load_step.table_name)

        if table_info.exists:
            return self._generate_merge_sql(load_step, table_info)
        else:
            return self._generate_create_table_sql(load_step)

    def _generate_merge_sql(self, load_step: LoadStep, table_info: TableInfo) -> str:
        """Generate SQL for MERGE operation on existing table.

        Args:
        ----
            load_step: Load step configuration
            table_info: Information about the target table

        Returns:
        -------
            Complete MERGE SQL

        """
        # Validate schema compatibility and merge keys (this will also get source schema)
        source_schema = self.validation_helper.validate_for_load_mode(
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
        ----
            mode: Load mode (REPLACE, APPEND, MERGE)
            engine: DuckDB engine instance

        Returns:
        -------
            Appropriate load mode handler

        Raises:
        ------
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
