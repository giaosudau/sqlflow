"""
Schema evolution framework for SQLFlow transform operations.

This module provides comprehensive schema evolution capabilities for transform
operations, including type compatibility checking, automatic column addition,
and schema migration planning.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class SchemaCompatibilityStatus(Enum):
    """Schema compatibility status enumeration."""

    COMPATIBLE = "compatible"
    REQUIRES_MIGRATION = "requires_migration"
    INCOMPATIBLE = "incompatible"


class ColumnChangeType(Enum):
    """Types of column changes during schema evolution."""

    ADDED = "added"
    REMOVED = "removed"
    TYPE_WIDENED = "type_widened"
    TYPE_NARROWED = "type_narrowed"
    TYPE_INCOMPATIBLE = "type_incompatible"
    RENAMED = "renamed"


@dataclass
class Column:
    """Represents a database column with schema information."""

    name: str
    data_type: str
    nullable: bool = True
    default_value: Optional[str] = None

    def __post_init__(self):
        """Normalize data type for consistent comparisons."""
        # Normalize DuckDB type names for comparison
        self.data_type = self._normalize_type(self.data_type)

    def _normalize_type(self, data_type: str) -> str:
        """Normalize DuckDB data type names."""
        type_mapping = {
            "INT": "INTEGER",
            "INT4": "INTEGER",
            "INT8": "BIGINT",
            "FLOAT": "REAL",
            "FLOAT4": "REAL",
            "FLOAT8": "DOUBLE",
            "TEXT": "VARCHAR",
            "STRING": "VARCHAR",
            "BOOL": "BOOLEAN",
        }

        # Handle VARCHAR with length
        if "VARCHAR(" in data_type.upper():
            return "VARCHAR"

        return type_mapping.get(data_type.upper(), data_type.upper())


@dataclass
class Schema:
    """Represents a database table schema."""

    table_name: str
    columns: List[Column]

    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        for column in self.columns:
            if column.name.lower() == name.lower():
                return column
        return None

    def get_column_names(self) -> Set[str]:
        """Get set of column names (case-insensitive)."""
        return {col.name.lower() for col in self.columns}

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "table_name": self.table_name,
            "columns": [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                    "default_value": col.default_value,
                }
                for col in self.columns
            ],
        }


@dataclass
class ColumnChange:
    """Represents a change to a column during schema evolution."""

    column_name: str
    change_type: ColumnChangeType
    old_column: Optional[Column] = None
    new_column: Optional[Column] = None
    migration_sql: Optional[str] = None
    requires_data_migration: bool = False


@dataclass
class SchemaCompatibility:
    """Result of schema compatibility analysis."""

    status: SchemaCompatibilityStatus
    changes: List[ColumnChange]
    error_message: Optional[str] = None
    warnings: List[str] = None

    def __post_init__(self):
        """Initialize warnings list if None."""
        if self.warnings is None:
            self.warnings = []

    def is_compatible(self) -> bool:
        """Check if schemas are compatible."""
        return self.status in [
            SchemaCompatibilityStatus.COMPATIBLE,
            SchemaCompatibilityStatus.REQUIRES_MIGRATION,
        ]

    def requires_migration(self) -> bool:
        """Check if migration is required."""
        return self.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION


@dataclass
class EvolutionPlan:
    """Step-by-step schema evolution plan."""

    steps: List[str]
    estimated_time_ms: int
    requires_downtime: bool = False
    rollback_steps: List[str] = None

    def __post_init__(self):
        """Initialize rollback steps if None."""
        if self.rollback_steps is None:
            self.rollback_steps = []


class SchemaEvolutionPolicy:
    """Handle schema evolution for transform operations.

    Provides comprehensive schema compatibility checking, automatic migration
    planning, and safe schema evolution execution.
    """

    # Type compatibility matrix - maps (old_type, new_type) -> compatibility
    TYPE_COMPATIBILITY = {
        # Integer type widening
        ("INTEGER", "BIGINT"): "widening",
        ("SMALLINT", "INTEGER"): "widening",
        ("SMALLINT", "BIGINT"): "widening",
        # Numeric precision widening
        ("REAL", "DOUBLE"): "widening",
        ("INTEGER", "REAL"): "widening",
        ("INTEGER", "DOUBLE"): "widening",
        ("BIGINT", "DOUBLE"): "widening",
        # String length expansion (handled specially)
        ("VARCHAR", "VARCHAR"): "special_varchar",
        ("VARCHAR", "TEXT"): "widening",
        # Temporal type compatibility
        ("DATE", "TIMESTAMP"): "widening",
        ("TIME", "TIMESTAMP"): "widening",
        # Boolean compatibility
        ("BOOLEAN", "INTEGER"): "narrowing",  # Usually not allowed
        ("INTEGER", "BOOLEAN"): "narrowing",  # Usually not allowed
    }

    def __init__(self, engine=None):
        """Initialize schema evolution policy.

        Args:
            engine: Database engine for executing schema operations
        """
        self.engine = engine
        self.performance_threshold_ms = 100  # Maximum time for schema checking

    def check_compatibility(
        self, old_schema: Schema, new_schema: Schema
    ) -> SchemaCompatibility:
        """Check if schema evolution is compatible.

        Args:
            old_schema: Current table schema
            new_schema: Target schema from transform query

        Returns:
            SchemaCompatibility result with analysis and recommendations
        """
        start_time = self._get_time_ms()

        try:
            changes = self._analyze_schema_changes(old_schema, new_schema)
            status, error_message, warnings = self._evaluate_compatibility(changes)

            compatibility = SchemaCompatibility(
                status=status,
                changes=changes,
                error_message=error_message,
                warnings=warnings,
            )

            # Check performance requirement
            elapsed = self._get_time_ms() - start_time
            if elapsed > self.performance_threshold_ms:
                logger.warning(
                    f"Schema compatibility check took {elapsed}ms (>{self.performance_threshold_ms}ms)"
                )

            return compatibility

        except Exception as e:
            logger.error(f"Schema compatibility check failed: {e}")
            return SchemaCompatibility(
                status=SchemaCompatibilityStatus.INCOMPATIBLE,
                changes=[],
                error_message=f"Schema analysis failed: {str(e)}",
            )

    def _analyze_schema_changes(
        self, old_schema: Schema, new_schema: Schema
    ) -> List[ColumnChange]:
        """Analyze changes between old and new schemas."""
        changes = []

        old_columns = {col.name.lower(): col for col in old_schema.columns}
        new_columns = {col.name.lower(): col for col in new_schema.columns}

        # Check for added columns
        for col_name, new_col in new_columns.items():
            if col_name not in old_columns:
                changes.append(
                    ColumnChange(
                        column_name=new_col.name,
                        change_type=ColumnChangeType.ADDED,
                        new_column=new_col,
                        migration_sql=self._generate_add_column_sql(
                            new_schema.table_name, new_col
                        ),
                        requires_data_migration=False,
                    )
                )

        # Check for removed columns
        for col_name, old_col in old_columns.items():
            if col_name not in new_columns:
                changes.append(
                    ColumnChange(
                        column_name=old_col.name,
                        change_type=ColumnChangeType.REMOVED,
                        old_column=old_col,
                        requires_data_migration=True,
                    )
                )

        # Check for type changes
        for col_name in old_columns.keys() & new_columns.keys():
            old_col = old_columns[col_name]
            new_col = new_columns[col_name]

            if old_col.data_type != new_col.data_type:
                change_type = self._analyze_type_change(
                    old_col.data_type, new_col.data_type
                )
                changes.append(
                    ColumnChange(
                        column_name=old_col.name,
                        change_type=change_type,
                        old_column=old_col,
                        new_column=new_col,
                        migration_sql=self._generate_type_change_sql(
                            new_schema.table_name, old_col, new_col
                        ),
                        requires_data_migration=change_type
                        == ColumnChangeType.TYPE_NARROWED,
                    )
                )

        return changes

    def _analyze_type_change(self, old_type: str, new_type: str) -> ColumnChangeType:
        """Analyze the type of change between two data types."""
        compatibility = self.TYPE_COMPATIBILITY.get((old_type, new_type))

        if compatibility == "widening":
            return ColumnChangeType.TYPE_WIDENED
        elif compatibility == "narrowing":
            return ColumnChangeType.TYPE_NARROWED
        elif compatibility == "special_varchar":
            # Handle VARCHAR length changes specially
            return ColumnChangeType.TYPE_WIDENED  # Assume expansion for now
        elif old_type == new_type:
            return ColumnChangeType.TYPE_WIDENED  # No change, treat as compatible
        else:
            return ColumnChangeType.TYPE_INCOMPATIBLE

    def _evaluate_compatibility(
        self, changes: List[ColumnChange]
    ) -> Tuple[SchemaCompatibilityStatus, Optional[str], List[str]]:
        """Evaluate overall compatibility status from changes."""
        if not changes:
            return SchemaCompatibilityStatus.COMPATIBLE, None, []

        warnings = []
        has_incompatible = False
        has_migration_required = False

        for change in changes:
            if change.change_type == ColumnChangeType.TYPE_INCOMPATIBLE:
                has_incompatible = True
            elif change.change_type in [
                ColumnChangeType.REMOVED,
                ColumnChangeType.TYPE_NARROWED,
            ]:
                has_incompatible = True
            elif change.change_type in [
                ColumnChangeType.ADDED,
                ColumnChangeType.TYPE_WIDENED,
            ]:
                has_migration_required = True
                if change.change_type == ColumnChangeType.ADDED:
                    warnings.append(
                        f"New column '{change.column_name}' will be added with default values"
                    )

        if has_incompatible:
            incompatible_changes = [
                c
                for c in changes
                if c.change_type
                in [
                    ColumnChangeType.TYPE_INCOMPATIBLE,
                    ColumnChangeType.REMOVED,
                    ColumnChangeType.TYPE_NARROWED,
                ]
            ]
            error_details = ", ".join(
                [
                    f"{c.column_name} ({c.change_type.value})"
                    for c in incompatible_changes
                ]
            )
            return (
                SchemaCompatibilityStatus.INCOMPATIBLE,
                f"Incompatible schema changes detected: {error_details}",
                warnings,
            )
        elif has_migration_required:
            return SchemaCompatibilityStatus.REQUIRES_MIGRATION, None, warnings
        else:
            return SchemaCompatibilityStatus.COMPATIBLE, None, warnings

    def _generate_add_column_sql(self, table_name: str, column: Column) -> str:
        """Generate SQL to add a new column."""
        # DuckDB has limitations with adding columns with constraints
        # Start with basic column addition
        sql = f"ALTER TABLE {table_name} ADD COLUMN {column.name} {column.data_type}"

        # For DuckDB, we can't add NOT NULL columns directly
        # Add as nullable first, then update with default value
        if column.default_value:
            sql += f" DEFAULT {column.default_value}"
        elif not column.nullable:
            # Add a sensible default for NOT NULL columns
            default_values = {
                "INTEGER": "0",
                "BIGINT": "0",
                "REAL": "0.0",
                "DOUBLE": "0.0",
                "VARCHAR": "''",
                "BOOLEAN": "FALSE",
                "DATE": "CURRENT_DATE",
                "TIMESTAMP": "CURRENT_TIMESTAMP",
            }
            default = default_values.get(column.data_type, "''")
            sql += f" DEFAULT {default}"

        return sql

    def _generate_type_change_sql(
        self, table_name: str, old_column: Column, new_column: Column
    ) -> str:
        """Generate SQL to change column type."""
        return f"ALTER TABLE {table_name} ALTER COLUMN {old_column.name} TYPE {new_column.data_type}"

    def apply_evolution(
        self, table_name: str, compatibility: SchemaCompatibility
    ) -> None:
        """Apply compatible schema changes automatically.

        Args:
            table_name: Target table name
            compatibility: Schema compatibility analysis result

        Raises:
            ValueError: If schema changes are not compatible or no engine provided
        """
        if not self.engine:
            raise ValueError("No database engine provided for schema evolution")

        if not compatibility.is_compatible():
            raise ValueError(
                f"Cannot apply incompatible schema changes: {compatibility.error_message}"
            )

        if compatibility.status == SchemaCompatibilityStatus.COMPATIBLE:
            logger.info(f"No schema evolution required for {table_name}")
            return

        # Apply migration steps
        for change in compatibility.changes:
            if change.migration_sql:
                try:
                    logger.info(
                        f"Applying schema change: {change.change_type.value} for column {change.column_name}"
                    )
                    self.engine.execute_query(change.migration_sql)
                    logger.info(f"Successfully applied: {change.migration_sql}")
                except Exception as e:
                    logger.error(
                        f"Failed to apply schema change: {change.migration_sql} - {e}"
                    )
                    raise

    def get_evolution_plan(self, current: Schema, target: Schema) -> EvolutionPlan:
        """Generate step-by-step evolution plan.

        Args:
            current: Current table schema
            target: Target schema from transform

        Returns:
            Detailed evolution plan with steps and timing estimates
        """
        compatibility = self.check_compatibility(current, target)

        if not compatibility.is_compatible():
            return EvolutionPlan(
                steps=[f"ERROR: {compatibility.error_message}"],
                estimated_time_ms=0,
                requires_downtime=True,
            )

        steps = []
        rollback_steps = []
        estimated_time = 0
        requires_downtime = False

        if compatibility.status == SchemaCompatibilityStatus.COMPATIBLE:
            steps.append("No schema changes required")
            return EvolutionPlan(steps=steps, estimated_time_ms=0)

        # Generate plan for each change
        for change in compatibility.changes:
            if change.change_type == ColumnChangeType.ADDED:
                steps.append(f"Add column: {change.migration_sql}")
                rollback_steps.insert(
                    0,
                    f"ALTER TABLE {target.table_name} DROP COLUMN {change.column_name}",
                )
                estimated_time += 50  # 50ms per column addition

            elif change.change_type == ColumnChangeType.TYPE_WIDENED:
                steps.append(f"Widen column type: {change.migration_sql}")
                rollback_steps.insert(
                    0,
                    f"ALTER TABLE {target.table_name} ALTER COLUMN {change.column_name} TYPE {change.old_column.data_type}",
                )
                estimated_time += 100  # 100ms per type change
                requires_downtime = True  # Type changes may require table rebuild

        return EvolutionPlan(
            steps=steps,
            estimated_time_ms=estimated_time,
            requires_downtime=requires_downtime,
            rollback_steps=rollback_steps,
        )

    def get_schema_from_engine(self, table_name: str) -> Optional[Schema]:
        """Get current schema from database engine.

        Args:
            table_name: Name of the table to analyze

        Returns:
            Schema object or None if table doesn't exist
        """
        if not self.engine:
            return None

        try:
            # Query information_schema for table structure
            result = self.engine.execute_query(
                f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            )

            rows = result.fetchall() if hasattr(result, "fetchall") else []

            if not rows:
                return None

            columns = []
            for row in rows:
                column = Column(
                    name=row[0],
                    data_type=row[1],
                    nullable=row[2].upper() == "YES" if row[2] else True,
                    default_value=row[3] if row[3] else None,
                )
                columns.append(column)

            return Schema(table_name=table_name, columns=columns)

        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return None

    def _get_time_ms(self) -> int:
        """Get current time in milliseconds (for performance tracking)."""
        import time

        return int(time.time() * 1000)
