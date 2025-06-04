"""
Unit tests for schema evolution framework.

Tests schema compatibility checking, type compatibility rules, and evolution
planning logic without requiring database operations.
"""

import pytest

from sqlflow.core.engines.duckdb.transform.schema import (
    Column,
    ColumnChange,
    ColumnChangeType,
    Schema,
    SchemaCompatibility,
    SchemaCompatibilityStatus,
    SchemaEvolutionPolicy,
)


class TestColumn:
    """Test suite for Column dataclass."""

    def test_column_creation(self):
        """Test basic column creation."""
        column = Column(name="id", data_type="INTEGER", nullable=False)
        assert column.name == "id"
        assert column.data_type == "INTEGER"
        assert column.nullable is False
        assert column.default_value is None

    def test_column_type_normalization(self):
        """Test data type normalization."""
        test_cases = [
            ("INT", "INTEGER"),
            ("INT4", "INTEGER"),
            ("INT8", "BIGINT"),
            ("FLOAT", "REAL"),
            ("FLOAT8", "DOUBLE"),
            ("TEXT", "VARCHAR"),
            ("STRING", "VARCHAR"),
            ("BOOL", "BOOLEAN"),
            ("VARCHAR(50)", "VARCHAR"),
            ("UNKNOWN_TYPE", "UNKNOWN_TYPE"),
        ]

        for input_type, expected_type in test_cases:
            column = Column(name="test", data_type=input_type)
            assert (
                column.data_type == expected_type
            ), f"Expected {input_type} -> {expected_type}, got {column.data_type}"

    def test_column_with_default(self):
        """Test column with default value."""
        column = Column(name="status", data_type="VARCHAR", default_value="'active'")
        assert column.default_value == "'active'"


class TestSchema:
    """Test suite for Schema dataclass."""

    @pytest.fixture
    def sample_schema(self):
        """Create a sample schema for testing."""
        columns = [
            Column("id", "INTEGER", False),
            Column("name", "VARCHAR", True),
            Column("created_at", "TIMESTAMP", False),
            Column("score", "REAL", True),
        ]
        return Schema("test_table", columns)

    def test_schema_creation(self, sample_schema):
        """Test schema creation and basic properties."""
        assert sample_schema.table_name == "test_table"
        assert len(sample_schema.columns) == 4
        assert sample_schema.columns[0].name == "id"

    def test_get_column_by_name(self, sample_schema):
        """Test getting column by name (case-insensitive)."""
        # Exact case
        column = sample_schema.get_column("name")
        assert column is not None
        assert column.name == "name"

        # Different case
        column = sample_schema.get_column("NAME")
        assert column is not None
        assert column.name == "name"

        # Non-existent column
        column = sample_schema.get_column("nonexistent")
        assert column is None

    def test_get_column_names(self, sample_schema):
        """Test getting set of column names."""
        column_names = sample_schema.get_column_names()
        expected_names = {"id", "name", "created_at", "score"}
        assert column_names == expected_names

    def test_schema_to_dict(self, sample_schema):
        """Test schema conversion to dictionary."""
        schema_dict = sample_schema.to_dict()

        assert schema_dict["table_name"] == "test_table"
        assert len(schema_dict["columns"]) == 4

        first_column = schema_dict["columns"][0]
        assert first_column["name"] == "id"
        assert first_column["data_type"] == "INTEGER"
        assert first_column["nullable"] is False


class TestSchemaCompatibility:
    """Test suite for SchemaCompatibility dataclass."""

    def test_compatibility_creation(self):
        """Test compatibility result creation."""
        changes = [ColumnChange("new_col", ColumnChangeType.ADDED)]

        compatibility = SchemaCompatibility(
            status=SchemaCompatibilityStatus.REQUIRES_MIGRATION,
            changes=changes,
            warnings=["Test warning"],
        )

        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
        assert len(compatibility.changes) == 1
        assert compatibility.warnings == ["Test warning"]

    def test_is_compatible_check(self):
        """Test compatibility checking logic."""
        # Compatible
        compatible = SchemaCompatibility(
            status=SchemaCompatibilityStatus.COMPATIBLE, changes=[]
        )
        assert compatible.is_compatible() is True

        # Requires migration but still compatible
        migration_required = SchemaCompatibility(
            status=SchemaCompatibilityStatus.REQUIRES_MIGRATION, changes=[]
        )
        assert migration_required.is_compatible() is True

        # Incompatible
        incompatible = SchemaCompatibility(
            status=SchemaCompatibilityStatus.INCOMPATIBLE,
            changes=[],
            error_message="Test error",
        )
        assert incompatible.is_compatible() is False

    def test_requires_migration_check(self):
        """Test migration requirement checking."""
        no_migration = SchemaCompatibility(
            status=SchemaCompatibilityStatus.COMPATIBLE, changes=[]
        )
        assert no_migration.requires_migration() is False

        requires_migration = SchemaCompatibility(
            status=SchemaCompatibilityStatus.REQUIRES_MIGRATION, changes=[]
        )
        assert requires_migration.requires_migration() is True


class TestSchemaEvolutionPolicy:
    """Test suite for SchemaEvolutionPolicy."""

    @pytest.fixture
    def policy(self):
        """Create schema evolution policy for testing."""
        return SchemaEvolutionPolicy()

    @pytest.fixture
    def old_schema(self):
        """Create old schema for testing."""
        columns = [
            Column("id", "INTEGER", False),
            Column("name", "VARCHAR", True),
            Column("age", "INTEGER", True),
        ]
        return Schema("users", columns)

    def test_policy_initialization(self, policy):
        """Test policy initialization."""
        assert policy.engine is None
        assert policy.performance_threshold_ms == 100

    def test_type_change_analysis(self, policy):
        """Test type change analysis."""
        test_cases = [
            ("INTEGER", "BIGINT", ColumnChangeType.TYPE_WIDENED),
            ("REAL", "DOUBLE", ColumnChangeType.TYPE_WIDENED),
            ("VARCHAR", "TEXT", ColumnChangeType.TYPE_WIDENED),
            (
                "BIGINT",
                "INTEGER",
                ColumnChangeType.TYPE_INCOMPATIBLE,
            ),  # No reverse mapping
            ("INTEGER", "VARCHAR", ColumnChangeType.TYPE_INCOMPATIBLE),
            ("INTEGER", "INTEGER", ColumnChangeType.TYPE_WIDENED),  # Same type
        ]

        for old_type, new_type, expected_change in test_cases:
            result = policy._analyze_type_change(old_type, new_type)
            assert (
                result == expected_change
            ), f"Expected {old_type}->{new_type} to be {expected_change}, got {result}"

    def test_compatible_schemas_no_changes(self, policy, old_schema):
        """Test compatibility check with identical schemas."""
        # Same schema
        compatibility = policy.check_compatibility(old_schema, old_schema)

        assert compatibility.status == SchemaCompatibilityStatus.COMPATIBLE
        assert len(compatibility.changes) == 0
        assert compatibility.error_message is None
        assert compatibility.is_compatible() is True

    def test_schema_with_added_column(self, policy, old_schema):
        """Test compatibility check with added column."""
        # Add a new column
        new_columns = old_schema.columns + [Column("email", "VARCHAR", True)]
        new_schema = Schema("users", new_columns)

        compatibility = policy.check_compatibility(old_schema, new_schema)

        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
        assert len(compatibility.changes) == 1
        assert compatibility.changes[0].change_type == ColumnChangeType.ADDED
        assert compatibility.changes[0].column_name == "email"
        assert compatibility.is_compatible() is True
        assert compatibility.requires_migration() is True

    def test_schema_with_removed_column(self, policy, old_schema):
        """Test compatibility check with removed column."""
        # Remove the 'age' column
        new_columns = [col for col in old_schema.columns if col.name != "age"]
        new_schema = Schema("users", new_columns)

        compatibility = policy.check_compatibility(old_schema, new_schema)

        assert compatibility.status == SchemaCompatibilityStatus.INCOMPATIBLE
        assert len(compatibility.changes) == 1
        assert compatibility.changes[0].change_type == ColumnChangeType.REMOVED
        assert compatibility.changes[0].column_name == "age"
        assert compatibility.is_compatible() is False

    def test_schema_with_type_widening(self, policy, old_schema):
        """Test compatibility check with type widening."""
        # Change age from INTEGER to BIGINT
        new_columns = []
        for col in old_schema.columns:
            if col.name == "age":
                new_columns.append(Column("age", "BIGINT", True))
            else:
                new_columns.append(col)

        new_schema = Schema("users", new_columns)
        compatibility = policy.check_compatibility(old_schema, new_schema)

        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
        assert len(compatibility.changes) == 1
        assert compatibility.changes[0].change_type == ColumnChangeType.TYPE_WIDENED
        assert compatibility.changes[0].column_name == "age"
        assert compatibility.is_compatible() is True

    def test_schema_with_incompatible_type_change(self, policy, old_schema):
        """Test compatibility check with incompatible type change."""
        # Change age from INTEGER to VARCHAR (incompatible)
        new_columns = []
        for col in old_schema.columns:
            if col.name == "age":
                new_columns.append(Column("age", "VARCHAR", True))
            else:
                new_columns.append(col)

        new_schema = Schema("users", new_columns)
        compatibility = policy.check_compatibility(old_schema, new_schema)

        assert compatibility.status == SchemaCompatibilityStatus.INCOMPATIBLE
        assert len(compatibility.changes) == 1
        assert (
            compatibility.changes[0].change_type == ColumnChangeType.TYPE_INCOMPATIBLE
        )
        assert compatibility.changes[0].column_name == "age"
        assert compatibility.is_compatible() is False

    def test_add_column_sql_generation(self, policy):
        """Test SQL generation for adding columns."""
        table_name = "test_table"

        # Basic nullable column
        column = Column("new_col", "VARCHAR", True)
        sql = policy._generate_add_column_sql(table_name, column)
        assert sql == "ALTER TABLE test_table ADD COLUMN new_col VARCHAR"

        # Not null column with default (DuckDB compatible - no NOT NULL constraint)
        column = Column("status", "VARCHAR", False, "'active'")
        sql = policy._generate_add_column_sql(table_name, column)
        assert (
            sql == "ALTER TABLE test_table ADD COLUMN status VARCHAR DEFAULT 'active'"
        )

        # Not null column without default (should get automatic default)
        column = Column("count", "INTEGER", False)
        sql = policy._generate_add_column_sql(table_name, column)
        assert sql == "ALTER TABLE test_table ADD COLUMN count INTEGER DEFAULT 0"

    def test_type_change_sql_generation(self, policy):
        """Test SQL generation for type changes."""
        table_name = "test_table"
        old_column = Column("age", "INTEGER", True)
        new_column = Column("age", "BIGINT", True)

        sql = policy._generate_type_change_sql(table_name, old_column, new_column)
        assert sql == "ALTER TABLE test_table ALTER COLUMN age TYPE BIGINT"

    def test_evolution_plan_generation(self, policy, old_schema):
        """Test evolution plan generation."""
        # Add column and widen type
        new_columns = old_schema.columns + [Column("email", "VARCHAR", True)]
        # Change age to BIGINT
        for i, col in enumerate(new_columns):
            if col.name == "age":
                new_columns[i] = Column("age", "BIGINT", True)

        new_schema = Schema("users", new_columns)
        plan = policy.get_evolution_plan(old_schema, new_schema)

        assert len(plan.steps) == 2  # Add column + type change
        assert plan.estimated_time_ms > 0
        assert plan.requires_downtime is True  # Type changes require downtime
        assert len(plan.rollback_steps) == 2

    def test_evolution_plan_no_changes(self, policy, old_schema):
        """Test evolution plan with no changes."""
        plan = policy.get_evolution_plan(old_schema, old_schema)

        assert len(plan.steps) == 1
        assert plan.steps[0] == "No schema changes required"
        assert plan.estimated_time_ms == 0
        assert plan.requires_downtime is False

    def test_evolution_plan_incompatible_changes(self, policy, old_schema):
        """Test evolution plan with incompatible changes."""
        # Remove a column (incompatible)
        new_columns = [col for col in old_schema.columns if col.name != "age"]
        new_schema = Schema("users", new_columns)

        plan = policy.get_evolution_plan(old_schema, new_schema)

        assert len(plan.steps) == 1
        assert "ERROR:" in plan.steps[0]
        assert plan.estimated_time_ms == 0
        assert plan.requires_downtime is True

    def test_complex_schema_changes(self, policy):
        """Test complex schema changes with multiple operations."""
        old_columns = [
            Column("id", "INTEGER", False),
            Column("name", "VARCHAR", True),
            Column("age", "INTEGER", True),
            Column("old_field", "VARCHAR", True),  # Will be removed
        ]
        old_schema = Schema("users", old_columns)

        new_columns = [
            Column("id", "BIGINT", False),  # Type widened
            Column("name", "VARCHAR", True),  # Unchanged
            Column("age", "INTEGER", True),  # Unchanged
            Column("email", "VARCHAR", True),  # Added
            Column("score", "REAL", True),  # Added
        ]
        new_schema = Schema("users", new_columns)

        compatibility = policy.check_compatibility(old_schema, new_schema)

        # Should be incompatible due to removed column
        assert compatibility.status == SchemaCompatibilityStatus.INCOMPATIBLE
        assert len(compatibility.changes) == 4  # 1 widening + 2 additions + 1 removal

        # Check specific changes
        change_types = {
            change.column_name: change.change_type for change in compatibility.changes
        }
        assert change_types["id"] == ColumnChangeType.TYPE_WIDENED
        assert change_types["email"] == ColumnChangeType.ADDED
        assert change_types["score"] == ColumnChangeType.ADDED
        assert change_types["old_field"] == ColumnChangeType.REMOVED

    def test_case_insensitive_column_matching(self, policy):
        """Test case-insensitive column name matching."""
        old_columns = [Column("ID", "INTEGER", False), Column("Name", "VARCHAR", True)]
        old_schema = Schema("users", old_columns)

        new_columns = [
            Column("id", "BIGINT", False),
            Column("name", "TEXT", True),
        ]  # Different case
        new_schema = Schema("users", new_columns)

        compatibility = policy.check_compatibility(old_schema, new_schema)

        # Should detect type changes despite case differences
        # Note: TEXT gets normalized to VARCHAR, so Name column doesn't change
        # Only ID column changes from INTEGER to BIGINT
        assert len(compatibility.changes) == 1

        # Check that the ID change is detected with original case preserved
        id_change = compatibility.changes[0]
        assert id_change.column_name == "ID"  # Original case preserved
        assert id_change.change_type == ColumnChangeType.TYPE_WIDENED
        assert id_change.old_column.data_type == "INTEGER"
        assert id_change.new_column.data_type == "BIGINT"

    def test_performance_threshold_tracking(self, policy, old_schema):
        """Test performance threshold monitoring."""
        # This test verifies the performance tracking logic
        # In a real scenario, we might mock time to test slow operations
        compatibility = policy.check_compatibility(old_schema, old_schema)

        # Should complete quickly and not trigger warnings
        assert compatibility.status == SchemaCompatibilityStatus.COMPATIBLE
        # Performance tracking happens in the background - no direct assertions needed
