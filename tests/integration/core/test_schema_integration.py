"""
Integration tests for schema evolution framework with real DuckDB engine.

These tests validate schema evolution operations using real database operations,
ensuring that schema changes are applied correctly and performance requirements are met.
"""

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.engines.duckdb.transform.schema import (
    Column,
    ColumnChangeType,
    Schema,
    SchemaCompatibilityStatus,
    SchemaEvolutionPolicy,
)


class TestSchemaEvolutionIntegration:
    """Integration tests for schema evolution with real DuckDB engine."""

    @pytest.fixture
    def duckdb_engine(self):
        """Create a real DuckDB engine for testing."""
        # Use in-memory database to avoid file issues
        engine = DuckDBEngine(":memory:")
        yield engine
        engine.close()

    @pytest.fixture
    def schema_policy(self, duckdb_engine):
        """Create schema evolution policy with real engine."""
        return SchemaEvolutionPolicy(duckdb_engine)

    @pytest.fixture
    def test_table(self, duckdb_engine):
        """Create a test table with sample schema."""
        table_name = "users"

        # Create initial table
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert sample data
        duckdb_engine.execute_query(
            f"""
            INSERT INTO {table_name} (id, name, age) VALUES 
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 35)
        """
        )

        return table_name

    def test_get_schema_from_engine(self, schema_policy, test_table):
        """Test getting current schema from database engine."""
        schema = schema_policy.get_schema_from_engine(test_table)

        assert schema is not None
        assert schema.table_name == test_table
        assert len(schema.columns) == 4  # id, name, age, created_at

        # Check specific columns
        id_col = schema.get_column("id")
        assert id_col is not None
        assert id_col.data_type == "INTEGER"
        assert id_col.nullable is False

        name_col = schema.get_column("name")
        assert name_col is not None
        assert name_col.data_type == "VARCHAR"
        assert name_col.nullable is False

    def test_get_schema_nonexistent_table(self, schema_policy):
        """Test getting schema for non-existent table."""
        schema = schema_policy.get_schema_from_engine("nonexistent_table")
        assert schema is None

    def test_add_column_evolution(self, schema_policy, test_table, duckdb_engine):
        """Test automatic column addition through schema evolution."""
        # Get current schema
        current_schema = schema_policy.get_schema_from_engine(test_table)
        assert current_schema is not None

        # Define target schema with additional column
        target_columns = current_schema.columns + [
            Column("email", "VARCHAR", True, "'unknown@example.com'")
        ]
        target_schema = Schema(test_table, target_columns)

        # Check compatibility
        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
        assert len(compatibility.changes) == 1
        assert compatibility.changes[0].change_type == ColumnChangeType.ADDED

        # Apply evolution
        schema_policy.apply_evolution(test_table, compatibility)

        # Verify column was added
        updated_schema = schema_policy.get_schema_from_engine(test_table)
        email_col = updated_schema.get_column("email")
        assert email_col is not None
        assert email_col.data_type == "VARCHAR"

        # Verify existing data preserved and new column has default
        result = duckdb_engine.execute_query(
            f"SELECT id, name, email FROM {test_table} ORDER BY id"
        )
        rows = result.fetchall()
        assert len(rows) == 3
        assert rows[0][2] == "unknown@example.com"  # Default value applied

    def test_type_widening_evolution(self, schema_policy, test_table, duckdb_engine):
        """Test automatic type widening through schema evolution."""
        # Get current schema
        current_schema = schema_policy.get_schema_from_engine(test_table)

        # Define target schema with widened type (age: INTEGER -> BIGINT)
        target_columns = []
        for col in current_schema.columns:
            if col.name == "age":
                target_columns.append(Column("age", "BIGINT", col.nullable))
            else:
                target_columns.append(col)
        target_schema = Schema(test_table, target_columns)

        # Check compatibility
        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
        assert len(compatibility.changes) == 1
        assert compatibility.changes[0].change_type == ColumnChangeType.TYPE_WIDENED

        # Apply evolution
        schema_policy.apply_evolution(test_table, compatibility)

        # Verify type was changed
        updated_schema = schema_policy.get_schema_from_engine(test_table)
        age_col = updated_schema.get_column("age")
        assert age_col is not None
        assert age_col.data_type == "BIGINT"

        # Verify existing data preserved
        result = duckdb_engine.execute_query(
            f"SELECT id, age FROM {test_table} ORDER BY id"
        )
        rows = result.fetchall()
        assert len(rows) == 3
        assert rows[0][1] == 25  # Original data preserved

    def test_incompatible_schema_evolution(self, schema_policy, test_table):
        """Test handling of incompatible schema changes."""
        # Get current schema
        current_schema = schema_policy.get_schema_from_engine(test_table)

        # Define target schema with removed column (incompatible)
        target_columns = [col for col in current_schema.columns if col.name != "name"]
        target_schema = Schema(test_table, target_columns)

        # Check compatibility
        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        assert compatibility.status == SchemaCompatibilityStatus.INCOMPATIBLE
        assert len(compatibility.changes) == 1
        assert compatibility.changes[0].change_type == ColumnChangeType.REMOVED

        # Attempt to apply evolution should fail
        with pytest.raises(
            ValueError, match="Cannot apply incompatible schema changes"
        ):
            schema_policy.apply_evolution(test_table, compatibility)

    def test_multiple_column_evolution(self, schema_policy, test_table, duckdb_engine):
        """Test evolution with multiple column changes."""
        # Get current schema
        current_schema = schema_policy.get_schema_from_engine(test_table)

        # Define target schema with multiple changes
        target_columns = []
        for col in current_schema.columns:
            if col.name == "age":
                # Widen age type
                target_columns.append(Column("age", "BIGINT", col.nullable))
            else:
                target_columns.append(col)

        # Add new columns
        target_columns.extend(
            [Column("email", "VARCHAR", True), Column("score", "REAL", True, "0.0")]
        )

        target_schema = Schema(test_table, target_columns)

        # Check compatibility
        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
        assert len(compatibility.changes) == 3  # 1 type change + 2 additions

        # Apply evolution
        schema_policy.apply_evolution(test_table, compatibility)

        # Verify all changes applied
        updated_schema = schema_policy.get_schema_from_engine(test_table)
        assert len(updated_schema.columns) == 6  # Original 4 + 2 new

        age_col = updated_schema.get_column("age")
        assert age_col.data_type == "BIGINT"

        email_col = updated_schema.get_column("email")
        assert email_col is not None

        score_col = updated_schema.get_column("score")
        assert score_col is not None
        assert score_col.data_type == "REAL"

    def test_evolution_plan_generation(self, schema_policy, test_table):
        """Test evolution plan generation with real schema."""
        # Get current schema
        current_schema = schema_policy.get_schema_from_engine(test_table)

        # Define target schema with changes
        target_columns = current_schema.columns + [
            Column("status", "VARCHAR", False, "'active'")
        ]
        target_schema = Schema(test_table, target_columns)

        # Generate evolution plan
        plan = schema_policy.get_evolution_plan(current_schema, target_schema)

        assert len(plan.steps) == 1
        assert "ADD COLUMN" in plan.steps[0]
        assert plan.estimated_time_ms > 0
        assert (
            plan.requires_downtime is False
        )  # Column addition doesn't require downtime
        assert len(plan.rollback_steps) == 1
        assert "DROP COLUMN" in plan.rollback_steps[0]

    def test_schema_evolution_performance(self, schema_policy, test_table):
        """Test schema evolution performance meets requirements."""
        # Get current schema
        current_schema = schema_policy.get_schema_from_engine(test_table)

        # Define target schema (no changes for performance test)
        target_schema = Schema(test_table, current_schema.columns)

        # Test performance of compatibility check
        import time

        start_time = time.time()
        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        end_time = time.time()

        # Should complete within performance threshold
        elapsed_ms = (end_time - start_time) * 1000
        assert elapsed_ms < schema_policy.performance_threshold_ms
        assert compatibility.status == SchemaCompatibilityStatus.COMPATIBLE

    def test_complex_type_compatibility_scenarios(self, schema_policy, duckdb_engine):
        """Test complex type compatibility scenarios."""
        # Create table with various data types
        table_name = "type_test_table"
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                small_int SMALLINT,
                regular_int INTEGER,
                big_int BIGINT,
                float_val REAL,
                double_val DOUBLE,
                text_val VARCHAR(50),
                bool_val BOOLEAN,
                date_val DATE,
                timestamp_val TIMESTAMP
            )
        """
        )

        current_schema = schema_policy.get_schema_from_engine(table_name)

        # Test various type widenings
        test_cases = [
            ("small_int", "INTEGER", ColumnChangeType.TYPE_WIDENED),
            ("regular_int", "BIGINT", ColumnChangeType.TYPE_WIDENED),
            ("float_val", "DOUBLE", ColumnChangeType.TYPE_WIDENED),
            (
                "text_val",
                "TEXT",
                ColumnChangeType.TYPE_WIDENED,
            ),  # VARCHAR -> TEXT (normalized)
            ("date_val", "TIMESTAMP", ColumnChangeType.TYPE_WIDENED),
        ]

        for column_name, new_type, expected_change in test_cases:
            # Create target schema with single type change
            target_columns = []
            for col in current_schema.columns:
                if col.name == column_name:
                    target_columns.append(Column(col.name, new_type, col.nullable))
                else:
                    target_columns.append(col)

            target_schema = Schema(table_name, target_columns)
            compatibility = schema_policy.check_compatibility(
                current_schema, target_schema
            )

            if (
                new_type == "TEXT"
            ):  # TEXT gets normalized to VARCHAR, so no change detected
                assert compatibility.status == SchemaCompatibilityStatus.COMPATIBLE
            else:
                assert (
                    compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION
                )
                assert len(compatibility.changes) == 1
                assert compatibility.changes[0].change_type == expected_change

    def test_error_handling_in_schema_operations(self, schema_policy, duckdb_engine):
        """Test error handling in schema operations."""
        # Test with closed engine
        duckdb_engine.close()

        # Should handle database errors gracefully
        schema = schema_policy.get_schema_from_engine("any_table")
        assert schema is None

        # Test compatibility check with error
        old_schema = Schema("test", [Column("id", "INTEGER")])
        new_schema = Schema("test", [Column("id", "VARCHAR")])

        compatibility = schema_policy.check_compatibility(old_schema, new_schema)
        # Should still work since it's pure logic
        assert compatibility.status == SchemaCompatibilityStatus.INCOMPATIBLE

    def test_schema_evolution_with_constraints(self, schema_policy, duckdb_engine):
        """Test schema evolution with table constraints."""
        table_name = "constrained_table"

        # Create table with constraints
        duckdb_engine.execute_query(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                email VARCHAR UNIQUE,
                age INTEGER CHECK (age > 0)
            )
        """
        )

        current_schema = schema_policy.get_schema_from_engine(table_name)

        # Add non-nullable column (should work with default)
        target_columns = current_schema.columns + [
            Column("status", "VARCHAR", False, "'active'")
        ]
        target_schema = Schema(table_name, target_columns)

        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        assert compatibility.status == SchemaCompatibilityStatus.REQUIRES_MIGRATION

        # Apply evolution
        schema_policy.apply_evolution(table_name, compatibility)

        # Verify column added successfully despite constraints
        updated_schema = schema_policy.get_schema_from_engine(table_name)
        status_col = updated_schema.get_column("status")
        assert status_col is not None

    def test_evolution_with_existing_data_integrity(
        self, schema_policy, test_table, duckdb_engine
    ):
        """Test that schema evolution preserves data integrity."""
        # Insert more test data
        duckdb_engine.execute_query(
            f"""
            INSERT INTO {test_table} (id, name, age) VALUES 
            (4, 'David', 40),
            (5, 'Eve', 28)
        """
        )

        # Get row count before evolution
        result = duckdb_engine.execute_query(f"SELECT COUNT(*) FROM {test_table}")
        original_count = result.fetchone()[0]

        # Apply schema evolution (add column + type widening)
        current_schema = schema_policy.get_schema_from_engine(test_table)
        target_columns = []
        for col in current_schema.columns:
            if col.name == "age":
                target_columns.append(Column("age", "BIGINT", col.nullable))
            else:
                target_columns.append(col)
        target_columns.append(Column("department", "VARCHAR", True, "'Engineering'"))

        target_schema = Schema(test_table, target_columns)
        compatibility = schema_policy.check_compatibility(current_schema, target_schema)
        schema_policy.apply_evolution(test_table, compatibility)

        # Verify data integrity preserved
        result = duckdb_engine.execute_query(f"SELECT COUNT(*) FROM {test_table}")
        new_count = result.fetchone()[0]
        assert new_count == original_count

        # Verify all original data preserved
        result = duckdb_engine.execute_query(
            f"""
            SELECT id, name, age, department FROM {test_table} ORDER BY id
        """
        )
        rows = result.fetchall()
        assert len(rows) == original_count
        assert rows[0][3] == "Engineering"  # Default value applied
        assert all(
            row[0] is not None and row[1] is not None for row in rows
        )  # Original data preserved
