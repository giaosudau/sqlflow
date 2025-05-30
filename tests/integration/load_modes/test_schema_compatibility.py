"""Core Schema Compatibility Tests.

This module contains focused tests for schema compatibility validation,
including type checking, compatibility matrices, and validation logic.
"""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine


class TestSchemaCompatibility:
    """Core schema compatibility validation tests."""

    def test_schema_compatibility_validation_basic(self, duckdb_engine: DuckDBEngine):
        """Test basic schema compatibility validation."""
        # Create source table
        source_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                ],
            }
        )
        duckdb_engine.register_table("source_table", source_df)

        # Create target table with identical schema
        target_df = pd.DataFrame(
            {
                "id": [4, 5],
                "name": ["David", "Eve"],
                "email": ["david@example.com", "eve@example.com"],
            }
        )
        duckdb_engine.register_table("target_table", target_df)

        # Get source schema and validate compatibility
        source_schema = duckdb_engine.get_table_schema("source_table")
        is_compatible = duckdb_engine.validate_schema_compatibility(
            "target_table", source_schema
        )
        assert is_compatible is True

    def test_schema_compatibility_validation_extra_columns_in_source(
        self, duckdb_engine: DuckDBEngine
    ):
        """Test schema compatibility when source has extra columns."""
        # Source with extra column
        source_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                ],
                "age": [25, 30, 35],  # Extra column
            }
        )
        duckdb_engine.register_table("source_table", source_df)

        # Target without extra column
        target_df = pd.DataFrame(
            {
                "id": [4, 5],
                "name": ["David", "Eve"],
                "email": ["david@example.com", "eve@example.com"],
            }
        )
        duckdb_engine.register_table("target_table", target_df)

        # Get source schema and validate compatibility
        source_schema = duckdb_engine.get_table_schema("source_table")

        # Should fail because source has extra column not in target
        with pytest.raises(ValueError, match="does not exist in target"):
            duckdb_engine.validate_schema_compatibility("target_table", source_schema)

    def test_schema_compatibility_validation_missing_columns_in_source(
        self, duckdb_engine: DuckDBEngine
    ):
        """Test schema compatibility when source is missing required columns."""
        # Source missing email column
        source_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        duckdb_engine.register_table("source_table", source_df)

        # Target requires email column
        target_df = pd.DataFrame(
            {
                "id": [4, 5],
                "name": ["David", "Eve"],
                "email": ["david@example.com", "eve@example.com"],
            }
        )
        duckdb_engine.register_table("target_table", target_df)

        # Get source schema and validate compatibility
        source_schema = duckdb_engine.get_table_schema("source_table")

        # Should be compatible (source is subset of target - missing columns can have defaults)
        is_compatible = duckdb_engine.validate_schema_compatibility(
            "target_table", source_schema
        )
        assert is_compatible is True

    def test_schema_compatibility_validation_compatible_types(
        self, duckdb_engine: DuckDBEngine
    ):
        """Test schema compatibility with compatible but different types."""
        # Create source with specific types using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_table AS
            SELECT
                1::INTEGER AS id,
                10.5::DOUBLE AS amount,
                TRUE AS is_active,
                'Alice' AS name,
                '2023-01-01' AS created_date
        """
        )

        # Create target with compatible types using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table AS
            SELECT
                4::INTEGER AS id,
                40.0::DOUBLE AS amount,
                FALSE AS is_active,
                'David' AS name,
                '2023-01-04' AS created_date
        """
        )

        # Get source schema and validate compatibility
        source_schema = duckdb_engine.get_table_schema("source_table")
        is_compatible = duckdb_engine.validate_schema_compatibility(
            "target_table", source_schema
        )
        assert is_compatible is True

    def test_schema_compatibility_validation_incompatible_types(
        self, duckdb_engine: DuckDBEngine
    ):
        """Test schema compatibility with incompatible types."""
        # Create source with incompatible types using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_table AS
            SELECT
                'abc' AS id,  -- VARCHAR instead of INTEGER
                10.5::DOUBLE AS amount,
                'Alice' AS name
        """
        )

        # Create target expecting INTEGER for id using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table AS
            SELECT
                4::INTEGER AS id,
                40.0::DOUBLE AS amount,
                'David' AS name
        """
        )

        # Get source schema and validate compatibility
        source_schema = duckdb_engine.get_table_schema("source_table")

        # Should not be compatible (type mismatch for 'id')
        with pytest.raises(ValueError, match="incompatible types"):
            duckdb_engine.validate_schema_compatibility("target_table", source_schema)

    def test_schema_compatibility_validation_complex_schema(
        self, duckdb_engine: DuckDBEngine
    ):
        """Test schema compatibility with complex schemas and multiple types."""
        # Create complex source schema using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_table AS
            SELECT
                1::INTEGER AS user_id,
                'alice' AS username,
                100.50::DOUBLE AS balance,
                TRUE AS is_premium,
                '2023-01-01' AS signup_date,
                '{"key": "value"}' AS metadata,
                'tag1,tag2' AS tags
        """
        )

        # Create compatible target schema (subset with same types) using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table AS
            SELECT
                4::INTEGER AS user_id,
                'david' AS username,
                400.00::DOUBLE AS balance,
                FALSE AS is_premium,
                '2023-04-01' AS signup_date
        """
        )

        # Get source schema and validate compatibility
        source_schema = duckdb_engine.get_table_schema("source_table")

        # Should fail because source has extra columns not in target
        with pytest.raises(ValueError, match="does not exist in target"):
            duckdb_engine.validate_schema_compatibility("target_table", source_schema)

    def test_are_types_compatible_method(self, duckdb_engine: DuckDBEngine):
        """Test the are_types_compatible method directly."""
        # Test compatible type pairs
        assert duckdb_engine._are_types_compatible("INTEGER", "INTEGER") is True
        assert duckdb_engine._are_types_compatible("VARCHAR", "VARCHAR") is True
        assert duckdb_engine._are_types_compatible("DOUBLE", "DOUBLE") is True
        assert duckdb_engine._are_types_compatible("BOOLEAN", "BOOLEAN") is True

        # Test some type promotion cases (if implemented)
        # These may be compatible depending on implementation
        # assert duckdb_engine._are_types_compatible("INTEGER", "BIGINT") is True
        # assert duckdb_engine._are_types_compatible("FLOAT", "DOUBLE") is True

        # Test incompatible type pairs
        assert duckdb_engine._are_types_compatible("INTEGER", "VARCHAR") is False
        assert duckdb_engine._are_types_compatible("BOOLEAN", "DOUBLE") is False
        assert duckdb_engine._are_types_compatible("VARCHAR", "INTEGER") is False

    def test_validate_schema_compatibility_empty_source(
        self, duckdb_engine: DuckDBEngine
    ):
        """Test schema compatibility validation with empty source table."""
        # Create non-empty target
        target_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )
        duckdb_engine.register_table("target_table", target_df)

        # Empty source schema should be compatible (nothing to validate)
        empty_source_schema = {}
        is_compatible = duckdb_engine.validate_schema_compatibility(
            "target_table", empty_source_schema
        )
        assert is_compatible is True

    def test_type_compatibility_matrix(self, duckdb_engine: DuckDBEngine):
        """Test comprehensive type compatibility matrix."""
        # Create tables with various DuckDB types for testing
        duckdb_engine.execute_query(
            """
            CREATE TABLE type_test_source AS
            SELECT
                1::INTEGER AS int_col,
                1000000000::BIGINT AS bigint_col,
                1.1::DOUBLE AS double_col,
                1.5::FLOAT AS float_col,
                'text' AS varchar_col,
                TRUE AS bool_col,
                '2023-01-01'::DATE AS date_col,
                '2023-01-01 12:00:00'::TIMESTAMP AS timestamp_col
        """
        )

        # Test some basic compatible pairs
        # INTEGER to INTEGER
        duckdb_engine.execute_query(
            "CREATE TABLE target_int AS SELECT 1::INTEGER AS int_col"
        )
        source_schema = {"int_col": "INTEGER"}
        assert (
            duckdb_engine.validate_schema_compatibility("target_int", source_schema)
            is True
        )

        # VARCHAR to VARCHAR
        duckdb_engine.execute_query(
            "CREATE TABLE target_varchar AS SELECT 'text' AS varchar_col"
        )
        source_schema = {"varchar_col": "VARCHAR"}
        assert (
            duckdb_engine.validate_schema_compatibility("target_varchar", source_schema)
            is True
        )

        # Test some incompatible pairs
        # INTEGER to VARCHAR should fail
        duckdb_engine.execute_query(
            "CREATE TABLE target_varchar_mismatch AS SELECT 'text' AS int_col"
        )
        source_schema = {"int_col": "INTEGER"}
        with pytest.raises(ValueError, match="incompatible types"):
            duckdb_engine.validate_schema_compatibility(
                "target_varchar_mismatch", source_schema
            )
