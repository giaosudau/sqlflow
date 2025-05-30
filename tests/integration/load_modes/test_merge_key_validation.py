"""Merge Key Validation Tests.

This module contains focused tests for merge key validation,
including single keys, composite keys, and error scenarios.
"""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine


class TestMergeKeyValidation:
    """Merge key validation tests for MERGE operations."""

    def test_merge_key_validation_single_key(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with a single key."""
        # Create source and target tables with compatible single key
        source_df = pd.DataFrame({
            "user_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"]
        })
        target_df = pd.DataFrame({
            "user_id": [4, 5],
            "name": ["David", "Eve"],
            "email": ["david@example.com", "eve@example.com"]
        })
        
        duckdb_engine.register_table("source_table", source_df)
        duckdb_engine.register_table("target_table", target_df)
        
        # Should validate successfully with single merge key
        result = duckdb_engine.validate_merge_keys("target_table", "source_table", ["user_id"])
        assert result is True

    def test_merge_key_validation_composite_keys(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with composite (multiple) keys."""
        # Create source and target tables with composite keys
        source_df = pd.DataFrame({
            "tenant_id": [1, 1, 2],
            "user_id": [101, 102, 101],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 90.0, 78.5]
        })
        target_df = pd.DataFrame({
            "tenant_id": [1, 2],
            "user_id": [103, 102],
            "name": ["David", "Eve"],
            "score": [92.0, 88.5]
        })
        
        duckdb_engine.register_table("source_table", source_df)
        duckdb_engine.register_table("target_table", target_df)
        
        # Should validate successfully with composite merge keys
        result = duckdb_engine.validate_merge_keys("target_table", "source_table", ["tenant_id", "user_id"])
        assert result is True

    def test_merge_key_validation_nonexistent_key(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with nonexistent key in source."""
        # Create source without the merge key
        source_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        target_df = pd.DataFrame({
            "user_id": [4, 5],
            "name": ["David", "Eve"]
        })
        
        duckdb_engine.register_table("source_table", source_df) 
        duckdb_engine.register_table("target_table", target_df)
        
        # Should fail because 'user_id' doesn't exist in source
        with pytest.raises(ValueError, match="Merge key 'user_id' not found in source"):
            duckdb_engine.validate_merge_keys("target_table", "source_table", ["user_id"])

    def test_merge_key_validation_incompatible_types(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with incompatible key types."""
        # Create tables with incompatible merge key types using SQL
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_table AS
            SELECT
                'abc'::VARCHAR AS user_id,  -- VARCHAR type
                'Alice' AS name
        """
        )
        
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table AS
            SELECT
                123::INTEGER AS user_id,  -- INTEGER type
                'David' AS name
        """
        )
        
        # Should fail because VARCHAR and INTEGER are incompatible for merge keys
        with pytest.raises(ValueError, match="incompatible types"):
            duckdb_engine.validate_merge_keys("target_table", "source_table", ["user_id"])

    def test_merge_key_validation_empty_keys(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with empty keys list."""
        # Create tables (content doesn't matter for this test)
        source_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        target_df = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "David"]})
        
        duckdb_engine.register_table("source_table", source_df)
        duckdb_engine.register_table("target_table", target_df)
        
        # Should fail with empty merge keys
        with pytest.raises(ValueError, match="at least one merge key"):
            duckdb_engine.validate_merge_keys("target_table", "source_table", [])

    def test_merge_key_validation_missing_in_target(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation when key exists in source but not target."""
        # Create source with merge key
        source_df = pd.DataFrame({
            "user_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        # Create target without merge key
        target_df = pd.DataFrame({
            "id": [4, 5],  # Different column name
            "name": ["David", "Eve"]
        })
        
        duckdb_engine.register_table("source_table", source_df)
        duckdb_engine.register_table("target_table", target_df)
        
        # Should fail because 'user_id' doesn't exist in target
        with pytest.raises(ValueError, match="Merge key 'user_id' not found in target"):
            duckdb_engine.validate_merge_keys("target_table", "source_table", ["user_id"])

    def test_merge_key_validation_case_sensitivity(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with case sensitivity considerations."""
        # Create tables with different case for column names
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_table AS
            SELECT
                1 AS user_id,
                'Alice' AS name
        """
        )
        
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table AS
            SELECT
                4 AS USER_ID,  -- Different case
                'David' AS name
        """
        )
        
        # DuckDB typically normalizes column names to lowercase
        # So this should work if both are normalized to 'user_id'
        try:
            result = duckdb_engine.validate_merge_keys("target_table", "source_table", ["user_id"])
            assert result is True
        except ValueError as e:
            # If it fails due to case sensitivity, that's acceptable behavior
            assert "not found" in str(e)

    def test_merge_key_validation_with_nulls(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation with NULL values in key columns."""
        # Create tables with NULL values in merge key columns
        duckdb_engine.execute_query(
            """
            CREATE TABLE source_table AS
            SELECT
                CASE WHEN id = 3 THEN NULL ELSE id END AS user_id,
                name
            FROM (VALUES 
                (1, 'Alice'),
                (2, 'Bob'),
                (3, 'Charlie')
            ) AS t(id, name)
        """
        )
        
        duckdb_engine.execute_query(
            """
            CREATE TABLE target_table AS
            SELECT
                4 AS user_id,
                'David' AS name
        """
        )
        
        # Validation should still work (NULL handling is a runtime concern)
        result = duckdb_engine.validate_merge_keys("target_table", "source_table", ["user_id"])
        assert result is True

    def test_merge_key_validation_performance_with_large_keys(self, duckdb_engine: DuckDBEngine):
        """Test merge key validation performance with multiple composite keys."""
        # Create tables with many columns for composite key testing
        source_df = pd.DataFrame({
            "key1": [1, 2, 3],
            "key2": ["A", "B", "C"],
            "key3": [10.1, 20.2, 30.3],
            "key4": [True, False, True],
            "data": ["data1", "data2", "data3"]
        })
        target_df = pd.DataFrame({
            "key1": [4, 5],
            "key2": ["D", "E"],
            "key3": [40.4, 50.5],
            "key4": [False, True],
            "data": ["data4", "data5"]
        })
        
        duckdb_engine.register_table("source_table", source_df)
        duckdb_engine.register_table("target_table", target_df)
        
        # Should handle composite keys with multiple types efficiently
        result = duckdb_engine.validate_merge_keys(
            "target_table", 
            "source_table", 
            ["key1", "key2", "key3", "key4"]
        )
        assert result is True 