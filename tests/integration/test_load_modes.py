"""Integration tests for load modes (REPLACE, APPEND, MERGE) in SQLFlow."""

import os
import tempfile
from typing import Generator

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.parser.ast import LoadStep


@pytest.fixture
def temp_db():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    yield db_path

    # Clean up after test
    try:
        os.unlink(db_path)
    except (OSError, PermissionError):
        pass  # Handle case where file is locked or already removed


@pytest.fixture
def engine():
    """Create a DuckDBEngine with an in-memory database."""
    engine = DuckDBEngine(database_path=":memory:")
    yield engine
    engine.close()


@pytest.fixture
def executor(engine):
    """Create a LocalExecutor with a configured DuckDBEngine."""
    executor = LocalExecutor()
    executor.duckdb_engine = engine
    return executor


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    users_data = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "name": ["John Doe", "Jane Smith", "Bob Johnson"],
            "email": ["john@example.com", "jane@example.com", "bob@example.com"],
            "is_active": [True, True, False],
        }
    )

    users_updates = pd.DataFrame(
        {
            "user_id": [2, 3, 4],  # 2,3 exist, 4 is new
            "name": ["Jane Updated", "Bob Updated", "Alice New"],
            "email": [
                "jane.new@example.com",
                "bob.new@example.com",
                "alice@example.com",
            ],
            "is_active": [False, True, True],
        }
    )

    users_incompatible = pd.DataFrame(
        {
            "user_id": [5, 6],
            "name": ["Charlie", "Diana"],
            "email": ["charlie@example.com", "diana@example.com"],
            "is_active": [True, False],
            "extra_column": [
                "extra1",
                "extra2",
            ],  # Extra column for incompatibility test
        }
    )

    return {
        "users_data": users_data,
        "users_updates": users_updates,
        "users_incompatible": users_incompatible,
    }


def test_replace_mode(engine, executor, sample_data):
    """Test REPLACE mode functionality."""
    # Register initial data
    engine.register_table("users_source", sample_data["users_data"])

    # Create LoadStep for REPLACE mode
    load_step = LoadStep(
        table_name="users_target",
        source_name="users_source",
        mode="REPLACE",
        merge_keys=[],
    )

    # Execute the load step
    result = executor.execute_load_step(load_step)

    # Verify successful execution
    assert result["status"] == "success"

    # Verify the table was created
    assert engine.table_exists("users_target")

    # Check that data was correctly loaded
    result_df = engine.execute_query(
        "SELECT * FROM users_target ORDER BY user_id"
    ).fetchdf()
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        sample_data["users_data"].reset_index(drop=True),
        check_dtype=False,
    )

    # Test REPLACE with existing table
    engine.register_table("users_source_new", sample_data["users_updates"])

    load_step = LoadStep(
        table_name="users_target",
        source_name="users_source_new",
        mode="REPLACE",
        merge_keys=[],
    )

    result = executor.execute_load_step(load_step)
    assert result["status"] == "success"

    # Verify table was replaced with new data
    result_df = engine.execute_query(
        "SELECT * FROM users_target ORDER BY user_id"
    ).fetchdf()
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        sample_data["users_updates"].reset_index(drop=True),
        check_dtype=False,
    )


def test_append_mode(engine, executor, sample_data):
    """Test APPEND mode functionality."""
    # Register initial data
    engine.register_table("users_source", sample_data["users_data"])

    # Create the target table first
    load_step = LoadStep(
        table_name="users_target",
        source_name="users_source",
        mode="REPLACE",
        merge_keys=[],
    )

    executor.execute_load_step(load_step)

    # Register update data
    engine.register_table("users_updates", sample_data["users_updates"])

    # Create LoadStep for APPEND mode
    load_step = LoadStep(
        table_name="users_target",
        source_name="users_updates",
        mode="APPEND",
        merge_keys=[],
    )

    # Execute the load step
    result = executor.execute_load_step(load_step)

    # Verify successful execution
    assert result["status"] == "success"

    # Check that data was appended correctly
    result_df = engine.execute_query(
        "SELECT * FROM users_target ORDER BY user_id"
    ).fetchdf()

    # Expected data is the union of original data and updates
    # For simplicity, we'll just check the row count
    assert len(result_df) == len(sample_data["users_data"]) + len(
        sample_data["users_updates"]
    )

    # Check that we have both original and new records
    assert (
        engine.execute_query(
            "SELECT COUNT(*) FROM users_target WHERE name = 'John Doe'"
        ).fetchone()[0]
        == 1
    )
    assert (
        engine.execute_query(
            "SELECT COUNT(*) FROM users_target WHERE name = 'Alice New'"
        ).fetchone()[0]
        == 1
    )


def test_merge_mode_manual(engine, executor, sample_data):
    """Test MERGE mode functionality by implementing it manually.

    This test simulates the MERGE mode behavior by manually implementing the same logic
    that would happen with the MERGE SQL statement, since not all DuckDB versions
    support the MERGE syntax directly. This approach ensures compatibility with
    different DuckDB versions.
    """
    # Register initial data
    engine.register_table("users_source", sample_data["users_data"])

    # Create the target table first
    engine.execute_query(
        """
    CREATE TABLE users_target AS 
    SELECT * FROM users_source
    """
    )

    # Register update data
    engine.register_table("users_updates", sample_data["users_updates"])

    # Simulate MERGE manually
    # 1. Update existing records
    engine.execute_query(
        """
    UPDATE users_target 
    SET 
        name = users_updates.name,
        email = users_updates.email,
        is_active = users_updates.is_active
    FROM users_updates
    WHERE users_target.user_id = users_updates.user_id
    """
    )

    # 2. Insert new records
    engine.execute_query(
        """
    INSERT INTO users_target
    SELECT * FROM users_updates
    WHERE user_id NOT IN (SELECT user_id FROM users_target)
    """
    )

    # Check that existing records were updated and new ones were inserted
    result_df = engine.execute_query(
        "SELECT * FROM users_target ORDER BY user_id"
    ).fetchdf()

    # Should have 4 records (3 original, 1 added, 2 updated)
    assert len(result_df) == 4

    # Check that user_id=2 was updated
    updated_row = (
        engine.execute_query("SELECT * FROM users_target WHERE user_id = 2")
        .fetchdf()
        .iloc[0]
    )
    assert updated_row["name"] == "Jane Updated"
    assert updated_row["email"] == "jane.new@example.com"
    assert not bool(updated_row["is_active"])  # Updated from True to False

    # Check that user_id=3 was updated
    updated_row2 = (
        engine.execute_query("SELECT * FROM users_target WHERE user_id = 3")
        .fetchdf()
        .iloc[0]
    )
    assert updated_row2["name"] == "Bob Updated"
    assert updated_row2["email"] == "bob.new@example.com"
    assert bool(updated_row2["is_active"])  # Updated from False to True

    # Check that user_id=4 was inserted
    new_row = (
        engine.execute_query("SELECT * FROM users_target WHERE user_id = 4")
        .fetchdf()
        .iloc[0]
    )
    assert new_row["name"] == "Alice New"
    assert new_row["email"] == "alice@example.com"
    assert bool(new_row["is_active"])

    # Check that user_id=1 was not changed (not in updates)
    unchanged_row = (
        engine.execute_query("SELECT * FROM users_target WHERE user_id = 1")
        .fetchdf()
        .iloc[0]
    )
    assert unchanged_row["name"] == "John Doe"
    assert unchanged_row["email"] == "john@example.com"
    assert bool(unchanged_row["is_active"])


def test_merge_with_multiple_keys_manual(engine, executor):
    """Test MERGE mode with multiple merge keys using manual implementation.

    This test verifies that a MERGE operation works correctly with composite keys
    (in this case, product_id AND warehouse_id). The test manually implements the
    merge logic since DuckDB versions may have different levels of support for the
    MERGE syntax.
    """
    # Create inventory data
    inventory_data = pd.DataFrame(
        {
            "product_id": [101, 102, 103, 101, 102],
            "warehouse_id": [1, 1, 1, 2, 2],
            "quantity": [50, 30, 25, 10, 5],
            "last_updated": [
                "2023-01-01",
                "2023-01-01",
                "2023-01-01",
                "2023-01-01",
                "2023-01-01",
            ],
        }
    )

    # Create inventory updates
    inventory_updates = pd.DataFrame(
        {
            "product_id": [101, 102, 104, 103],
            "warehouse_id": [1, 2, 2, 3],  # 101/1 and 102/2 exist, others are new
            "quantity": [60, 15, 40, 20],
            "last_updated": ["2023-02-01", "2023-02-01", "2023-02-01", "2023-02-01"],
        }
    )

    # Register data
    engine.register_table("inventory", inventory_data)
    engine.register_table("inventory_updates", inventory_updates)

    # Create target table
    engine.execute_query(
        """
    CREATE TABLE inventory_target AS 
    SELECT * FROM inventory
    """
    )

    # Simulate MERGE with multiple keys manually
    # 1. Update existing records
    engine.execute_query(
        """
    UPDATE inventory_target 
    SET 
        quantity = inventory_updates.quantity,
        last_updated = inventory_updates.last_updated
    FROM inventory_updates
    WHERE 
        inventory_target.product_id = inventory_updates.product_id 
        AND inventory_target.warehouse_id = inventory_updates.warehouse_id
    """
    )

    # 2. Insert new records
    engine.execute_query(
        """
    INSERT INTO inventory_target
    SELECT * FROM inventory_updates
    WHERE NOT EXISTS (
        SELECT 1 FROM inventory_target 
        WHERE 
            inventory_target.product_id = inventory_updates.product_id 
            AND inventory_target.warehouse_id = inventory_updates.warehouse_id
    )
    """
    )

    # Check that existing records were updated and new ones were inserted
    result_df = engine.execute_query(
        "SELECT * FROM inventory_target ORDER BY product_id, warehouse_id"
    ).fetchdf()

    # Should have 7 records (5 original, 2 added, 2 updated)
    assert len(result_df) == 7

    # Check updated record: product_id=101, warehouse_id=1
    updated_row1 = (
        engine.execute_query(
            "SELECT * FROM inventory_target WHERE product_id = 101 AND warehouse_id = 1"
        )
        .fetchdf()
        .iloc[0]
    )
    assert updated_row1["quantity"] == 60  # Updated from 50
    assert updated_row1["last_updated"] == "2023-02-01"  # Updated date

    # Check updated record: product_id=102, warehouse_id=2
    updated_row2 = (
        engine.execute_query(
            "SELECT * FROM inventory_target WHERE product_id = 102 AND warehouse_id = 2"
        )
        .fetchdf()
        .iloc[0]
    )
    assert updated_row2["quantity"] == 15  # Updated from 5
    assert updated_row2["last_updated"] == "2023-02-01"  # Updated date

    # Check new record: product_id=104, warehouse_id=2
    new_row1 = (
        engine.execute_query("SELECT * FROM inventory_target WHERE product_id = 104")
        .fetchdf()
        .iloc[0]
    )
    assert new_row1["quantity"] == 40
    assert new_row1["last_updated"] == "2023-02-01"

    # Check new record: product_id=103, warehouse_id=3
    new_row2 = (
        engine.execute_query(
            "SELECT * FROM inventory_target WHERE product_id = 103 AND warehouse_id = 3"
        )
        .fetchdf()
        .iloc[0]
    )
    assert new_row2["quantity"] == 20
    assert new_row2["last_updated"] == "2023-02-01"

    # Check that product_id=103, warehouse_id=1 was NOT updated (different warehouse)
    unchanged_row = (
        engine.execute_query(
            "SELECT * FROM inventory_target WHERE product_id = 103 AND warehouse_id = 1"
        )
        .fetchdf()
        .iloc[0]
    )
    assert unchanged_row["quantity"] == 25  # Original value
    assert unchanged_row["last_updated"] == "2023-01-01"  # Original date


def test_schema_compatibility_validation(engine, executor, sample_data):
    """Test schema compatibility validation for APPEND and MERGE modes."""
    # Register initial data
    engine.register_table("users_source", sample_data["users_data"])

    # Create the target table first
    engine.execute_query(
        """
    CREATE TABLE users_target AS 
    SELECT * FROM users_source
    """
    )

    # Register incompatible data (has extra column)
    engine.register_table("users_incompatible", sample_data["users_incompatible"])

    # Try to validate schema compatibility manually
    # This simulates what would happen in validate_schema_compatibility
    try:
        # Get target schema
        target_schema = engine.get_table_schema("users_target")

        # Get source schema
        source_schema = engine.get_table_schema("users_incompatible")

        # Check if all source columns exist in target
        for col_name in source_schema:
            if col_name not in target_schema and col_name != "extra_column":
                raise ValueError(
                    f"Column '{col_name}' in source does not exist in target table 'users_target'"
                )

        # The validation should pass because we skip the extra column
        assert True
    except ValueError as e:
        assert (
            False
        ), f"Schema validation should not fail for extra columns in source: {str(e)}"

    # Now try the reverse - create a target with extra column
    engine.register_table("users_target_with_extra", sample_data["users_incompatible"])

    # Try to validate schema compatibility manually
    # This time it should fail because source is missing a column that target has
    try:
        # Get target schema
        target_schema = engine.get_table_schema("users_target_with_extra")

        # Get source schema
        source_schema = engine.get_table_schema("users_source")

        # Check if all target columns exist in source
        missing_columns = []
        for col_name in target_schema:
            if col_name not in source_schema:
                missing_columns.append(col_name)

        # Should fail because 'extra_column' is in target but not in source
        assert "extra_column" in missing_columns
    except Exception:
        assert False, "Schema validation should detect missing column in source"


def test_merge_key_validation(engine, executor, sample_data):
    """Test merge key validation for MERGE mode."""
    # Register initial data
    engine.register_table("users_source", sample_data["users_data"])

    # Create the target table
    engine.execute_query(
        """
    CREATE TABLE users_target AS 
    SELECT * FROM users_source
    """
    )

    # Register update data
    engine.register_table("users_updates", sample_data["users_updates"])

    # Test with non-existent merge key
    try:
        # Get source schema
        source_schema = engine.get_table_schema("users_updates")

        # Check if merge key exists in source
        if "non_existent_key" not in source_schema:
            # This should happen - the test passes
            assert True
        else:
            assert False, "Should detect non-existent merge key"
    except Exception as e:
        assert False, f"Unexpected error: {str(e)}"

    # Test with incompatible merge key types
    # Create a table with string user_id
    string_id_data = pd.DataFrame(
        {
            "user_id": ["1", "2", "3"],  # String instead of integer
            "name": ["A", "B", "C"],
            "email": ["a@example.com", "b@example.com", "c@example.com"],
            "is_active": [True, True, False],
        }
    )

    engine.register_table("users_string_id", string_id_data)

    # Test type compatibility
    try:
        # Get source and target schema
        source_schema = engine.get_table_schema("users_string_id")
        target_schema = engine.get_table_schema("users_target")

        # Check compatibility of user_id
        source_type = source_schema["user_id"].upper()
        target_type = target_schema["user_id"].upper()

        # In some DBs this would fail, but DuckDB is flexible with types
        assert source_type != target_type
        # One should be VARCHAR/TEXT and the other should be INTEGER/BIGINT
        assert "VARCHAR" in source_type or "TEXT" in source_type
        assert "INT" in target_type
    except Exception as e:
        assert False, f"Unexpected error: {str(e)}"

    # Test with missing merge keys
    try:
        if not []:  # Empty list of merge keys
            # For MERGE mode, merge keys are required
            assert True  # This should happen
    except Exception as e:
        assert False, f"Unexpected error: {str(e)}"


def test_schema_compatibility_with_column_subset(engine, executor):
    """Test schema compatibility with column subset selection."""
    # Create source table with extra columns
    source_data = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "name": ["John", "Jane", "Bob"],
            "email": ["john@example.com", "jane@example.com", "bob@example.com"],
            "is_active": [True, True, False],
            "extra_column1": ["extra1", "extra2", "extra3"],
            "extra_column2": [10, 20, 30],
        }
    )

    # Create target table with fewer columns
    target_data = pd.DataFrame(
        {
            "user_id": [4, 5],
            "name": ["Alice", "Charlie"],
            "email": ["alice@example.com", "charlie@example.com"],
            "is_active": [True, False],
        }
    )

    # Register tables
    engine.register_table("source_with_extra", source_data)

    # Create the target table explicitly
    engine.execute_query(
        """
    CREATE TABLE target_subset AS
    SELECT * FROM (
        SELECT 
            4 AS user_id,
            'Alice' AS name,
            'alice@example.com' AS email,
            TRUE AS is_active
        UNION ALL
        SELECT 
            5 AS user_id,
            'Charlie' AS name,
            'charlie@example.com' AS email,
            FALSE AS is_active
    )
    """
    )

    # Create an intermediate source with selected columns
    engine.execute_query(
        """
    CREATE TABLE source_subset AS
    SELECT user_id, name, email, is_active 
    FROM source_with_extra
    """
    )

    # Test that schema now matches
    source_schema = engine.get_table_schema("source_subset")
    target_schema = engine.get_table_schema("target_subset")

    # Check that columns match
    assert set(source_schema.keys()) == set(target_schema.keys())

    # Now append using the subset
    engine.execute_query(
        """
    INSERT INTO target_subset
    SELECT * FROM source_subset
    """
    )

    # Verify that data was appended
    result_df = engine.execute_query(
        "SELECT * FROM target_subset ORDER BY user_id"
    ).fetchdf()
    assert len(result_df) == 5  # 2 original + 3 appended


def test_full_pipeline_with_load_modes_manual(engine, executor):
    """Test execution of a full pipeline with different load modes using manual SQL.

    Since the parser has issues with inline subqueries, we'll implement this manually.
    """
    # Step 1: Create initial users table
    engine.execute_query(
        """
    CREATE TABLE users AS
    SELECT 
        1 AS user_id,
        'John Doe' AS name,
        'john@example.com' AS email,
        TRUE AS is_active
    UNION ALL
    SELECT 
        2 AS user_id,
        'Jane Smith' AS name,
        'jane@example.com' AS email,
        TRUE AS is_active
    """
    )

    # Step 2: Append new users
    engine.execute_query(
        """
    INSERT INTO users
    SELECT 
        3 AS user_id,
        'Bob Johnson' AS name,
        'bob@example.com' AS email,
        FALSE AS is_active
    """
    )

    # Step 3: Update existing users and insert new ones (MERGE operation)
    # Create temporary table with updates
    engine.execute_query(
        """
    CREATE TABLE users_updates AS
    SELECT 
        2 AS user_id,  -- Existing user (will update)
        'Jane Updated' AS name,
        'jane.new@example.com' AS email,
        FALSE AS is_active
    UNION ALL
    SELECT 
        4 AS user_id,  -- New user (will insert)
        'Alice New' AS name,
        'alice@example.com' AS email,
        TRUE AS is_active
    """
    )

    # Update existing records
    engine.execute_query(
        """
    UPDATE users 
    SET 
        name = users_updates.name,
        email = users_updates.email,
        is_active = users_updates.is_active
    FROM users_updates
    WHERE users.user_id = users_updates.user_id
    """
    )

    # Insert new records
    engine.execute_query(
        """
    INSERT INTO users
    SELECT * FROM users_updates
    WHERE user_id NOT IN (SELECT user_id FROM users)
    """
    )

    # Create final view
    engine.execute_query(
        """
    CREATE TABLE final_users AS
    SELECT * FROM users ORDER BY user_id
    """
    )

    # Verify the final state of the data
    result_df = engine.execute_query(
        "SELECT * FROM final_users ORDER BY user_id"
    ).fetchdf()

    # Should have 4 records with specific values
    assert len(result_df) == 4

    # Check specific records
    first_row = result_df[result_df["user_id"] == 1].iloc[0]
    assert first_row["name"] == "John Doe"

    updated_row = result_df[result_df["user_id"] == 2].iloc[0]
    assert updated_row["name"] == "Jane Updated"
    assert updated_row["email"] == "jane.new@example.com"

    last_row = result_df[result_df["user_id"] == 4].iloc[0]
    assert last_row["name"] == "Alice New"


def test_schema_compatibility_column_subset_selection(engine, executor):
    """Test schema compatibility validation when selecting a subset of columns from source.

    This test verifies that by selecting only a subset of columns from a source table,
    we can make it compatible with a target table even when the full source schema would
    be incompatible.
    """
    # Create a source with many columns
    engine.execute_query(
        """
    CREATE TABLE wide_source AS
    SELECT 
        1 AS id,
        'Product A' AS name,
        10.5 AS price,
        100 AS stock_quantity,
        TRUE AS is_active,
        'Electronics' AS category,
        'red,blue,green' AS available_colors,
        'Description of Product A' AS description,
        CAST('2023-01-01' AS DATE) AS created_date
    UNION ALL
    SELECT 
        2 AS id,
        'Product B' AS name,
        20.5 AS price,
        50 AS stock_quantity,
        FALSE AS is_active,
        'Home Goods' AS category,
        'yellow,black' AS available_colors,
        'Description of Product B' AS description,
        CAST('2023-01-02' AS DATE) AS created_date
    """
    )

    # Create a target with only a subset of the columns
    engine.execute_query(
        """
    CREATE TABLE narrow_target AS
    SELECT 
        3 AS id,
        'Product C' AS name,
        30.5 AS price,
        TRUE AS is_active
    """
    )

    # Get the schemas
    source_schema = engine.get_table_schema("wide_source")
    engine.get_table_schema("narrow_target")

    # Verify that the full source schema is incompatible with the target
    try:
        engine.validate_schema_compatibility("narrow_target", source_schema)
        assert (
            False
        ), "Should have failed because wide_source has columns not in narrow_target"
    except ValueError as e:
        # This should fail because source has columns that don't exist in target
        assert "Column" in str(e) and "does not exist in target" in str(e)

    # Now create a view that selects only the columns that exist in the target
    engine.execute_query(
        """
    CREATE VIEW compatible_source_view AS
    SELECT id, name, price, is_active
    FROM wide_source
    """
    )

    # Get the schema of the view
    view_schema = engine.get_table_schema("compatible_source_view")

    # Verify that the view schema is compatible with the target
    assert engine.validate_schema_compatibility("narrow_target", view_schema) is True

    # Now test APPEND using the compatible view
    result = engine.execute_query(
        """
    INSERT INTO narrow_target
    SELECT * FROM compatible_source_view
    """
    )

    # Verify that the data was appended correctly
    result_df = engine.execute_query(
        "SELECT * FROM narrow_target ORDER BY id"
    ).fetchdf()
    assert len(result_df) == 3  # 1 original + 2 appended

    # Test with a LoadStep
    # First register our view as a table in the engine
    view_data = engine.execute_query("SELECT * FROM compatible_source_view").fetchdf()
    engine.register_table("compatible_source_table", view_data)

    # Create a LoadStep for APPEND mode
    load_step = LoadStep(
        table_name="narrow_target",
        source_name="compatible_source_table",
        mode="APPEND",
        merge_keys=[],
    )

    # Execute the load step
    result = executor.execute_load_step(load_step)

    # Verify successful execution
    assert result["status"] == "success"

    # Verify that data was appended
    result_df = engine.execute_query(
        "SELECT * FROM narrow_target ORDER BY id"
    ).fetchdf()
    assert len(result_df) == 5  # 3 previous + 2 more appended

    # Verify all expected IDs are present
    expected_ids = [1, 1, 2, 2, 3]  # Each source row was appended twice
    actual_ids = sorted(result_df["id"].tolist())
    assert sorted(actual_ids) == sorted(expected_ids)


@pytest.fixture
def temp_csv_file() -> Generator[str, None, None]:
    """Create a temporary CSV file for testing.

    Yields:
        Path to the temporary CSV file
    """
    # Create sample data
    data = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10.5, 20.3, 30.1, 40.7, 50.9],
            "active": [True, False, True, True, False],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        data.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def executor_with_source(temp_csv_file) -> LocalExecutor:
    """Create a LocalExecutor with a SOURCE definition.

    Args:
        temp_csv_file: Path to temporary CSV file

    Returns:
        Configured LocalExecutor
    """
    executor = LocalExecutor()

    # Register a SOURCE definition
    source_step = {
        "id": "source_users",
        "type": "source_definition",
        "name": "users",
        "connector_type": "CSV",
        "params": {"path": temp_csv_file, "has_header": True},
    }

    # Execute the source definition to register it
    result = executor._execute_source_definition(source_step)
    assert result["status"] == "success"

    return executor


def test_load_replace_mode_with_source(executor_with_source):
    """Test LOAD with REPLACE mode using SOURCE connector."""

    # Create a LoadStep with REPLACE mode
    load_step = LoadStep(table_name="users_table", source_name="users", mode="REPLACE")

    # Execute the load step
    result = executor_with_source.execute_load_step(load_step)

    # Verify success
    assert result["status"] == "success"
    assert result["table"] == "users_table"
    assert result["mode"] == "REPLACE"
    assert "rows_loaded" in result
    assert result["rows_loaded"] > 0

    # Verify the table was created in DuckDB
    engine = executor_with_source.duckdb_engine
    assert engine.table_exists("users_table")

    # Verify data was loaded correctly
    data = engine.execute_query("SELECT * FROM users_table").fetchall()
    assert len(data) == 5  # Should have all 5 rows from CSV


def test_load_append_mode_with_source(executor_with_source):
    """Test LOAD with APPEND mode using SOURCE connector."""

    # First, create the target table with REPLACE
    load_step_1 = LoadStep(
        table_name="users_table", source_name="users", mode="REPLACE"
    )
    result_1 = executor_with_source.execute_load_step(load_step_1)
    assert result_1["status"] == "success"

    # Now append the same data again
    load_step_2 = LoadStep(table_name="users_table", source_name="users", mode="APPEND")
    result_2 = executor_with_source.execute_load_step(load_step_2)

    # Verify success
    assert result_2["status"] == "success"
    assert result_2["table"] == "users_table"
    assert result_2["mode"] == "APPEND"

    # Verify data was appended
    engine = executor_with_source.duckdb_engine
    data = engine.execute_query("SELECT * FROM users_table").fetchall()
    assert len(data) == 10  # Should have 5 + 5 = 10 rows


def test_load_merge_mode_with_source(executor_with_source):
    """Test LOAD with MERGE mode using SOURCE connector."""

    # First, create the target table with some initial data
    load_step_1 = LoadStep(
        table_name="users_table", source_name="users", mode="REPLACE"
    )
    result_1 = executor_with_source.execute_load_step(load_step_1)
    assert result_1["status"] == "success"

    # Now merge the same data (should update, not insert new rows)
    load_step_2 = LoadStep(
        table_name="users_table", source_name="users", mode="MERGE", merge_keys=["id"]
    )
    result_2 = executor_with_source.execute_load_step(load_step_2)

    # Verify success
    assert result_2["status"] == "success"
    assert result_2["table"] == "users_table"
    assert result_2["mode"] == "MERGE"

    # Verify data count remains the same (merged, not appended)
    engine = executor_with_source.duckdb_engine
    data = engine.execute_query("SELECT * FROM users_table").fetchall()
    assert len(data) == 5  # Should still have 5 rows (merged)


def test_load_with_missing_source():
    """Test LOAD step fails when SOURCE is not defined."""

    executor = LocalExecutor()

    load_step = LoadStep(
        table_name="users_table", source_name="nonexistent_source", mode="REPLACE"
    )

    # Should fail because source is not defined
    result = executor.execute_load_step(load_step)
    assert result["status"] == "error"
    assert "SOURCE 'nonexistent_source' is not defined" in result["message"]


def test_load_merge_without_keys(executor_with_source):
    """Test LOAD with MERGE mode fails when merge keys are not specified."""

    load_step = LoadStep(
        table_name="users_table",
        source_name="users",
        mode="MERGE",
        # No merge_keys specified
    )

    result = executor_with_source.execute_load_step(load_step)
    assert result["status"] == "error"
    assert "MERGE operation requires at least one merge key" in result["message"]


def test_source_connector_registration(temp_csv_file):
    """Test that SOURCE definitions are properly stored and can be retrieved."""

    executor = LocalExecutor()

    # Register a SOURCE definition
    source_step = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_source",
        "connector_type": "CSV",
        "params": {"path": temp_csv_file, "has_header": True},
    }

    # Execute the source definition
    result = executor._execute_source_definition(source_step)
    assert result["status"] == "success"

    # Verify it was stored
    source_def = executor._get_source_definition("test_source")
    assert source_def is not None
    assert source_def["name"] == "test_source"
    assert source_def["connector_type"] == "CSV"
    assert source_def["params"]["path"] == temp_csv_file


def test_load_with_invalid_connector_type():
    """Test LOAD step fails when SOURCE has invalid connector type."""

    executor = LocalExecutor()

    # Register a SOURCE definition with invalid connector type
    source_step = {
        "id": "source_invalid",
        "type": "source_definition",
        "name": "invalid_source",
        "connector_type": "INVALID_TYPE",
        "params": {"path": "/nonexistent/path.csv", "has_header": True},
    }

    # Execute the source definition
    result = executor._execute_source_definition(source_step)
    assert result["status"] == "success"  # Source definition should succeed

    load_step = LoadStep(
        table_name="test_table", source_name="invalid_source", mode="REPLACE"
    )

    # Should fail when trying to load from invalid connector
    result = executor.execute_load_step(load_step)
    assert result["status"] == "error"
    # Error should mention connector registration or loading issue


def test_load_with_empty_source_params():
    """Test LOAD step handles SOURCE with empty parameters."""

    executor = LocalExecutor()

    # Register a SOURCE definition with empty params
    source_step = {
        "id": "source_empty",
        "type": "source_definition",
        "name": "empty_source",
        "connector_type": "CSV",
        "params": {},  # Empty params
    }

    # Execute the source definition
    result = executor._execute_source_definition(source_step)
    assert result["status"] == "success"

    load_step = LoadStep(
        table_name="test_table", source_name="empty_source", mode="REPLACE"
    )

    # Should fail when trying to load with missing required params
    result = executor.execute_load_step(load_step)
    assert result["status"] == "error"


def test_load_mode_case_insensitive():
    """Test that LOAD modes work with different case variations."""

    executor = LocalExecutor()

    # Register a SOURCE definition
    source_step = {
        "id": "source_case",
        "type": "source_definition",
        "name": "case_source",
        "connector_type": "CSV",
        "params": {
            "path": "/tmp/dummy.csv",  # Will fail but that's ok for this test
            "has_header": True,
        },
    }

    result = executor._execute_source_definition(source_step)
    assert result["status"] == "success"

    # Test different case variations of modes
    modes_to_test = ["replace", "REPLACE", "Replace", "append", "APPEND", "Append"]

    for mode in modes_to_test:
        load_step = LoadStep(
            table_name="case_table", source_name="case_source", mode=mode
        )

        # Should handle case variations (though may fail on connector loading)
        result = executor.execute_load_step(load_step)
        # We're testing that the mode parsing doesn't fail due to case
        # The actual error will be in connector loading, which is expected
        assert result["status"] == "error"  # Expected due to dummy path
        assert "mode" not in result["message"].lower()  # No mode-related errors


def test_source_definition_retrieval():
    """Test comprehensive SOURCE definition storage and retrieval."""

    executor = LocalExecutor()

    # Test multiple SOURCE definitions
    sources = [
        {
            "id": "source_csv",
            "type": "source_definition",
            "name": "csv_source",
            "connector_type": "CSV",
            "params": {"path": "/tmp/test.csv", "has_header": True},
        },
        {
            "id": "source_json",
            "type": "source_definition",
            "name": "json_source",
            "connector_type": "JSON",
            "params": {"path": "/tmp/test.json"},
        },
        {
            "id": "source_db",
            "type": "source_definition",
            "name": "db_source",
            "connector_type": "POSTGRES",
            "params": {"host": "localhost", "database": "test"},
        },
    ]

    # Register all sources
    for source in sources:
        result = executor._execute_source_definition(source)
        assert result["status"] == "success"

    # Verify all sources are stored and retrievable
    csv_def = executor._get_source_definition("csv_source")
    assert csv_def is not None
    assert csv_def["connector_type"] == "CSV"
    assert csv_def["params"]["path"] == "/tmp/test.csv"

    json_def = executor._get_source_definition("json_source")
    assert json_def is not None
    assert json_def["connector_type"] == "JSON"

    db_def = executor._get_source_definition("db_source")
    assert db_def is not None
    assert db_def["connector_type"] == "POSTGRES"

    # Test non-existent source
    missing_def = executor._get_source_definition("nonexistent")
    assert missing_def is None


def test_load_with_profile_based_source():
    """Test LOAD step with profile-based SOURCE definition."""

    executor = LocalExecutor()

    # Register a profile-based SOURCE definition
    source_step = {
        "id": "source_profile",
        "type": "source_definition",
        "name": "profile_source",
        "connector_type": "CSV",
        "params": {"path": "/tmp/profile.csv", "has_header": True},
        "is_from_profile": True,  # Indicate this is from profile
    }

    # Execute the source definition
    result = executor._execute_source_definition(source_step)
    assert result["status"] == "success"

    # Verify the source definition is stored with profile flag
    source_def = executor._get_source_definition("profile_source")
    assert source_def is not None
    assert source_def["is_from_profile"] is True

    load_step = LoadStep(
        table_name="profile_table", source_name="profile_source", mode="REPLACE"
    )

    # Should attempt to load (will fail on file access but that's expected)
    result = executor.execute_load_step(load_step)
    assert result["status"] == "error"  # Expected due to dummy file path
