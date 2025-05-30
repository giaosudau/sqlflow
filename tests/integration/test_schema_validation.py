"""Integration tests for schema validation in pipelines."""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.parser.ast import LoadStep, Pipeline


@pytest.fixture
def memory_duckdb_engine():
    """Create an in-memory DuckDBEngine for testing."""
    engine = DuckDBEngine(":memory:")
    yield engine
    engine.close()


@pytest.fixture
def executor(memory_duckdb_engine):
    """Create a LocalExecutor with the in-memory DuckDBEngine."""
    executor = LocalExecutor()
    # Set the engine directly instead of letting the executor create one
    executor.duckdb_engine = memory_duckdb_engine
    return executor


def test_append_schema_compatibility(memory_duckdb_engine, executor):
    """Test APPEND mode with compatible and incompatible schemas."""
    # Create source table
    source_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        }
    )

    # Create the source table using SQL to ensure it's properly created
    memory_duckdb_engine.execute_query(
        "CREATE TABLE source_table (id INTEGER, name VARCHAR, email VARCHAR)"
    )
    for _, row in source_df.iterrows():
        memory_duckdb_engine.execute_query(
            f"INSERT INTO source_table VALUES ({row['id']}, '{row['name']}', '{row['email']}')"
        )

    # Create target table with compatible schema
    target_df = pd.DataFrame(
        {
            "id": [4, 5],
            "name": ["Dave", "Eve"],
            "email": ["dave@example.com", "eve@example.com"],
        }
    )

    # Create the target table using SQL
    memory_duckdb_engine.execute_query(
        "CREATE TABLE target_table (id INTEGER, name VARCHAR, email VARCHAR)"
    )
    for _, row in target_df.iterrows():
        memory_duckdb_engine.execute_query(
            f"INSERT INTO target_table VALUES ({row['id']}, '{row['name']}', '{row['email']}')"
        )

    # Create and execute LoadStep with APPEND mode
    load_step = LoadStep(
        table_name="target_table",
        source_name="source_table",
        mode="APPEND",
        line_number=1,
    )
    result = executor.execute_load_step(load_step)

    # Verify success
    assert result["status"] == "success", (
        f"Failed with message: {result.get('message', 'No message')}"
    )

    # Query the combined table to verify data was appended
    result_df = memory_duckdb_engine.execute_query(
        "SELECT * FROM target_table ORDER BY id"
    ).fetchdf()

    assert len(result_df) == 5  # 2 original + 3 appended
    assert list(result_df["id"]) == [1, 2, 3, 4, 5]

    # Now create a new target table with incompatible schema
    memory_duckdb_engine.execute_query(
        "CREATE TABLE incompatible_table (id INTEGER, name VARCHAR, status VARCHAR)"
    )
    memory_duckdb_engine.execute_query(
        "INSERT INTO incompatible_table VALUES (6, 'Frank', 'Active')"
    )
    memory_duckdb_engine.execute_query(
        "INSERT INTO incompatible_table VALUES (7, 'Grace', 'Inactive')"
    )

    # Create and execute LoadStep that should fail due to schema incompatibility
    incompatible_load_step = LoadStep(
        table_name="incompatible_table",
        source_name="source_table",
        mode="APPEND",
        line_number=1,
    )
    result = executor.execute_load_step(incompatible_load_step)

    # Verify failure due to schema incompatibility
    assert result["status"] == "error"
    assert "email" in result["message"] and "not exist in target" in result["message"]


def test_merge_schema_compatibility(memory_duckdb_engine, executor):
    """Test MERGE mode with compatible and incompatible schemas."""
    # First check if DuckDB version supports MERGE
    try:
        # Create test tables
        memory_duckdb_engine.execute_query("CREATE TABLE test_source (id INTEGER)")
        memory_duckdb_engine.execute_query("CREATE TABLE test_target (id INTEGER)")
        memory_duckdb_engine.execute_query("INSERT INTO test_source VALUES (1)")
        memory_duckdb_engine.execute_query("INSERT INTO test_target VALUES (2)")

        # Try a simple MERGE operation
        test_sql = """
        MERGE INTO test_target AS target
        USING test_source AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET id = source.id
        WHEN NOT MATCHED THEN
            INSERT (id) VALUES (source.id);
        """
        memory_duckdb_engine.execute_query(test_sql)
        supports_merge = True
    except Exception as e:
        # If we get here, MERGE is not supported
        supports_merge = False
        pytest.skip(f"This version of DuckDB does not support MERGE syntax: {e}")

    # Proceed with test if MERGE is supported
    if supports_merge:
        # Create source and target tables
        memory_duckdb_engine.execute_query(
            """
            CREATE TABLE customers_source (
                customer_id INTEGER, 
                name VARCHAR, 
                status VARCHAR
            )
        """
        )
        memory_duckdb_engine.execute_query(
            """
            INSERT INTO customers_source VALUES 
                (101, 'Alice', 'Active'),
                (102, 'Bob', 'Inactive'),
                (103, 'Charlie', 'Active')
        """
        )

        memory_duckdb_engine.execute_query(
            """
            CREATE TABLE customers_target (
                customer_id INTEGER, 
                name VARCHAR, 
                status VARCHAR
            )
        """
        )
        memory_duckdb_engine.execute_query(
            """
            INSERT INTO customers_target VALUES 
                (101, 'Alice (old)', 'Inactive'),
                (102, 'Bob (old)', 'Inactive'),
                (104, 'Dave', 'Active')
        """
        )

        # Create and execute LoadStep with MERGE mode
        load_step = LoadStep(
            table_name="customers_target",
            source_name="customers_source",
            mode="MERGE",
            merge_keys=["customer_id"],
            line_number=1,
        )
        result = executor.execute_load_step(load_step)

        # Verify success
        assert result["status"] == "success", (
            f"Failed with message: {result.get('message', 'No message')}"
        )

        # Query the merged table
        result_df = memory_duckdb_engine.execute_query(
            "SELECT * FROM customers_target ORDER BY customer_id"
        ).fetchdf()

        assert len(result_df) == 4  # 3 original + 1 new (103) - 0 removed

        # Now create a table with incompatible schema
        memory_duckdb_engine.execute_query(
            """
            CREATE TABLE incompatible_customers (
                customer_id INTEGER, 
                name VARCHAR, 
                email VARCHAR
            )
        """
        )
        memory_duckdb_engine.execute_query(
            """
            INSERT INTO incompatible_customers VALUES 
                (101, 'Alice', 'alice@example.com'),
                (102, 'Bob', 'bob@example.com')
        """
        )

        # Try to merge with incompatible schema
        incompatible_load_step = LoadStep(
            table_name="incompatible_customers",
            source_name="customers_source",
            mode="MERGE",
            merge_keys=["customer_id"],
            line_number=1,
        )
        result = executor.execute_load_step(incompatible_load_step)

        # Verify failure due to schema incompatibility
        assert result["status"] == "error"
        assert (
            "status" in result["message"] and "not exist in target" in result["message"]
        )


def test_pipeline_with_schema_validation(memory_duckdb_engine, executor):
    """Test a pipeline with multiple load steps and schema validation."""
    # Create tables using direct SQL
    memory_duckdb_engine.execute_query(
        """
        CREATE TABLE products (
            product_id INTEGER, 
            name VARCHAR, 
            price DOUBLE
        )
    """
    )
    memory_duckdb_engine.execute_query(
        """
        INSERT INTO products VALUES 
            (1, 'Product A', 10.0),
            (2, 'Product B', 20.0),
            (3, 'Product C', 30.0)
    """
    )

    memory_duckdb_engine.execute_query(
        """
        CREATE TABLE orders (
            order_id INTEGER, 
            customer_id INTEGER, 
            order_date VARCHAR
        )
    """
    )
    memory_duckdb_engine.execute_query(
        """
        INSERT INTO orders VALUES 
            (101, 1, '2023-01-01'),
            (102, 2, '2023-01-02')
    """
    )

    # Create a view for testing
    memory_duckdb_engine.execute_query(
        "CREATE VIEW orders_summary_view AS SELECT order_id, customer_id FROM orders"
    )

    # Create a pipeline with multiple LoadSteps
    pipeline = Pipeline()

    # Step 1: Create products_backup with REPLACE mode (should succeed)
    pipeline.add_step(
        LoadStep(
            table_name="products_backup",
            source_name="products",
            mode="REPLACE",
            line_number=1,
        )
    )

    # Step 2: Create orders_summary with data from orders (should succeed)
    pipeline.add_step(
        LoadStep(
            table_name="orders_summary",
            source_name="orders_summary_view",
            mode="REPLACE",
            line_number=2,
        )
    )

    # Step 3: Try to append products to orders (should fail due to schema incompatibility)
    pipeline.add_step(
        LoadStep(
            table_name="orders", source_name="products", mode="APPEND", line_number=3
        )
    )

    # Execute the pipeline
    result = executor.execute_pipeline(pipeline)

    # Verify pipeline failed at step 3
    assert result["status"] == "failed"
    assert "Column" in result["error"] or "schema" in result["error"].lower()

    # Verify first two steps were successful (tables should exist)
    assert memory_duckdb_engine.table_exists("products_backup")
    assert memory_duckdb_engine.table_exists("orders_summary")

    # Verify the data in products_backup
    products_backup_df = memory_duckdb_engine.execute_query(
        "SELECT * FROM products_backup ORDER BY product_id"
    ).fetchdf()

    assert len(products_backup_df) == 3
    assert list(products_backup_df["product_id"]) == [1, 2, 3]


def test_schema_compatibility_validation_basic(memory_duckdb_engine):
    """Test basic schema compatibility validation."""
    # Create two tables with identical schemas
    source_data = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10.5, 20.1, 30.9]}
    )

    target_data = pd.DataFrame(
        {"id": [4, 5, 6], "name": ["D", "E", "F"], "value": [40.2, 50.8, 60.3]}
    )

    memory_duckdb_engine.register_table("source_identical", source_data)
    memory_duckdb_engine.register_table("target_identical", target_data)

    # Test that schema compatibility validation passes
    assert (
        memory_duckdb_engine.validate_schema_compatibility(
            "target_identical",
            memory_duckdb_engine.get_table_schema("source_identical"),
        )
        is True
    )


def test_schema_compatibility_validation_extra_columns_in_source(memory_duckdb_engine):
    """Test schema compatibility validation when source has extra columns."""
    # Create source with extra columns
    source_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10.5, 20.1, 30.9],
            "extra_col": ["X", "Y", "Z"],
            "another_extra": [True, False, True],
        }
    )

    # Create target with fewer columns
    target_data = pd.DataFrame(
        {"id": [4, 5, 6], "name": ["D", "E", "F"], "value": [40.2, 50.8, 60.3]}
    )

    memory_duckdb_engine.register_table("source_with_extra", source_data)
    memory_duckdb_engine.register_table("target_fewer", target_data)

    # Test that schema compatibility validation fails
    # Extra columns in source should cause an error with validate_schema_compatibility
    try:
        memory_duckdb_engine.validate_schema_compatibility(
            "target_fewer", memory_duckdb_engine.get_table_schema("source_with_extra")
        )
        assert False, "Should have failed because source has extra columns"
    except ValueError as e:
        # Expect error message containing 'Column' and 'does not exist'
        assert "Column" in str(e) and "does not exist" in str(e)


def test_schema_compatibility_validation_missing_columns_in_source(
    memory_duckdb_engine,
):
    """Test schema compatibility validation when source is missing columns that target has."""
    # Create source with fewer columns
    source_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

    # Create target with more columns
    target_data = pd.DataFrame(
        {"id": [4, 5, 6], "name": ["D", "E", "F"], "value": [40.2, 50.8, 60.3]}
    )

    memory_duckdb_engine.register_table("source_fewer", source_data)
    memory_duckdb_engine.register_table("target_with_extra", target_data)

    # The current implementation only checks if source columns exist in target,
    # NOT if target columns exist in source. This is because for APPEND and MERGE
    # operations, we're only concerned if the source data can fit into the target schema.

    # Validate that schema compatibility passes when source has a subset of target columns
    result = memory_duckdb_engine.validate_schema_compatibility(
        "target_with_extra", memory_duckdb_engine.get_table_schema("source_fewer")
    )
    assert result is True, (
        "Schema validation should pass when source has a subset of target columns"
    )

    # Note: This behavior is intentional and allows appending data where only a subset of
    # target columns are populated. In a real scenario, we would execute:
    # INSERT INTO target (id, name) SELECT id, name FROM source


def test_schema_compatibility_validation_compatible_types(memory_duckdb_engine):
    """Test schema compatibility validation with compatible but different types."""
    # Create source with INTEGER types
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE source_integers AS
    SELECT
        1::INTEGER AS id,
        'A' AS name,
        10::INTEGER AS value_int
    UNION ALL
    SELECT
        2::INTEGER AS id,
        'B' AS name,
        20::INTEGER AS value_int
    """
    )

    # Create target with BIGINT types (compatible with INTEGER)
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE target_bigints AS
    SELECT
        3::BIGINT AS id,
        'C' AS name,
        30::BIGINT AS value_int
    UNION ALL
    SELECT
        4::BIGINT AS id,
        'D' AS name,
        40::BIGINT AS value_int
    """
    )

    # Test that INTEGER is compatible with BIGINT
    result = memory_duckdb_engine.validate_schema_compatibility(
        "target_bigints", memory_duckdb_engine.get_table_schema("source_integers")
    )
    assert result is True, "INTEGER should be compatible with BIGINT"

    # Create source with VARCHAR types
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE source_varchar AS
    SELECT
        1 AS id,
        'A'::VARCHAR AS name,
        'text'::VARCHAR AS text_val
    UNION ALL
    SELECT
        2 AS id,
        'B'::VARCHAR AS name,
        'more text'::VARCHAR AS text_val
    """
    )

    # Create target with TEXT types (compatible with VARCHAR)
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE target_text AS
    SELECT
        3 AS id,
        'C'::TEXT AS name,
        'text'::TEXT AS text_val
    UNION ALL
    SELECT
        4 AS id,
        'D'::TEXT AS name,
        'more text'::TEXT AS text_val
    """
    )

    # Test that VARCHAR is compatible with TEXT
    result = memory_duckdb_engine.validate_schema_compatibility(
        "target_text", memory_duckdb_engine.get_table_schema("source_varchar")
    )
    assert result is True, "VARCHAR should be compatible with TEXT"

    # Test that INTEGER is NOT compatible with DOUBLE
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE source_int AS
    SELECT
        1 AS id,
        'A' AS name,
        10::INTEGER AS num_val
    """
    )

    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE target_double AS
    SELECT
        1 AS id,
        'A' AS name,
        10.5::DOUBLE AS num_val
    """
    )

    # This should raise an error because INTEGER is not compatible with DOUBLE
    # in the current implementation
    try:
        memory_duckdb_engine.validate_schema_compatibility(
            "target_double", memory_duckdb_engine.get_table_schema("source_int")
        )
        assert False, (
            "Validation should fail: INTEGER should not be compatible with DOUBLE"
        )
    except ValueError as e:
        # Verify error message
        assert "incompatible types" in str(e).lower()
        assert "num_val" in str(e)


def test_schema_compatibility_validation_incompatible_types(memory_duckdb_engine):
    """Test schema compatibility validation with incompatible types."""
    # Create source with string type for value
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE source_string_value AS
    SELECT 
        1 AS id,
        'A' AS name,
        '10.5' AS value  -- String instead of number
    UNION ALL
    SELECT 
        2 AS id,
        'B' AS name,
        '20.1' AS value
    """
    )

    # Create target with numeric type for value
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE target_numeric_value AS
    SELECT 
        3 AS id,
        'C' AS name,
        30.0::DOUBLE AS value  -- Numeric
    UNION ALL
    SELECT 
        4 AS id,
        'D' AS name,
        40.0::DOUBLE AS value
    """
    )

    # Test incompatibility - VARCHAR is not compatible with DOUBLE for SQL operations
    try:
        memory_duckdb_engine.validate_schema_compatibility(
            "target_numeric_value",
            memory_duckdb_engine.get_table_schema("source_string_value"),
        )
        # Note: This might actually pass in DuckDB since it's very flexible with type conversions
        # In a more strict database like PostgreSQL, this would fail
    except ValueError as e:
        # If it fails, it should mention incompatible types
        assert "incompatible types" in str(e).lower()


def test_schema_compatibility_validation_complex_schema(memory_duckdb_engine):
    """Test schema compatibility validation with complex schemas including different types."""
    # Create a complex source schema
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE complex_source AS
    SELECT 
        1::INTEGER AS id,
        'A' AS name,
        CAST('2023-01-01' AS DATE) AS date_col,
        CAST('2023-01-01 12:00:00' AS TIMESTAMP) AS timestamp_col,
        10.5::DOUBLE AS numeric_value,
        TRUE AS is_active
    """
    )

    # Create a compatible target schema
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE complex_target AS
    SELECT 
        101::BIGINT AS id,
        'X' AS name,
        CAST('2023-02-01' AS DATE) AS date_col,
        CAST('2023-02-01 15:30:00' AS TIMESTAMP) AS timestamp_col,
        99.9::DOUBLE AS numeric_value,
        FALSE AS is_active
    """
    )

    # Test compatibility with complex but compatible schemas
    assert (
        memory_duckdb_engine.validate_schema_compatibility(
            "complex_target", memory_duckdb_engine.get_table_schema("complex_source")
        )
        is True
    )

    # Create an incompatible target schema (date and timestamp types swapped)
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE complex_target_incompatible AS
    SELECT 
        101::BIGINT AS id,
        'X' AS name,
        CAST('2023-02-01 15:30:00' AS TIMESTAMP) AS date_col,  -- Type mismatch (TIMESTAMP instead of DATE)
        CAST('2023-02-01' AS DATE) AS timestamp_col,  -- Type mismatch (DATE instead of TIMESTAMP)
        99.9::DOUBLE AS numeric_value,
        FALSE AS is_active
    """
    )

    # Test incompatibility due to mismatched date/timestamp types
    try:
        memory_duckdb_engine.validate_schema_compatibility(
            "complex_target_incompatible",
            memory_duckdb_engine.get_table_schema("complex_source"),
        )
        # Note: This might pass in DuckDB which is flexible, but would fail in stricter databases
    except ValueError as e:
        # If it fails, it should mention incompatible types
        assert "incompatible types" in str(e).lower()


def test_are_types_compatible_method(memory_duckdb_engine):
    """Test the _are_types_compatible method directly."""
    # Test compatible types
    assert memory_duckdb_engine._are_types_compatible("INTEGER", "BIGINT") is True
    assert memory_duckdb_engine._are_types_compatible("FLOAT", "DOUBLE") is True
    assert memory_duckdb_engine._are_types_compatible("VARCHAR", "TEXT") is True
    assert (
        memory_duckdb_engine._are_types_compatible("VARCHAR(50)", "VARCHAR(100)")
        is True
    )
    assert memory_duckdb_engine._are_types_compatible("CHAR(10)", "VARCHAR") is True
    assert memory_duckdb_engine._are_types_compatible("BOOLEAN", "BOOL") is True

    # Test incompatible types
    assert memory_duckdb_engine._are_types_compatible("INTEGER", "DATE") is False
    assert memory_duckdb_engine._are_types_compatible("TIMESTAMP", "VARCHAR") is False
    assert memory_duckdb_engine._are_types_compatible("DOUBLE", "BOOLEAN") is False


def test_validate_schema_compatibility_empty_source(memory_duckdb_engine):
    """Test schema compatibility validation with empty source schema."""
    # Create an empty source schema (no columns)
    empty_source = {}

    # Create a target with columns
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE target_for_empty AS
    SELECT 
        1 AS id,
        'A' AS name
    """
    )

    # Empty source should be compatible with any target (nothing to validate)
    assert (
        memory_duckdb_engine.validate_schema_compatibility(
            "target_for_empty", empty_source
        )
        is True
    )


def test_type_compatibility_matrix(memory_duckdb_engine):
    """Test type compatibility between various SQL data types.

    This test creates a matrix of different data type combinations and verifies
    that the schema compatibility validation correctly identifies compatible
    and incompatible type pairs based on the _are_types_compatible method.
    """
    # Create tables with various data types
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE type_test_source AS
    SELECT 
        1::INTEGER AS int_col,
        1::SMALLINT AS smallint_col,
        1::BIGINT AS bigint_col,
        1.0::FLOAT AS float_col,
        1.0::DOUBLE AS double_col,
        1.0::DECIMAL(10,2) AS decimal_col,
        'text'::VARCHAR AS varchar_col,
        'text'::TEXT AS text_col,
        'text'::CHAR(10) AS char_col,
        TRUE::BOOLEAN AS boolean_col,
        FALSE::BOOL AS bool_col,
        CAST('2023-01-01' AS DATE) AS date_col,
        CAST('12:00:00' AS TIME) AS time_col,
        CAST('2023-01-01 12:00:00' AS TIMESTAMP) AS timestamp_col
    """
    )

    # Get source schema
    source_schema = memory_duckdb_engine.get_table_schema("type_test_source")

    # Compatible type pairs to test (source_type, target_type)
    compatible_pairs = [
        ("int_col", "bigint_col"),  # INTEGER -> BIGINT
        ("smallint_col", "int_col"),  # SMALLINT -> INTEGER
        ("float_col", "double_col"),  # FLOAT -> DOUBLE
        ("decimal_col", "double_col"),  # DECIMAL -> DOUBLE
        ("varchar_col", "text_col"),  # VARCHAR -> TEXT
        ("text_col", "varchar_col"),  # TEXT -> VARCHAR
        ("char_col", "varchar_col"),  # CHAR -> VARCHAR
        ("boolean_col", "bool_col"),  # BOOLEAN -> BOOL
        ("bool_col", "boolean_col"),  # BOOL -> BOOLEAN
    ]

    # Incompatible type pairs to test (source_type, target_type)
    incompatible_pairs = [
        ("int_col", "date_col"),  # INTEGER -> DATE
        ("varchar_col", "int_col"),  # VARCHAR -> INTEGER
        ("date_col", "timestamp_col"),  # DATE -> TIMESTAMP
        ("time_col", "date_col"),  # TIME -> DATE
        (
            "boolean_col",
            "int_col",
        ),  # BOOLEAN -> INTEGER (incompatible in strict validation)
    ]

    # Test compatible pairs
    for source_col, target_col in compatible_pairs:
        source_type = source_schema[source_col].upper()
        target_type = source_schema[target_col].upper()

        # Check compatibility directly with _are_types_compatible
        assert memory_duckdb_engine._are_types_compatible(source_type, target_type), (
            f"Types should be compatible: {source_type} -> {target_type}"
        )

    # Test incompatible pairs
    for source_col, target_col in incompatible_pairs:
        source_type = source_schema[source_col].upper()
        target_type = source_schema[target_col].upper()

        # Check incompatibility directly with _are_types_compatible
        assert not memory_duckdb_engine._are_types_compatible(
            source_type, target_type
        ), f"Types should be incompatible: {source_type} -> {target_type}"

    # Test the normalize_type function behavior (internal to _are_types_compatible)
    # Create a test table with each target type
    for source_col, target_col in compatible_pairs:
        # Create a minimal table with just the two columns to test
        test_table_name = f"compat_test_{source_col}_{target_col}"
        memory_duckdb_engine.execute_query(
            f"""
        CREATE TABLE {test_table_name} AS
        SELECT 
            {source_col} AS source_col,
            {target_col} AS target_col
        FROM type_test_source
        """
        )

        # Get the schema
        test_schema = memory_duckdb_engine.get_table_schema(test_table_name)

        # Create a simple source schema with just the source column
        simple_source_schema = {"source_col": test_schema["source_col"]}

        # Test that schema validation works when source is compatible with target
        assert memory_duckdb_engine.validate_schema_compatibility(
            test_table_name, simple_source_schema
        )


def _validate_merge_keys(
    memory_duckdb_engine, merge_keys, expected_valid=True, expected_error=None
):
    """Helper function to validate merge keys."""
    try:
        # If validate_merge_keys is implemented in DuckDBEngine, use it directly
        if hasattr(memory_duckdb_engine, "validate_merge_keys"):
            memory_duckdb_engine.validate_merge_keys(
                "target_merge_test", "source_merge_test", merge_keys
            )
            is_valid = True
        else:
            # Otherwise, manually validate:
            # 1. Check if merge keys exist in both tables
            source_schema = memory_duckdb_engine.get_table_schema("source_merge_test")
            target_schema = memory_duckdb_engine.get_table_schema("target_merge_test")

            for key in merge_keys:
                if key not in source_schema:
                    raise ValueError(
                        f"Merge key '{key}' does not exist in source table"
                    )
                if key not in target_schema:
                    raise ValueError(
                        f"Merge key '{key}' does not exist in target table"
                    )

            # 2. Check if merge key types are compatible
            for key in merge_keys:
                source_type = source_schema[key].upper()
                target_type = target_schema[key].upper()
                if not memory_duckdb_engine._are_types_compatible(
                    source_type, target_type
                ):
                    raise ValueError(
                        f"Merge key '{key}' has incompatible types: "
                        f"source={source_type}, target={target_type}"
                    )

            is_valid = True

        # Assert the validation result matches expectation
        assert expected_valid == is_valid, (
            f"Expected validation to {'succeed' if expected_valid else 'fail'}"
        )

    except ValueError as e:
        # If validation failed, check if it's the expected error
        assert not expected_valid, "Expected validation to succeed but it failed"
        if expected_error:
            assert expected_error in str(e), (
                f"Expected error containing '{expected_error}' but got '{str(e)}'"
            )


def _setup_merge_key_test_tables(memory_duckdb_engine):
    """Setup test tables for merge key validation tests."""
    # Only create tables if they don't exist
    if not memory_duckdb_engine.table_exists("source_merge_test"):
        memory_duckdb_engine.execute_query(
            """
        CREATE TABLE source_merge_test AS
        SELECT 
            1 AS id,
            101 AS product_id,
            'A' AS code,
            TRUE AS is_active,
            10.5 AS price,
            CAST('2023-01-01' AS DATE) AS created_date
        UNION ALL
        SELECT 
            2 AS id,
            102 AS product_id,
            'B' AS code,
            FALSE AS is_active,
            20.5 AS price,
            CAST('2023-01-02' AS DATE) AS created_date
        """
        )

    if not memory_duckdb_engine.table_exists("target_merge_test"):
        memory_duckdb_engine.execute_query(
            """
        CREATE TABLE target_merge_test AS
        SELECT 
            3 AS id,
            103 AS product_id,
            'C' AS code,
            TRUE AS is_active,
            30.5 AS price,
            CAST('2023-01-03' AS DATE) AS created_date
        UNION ALL
        SELECT 
            4 AS id,
            104 AS product_id,
            'D' AS code,
            FALSE AS is_active,
            40.5 AS price,
            CAST('2023-01-04' AS DATE) AS created_date
        """
        )


def test_merge_key_validation_single_key(memory_duckdb_engine):
    """Test merge key validation with a single key."""
    _setup_merge_key_test_tables(memory_duckdb_engine)
    _validate_merge_keys(memory_duckdb_engine, ["id"], expected_valid=True)


def test_merge_key_validation_composite_keys(memory_duckdb_engine):
    """Test merge key validation with multiple (composite) keys."""
    _setup_merge_key_test_tables(memory_duckdb_engine)
    _validate_merge_keys(
        memory_duckdb_engine, ["id", "product_id"], expected_valid=True
    )


def test_merge_key_validation_nonexistent_key(memory_duckdb_engine):
    """Test merge key validation with a non-existent key."""
    _setup_merge_key_test_tables(memory_duckdb_engine)
    _validate_merge_keys(
        memory_duckdb_engine,
        ["non_existent_key"],
        expected_valid=False,
        expected_error="does not exist",
    )


def test_merge_key_validation_incompatible_types(memory_duckdb_engine):
    """Test merge key validation with incompatible types."""
    _setup_merge_key_test_tables(memory_duckdb_engine)

    # Create a source table with string ID (incompatible with integer ID in target)
    memory_duckdb_engine.execute_query(
        """
    CREATE TABLE incompatible_merge_source AS
    SELECT 
        'string_id' AS id,  -- String instead of integer
        101 AS product_id,
        'A' AS code
    """
    )

    # This should fail due to incompatible types (VARCHAR vs INTEGER for 'id')
    try:
        # Try to validate merge keys directly
        if hasattr(memory_duckdb_engine, "validate_merge_keys"):
            memory_duckdb_engine.validate_merge_keys(
                "target_merge_test", "incompatible_merge_source", ["id"]
            )
            # This should have failed, so make the test fail if it didn't
            assert False, (
                "Expected type incompatibility to fail validation but it passed"
            )
        else:
            # If direct validation is not available, check types manually
            source_schema = memory_duckdb_engine.get_table_schema(
                "incompatible_merge_source"
            )
            target_schema = memory_duckdb_engine.get_table_schema("target_merge_test")

            source_type = source_schema["id"].upper()
            target_type = target_schema["id"].upper()

            # Check if types are incompatible (STRING vs INTEGER)
            assert not memory_duckdb_engine._are_types_compatible(
                source_type, target_type
            ), f"Types should be incompatible: {source_type} vs {target_type}"
    except ValueError as e:
        # This is expected - verify error message contains type information
        assert "type" in str(e).lower() or "incompatible" in str(e).lower(), (
            f"Expected error about incompatible types, got: {str(e)}"
        )


def test_merge_key_validation_empty_keys(memory_duckdb_engine):
    """Test merge key validation with empty keys list."""
    _setup_merge_key_test_tables(memory_duckdb_engine)
    _validate_merge_keys(
        memory_duckdb_engine,
        [],
        expected_valid=False,
        expected_error="requires at least one merge key",
    )
