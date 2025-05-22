"""Integration tests for schema validation in pipelines."""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
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
    assert (
        result["status"] == "success"
    ), f"Failed with message: {result.get('message', 'No message')}"

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
        assert (
            result["status"] == "success"
        ), f"Failed with message: {result.get('message', 'No message')}"

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
