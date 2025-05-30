"""Integration tests for database persistence functionality.

These tests verify that SQLFlow correctly handles persistent database operations,
including data persistence across sessions and proper cleanup.
"""

import os
import tempfile
from pathlib import Path

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.parser import SQLFlowParser


@pytest.mark.skip(
    reason="Profile configuration for persistent mode needs stabilization - part of test refactoring"
)
def test_persistent_database_creation(temp_persistent_project, persistent_executor):
    """Test that persistent database files are created correctly.

    This verifies the basic functionality that users expect: when using
    persistent mode, the database file should be created and data should
    persist across executor instances.
    """
    # Verify database path is set correctly
    db_path = temp_persistent_project["db_path"]
    assert persistent_executor.database_path != ":memory:"
    assert persistent_executor.duckdb_mode == "persistent"

    # Execute a simple operation to create the database
    plan = [
        {
            "type": "transform",
            "id": "create_test_table",
            "name": "test_table",
            "query": "SELECT 1 as test_value, 'persistent' as mode",
        }
    ]

    result = persistent_executor.execute(plan)
    assert result["status"] == "success"

    # Verify database file was created
    assert db_path.exists()

    # Verify file has some content (not empty)
    assert db_path.stat().st_size > 0


@pytest.mark.skip(
    reason="Profile configuration for persistent mode needs stabilization - part of test refactoring"
)
def test_data_persistence_across_sessions(
    temp_persistent_project, sample_persistent_data
):
    """Test that data persists across different executor sessions.

    This is the core persistence behavior: data created in one session
    should be available in subsequent sessions using the same database.
    """
    db_path = temp_persistent_project["db_path"]

    # First session: Create and populate data
    executor1 = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    # Execute SQL to create persistent data
    plan1 = [
        {
            "type": "transform",
            "id": "setup_customers",
            "name": "customers",
            "query": """
                CREATE TABLE customers AS
                SELECT 1 as customer_id, 'Alice Johnson' as name, 'alice@example.com' as email
                UNION ALL
                SELECT 2 as customer_id, 'Bob Smith' as name, 'bob@example.com' as email
            """,
        }
    ]

    result1 = executor1.execute(plan1)
    assert result1["status"] == "success"

    # Close first session
    if hasattr(executor1, "duckdb_engine") and executor1.duckdb_engine:
        executor1.duckdb_engine.close()

    # Second session: Verify data persists
    executor2 = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    # Query the data that should have persisted
    plan2 = [
        {
            "type": "transform",
            "id": "verify_persistence",
            "name": "customer_count",
            "query": "SELECT COUNT(*) as count FROM customers",
        }
    ]

    result2 = executor2.execute(plan2)

    # Should be able to access data from previous session
    assert result2["status"] == "success"

    # Clean up
    if hasattr(executor2, "duckdb_engine") and executor2.duckdb_engine:
        executor2.duckdb_engine.close()


@pytest.mark.skip(
    reason="Profile configuration for persistent mode needs stabilization - part of test refactoring"
)
def test_persistent_vs_memory_mode_behavior(temp_persistent_project):
    """Test the behavioral difference between persistent and memory modes.

    This test demonstrates to users the key difference: memory mode data
    disappears, while persistent mode data remains available.
    """
    # Test memory mode executor
    memory_executor = LocalExecutor()  # Default is memory mode

    memory_plan = [
        {
            "type": "transform",
            "id": "memory_data",
            "name": "temp_table",
            "query": "SELECT 'memory_data' as source, 1 as value",
        }
    ]

    memory_result = memory_executor.execute(memory_plan)
    assert memory_result["status"] == "success"

    # Test persistent mode executor
    persistent_executor = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    persistent_plan = [
        {
            "type": "transform",
            "id": "persistent_data",
            "name": "persistent_table",
            "query": "SELECT 'persistent_data' as source, 1 as value",
        }
    ]

    persistent_result = persistent_executor.execute(persistent_plan)
    assert persistent_result["status"] == "success"

    # Verify database file exists for persistent mode
    db_path = temp_persistent_project["db_path"]
    assert db_path.exists()

    # Verify file has content
    assert db_path.stat().st_size > 0


@pytest.mark.skip(
    reason="Profile configuration for persistent mode needs stabilization - part of test refactoring"
)
def test_concurrent_access_to_persistent_database(temp_persistent_project):
    """Test that multiple executors can access the same persistent database.

    This tests a common scenario where multiple processes or pipeline runs
    might need to access the same persistent database concurrently.
    """
    # Create first executor and add some data
    executor1 = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    plan1 = [
        {
            "type": "transform",
            "id": "create_shared_data",
            "name": "shared_table",
            "query": "SELECT 'executor1' as source, CURRENT_TIMESTAMP as created_at",
        }
    ]

    result1 = executor1.execute(plan1)
    assert result1["status"] == "success"

    # Create second executor that accesses the same database
    executor2 = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    plan2 = [
        {
            "type": "transform",
            "id": "read_shared_data",
            "name": "data_check",
            "query": "SELECT COUNT(*) as record_count FROM shared_table",
        }
    ]

    result2 = executor2.execute(plan2)
    assert result2["status"] == "success"

    # Should be able to read data created by first executor
    data_check = executor2.table_data["data_check"]
    assert len(data_check) == 1
    assert data_check.iloc[0]["record_count"] == 1


@pytest.mark.skip(
    reason="Profile configuration for persistent mode needs stabilization - part of test refactoring"
)
def test_persistence_with_complex_pipeline(temp_persistent_project):
    """Test persistence with a complex multi-step pipeline.

    This tests that persistence works correctly with real-world pipelines
    that have multiple steps, dependencies, and transformations.
    """
    # Create sample data file
    data_file = temp_persistent_project["data_dir"] / "orders.csv"
    data_content = """order_id,customer_id,amount,status
1,1,100.50,completed
2,1,250.00,pending
3,2,75.25,completed
4,3,320.75,completed"""
    data_file.write_text(data_content)

    # Create executor
    executor = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    # Complex pipeline with multiple steps
    complex_plan = [
        {
            "type": "load",
            "id": "load_orders",
            "table_name": "orders",
            "mode": "REPLACE",
            "query": {"source_name": "orders_source"},
        },
        {
            "type": "transform",
            "id": "create_customer_summary",
            "name": "customer_summary",
            "query": """
                SELECT 
                    customer_id,
                    COUNT(*) as order_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM orders 
                WHERE status = 'completed'
                GROUP BY customer_id
            """,
        },
        {
            "type": "transform",
            "id": "create_metrics",
            "name": "business_metrics",
            "query": """
                SELECT 
                    'total_customers' as metric_name,
                    COUNT(*) as metric_value
                FROM customer_summary
                UNION ALL
                SELECT 
                    'total_revenue' as metric_name,
                    SUM(total_amount) as metric_value
                FROM customer_summary
            """,
        },
    ]

    result = executor.execute(complex_plan)

    # Pipeline should execute successfully
    assert result["status"] == "success"

    # Verify that tables were created and persisted
    verification_plan = [
        {
            "type": "transform",
            "id": "verify_tables",
            "name": "table_verification",
            "query": """
                SELECT 
                    'orders' as table_name, COUNT(*) as record_count 
                FROM orders
                UNION ALL
                SELECT 
                    'customer_summary' as table_name, COUNT(*) as record_count
                FROM customer_summary  
                UNION ALL
                SELECT
                    'business_metrics' as table_name, COUNT(*) as record_count
                FROM business_metrics
            """,
        }
    ]

    verification_result = executor.execute(verification_plan)
    assert verification_result["status"] == "success"

    # Cleanup
    if hasattr(executor, "duckdb_engine") and executor.duckdb_engine:
        executor.duckdb_engine.close()


def test_persistence_error_handling(temp_persistent_project):
    """Test error handling in persistent mode.

    Ensures that errors in persistent mode are handled gracefully
    and don't corrupt the database or leave it in an unusable state.
    """
    executor = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )

    # First, create some valid data
    valid_plan = [
        {
            "type": "transform",
            "id": "create_valid_data",
            "name": "valid_table",
            "query": "SELECT 1 as id, 'valid' as status",
        }
    ]

    result1 = executor.execute(valid_plan)
    assert result1["status"] == "success"

    # Now try to execute an invalid query
    invalid_plan = [
        {
            "type": "transform",
            "id": "create_invalid_data",
            "name": "invalid_table",
            "query": "SELECT * FROM non_existent_table",  # This should fail
        }
    ]

    result2 = executor.execute(invalid_plan)
    # Should fail gracefully
    assert result2["status"] == "failed"

    # Verify that the database is still usable after the error
    recovery_plan = [
        {
            "type": "transform",
            "id": "verify_recovery",
            "name": "recovery_check",
            "query": "SELECT COUNT(*) as count FROM valid_table",
        }
    ]

    result3 = executor.execute(recovery_plan)
    assert result3["status"] == "success"

    # Database should still be accessible and contain original data
    # Cleanup
    if hasattr(executor, "duckdb_engine") and executor.duckdb_engine:
        executor.duckdb_engine.close()
