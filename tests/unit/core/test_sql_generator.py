"""Tests for the SQLGenerator class."""

from sqlflow.core.sql_generator import SQLGenerator


def test_sql_generator_source_csv():
    """Test SQL generation for a CSV source operation."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_source",
        "source_connector_type": "CSV",
        "query": {"path": "data/test.csv", "has_header": True},
        "depends_on": [],
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: source_test" in sql
    assert "-- Dependencies: " in sql
    assert "-- Source type: CSV" in sql
    assert "CREATE OR REPLACE TABLE test_source AS" in sql
    assert "SELECT * FROM read_csv_auto('data/test.csv'" in sql
    assert "header=true" in sql


def test_sql_generator_transform_table():
    """Test SQL generation for a transform operation with TABLE materialization."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "transform_test",
        "type": "transform",
        "name": "test_transform",
        "materialized": "table",
        "query": "SELECT * FROM source",
        "depends_on": ["source_test"],
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: transform_test" in sql
    assert "-- Dependencies: source_test" in sql
    assert "-- Materialization: TABLE" in sql
    assert "CREATE OR REPLACE TABLE test_transform AS" in sql
    assert "SELECT * FROM source" in sql
    assert "ANALYZE test_transform" in sql


def test_sql_generator_transform_view():
    """Test SQL generation for a transform operation with VIEW materialization."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "view_test",
        "type": "transform",
        "name": "test_view",
        "materialized": "view",
        "query": "SELECT * FROM source WHERE value > 100",
        "depends_on": ["source_test"],
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: view_test" in sql
    assert "-- Dependencies: source_test" in sql
    assert "-- Materialization: VIEW" in sql
    assert "CREATE OR REPLACE VIEW test_view AS" in sql
    assert "SELECT * FROM source WHERE value > 100" in sql
    assert "ANALYZE" not in sql  # No ANALYZE for views


def test_sql_generator_load():
    """Test SQL generation for a load operation."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "load_test",
        "type": "load",
        "name": "load_data",
        "query": {"source_name": "source_table", "table_name": "destination_table"},
        "depends_on": ["source_definition"],
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: load_test" in sql
    assert "-- Dependencies: source_definition" in sql
    assert "-- Load operation" in sql
    assert "CREATE OR REPLACE TABLE destination_table AS" in sql
    assert "SELECT * FROM source_table" in sql


def test_sql_generator_export_csv():
    """Test SQL generation for a CSV export operation."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "export_test",
        "type": "export",
        "query": {
            "query": "SELECT * FROM transformed_data",
            "destination_uri": "output/results.csv",
            "type": "CSV",
        },
        "depends_on": ["transform_test"],
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: export_test" in sql
    assert "-- Dependencies: transform_test" in sql
    assert "-- Export to CSV" in sql
    assert "COPY (" in sql
    assert "SELECT * FROM transformed_data" in sql
    assert ") TO 'output/results.csv' (FORMAT CSV, HEADER)" in sql


def test_multiple_dependencies():
    """Test SQL generation with multiple dependencies."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "join_tables",
        "type": "transform",
        "name": "customer_orders",
        "materialized": "table",
        "query": "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id",
        "depends_on": ["source_orders", "source_customers"],
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: join_tables" in sql
    assert "-- Dependencies: source_orders, source_customers" in sql


def test_sql_generator_transform_explicit_no_replace():
    """Test SQL generation for transform with explicit is_replace=False for backward compatibility."""
    # Arrange
    generator = SQLGenerator()
    operation = {
        "id": "transform_no_replace",
        "type": "transform",
        "name": "test_transform",
        "materialized": "table",
        "query": "SELECT * FROM source",
        "depends_on": ["source_test"],
        "is_replace": False,
    }
    context = {"variables": {}}

    # Act
    sql = generator.generate_operation_sql(operation, context)

    # Assert
    assert "-- Operation: transform_no_replace" in sql
    assert "-- Dependencies: source_test" in sql
    assert "-- Materialization: TABLE" in sql
    assert "CREATE TABLE test_transform AS" in sql
    assert "CREATE OR REPLACE" not in sql
    assert "SELECT * FROM source" in sql
    assert "ANALYZE test_transform" in sql


def test_create_or_replace_consistency_across_operations():
    """Test that all table-creating operations consistently use CREATE OR REPLACE by default."""
    generator = SQLGenerator()
    context = {"variables": {}}

    # Test SOURCE operation
    source_operation = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_source",
        "source_connector_type": "CSV",
        "query": {"path": "data/test.csv", "has_header": True},
        "depends_on": [],
    }
    source_sql = generator.generate_operation_sql(source_operation, context)
    assert "CREATE OR REPLACE TABLE test_source AS" in source_sql

    # Test LOAD operation
    load_operation = {
        "id": "load_test",
        "type": "load",
        "name": "load_data",
        "query": {"source_name": "source_table", "table_name": "destination_table"},
        "depends_on": ["source_definition"],
    }
    load_sql = generator.generate_operation_sql(load_operation, context)
    assert "CREATE OR REPLACE TABLE destination_table AS" in load_sql

    # Test TRANSFORM operation (default behavior)
    transform_operation = {
        "id": "transform_test",
        "type": "transform",
        "name": "test_transform",
        "materialized": "table",
        "query": "SELECT * FROM source",
        "depends_on": ["source_test"],
        # No is_replace specified - should default to True
    }
    transform_sql = generator.generate_operation_sql(transform_operation, context)
    assert "CREATE OR REPLACE TABLE test_transform AS" in transform_sql

    # Test TRANSFORM with VIEW materialization
    view_operation = {
        "id": "view_test",
        "type": "transform",
        "name": "test_view",
        "materialized": "view",
        "query": "SELECT * FROM source WHERE value > 100",
        "depends_on": ["source_test"],
    }
    view_sql = generator.generate_operation_sql(view_operation, context)
    assert "CREATE OR REPLACE VIEW test_view AS" in view_sql

    print(
        "✅ All table-creating operations consistently use CREATE OR REPLACE by default"
    )
    print("✅ This ensures idempotency - pipelines can be re-run safely")
    print("✅ Consistency maintained across sources, loads, transforms, and views")
