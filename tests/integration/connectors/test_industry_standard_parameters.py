"""Integration tests for industry-standard SOURCE parameters.

These tests verify the complete functionality of sync_mode, primary_key, and cursor_field
parameters across parsing, validation, and execution.

Follows testing standards:
- Real implementations, no mocking
- End-to-end scenarios
- Tests as documentation
- Real data flows
"""

import os
import tempfile
from pathlib import Path

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.parser.ast import LoadStep, SourceDefinitionStep
from sqlflow.parser.parser import Parser
from sqlflow.project import Project
from sqlflow.validation.schemas import ConnectorSchema, FieldSchema


class TestIndustryStandardParameters:
    """Test industry-standard SOURCE parameters functionality."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary project directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create project structure
            project_dir = Path(temp_dir) / "test_project"
            project_dir.mkdir()

            # Create profiles directory and dev profile
            profiles_dir = project_dir / "profiles"
            profiles_dir.mkdir()

            dev_profile = profiles_dir / "dev.yml"
            dev_profile.write_text(
                """
engines:
  duckdb:
    mode: memory
    path: ":memory:"
paths:
  pipelines: "pipelines"
  output: "output"
variables:
  data_dir: "data"
  output_dir: "output"
"""
            )

            # Create pipelines directory
            pipelines_dir = project_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create output directory
            output_dir = project_dir / "output"
            output_dir.mkdir()

            yield str(project_dir)

    @pytest.fixture
    def incremental_data_dir(self, temp_project_dir):
        """Create sample data files for incremental loading tests."""
        data_dir = Path(temp_project_dir) / "data"
        data_dir.mkdir()

        # Create initial customer data
        initial_csv = data_dir / "customers_initial.csv"
        initial_csv.write_text(
            """customer_id,name,email,status,updated_at
1,Alice Johnson,alice@example.com,active,2024-01-01T10:00:00Z
2,Bob Smith,bob@example.com,active,2024-01-01T11:00:00Z
3,Charlie Brown,charlie@example.com,pending,2024-01-01T12:00:00Z
"""
        )

        # Create incremental update data
        updates_csv = data_dir / "customers_updates.csv"
        updates_csv.write_text(
            """customer_id,name,email,status,updated_at
2,Bob Smith,bob.smith@newdomain.com,active,2024-01-02T10:00:00Z
3,Charlie Brown,charlie@example.com,active,2024-01-02T11:00:00Z
4,Diana Prince,diana@example.com,active,2024-01-02T12:00:00Z
"""
        )

        # Create orders data
        orders_csv = data_dir / "orders.csv"
        orders_csv.write_text(
            """order_id,customer_id,amount,order_date
1001,1,100.50,2024-01-01T14:00:00Z
1002,2,250.75,2024-01-01T15:00:00Z
1003,3,75.25,2024-01-01T16:00:00Z
1004,1,180.00,2024-01-02T14:00:00Z
1005,4,320.25,2024-01-02T15:00:00Z
"""
        )

        return str(data_dir)

    def test_source_parameter_validation_success(self):
        """Test that valid industry-standard parameters are accepted."""
        parser = Parser()

        # Test valid incremental configuration
        valid_pipeline = """
        SOURCE customers TYPE CSV PARAMS {
            "path": "data/customers.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "customer_id",
            "cursor_field": "updated_at"
        };
        """

        # Should parse without errors
        pipeline = parser.parse(valid_pipeline)
        source_step = pipeline.steps[0]

        assert isinstance(source_step, SourceDefinitionStep)
        assert source_step.name == "customers"
        assert source_step.params["sync_mode"] == "incremental"
        assert source_step.params["primary_key"] == "customer_id"
        assert source_step.params["cursor_field"] == "updated_at"

    def test_source_parameter_validation_errors(self):
        """Test validation errors for invalid industry-standard parameters."""
        parser = Parser()

        # Test invalid sync_mode
        invalid_sync_mode = """
        SOURCE customers TYPE CSV PARAMS {
            "path": "data/customers.csv",
            "has_header": true,
            "sync_mode": "invalid_mode"
        };
        """

        with pytest.raises(Exception) as exc_info:
            parser.parse(invalid_sync_mode, validate=True)

        assert "sync_mode" in str(exc_info.value)
        assert "invalid_mode" in str(exc_info.value)

    def test_airbyte_compatibility_parameters(self):
        """Test that Airbyte-compatible parameters work correctly."""
        parser = Parser()

        # Airbyte-style configuration
        airbyte_pipeline = """
        SOURCE airbyte_users TYPE CSV PARAMS {
            "path": "data/users.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "primary_key": "id"
        };
        """

        pipeline = parser.parse(airbyte_pipeline)
        source_step = pipeline.steps[0]

        assert isinstance(source_step, SourceDefinitionStep)
        # Verify Airbyte-compatible parameters are preserved
        assert source_step.params["sync_mode"] == "incremental"
        assert source_step.params["cursor_field"] == "updated_at"
        assert source_step.params["primary_key"] == "id"

    def test_full_refresh_mode_execution(self, temp_project_dir, incremental_data_dir):
        """Test full_refresh sync_mode execution."""
        os.chdir(temp_project_dir)

        pipeline_content = f"""
        SOURCE customers TYPE CSV PARAMS {{
            "path": "{incremental_data_dir}/customers_initial.csv",
            "has_header": true,
            "sync_mode": "full_refresh"
        }};
        
        LOAD customers_table FROM customers;
        
        CREATE TABLE customer_summary AS
        SELECT 
            COUNT(*) as total_customers,
            COUNT(CASE WHEN status = 'active' THEN 1 END) as active_customers
        FROM customers_table;
        """

        # Execute the pipeline
        parser = Parser()
        planner = Planner()
        executor = LocalExecutor(project=Project(temp_project_dir, profile_name="dev"))

        pipeline = parser.parse(pipeline_content)
        operations = planner.create_plan(pipeline)
        result = executor.execute(operations)

        # Verify execution succeeded
        assert result["status"] == "success"

        # Verify data was loaded
        summary_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM customer_summary"
        ).fetchdf()

        assert len(summary_result) == 1
        assert summary_result.iloc[0]["total_customers"] == 3
        assert summary_result.iloc[0]["active_customers"] == 2

    def test_incremental_mode_parameters_in_plan(self):
        """Test that incremental parameters are correctly passed to execution plan."""
        parser = Parser()
        planner = Planner()

        pipeline_text = """
        SOURCE orders TYPE CSV PARAMS {
            "path": "data/orders.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "order_id",
            "cursor_field": "order_date"
        };
        
        LOAD orders_table FROM orders;
        """

        pipeline = parser.parse(pipeline_text)
        operations = planner.create_plan(pipeline)

        # Find the source definition operation (where parameters are stored)
        source_ops = [op for op in operations if op["type"] == "source_definition"]
        assert len(source_ops) == 1

        source_op = source_ops[0]

        # Verify incremental parameters are in the source operation
        source_params = source_op.get("query", {})
        assert source_params.get("sync_mode") == "incremental"
        assert source_params.get("primary_key") == "order_id"
        assert source_params.get("cursor_field") == "order_date"

        # Also verify the load operation exists and references the correct source
        load_ops = [op for op in operations if op["type"] == "load"]
        assert len(load_ops) == 1

        load_op = load_ops[0]
        assert load_op["source_name"] == "orders"
        assert load_op["target_table"] == "orders_table"

    def test_mixed_sync_modes_in_pipeline(self, temp_project_dir, incremental_data_dir):
        """Test pipeline with different sync modes for different sources."""
        os.chdir(temp_project_dir)

        pipeline_content = f"""
        -- Full refresh for reference data
        SOURCE customers_initial TYPE CSV PARAMS {{
            "path": "{incremental_data_dir}/customers_initial.csv",
            "has_header": true,
            "sync_mode": "full_refresh"
        }};
        
        -- Incremental for transactional data
        SOURCE orders TYPE CSV PARAMS {{
            "path": "{incremental_data_dir}/orders.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "order_id",
            "cursor_field": "order_date"
        }};
        
        LOAD customers_table FROM customers_initial;
        LOAD orders_table FROM orders;
        
        CREATE TABLE customer_order_summary AS
        SELECT 
            c.customer_id,
            c.name,
            COUNT(o.order_id) as order_count,
            SUM(CAST(o.amount AS DECIMAL(10,2))) as total_amount
        FROM customers_table c
        LEFT JOIN orders_table o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name;
        """

        # Execute the pipeline
        parser = Parser()
        planner = Planner()
        executor = LocalExecutor(project=Project(temp_project_dir, profile_name="dev"))

        pipeline = parser.parse(pipeline_content)
        operations = planner.create_plan(pipeline)
        result = executor.execute(operations)

        # Verify execution succeeded
        assert result["status"] == "success"

        # Verify the join worked correctly
        summary_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM customer_order_summary ORDER BY customer_id"
        ).fetchdf()

        assert len(summary_result) == 3
        # Alice (customer 1) should have 2 orders
        alice_row = summary_result[summary_result["customer_id"] == 1].iloc[0]
        assert alice_row["order_count"] == 2
        assert float(alice_row["total_amount"]) == 280.50  # 100.50 + 180.00

    def _get_csv_connector_schema(self):
        """Helper method to get CSV connector schema for testing."""
        # Create a basic CSV schema with industry-standard parameters
        return ConnectorSchema(
            name="CSV",
            description="CSV file connector with industry-standard parameters",
            fields=[
                FieldSchema(
                    name="path",
                    required=True,
                    field_type="string",
                    description="Path to CSV file",
                ),
                FieldSchema(
                    name="has_header",
                    required=False,
                    field_type="boolean",
                    description="Whether CSV has header",
                ),
                FieldSchema(
                    name="sync_mode",
                    required=False,
                    field_type="string",
                    allowed_values=["full_refresh", "incremental"],
                    description="Sync mode",
                ),
                FieldSchema(
                    name="primary_key",
                    required=False,
                    field_type="string",
                    description="Primary key field",
                ),
                FieldSchema(
                    name="cursor_field",
                    required=False,
                    field_type="string",
                    description="Cursor field for incremental",
                ),
            ],
        )

    def test_schema_validation_integration(self):
        """Test that connector schema validation works with industry-standard parameters."""
        # Test CSV connector schema validation
        csv_schema = self._get_csv_connector_schema()

        # Valid parameters should pass
        valid_params = {
            "path": "data/test.csv",
            "has_header": True,
            "sync_mode": "incremental",
            "primary_key": "id",
            "cursor_field": "updated_at",
        }

        errors = csv_schema.validate(valid_params)
        assert len(errors) == 0

        # Invalid sync_mode should fail
        invalid_params = {
            "path": "data/test.csv",
            "has_header": True,
            "sync_mode": "invalid_mode",
        }

        errors = csv_schema.validate(invalid_params)
        assert len(errors) > 0
        assert any("sync_mode" in error for error in errors)

    def test_parameter_documentation_examples(self):
        """Test the examples from documentation work correctly."""
        parser = Parser()

        # Example 1: Full refresh (from docs)
        example1 = """
        SOURCE products TYPE CSV PARAMS {
            "path": "data/products.csv",
            "has_header": true,
            "sync_mode": "full_refresh"
        };
        """

        pipeline1 = parser.parse(example1)
        source_step1 = pipeline1.steps[0]
        assert isinstance(source_step1, SourceDefinitionStep)
        assert source_step1.params["sync_mode"] == "full_refresh"

        # Example 2: Incremental with timestamp cursor (from docs)
        example2 = """
        SOURCE orders TYPE CSV PARAMS {
            "path": "data/orders.csv", 
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "order_id",
            "cursor_field": "created_at"
        };
        """

        pipeline2 = parser.parse(example2)
        source_step2 = pipeline2.steps[0]
        assert isinstance(source_step2, SourceDefinitionStep)
        params = source_step2.params
        assert params["sync_mode"] == "incremental"
        assert params["primary_key"] == "order_id"
        assert params["cursor_field"] == "created_at"

        # Example 3: Incremental with ID cursor (from docs)
        example3 = """
        SOURCE events TYPE CSV PARAMS {
            "path": "data/events.csv",
            "has_header": true, 
            "sync_mode": "incremental",
            "primary_key": "event_id",
            "cursor_field": "sequence_id"
        };
        """

        pipeline3 = parser.parse(example3)
        source_step3 = pipeline3.steps[0]
        assert isinstance(source_step3, SourceDefinitionStep)
        params = source_step3.params
        assert params["sync_mode"] == "incremental"
        assert params["primary_key"] == "event_id"
        assert params["cursor_field"] == "sequence_id"

    def test_migration_compatibility(self):
        """Test that migration from other tools is straightforward."""
        parser = Parser()

        # Simulate Airbyte configuration migrated to SQLFlow
        airbyte_migration = """
        -- Original Airbyte source configuration migrated to SQLFlow
        SOURCE airbyte_users TYPE CSV PARAMS {
            "path": "data/users.csv",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "primary_key": "id"
        };
        
        -- Load with UPSERT mode matching incremental sync
        LOAD users_table FROM airbyte_users MODE UPSERT KEY (id);
        """

        pipeline = parser.parse(airbyte_migration)

        # Verify SOURCE parameters
        source_step = pipeline.steps[0]
        assert isinstance(source_step, SourceDefinitionStep)
        assert source_step.params["sync_mode"] == "incremental"
        assert source_step.params["cursor_field"] == "updated_at"
        assert source_step.params["primary_key"] == "id"

        # Verify parsed structure
        load_step = pipeline.steps[1]  # Second step should be the LOAD
        assert isinstance(load_step, LoadStep)
        assert load_step.mode == "UPSERT"
        assert load_step.upsert_keys == ["id"]  # Should be preserved as upsert_keys

        # Also check if upsert_keys attribute exists (new functionality)
        if hasattr(load_step, "upsert_keys"):
            assert load_step.upsert_keys == ["id"]

    def test_validation_error_messages(self):
        """Test that validation provides helpful error messages for parameters."""
        parser = Parser()

        # Test missing required parameter for incremental mode
        incomplete_incremental = """
        SOURCE customers TYPE CSV PARAMS {
            "path": "data/customers.csv",
            "has_header": true,
            "sync_mode": "incremental"
        };
        """

        with pytest.raises(Exception) as exc_info:
            parser.parse(incomplete_incremental, validate=True)

        error_message = str(exc_info.value)
        # Should mention what's missing for incremental mode
        assert "incremental" in error_message.lower()

    def test_end_to_end_incremental_workflow(
        self, temp_project_dir, incremental_data_dir
    ):
        """Test complete incremental loading workflow with industry-standard parameters."""
        os.chdir(temp_project_dir)

        # Step 1: Initial load
        initial_pipeline = f"""
        SOURCE customers TYPE CSV PARAMS {{
            "path": "{incremental_data_dir}/customers_initial.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "customer_id",
            "cursor_field": "updated_at"
        }};
        
        LOAD customers_table FROM customers MODE UPSERT KEY (customer_id);
        
        CREATE TABLE load_summary AS
        SELECT 
            COUNT(*) as total_customers,
            MAX(updated_at) as latest_update,
            'initial_load' as load_type
        FROM customers_table;
        """

        # Execute initial load
        parser = Parser()
        planner = Planner()
        executor = LocalExecutor(project=Project(temp_project_dir, profile_name="dev"))

        pipeline = parser.parse(initial_pipeline)
        operations = planner.create_plan(pipeline)
        result = executor.execute(operations)

        assert result["status"] == "success"

        # Verify initial data
        initial_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM load_summary"
        ).fetchdf()

        assert initial_result.iloc[0]["total_customers"] == 3
        assert initial_result.iloc[0]["load_type"] == "initial_load"

        # Step 2: Incremental update
        update_pipeline = f"""
        SOURCE customers_updates TYPE CSV PARAMS {{
            "path": "{incremental_data_dir}/customers_updates.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "customer_id",
            "cursor_field": "updated_at"
        }};
        
        LOAD customers_table FROM customers_updates MODE UPSERT KEY (customer_id);
        
        CREATE OR REPLACE TABLE load_summary AS
        SELECT 
            COUNT(*) as total_customers,
            MAX(updated_at) as latest_update,
            'incremental_update' as load_type
        FROM customers_table;
        """

        # Execute incremental update
        pipeline2 = parser.parse(update_pipeline)
        operations2 = planner.create_plan(pipeline2)
        result2 = executor.execute(operations2)

        assert result2["status"] == "success"

        # Verify incremental results
        final_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM load_summary"
        ).fetchdf()

        # Should now have 4 customers (3 initial + 1 new, with 2 updated)
        assert final_result.iloc[0]["total_customers"] == 4
        assert final_result.iloc[0]["load_type"] == "incremental_update"

        # Verify specific customer updates
        customer_data = executor.duckdb_engine.execute_query(
            "SELECT * FROM customers_table ORDER BY customer_id"
        ).fetchdf()

        # Customer 2 should have updated email
        customer_2 = customer_data[customer_data["customer_id"] == 2].iloc[0]
        assert customer_2["email"] == "bob.smith@newdomain.com"

        # Customer 4 should be newly added
        customer_4 = customer_data[customer_data["customer_id"] == 4].iloc[0]
        assert customer_4["name"] == "Diana Prince"
