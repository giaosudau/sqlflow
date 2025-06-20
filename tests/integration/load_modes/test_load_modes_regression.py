"""Integration tests for load modes demonstration regression prevention.

These tests ensure that the load modes examples functionality doesn't regress.
This covers the basic load modes, multiple upsert keys, and schema compatibility
examples that are showcased in the examples/load_modes directory.
"""

import os
import tempfile

import pandas as pd
import pytest

from sqlflow.core.executors import get_executor
from sqlflow.core.planner_main import Planner
from sqlflow.parser import SQLFlowParser


class TestLoadModesRegression:
    """Test load modes functionality to prevent regression."""

    @pytest.fixture
    def setup_load_modes_data(self):
        """Create test data files for load modes testing."""
        temp_dir = tempfile.mkdtemp()

        # Create initial users data
        users_data = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                    "diana@example.com",
                    "eve@example.com",
                ],
                "status": ["active", "active", "inactive", "active", "inactive"],
            }
        )
        users_path = os.path.join(temp_dir, "users.csv")
        users_data.to_csv(users_path, index=False)

        # Create new users data (for APPEND)
        new_users_data = pd.DataFrame(
            {
                "user_id": [6, 7, 8],
                "name": ["Frank", "Grace", "Henry"],
                "email": [
                    "frank@example.com",
                    "grace@example.com",
                    "henry@example.com",
                ],
                "status": ["active", "active", "active"],
            }
        )
        new_users_path = os.path.join(temp_dir, "new_users.csv")
        new_users_data.to_csv(new_users_path, index=False)

        # Create users updates data (for UPSERT)
        users_updates_data = pd.DataFrame(
            {
                "user_id": [4, 5, 9],
                "name": ["Diana Updated", "Eve Updated", "Ivan"],
                "email": [
                    "diana.new@example.com",
                    "eve.new@example.com",
                    "ivan@example.com",
                ],
                "status": ["active", "active", "active"],
            }
        )
        users_updates_path = os.path.join(temp_dir, "users_updates.csv")
        users_updates_data.to_csv(users_updates_path, index=False)

        return {
            "temp_dir": temp_dir,
            "users_path": users_path,
            "new_users_path": new_users_path,
            "users_updates_path": users_updates_path,
            "initial_users": users_data,
            "new_users": new_users_data,
            "users_updates": users_updates_data,
        }

    def test_basic_load_modes_pipeline(self, setup_load_modes_data):
        """Test the basic load modes pipeline that demonstrates REPLACE, APPEND, and UPSERT."""
        data = setup_load_modes_data

        # Create the basic load modes pipeline
        pipeline_sql = f"""
        -- Define data sources
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        SOURCE new_users_csv TYPE CSV PARAMS {{
            "path": "{data["new_users_path"]}", 
            "has_header": true
        }};
        
        SOURCE users_updates_csv TYPE CSV PARAMS {{
            "path": "{data["users_updates_path"]}", 
            "has_header": true
        }};
        
        -- Step 1: Initial REPLACE load
        LOAD users_table FROM users_csv MODE REPLACE;
        
        -- Step 2: APPEND new users
        LOAD users_table FROM new_users_csv MODE APPEND;
        
        -- Step 3: UPSERT updates
        LOAD users_table FROM users_updates_csv MODE UPSERT KEY (user_id);
        
        -- Export final results
        EXPORT SELECT * FROM users_table ORDER BY user_id
        TO "{os.path.join(data["temp_dir"], "basic_load_result.csv")}"
        TYPE CSV OPTIONS {{ "header": true }};
        """

        # Parse and execute
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        executor = get_executor()
        result = executor.execute(execution_plan)

        # Verify execution succeeded
        assert (
            result["status"] == "success"
        ), f"Basic load modes pipeline failed: {result.get('error')}"

        # Verify result file was created
        result_file = os.path.join(data["temp_dir"], "basic_load_result.csv")
        assert os.path.exists(result_file), "Result file should be created"

        # Verify the load modes worked correctly
        result_df = pd.read_csv(result_file)

        # Should have 9 rows total (5 initial + 3 new + 1 upserted new)
        assert len(result_df) == 9, f"Expected 9 rows, got {len(result_df)}"

        # Verify specific operations worked
        # Check that Diana and Eve were updated (UPSERT)
        diana_row = result_df[result_df["user_id"] == 4].iloc[0]
        assert (
            diana_row["name"] == "Diana Updated"
        ), "Diana should be updated via UPSERT"
        assert (
            diana_row["email"] == "diana.new@example.com"
        ), "Diana's email should be updated"

        eve_row = result_df[result_df["user_id"] == 5].iloc[0]
        assert eve_row["name"] == "Eve Updated", "Eve should be updated via UPSERT"
        assert eve_row["status"] == "active", "Eve's status should be updated to active"

        # Check that new users were added (APPEND and UPSERT)
        frank_row = result_df[result_df["user_id"] == 6]
        assert len(frank_row) == 1, "Frank should be added via APPEND"

        ivan_row = result_df[result_df["user_id"] == 9]
        assert len(ivan_row) == 1, "Ivan should be added via UPSERT"

    def test_multiple_upsert_keys_load_mode(self, setup_load_modes_data):
        """Test load mode with multiple upsert keys."""
        data = setup_load_modes_data

        # Create data with composite keys
        sales_data = pd.DataFrame(
            {
                "user_id": [1, 1, 2, 2, 3, 3],
                "product_id": ["A", "B", "A", "B", "A", "B"],
                "quantity": [10, 20, 15, 25, 12, 22],
                "price": [100.0, 200.0, 150.0, 250.0, 120.0, 220.0],
            }
        )
        sales_path = os.path.join(data["temp_dir"], "sales.csv")
        sales_data.to_csv(sales_path, index=False)

        # Create updates with composite keys
        sales_updates = pd.DataFrame(
            {
                "user_id": [1, 2, 4],
                "product_id": ["A", "B", "A"],
                "quantity": [15, 30, 10],  # Updated quantities
                "price": [110.0, 280.0, 100.0],  # Updated prices
            }
        )
        sales_updates_path = os.path.join(data["temp_dir"], "sales_updates.csv")
        sales_updates.to_csv(sales_updates_path, index=False)

        pipeline_sql = f"""
        SOURCE sales_csv TYPE CSV PARAMS {{
            "path": "{sales_path}", 
            "has_header": true
        }};
        
        SOURCE sales_updates_csv TYPE CSV PARAMS {{
            "path": "{sales_updates_path}", 
            "has_header": true
        }};
        
        -- Load initial sales data
        LOAD sales_table FROM sales_csv MODE REPLACE;
        
        -- Upsert updates using composite key
        LOAD sales_table FROM sales_updates_csv MODE UPSERT KEY (user_id, product_id);
        
        EXPORT SELECT * FROM sales_table ORDER BY user_id, product_id
        TO "{os.path.join(data["temp_dir"], "multiple_keys_result.csv")}"
        TYPE CSV OPTIONS {{ "header": true }};
        """

        # Execute pipeline
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        executor = get_executor()
        result = executor.execute(execution_plan)

        # Verify execution succeeded
        assert (
            result["status"] == "success"
        ), f"Multiple upsert keys pipeline failed: {result.get('error')}"

        # Verify results
        result_file = os.path.join(data["temp_dir"], "multiple_keys_result.csv")
        result_df = pd.read_csv(result_file)

        # Should have 7 rows (6 original, 2 updated, 1 new since user 4 product A doesn't exist in original)
        assert len(result_df) == 7, f"Expected 7 rows, got {len(result_df)}"

        # Check that updates worked for composite keys
        user1_productA = result_df[
            (result_df["user_id"] == 1) & (result_df["product_id"] == "A")
        ].iloc[0]
        assert (
            user1_productA["quantity"] == 15
        ), "User 1 Product A quantity should be updated"
        assert (
            user1_productA["price"] == 110.0
        ), "User 1 Product A price should be updated"

        user2_productB = result_df[
            (result_df["user_id"] == 2) & (result_df["product_id"] == "B")
        ].iloc[0]
        assert (
            user2_productB["quantity"] == 30
        ), "User 2 Product B quantity should be updated"

        # Check that new record was added (user 4 product A)
        user4_records = result_df[result_df["user_id"] == 4]
        assert (
            len(user4_records) == 1
        ), "User 4 should be added since composite key doesn't exist"

    def test_schema_compatibility_load_mode(self, setup_load_modes_data):
        """Test load mode with schema compatibility."""
        data = setup_load_modes_data

        # Create data with different but compatible schema (only compatible columns)
        extended_users = pd.DataFrame(
            {
                "user_id": [10, 11],
                "name": ["Jack", "Jill"],
                "email": ["jack@example.com", "jill@example.com"],
                "status": ["active", "inactive"],
                # Removed extra columns that would cause schema validation errors
            }
        )
        extended_users_path = os.path.join(data["temp_dir"], "extended_users.csv")
        extended_users.to_csv(extended_users_path, index=False)

        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        SOURCE extended_users_csv TYPE CSV PARAMS {{
            "path": "{extended_users_path}", 
            "has_header": true
        }};
        
        -- Load initial users (standard schema)
        LOAD users_table FROM users_csv MODE REPLACE;
        
        -- Append users with compatible schema
        LOAD users_table FROM extended_users_csv MODE APPEND;
        
        EXPORT SELECT user_id, name, email, status FROM users_table ORDER BY user_id
        TO "{os.path.join(data["temp_dir"], "schema_compatible_result.csv")}"
        TYPE CSV OPTIONS {{ "header": true }};
        """

        # Execute pipeline
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        executor = get_executor()
        result = executor.execute(execution_plan)

        # Verify execution succeeded
        assert (
            result["status"] == "success"
        ), f"Schema compatibility pipeline failed: {result.get('error')}"

        # Verify results
        result_file = os.path.join(data["temp_dir"], "schema_compatible_result.csv")
        result_df = pd.read_csv(result_file)

        # Should have 7 rows (5 original + 2 extended)
        assert len(result_df) == 7, f"Expected 7 rows, got {len(result_df)}"

        # Check that extended users were added
        jack_row = result_df[result_df["user_id"] == 10]
        assert len(jack_row) == 1, "Jack should be added with compatible schema"

        jill_row = result_df[result_df["user_id"] == 11]
        assert len(jill_row) == 1, "Jill should be added with compatible schema"

    def test_load_modes_execution_plan_structure(self, setup_load_modes_data):
        """Test that load modes execution plans have correct structure."""
        data = setup_load_modes_data

        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        LOAD users_table FROM users_csv MODE REPLACE;
        LOAD users_table FROM users_csv MODE APPEND;
        LOAD users_table FROM users_csv MODE UPSERT KEY (user_id, email);
        """

        # Parse and plan
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Find load operations in the plan
        load_steps = [step for step in execution_plan if step.get("type") == "load"]

        # Should have 3 load steps
        assert len(load_steps) == 3, f"Expected 3 load steps, got {len(load_steps)}"

        # Verify modes are preserved correctly
        modes = [step.get("mode") for step in load_steps]
        expected_modes = ["REPLACE", "APPEND", "UPSERT"]
        assert modes == expected_modes, f"Expected modes {expected_modes}, got {modes}"

        # Verify upsert keys are preserved for UPSERT operation
        upsert_step = [step for step in load_steps if step.get("mode") == "UPSERT"][0]
        assert upsert_step["upsert_keys"] == [
            "user_id",
            "email",
        ], "UPSERT step should preserve upsert_keys"

    def test_load_modes_error_handling(self, setup_load_modes_data):
        """Test proper error handling in load modes."""
        data = setup_load_modes_data

        # Test UPSERT without upsert keys (should fail at planning stage)
        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        LOAD users_table FROM users_csv MODE REPLACE;
        LOAD users_table FROM users_csv MODE UPSERT;  -- Missing KEY
        """

        # This should fail during parsing
        with pytest.raises(Exception) as exc_info:
            parser = SQLFlowParser(pipeline_sql)
            parser.parse()

        # Should fail with appropriate error message
        assert "KEY" in str(exc_info.value) or "upsert" in str(exc_info.value).lower()

    def test_load_modes_comprehensive_scenario(self, setup_load_modes_data):
        """Test a comprehensive scenario that exercises all load modes."""
        data = setup_load_modes_data

        # This replicates the exact scenario from the examples/load_modes demo
        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        SOURCE new_users_csv TYPE CSV PARAMS {{
            "path": "{data["new_users_path"]}", 
            "has_header": true
        }};
        
        SOURCE users_updates_csv TYPE CSV PARAMS {{
            "path": "{data["users_updates_path"]}", 
            "has_header": true
        }};
        
        -- Replicate the basic load modes example
        LOAD users_table FROM users_csv MODE REPLACE;
        LOAD users_table FROM new_users_csv MODE APPEND;
        LOAD users_table FROM users_updates_csv MODE UPSERT KEY (user_id);
        
        -- Add some analytics
        CREATE TABLE user_summary AS
        SELECT 
            status,
            COUNT(*) as user_count,
            AVG(user_id) as avg_user_id
        FROM users_table 
        GROUP BY status;
        
        EXPORT SELECT * FROM user_summary ORDER BY status
        TO "{os.path.join(data["temp_dir"], "comprehensive_result.csv")}"
        TYPE CSV OPTIONS {{ "header": true }};
        """

        # Execute the comprehensive pipeline
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        executor = get_executor()
        result = executor.execute(execution_plan)

        # This is the key regression test - comprehensive pipeline should work
        assert (
            result["status"] == "success"
        ), f"Comprehensive load modes scenario failed: {result.get('error')}"

        # Verify analytics results
        result_file = os.path.join(data["temp_dir"], "comprehensive_result.csv")
        assert os.path.exists(result_file), "Comprehensive result should be generated"

        result_df = pd.read_csv(result_file)

        # Should have active and inactive users
        statuses = result_df["status"].tolist()
        assert "active" in statuses, "Should have active users"
        assert "inactive" in statuses, "Should have inactive users"

        # Verify counts make sense
        total_users = result_df["user_count"].sum()
        assert total_users == 9, f"Should have 9 total users, got {total_users}"
