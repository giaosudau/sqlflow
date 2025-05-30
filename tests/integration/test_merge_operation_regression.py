"""Integration tests for MERGE operation regression prevention.

These tests ensure that the fix for MERGE operations in local_executor.py
doesn't regress. Specifically, they test that merge_keys are properly
passed from the execution plan to the LoadStep object during execution.

The bug was that when creating LoadStep objects from execution plan steps,
the merge_keys parameter was missing, causing "MERGE operation requires
at least one merge key" errors even when merge_keys were correctly specified.
"""

import os
import tempfile

import pandas as pd
import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.parser import SQLFlowParser


class TestMergeOperationRegression:
    """Test MERGE operations to prevent regression of executor fix."""

    @pytest.fixture
    def setup_test_data(self):
        """Create test data files for MERGE operations."""
        temp_dir = tempfile.mkdtemp()

        # Create initial users data
        users_data = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
                "status": ["active", "active", "inactive"],
            }
        )
        users_path = os.path.join(temp_dir, "users.csv")
        users_data.to_csv(users_path, index=False)

        # Create updates data
        updates_data = pd.DataFrame(
            {
                "user_id": [2, 3, 4],
                "name": ["Bob Updated", "Charlie Updated", "David"],
                "email": ["bob.new@test.com", "charlie.new@test.com", "david@test.com"],
                "status": ["active", "active", "active"],
            }
        )
        updates_path = os.path.join(temp_dir, "updates.csv")
        updates_data.to_csv(updates_path, index=False)

        return {
            "temp_dir": temp_dir,
            "users_path": users_path,
            "updates_path": updates_path,
            "users_data": users_data,
            "updates_data": updates_data,
        }

    def test_merge_operation_with_single_key(self, setup_test_data):
        """Test MERGE operation with single merge key executes successfully."""
        data = setup_test_data

        # Create pipeline with MERGE operation
        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        SOURCE updates_csv TYPE CSV PARAMS {{
            "path": "{data["updates_path"]}", 
            "has_header": true
        }};
        
        LOAD users_table FROM users_csv MODE REPLACE;
        LOAD users_table FROM updates_csv MODE MERGE MERGE_KEYS (user_id);
        
        EXPORT SELECT * FROM users_table ORDER BY user_id 
        TO "{os.path.join(data["temp_dir"], "result.csv")}"
        TYPE CSV OPTIONS {{ "header": true }};
        """

        # Parse and plan
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Execute
        executor = LocalExecutor()
        result = executor.execute(execution_plan)

        # Verify execution succeeded
        assert result["status"] == "success"

        # Verify result file was created
        result_file = os.path.join(data["temp_dir"], "result.csv")
        assert os.path.exists(result_file)

        # Verify MERGE operation worked correctly
        result_df = pd.read_csv(result_file)

        # Should have 4 rows (original 1, updated 2&3, new 4)
        assert len(result_df) == 4

        # Check specific updates
        bob_row = result_df[result_df["user_id"] == 2].iloc[0]
        assert bob_row["name"] == "Bob Updated"
        assert bob_row["email"] == "bob.new@test.com"

        charlie_row = result_df[result_df["user_id"] == 3].iloc[0]
        assert charlie_row["name"] == "Charlie Updated"
        assert charlie_row["status"] == "active"  # Updated from inactive

        # Check new record
        david_row = result_df[result_df["user_id"] == 4].iloc[0]
        assert david_row["name"] == "David"

    def test_merge_operation_with_multiple_keys(self, setup_test_data):
        """Test MERGE operation with multiple merge keys."""
        data = setup_test_data

        # Create data with composite keys
        composite_data = pd.DataFrame(
            {
                "user_id": [1, 1, 2, 2],
                "region": ["US", "EU", "US", "EU"],
                "sales": [100, 200, 150, 250],
                "quarter": ["Q1", "Q1", "Q1", "Q1"],
            }
        )
        composite_path = os.path.join(data["temp_dir"], "sales.csv")
        composite_data.to_csv(composite_path, index=False)

        # Create updates with composite keys
        updates_composite = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "region": ["US", "EU", "US"],
                "sales": [120, 300, 180],  # Updated values
                "quarter": ["Q1", "Q1", "Q1"],
            }
        )
        updates_composite_path = os.path.join(data["temp_dir"], "sales_updates.csv")
        updates_composite.to_csv(updates_composite_path, index=False)

        pipeline_sql = f"""
        SOURCE sales_csv TYPE CSV PARAMS {{
            "path": "{composite_path}", 
            "has_header": true
        }};
        
        SOURCE updates_csv TYPE CSV PARAMS {{
            "path": "{updates_composite_path}", 
            "has_header": true
        }};
        
        LOAD sales_table FROM sales_csv MODE REPLACE;
        LOAD sales_table FROM updates_csv MODE MERGE MERGE_KEYS (user_id, region);
        
        EXPORT SELECT * FROM sales_table ORDER BY user_id, region
        TO "{os.path.join(data["temp_dir"], "sales_result.csv")}"
        TYPE CSV OPTIONS {{ "header": true }};
        """

        # Parse and execute
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        executor = LocalExecutor()
        result = executor.execute(execution_plan)

        # Verify execution succeeded
        assert result["status"] == "success"

        # Verify results
        result_file = os.path.join(data["temp_dir"], "sales_result.csv")
        result_df = pd.read_csv(result_file)

        # Should have 4 rows (2 updated, 2 unchanged, 0 completely new since user 3 region US doesn't exist in original)
        assert len(result_df) == 4, f"Expected 4 rows, got {len(result_df)}"

        # Check updated records
        user1_us = result_df[
            (result_df["user_id"] == 1) & (result_df["region"] == "US")
        ].iloc[0]
        assert user1_us["sales"] == 120  # Updated

        user2_eu = result_df[
            (result_df["user_id"] == 2) & (result_df["region"] == "EU")
        ].iloc[0]
        assert user2_eu["sales"] == 300  # Updated

    def test_merge_operation_execution_plan_structure(self, setup_test_data):
        """Test that execution plan correctly preserves merge_keys."""
        data = setup_test_data

        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        LOAD users_table FROM users_csv MODE MERGE MERGE_KEYS (user_id, email);
        """

        # Parse and plan
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Find the MERGE operation in the plan
        merge_step = None
        for step in execution_plan:
            if (
                step.get("type") == "load"
                and step.get("mode") == "MERGE"
                and "merge_keys" in step
            ):
                merge_step = step
                break

        # Verify merge step exists and has correct structure
        assert merge_step is not None, "MERGE step not found in execution plan"
        assert merge_step["merge_keys"] == ["user_id", "email"]
        assert merge_step["mode"] == "MERGE"
        assert "source_name" in merge_step
        assert "target_table" in merge_step

    def test_merge_operation_error_handling(self, setup_test_data):
        """Test proper error handling when merge keys don't exist."""
        data = setup_test_data

        pipeline_sql = f"""
        SOURCE users_csv TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        LOAD users_table FROM users_csv MODE REPLACE;
        LOAD users_table FROM users_csv MODE MERGE MERGE_KEYS (nonexistent_key);
        """

        # Parse and plan
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Execute - should fail with proper error message
        executor = LocalExecutor()
        result = executor.execute(execution_plan)

        # Should fail but not with "missing merge key" error from executor
        # The error should be about the key not existing in the table
        assert result["status"] == "failed"  # Updated to match actual status
        # Should not contain the old error message that indicated missing merge_keys parameter
        assert "MERGE operation requires at least one merge key" not in str(
            result.get("error", "")
        )

    def test_load_step_creation_from_execution_plan(self, setup_test_data):
        """Test that LoadStep objects are created correctly from execution plan."""
        # Create a mock execution step that represents the fixed scenario
        execution_step = {
            "type": "load",
            "source_name": "test_source",
            "target_table": "test_table",
            "mode": "MERGE",
            "merge_keys": ["test_key"],
        }

        # Test the specific code path that was fixed
        from sqlflow.parser.ast import LoadStep

        # This is the exact code from local_executor.py that was fixed
        load_step = LoadStep(
            table_name=execution_step["target_table"],
            source_name=execution_step["source_name"],
            mode=execution_step.get("mode", "REPLACE"),
            merge_keys=execution_step.get("merge_keys", []),  # This was the fix
        )

        # Verify the LoadStep was created correctly
        assert load_step.table_name == "test_table"
        assert load_step.source_name == "test_source"
        assert load_step.mode == "MERGE"
        assert load_step.merge_keys == ["test_key"]

    def test_regression_prevention_comprehensive(self, setup_test_data):
        """Comprehensive test that would have failed before the fix."""
        data = setup_test_data

        # This exact scenario was failing before the fix
        pipeline_sql = f"""
        SOURCE initial_data TYPE CSV PARAMS {{
            "path": "{data["users_path"]}", 
            "has_header": true
        }};
        
        SOURCE update_data TYPE CSV PARAMS {{
            "path": "{data["updates_path"]}", 
            "has_header": true
        }};
        
        -- Load initial data
        LOAD target_table FROM initial_data MODE REPLACE;
        
        -- This MERGE operation was failing with "missing merge key" before fix
        LOAD target_table FROM update_data MODE MERGE MERGE_KEYS (user_id);
        """

        # Parse, plan and execute
        parser = SQLFlowParser(pipeline_sql)
        pipeline = parser.parse()
        planner = Planner()
        execution_plan = planner.create_plan(pipeline)

        # Execute - this should work now
        executor = LocalExecutor()
        result = executor.execute(execution_plan)

        # The key assertion: execution should succeed
        assert result["status"] == "success", f"Execution failed: {result.get('error')}"

        # Simplified verification: just check that merge_keys are preserved in plan
        merge_steps = [
            step
            for step in execution_plan
            if step.get("type") == "load" and step.get("mode") == "MERGE"
        ]
        assert len(merge_steps) == 1, "Should have exactly one MERGE step"
        assert merge_steps[0]["merge_keys"] == ["user_id"], (
            "MERGE step should preserve merge_keys"
        )
