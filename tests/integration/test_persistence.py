import os
import shutil
import tempfile
import unittest
import uuid

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.parser import SQLFlowParser


class TestPersistenceIntegration(unittest.TestCase):
    """Integration test for persistence with LocalExecutor."""

    def setUp(self):
        """Set up a temporary project with persistent DuckDB configuration."""
        # Create a temporary directory for the test project
        self.project_dir = tempfile.mkdtemp(prefix="sqlflow_test_")

        # Create profile directories
        os.makedirs(os.path.join(self.project_dir, "profiles"), exist_ok=True)
        os.makedirs(os.path.join(self.project_dir, "pipelines"), exist_ok=True)
        os.makedirs(os.path.join(self.project_dir, "data"), exist_ok=True)

        # Create a persistent profile
        self.db_path = os.path.join(self.project_dir, "data", "test_db.duckdb")
        self.profile_path = os.path.join(self.project_dir, "profiles", "persistent.yml")
        self.profile_content = f"""
engines:
  duckdb:
    mode: persistent
    path: {self.db_path}
    memory_limit: 1GB
log_level: info
"""
        with open(self.profile_path, "w") as f:
            f.write(self.profile_content)

        # Create a test pipeline
        self.pipeline_path = os.path.join(
            self.project_dir, "pipelines", "test_pipeline.sf"
        )
        self.pipeline_content = """
-- Create a test table
CREATE TABLE test_data AS 
SELECT 1 AS id, 'Test 1' AS name
UNION ALL 
SELECT 2 AS id, 'Test 2' AS name;

-- Query the table
CREATE TABLE result AS
SELECT * FROM test_data WHERE id = 1;
"""
        with open(self.pipeline_path, "w") as f:
            f.write(self.pipeline_content)

        # Create another pipeline to verify data
        self.verify_pipeline_path = os.path.join(
            self.project_dir, "pipelines", "verify_pipeline.sf"
        )
        self.verify_pipeline_content = """
-- Query the existing table that should persist
CREATE TABLE verification AS
SELECT * FROM test_data ORDER BY id;
"""
        with open(self.verify_pipeline_path, "w") as f:
            f.write(self.verify_pipeline_content)

    def tearDown(self):
        """Clean up temporary files."""
        if hasattr(self, "project_dir") and os.path.exists(self.project_dir):
            shutil.rmtree(self.project_dir)

    def test_persistence_across_executor_instances(self):
        """Test that data persists across different executor instances."""
        # First execution - create tables
        executor1 = LocalExecutor(
            profile_name="persistent", project_dir=self.project_dir
        )

        # Create unique execution ID
        execution_id = str(uuid.uuid4())
        executor1.execution_id = execution_id

        # Verify we're using persistent mode
        self.assertEqual(executor1.duckdb_mode, "persistent")
        self.assertTrue(hasattr(executor1.duckdb_engine, "is_persistent"))
        self.assertTrue(executor1.duckdb_engine.is_persistent)

        # Execute test pipeline
        with open(self.pipeline_path, "r") as f:
            content = f.read()
        parser = SQLFlowParser()
        pipeline = parser.parse(content)

        # Convert SQLBlockStep objects to dictionaries suitable for executor
        steps = []
        for i, step in enumerate(pipeline.steps):
            # Create step dictionary
            step_dict = {
                "id": f"step_{i + 1}",
                "type": "transform",
                "name": step.table_name,
                "query": step.sql_query,
                "depends_on": [],
            }
            # Add dependency on previous step
            if i > 0:
                step_dict["depends_on"] = [f"step_{i}"]
            steps.append(step_dict)

        # Execute the pipeline with converted steps
        result1 = executor1.execute(steps)

        # Verify execution was successful
        self.assertEqual(result1.get("status"), "success")

        # Verify the database file exists
        self.assertTrue(os.path.exists(self.db_path))
        self.assertGreater(os.path.getsize(self.db_path), 0)

        # Verify table was created
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        tables = executor1.duckdb_engine.execute_query(tables_query).fetchdf()
        self.assertIn("test_data", tables["table_name"].tolist())

        # Get data for verification
        data_query = "SELECT * FROM test_data ORDER BY id"
        data1 = executor1.duckdb_engine.execute_query(data_query).fetchdf()
        self.assertEqual(len(data1), 2)
        self.assertEqual(data1["name"][0], "Test 1")

        # Close the first executor
        executor1.duckdb_engine.close()

        # Create a second executor instance
        executor2 = LocalExecutor(
            profile_name="persistent", project_dir=self.project_dir
        )

        # Verify it's using the same database file
        self.assertEqual(executor2.duckdb_engine.database_path, self.db_path)

        # Run the verification pipeline
        with open(self.verify_pipeline_path, "r") as f:
            verify_content = f.read()
        verify_pipeline = parser.parse(verify_content)

        # Convert SQLBlockStep objects to dictionaries suitable for executor
        verify_steps = []
        for i, step in enumerate(verify_pipeline.steps):
            # Create step dictionary
            step_dict = {
                "id": f"verify_step_{i + 1}",
                "type": "transform",
                "name": step.table_name,
                "query": step.sql_query,
                "depends_on": [],
            }
            # Add dependency on previous step
            if i > 0:
                step_dict["depends_on"] = [f"verify_step_{i}"]
            verify_steps.append(step_dict)

        # Execute the verification pipeline
        result2 = executor2.execute(verify_steps)

        # Verify execution was successful
        self.assertEqual(result2.get("status"), "success")

        # Check that the original data is accessible
        data2 = executor2.duckdb_engine.execute_query(
            "SELECT * FROM verification ORDER BY id"
        ).fetchdf()
        self.assertEqual(len(data2), 2)
        self.assertEqual(data2["id"][0], 1)
        self.assertEqual(data2["name"][0], "Test 1")
        self.assertEqual(data2["id"][1], 2)
        self.assertEqual(data2["name"][1], "Test 2")


if __name__ == "__main__":
    unittest.main()
