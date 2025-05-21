import os
import shutil
import tempfile
import unittest

from sqlflow.cli.pipeline import _execute_pipeline_operations_and_report
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.core.storage.artifact_manager import ArtifactManager
from sqlflow.parser.parser import Parser


class TestVariableSubstitutionInExport(unittest.TestCase):
    """Test that variables are correctly substituted in export destinations."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for outputs
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_dir, "output")
        os.makedirs(self.output_dir, exist_ok=True)

        # Create test output subdirectory
        os.makedirs(os.path.join(self.output_dir, "test_output"), exist_ok=True)

        # Create test pipeline file
        self.pipeline_file = os.path.join(self.temp_dir, "test_pipeline.sf")
        with open(self.pipeline_file, "w") as f:
            f.write(
                """
            SET output_dir = "${output_dir}";
            SET export_format = "${export_format|csv}";
            SET run_id = "${run_id|test_run}";
            
            -- Create a test table with some data
            CREATE TABLE test_data AS
            SELECT 1 as id, 'test' as name
            UNION ALL
            SELECT 2 as id, 'test2' as name;
            
            -- Export data
            EXPORT SELECT * FROM test_data
            TO "${output_dir}/test_output/test_file_${run_id}.${export_format}"
            TYPE CSV OPTIONS {"header": true};
            """
            )

        # Variables for substitution
        self.variables = {
            "output_dir": self.output_dir,
            "run_id": "test123",
            "export_format": "csv",
        }

        # Set up artifact manager
        self.artifact_manager = ArtifactManager(self.temp_dir)
        self.execution_id = "test-execution-id"

        # Save output path for verification
        self.expected_output_file = os.path.join(
            self.output_dir,
            "test_output",
            f"test_file_{self.variables['run_id']}.{self.variables['export_format']}",
        )

    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)

    def test_variable_substitution_in_planner(self):
        """Test variable substitution in Planner (initial stage)."""
        # Parse the pipeline file
        with open(self.pipeline_file, "r") as f:
            pipeline_text = f.read()

        parser = Parser()
        pipeline = parser.parse(pipeline_text)

        # Create execution plan with planner
        planner = Planner()
        operations = planner.create_plan(pipeline, variables=self.variables.copy())

        # Check if variables were substituted in the plan
        export_steps = [op for op in operations if op["type"] == "export"]
        self.assertEqual(len(export_steps), 1, "Should have one export step")

        export_step = export_steps[0]
        destination_uri = export_step["query"]["destination_uri"]

        # Check for partial substitution in the planner
        # Planner should substitute at least some variables, but may not do all
        self.assertIn(
            self.output_dir,
            destination_uri,
            f"Output directory not substituted in destination: {destination_uri}",
        )

        print(f"Planner destination URI: {destination_uri}")
        return operations

    def test_variable_substitution_in_executor(self):
        """Test variable substitution in LocalExecutor."""
        # Get operations from planner
        operations = self.test_variable_substitution_in_planner()

        # Execute pipeline using local executor directly
        executor = LocalExecutor(profile_name="dev")
        executor.execution_id = self.execution_id
        executor.artifact_manager = self.artifact_manager

        # Run the execution plan
        result = executor.execute(
            operations, pipeline_name="test_pipeline", variables=self.variables.copy()
        )

        # Verify export was successful
        self.assertEqual(
            result.get("status"), "success", f"Execution failed with result: {result}"
        )

        # Verify the output file exists
        self.assertTrue(
            os.path.exists(self.expected_output_file),
            f"Output file not found: {self.expected_output_file}",
        )

    def test_full_pipeline_simulation(self):
        """Simulate the full CLI pipeline execution with _execute_pipeline_operations_and_report."""
        # Parse and plan the pipeline
        with open(self.pipeline_file, "r") as f:
            pipeline_text = f.read()

        parser = Parser()
        pipeline = parser.parse(pipeline_text)

        # Create execution plan
        planner = Planner()
        operations = planner.create_plan(pipeline)

        # Initialize execution tracking
        execution_id, _ = self.artifact_manager.initialize_execution(
            "test_pipeline", self.variables, "dev"
        )

        # Execute the pipeline using the CLI helper function
        _execute_pipeline_operations_and_report(
            operations=operations,
            pipeline_name="test_pipeline",
            profile_name="dev",
            variables=self.variables,
            artifact_manager=self.artifact_manager,
            execution_id=execution_id,
        )

        # Verify the output file exists
        self.assertTrue(
            os.path.exists(self.expected_output_file),
            f"Output file not found: {self.expected_output_file}",
        )


if __name__ == "__main__":
    unittest.main()
