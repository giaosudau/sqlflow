"""Regression tests for UDF system fixes.

These tests ensure that the key UDF system issues are never reintroduced:
1. UDF discovery and registration
2. Scalar UDF execution with real data
3. CSV data loading with all columns
4. Source definition handling
5. Query processing with UDFs
"""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser


@pytest.fixture
def temp_udf_project():
    """Create a temporary UDF project structure for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)

        # Create project structure
        pipelines_dir = project_path / "pipelines"
        profiles_dir = project_path / "profiles"
        data_dir = project_path / "data"
        python_udfs_dir = project_path / "python_udfs"

        pipelines_dir.mkdir()
        profiles_dir.mkdir()
        data_dir.mkdir()
        python_udfs_dir.mkdir()

        # Create test data CSV with multiple columns
        test_csv = data_dir / "test_customers.csv"
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice Johnson", "bob smith", "CHARLIE BROWN"],
                "email": ["alice@example.com", "bob@test.com", "charlie@demo.com"],
                "notes": ["VIP customer", "Regular user", "New signup"],
            }
        )
        test_data.to_csv(test_csv, index=False)

        # Create Python UDF file
        udf_file = python_udfs_dir / "__init__.py"
        udf_file.touch()  # Make it a package

        udf_file = python_udfs_dir / "text_processing.py"
        udf_file.write_text(
            """
\"\"\"Simple text processing UDFs for regression testing.\"\"\"

from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def capitalize_words(text: str) -> str:
    \"\"\"Capitalize each word in the text.\"\"\"
    if not text:
        return text
    return ' '.join(word.capitalize() for word in str(text).split())

@python_scalar_udf  
def extract_domain(email: str) -> str:
    \"\"\"Extract domain from email address.\"\"\"
    if not email or '@' not in str(email):
        return ''
    return str(email).split('@')[1]
"""
        )

        # Create test pipeline
        pipeline_file = pipelines_dir / "test_udf_regression.sf"
        pipeline_file.write_text(
            f"""
-- Regression test pipeline for UDF system
SOURCE customers TYPE CSV PARAMS {{
  "path": "{data_dir}/test_customers.csv",
  "has_header": true
}};

LOAD raw_customers FROM customers;

-- Test scalar UDFs with real data
CREATE TABLE processed_customers AS
SELECT
  id,
  name,
  PYTHON_FUNC("python_udfs.text_processing.capitalize_words", name) AS formatted_name,
  email,
  PYTHON_FUNC("python_udfs.text_processing.extract_domain", email) AS domain,
  notes
FROM raw_customers;

-- Export results
EXPORT SELECT * FROM processed_customers
TO "{tmpdir}/output/processed_results.csv"
TYPE CSV
OPTIONS {{ "header": true }};
"""
        )

        # Create profile
        profile_file = profiles_dir / "dev.yml"
        profile_file.write_text(
            """
engines:
  duckdb:
    mode: memory
"""
        )

        # Create output directory
        output_dir = project_path / "output"
        output_dir.mkdir()

        yield {
            "project_dir": str(project_path),
            "data_file": str(test_csv),
            "pipeline_file": str(pipeline_file),
            "output_dir": str(output_dir),
            "expected_csv": str(output_dir / "processed_results.csv"),
        }


class TestUDFSystemRegression:
    """Regression tests for UDF system fixes."""

    def test_udf_discovery_and_registration(self, temp_udf_project):
        """Test that UDF discovery and registration works correctly.

        Regression test for: UDF registration failures
        """
        # Create executor with UDF discovery
        executor = LocalExecutor(project_dir=temp_udf_project["project_dir"])

        # Verify UDFs were discovered
        assert hasattr(executor, "discovered_udfs")
        assert executor.discovered_udfs is not None
        assert len(executor.discovered_udfs) > 0

        # Check specific UDFs are discovered
        udf_names = list(executor.discovered_udfs.keys())
        assert any("capitalize_words" in name for name in udf_names)
        assert any("extract_domain" in name for name in udf_names)

        # Verify UDFs are registered with DuckDB engine
        assert executor.duckdb_engine is not None

    def test_scalar_udf_execution_with_real_data(self, temp_udf_project):
        """Test that scalar UDFs work with real CSV data.

        Regression test for: UDF query processing and real data loading
        """
        # Parse and execute pipeline
        with open(temp_udf_project["pipeline_file"], "r") as f:
            pipeline_text = f.read()

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        executor = LocalExecutor(project_dir=temp_udf_project["project_dir"])
        result = executor.execute(plan)

        # Verify execution succeeded
        assert result["status"] == "success"

        # Verify output file was created
        assert os.path.exists(temp_udf_project["expected_csv"])

        # Verify output contains processed data
        output_df = pd.read_csv(temp_udf_project["expected_csv"])

        # Check all columns are present (regression for CSV loading)
        expected_columns = {"id", "name", "formatted_name", "email", "domain", "notes"}
        assert set(output_df.columns) == expected_columns

        # Check UDF processing worked
        assert (
            "Alice Johnson" in output_df["formatted_name"].values
        )  # capitalize_words worked
        assert "Bob Smith" in output_df["formatted_name"].values  # lowercase converted
        assert (
            "Charlie Brown" in output_df["formatted_name"].values
        )  # all caps converted

        # Check domain extraction worked
        assert "example.com" in output_df["domain"].values
        assert "test.com" in output_df["domain"].values
        assert "demo.com" in output_df["domain"].values

    def test_csv_data_loading_all_columns(self, temp_udf_project):
        """Test that CSV loading includes all columns, not just the first one.

        Regression test for: CSV data loading only loading 'name' column
        """
        # Create simple pipeline that just loads and exports data
        simple_pipeline_text = f"""
SOURCE customers TYPE CSV PARAMS {{
  "path": "{temp_udf_project['data_file']}",
  "has_header": true
}};

LOAD raw_customers FROM customers;

EXPORT SELECT * FROM raw_customers
TO "{temp_udf_project['output_dir']}/raw_export.csv"
TYPE CSV
OPTIONS {{ "header": true }};
"""

        parser = Parser(simple_pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        executor = LocalExecutor(project_dir=temp_udf_project["project_dir"])
        result = executor.execute(plan)

        assert result["status"] == "success"

        # Check output file exists
        output_file = f"{temp_udf_project['output_dir']}/raw_export.csv"
        assert os.path.exists(output_file)

        # Verify all columns are present
        output_df = pd.read_csv(output_file)
        expected_columns = {"id", "name", "email", "notes"}
        assert set(output_df.columns) == expected_columns

        # Verify data integrity
        assert len(output_df) == 3
        assert "alice@example.com" in output_df["email"].values
        assert "VIP customer" in output_df["notes"].values

    def test_source_definition_step_handling(self, temp_udf_project):
        """Test that source definition steps are handled correctly.

        Regression test for: "Unknown connector type: source_definition" error
        """
        with open(temp_udf_project["pipeline_file"], "r") as f:
            pipeline_text = f.read()

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Check that source definition step has correct structure
        source_steps = [
            step for step in plan if step.get("type") == "source_definition"
        ]
        assert len(source_steps) > 0

        source_step = source_steps[0]
        # Should have source_connector_type, not just connector_type
        assert "source_connector_type" in source_step
        assert source_step["source_connector_type"] == "CSV"

        # Execute to verify no "unknown connector type" errors
        executor = LocalExecutor(project_dir=temp_udf_project["project_dir"])
        result = executor.execute(plan)

        assert result["status"] == "success"

    def test_udf_query_processing(self, temp_udf_project):
        """Test that PYTHON_FUNC calls are processed correctly.

        Regression test for: UDF queries not being processed before execution
        """
        # Create pipeline with UDF call
        udf_pipeline_text = f"""
SOURCE customers TYPE CSV PARAMS {{
  "path": "{temp_udf_project['data_file']}",
  "has_header": true
}};

LOAD raw_customers FROM customers;

CREATE TABLE test_udf AS
SELECT
  name,
  PYTHON_FUNC("python_udfs.text_processing.capitalize_words", name) AS processed_name
FROM raw_customers;
"""

        parser = Parser(udf_pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        executor = LocalExecutor(project_dir=temp_udf_project["project_dir"])

        # Execute and verify UDF processing occurred
        result = executor.execute(plan)
        assert result["status"] == "success"

        # Query the result to verify UDF worked
        query_result = executor.duckdb_engine.execute_query("SELECT * FROM test_udf")
        rows = query_result.fetchall()

        # Should have processed names with proper capitalization
        processed_names = [row[1] for row in rows]  # Second column is processed_name
        assert "Alice Johnson" in processed_names
        assert "Bob Smith" in processed_names
        assert "Charlie Brown" in processed_names


@pytest.mark.integration
class TestUDFSystemIntegration:
    """Integration tests for the complete UDF system."""

    def test_full_udf_pipeline_execution(self, temp_udf_project):
        """Test complete UDF pipeline execution end-to-end.

        This is a comprehensive test that exercises the entire UDF system
        that was fixed.
        """
        # Change to project directory to simulate real usage
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_udf_project["project_dir"])

            # Parse pipeline
            with open("pipelines/test_udf_regression.sf", "r") as f:
                pipeline_text = f.read()

            parser = Parser(pipeline_text)
            pipeline = parser.parse()

            # Create plan
            planner = Planner()
            plan = planner.create_plan(pipeline)

            # Execute with project-based executor
            executor = LocalExecutor(project_dir=".")
            result = executor.execute(plan)

            # Verify complete success
            assert result["status"] == "success"

            # Verify all expected outputs
            assert os.path.exists("output/processed_results.csv")

            # Verify data quality
            output_df = pd.read_csv("output/processed_results.csv")
            assert len(output_df) == 3
            assert set(output_df.columns) == {
                "id",
                "name",
                "formatted_name",
                "email",
                "domain",
                "notes",
            }

            # Verify UDF transformations
            assert "Alice Johnson" in output_df["formatted_name"].values
            assert "example.com" in output_df["domain"].values

        finally:
            os.chdir(original_cwd)
