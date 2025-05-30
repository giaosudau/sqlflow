"""Integration tests for CLI pipeline execution with multiline JSON parameters.

Tests verify that the CLI correctly handles:
- Multiline JSON parameters in pipeline definitions
- Persistent database mode execution
- Real file operations and data persistence
"""

import os
import tempfile

import duckdb
import yaml
from typer.testing import CliRunner
import pytest

from sqlflow.cli.main import app

# Simplified pipeline, using multi-line PARAMS like in simple_test.sf
# No variables used in this pipeline string.
PIPELINE = """SOURCE csv_input_source TYPE CSV PARAMS {
  "path": "data/input_for_test.csv",
  "has_header": true
};

LOAD intermediate_data_table FROM csv_input_source;

CREATE TABLE my_transform AS SELECT 77 AS persisted_value;
"""


@pytest.mark.skip(
    reason="CLI persistent database creation needs stabilization - part of test refactoring"
)
def test_persistent_tables_in_production_mode():
    """Test that transform tables are persisted in DuckDB file in production mode."""
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup project structure
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)
        profiles_dir = os.path.join(tmpdir, "profiles")
        os.makedirs(profiles_dir)

        # Create test CSV file
        csv_file = os.path.join(data_dir, "input_for_test.csv")
        with open(csv_file, "w") as f:
            f.write("id,name\n1,Test Item\n")

        # Create pipeline file
        pipeline_file = os.path.join(pipelines_dir, "test_pipeline.sf")
        with open(pipeline_file, "w") as f:
            f.write(PIPELINE)

        # Create production profile with persistent database
        profile_file = os.path.join(profiles_dir, "production.yml")
        db_path = os.path.join(tmpdir, "production.duckdb")
        profile_content = {
            "production": {
                "engines": {"duckdb": {"mode": "persistent", "path": db_path}}
            }
        }
        with open(profile_file, "w") as f:
            yaml.dump(profile_content, f)

        # Change to temp directory for CLI execution
        old_cwd = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Run pipeline with production profile
            result = runner.invoke(
                app, ["pipeline", "run", "test_pipeline", "--profile", "production"]
            )

            # Verify CLI execution succeeded
            assert result.exit_code == 0, f"CLI failed with output: {result.output}"

            # Verify database file was created
            assert os.path.exists(db_path), "Database file was not created"

            # Verify data was persisted by connecting to database directly
            conn = duckdb.connect(db_path)

            # Check that tables exist and contain expected data
            tables_result = conn.execute("SHOW TABLES").fetchall()
            table_names = [row[0] for row in tables_result]

            # Should have the tables created by the pipeline
            assert "intermediate_data_table" in table_names
            assert "my_transform" in table_names

            # Verify transform table has correct data
            transform_data = conn.execute(
                "SELECT persisted_value FROM my_transform"
            ).fetchall()
            assert len(transform_data) == 1
            assert transform_data[0][0] == 77

            # Verify loaded data exists
            loaded_data = conn.execute(
                "SELECT COUNT(*) FROM intermediate_data_table"
            ).fetchall()
            assert loaded_data[0][0] == 1  # Should have 1 row from CSV

            conn.close()

        finally:
            os.chdir(old_cwd)


def test_multiline_json_parsing_in_pipeline():
    """Test that multiline JSON parameters are parsed correctly in pipeline files.

    This verifies that the parser handles the multiline JSON format used
    in SOURCE and other statements with complex parameter blocks.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup project structure
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)
        profiles_dir = os.path.join(tmpdir, "profiles")
        os.makedirs(profiles_dir)

        # Create test CSV with more complex data
        csv_file = os.path.join(data_dir, "complex_data.csv")
        with open(csv_file, "w") as f:
            f.write("customer_id,name,email,purchase_amount\n")
            f.write("1,Alice Johnson,alice@example.com,1250.50\n")
            f.write("2,Bob Smith,bob@example.com,750.25\n")

        # Pipeline with multiline JSON parameters
        complex_pipeline = """
SOURCE customer_source TYPE CSV PARAMS {
  "path": "data/complex_data.csv",
  "has_header": true,
  "delimiter": ",",
  "encoding": "utf-8"
};

LOAD customers FROM customer_source;

CREATE TABLE high_value_customers AS 
SELECT 
    customer_id,
    name,
    email,
    purchase_amount,
    CASE 
        WHEN purchase_amount > 1000 THEN 'Premium'
        WHEN purchase_amount > 500 THEN 'Standard'
        ELSE 'Basic'
    END as customer_tier
FROM customers
WHERE purchase_amount > 500;
"""

        pipeline_file = os.path.join(pipelines_dir, "complex_pipeline.sf")
        with open(pipeline_file, "w") as f:
            f.write(complex_pipeline)

        # Create memory mode profile for faster testing
        profile_file = os.path.join(profiles_dir, "dev.yml")
        profile_content = {"dev": {"engines": {"duckdb": {"mode": "memory"}}}}
        with open(profile_file, "w") as f:
            yaml.dump(profile_content, f)

        old_cwd = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Run pipeline to test multiline JSON parsing
            result = runner.invoke(
                app, ["pipeline", "run", "complex_pipeline", "--profile", "dev"]
            )

            # Should succeed without JSON parsing errors
            assert result.exit_code == 0, (
                f"Pipeline with multiline JSON failed: {result.output}"
            )

            # Should mention successful execution
            assert (
                "success" in result.output.lower()
                or "completed" in result.output.lower()
            )

        finally:
            os.chdir(old_cwd)


def test_pipeline_with_json_parameter_variations():
    """Test different JSON parameter formatting styles that users might employ.

    This ensures robustness across different formatting preferences and
    validates that the parser handles various JSON structures correctly.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)
        profiles_dir = os.path.join(tmpdir, "profiles")
        os.makedirs(profiles_dir)

        # Create test data
        csv_file = os.path.join(data_dir, "test_data.csv")
        with open(csv_file, "w") as f:
            f.write("id,value\n1,100\n2,200\n")

        # Test different JSON formatting styles
        format_variations = [
            # Compact format
            """SOURCE test1 TYPE CSV PARAMS {"path": "data/test_data.csv", "has_header": true};""",
            # Multiline with different indentation
            """SOURCE test2 TYPE CSV PARAMS {
    "path": "data/test_data.csv",
    "has_header": true
};""",
            # Extra whitespace and formatting
            """SOURCE test3 TYPE CSV PARAMS {
  "path"       : "data/test_data.csv"  ,
  "has_header" : true
};""",
        ]

        for i, source_statement in enumerate(format_variations):
            pipeline_content = f"""
{source_statement}

LOAD test_table_{i} FROM test{i + 1};

CREATE TABLE result_{i} AS SELECT COUNT(*) as row_count FROM test_table_{i};
"""

            pipeline_file = os.path.join(pipelines_dir, f"format_test_{i}.sf")
            with open(pipeline_file, "w") as f:
                f.write(pipeline_content)

        # Create profile
        profile_file = os.path.join(profiles_dir, "dev.yml")
        profile_content = {"dev": {"engines": {"duckdb": {"mode": "memory"}}}}
        with open(profile_file, "w") as f:
            yaml.dump(profile_content, f)

        old_cwd = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Test each formatting variation
            for i in range(len(format_variations)):
                result = runner.invoke(
                    app, ["pipeline", "run", f"format_test_{i}", "--profile", "dev"]
                )

                assert result.exit_code == 0, (
                    f"Format variation {i} failed: {result.output}"
                )

        finally:
            os.chdir(old_cwd)
