import os
import tempfile

import duckdb
import yaml
from typer.testing import CliRunner

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


def test_persistent_tables_in_production_mode():
    """Test that transform tables are persisted in DuckDB file in production mode."""
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup project structure
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)
        # Output dir is not strictly needed for this version of the test, but good to have
        output_dir = os.path.join(tmpdir, "output")
        os.makedirs(output_dir)

        # Create dummy CSV data
        dummy_csv_file_path = os.path.join(data_dir, "input_for_test.csv")
        with open(dummy_csv_file_path, "w", encoding="utf-8") as f:
            f.write("header_col\\ncsv_data_value\\n")

        # Create pipeline file
        pipeline_path = os.path.join(pipelines_dir, "persist_test.sf")
        with open(pipeline_path, "w", encoding="utf-8") as f:
            f.write(PIPELINE)

        # Create profiles/production.yml (no variables needed for this pipeline)
        profiles_dir = os.path.join(tmpdir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        db_path = os.path.join(tmpdir, "prod.db")
        prod_profile = {
            "engines": {
                "duckdb": {
                    "mode": "persistent",
                    "path": db_path,
                    "memory_limit": "2GB",
                }
            }
            # No 'variables' key as the pipeline is fully hardcoded
        }
        with open(
            os.path.join(profiles_dir, "production.yml"), "w", encoding="utf-8"
        ) as f:
            yaml.dump(prod_profile, f)

        # Store the original CWD and change to tmpdir for the run
        original_cwd = os.getcwd()
        os.chdir(tmpdir)
        db_conn_for_assert = None  # Ensure con is defined for finally block
        try:
            result = runner.invoke(
                app,
                [
                    "pipeline",
                    "run",
                    "persist_test",  # Name of the .sf file without extension
                    "--profile",
                    "production",
                ],
            )
            assert result.exit_code == 0, f"Pipeline run failed: {result.stdout}"
            assert os.path.exists(db_path), f"DuckDB file not created: {db_path}"

            db_conn_for_assert = duckdb.connect(database=db_path, read_only=True)
            tables = set(
                row[0] for row in db_conn_for_assert.execute("SHOW TABLES;").fetchall()
            )
            assert "my_transform" in tables, f"Table 'my_transform' not in {tables}"

            rows = db_conn_for_assert.execute("SELECT * FROM my_transform;").fetchall()
            assert rows == [(77,)], f"Data mismatch in 'my_transform': {rows}"

        finally:
            if db_conn_for_assert:
                db_conn_for_assert.close()
            os.chdir(original_cwd)  # Change back to original CWD
