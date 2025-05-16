import os
import tempfile

import yaml
from typer.testing import CliRunner

from sqlflow.cli.main import app

PIPELINE = """
SET run_date = "${run_date|2023-10-25}";

SOURCE sales TYPE CSV PARAMS {
    "path": "data/sales_${run_date}.csv",
    "has_header": true
};

SOURCE customers TYPE CSV PARAMS {
    "path": "data/customers.csv",
    "has_header": true
};

EXPORT SELECT * FROM customers
TO "output/customers_${run_date}.csv"
TYPE CSV
OPTIONS {
    "header": true,
    "delimiter": ","
};
"""


def test_pipeline_multiline_json_compile_and_run():
    """Test that multiline JSON in pipeline directives is correctly handled."""
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup project structure
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)

        # Create sample data directory for mocking source files
        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)

        # Create output directory
        output_dir = os.path.join(tmpdir, "output")
        os.makedirs(output_dir)

        # Create mock data files to avoid file not found errors
        with open(os.path.join(data_dir, "customers.csv"), "w") as f:
            f.write("customer_id,name,email\n1,Test User,test@example.com")

        # Create pipeline file
        pipeline_path = os.path.join(pipelines_dir, "multi_json.sf")
        with open(pipeline_path, "w") as f:
            f.write(PIPELINE)
            print(f"Created pipeline file at {pipeline_path}")

        # Create minimal profiles/dev.yml for profile-driven config
        profiles_dir = os.path.join(tmpdir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        dev_profile = {"engines": {"duckdb": {"mode": "memory"}}}
        with open(os.path.join(profiles_dir, "dev.yml"), "w") as f:
            yaml.dump(dev_profile, f)

        # Change to the temporary directory for all commands
        os.chdir(tmpdir)

        try:
            # Compile with variable
            print("\nRunning compile command...")
            result = runner.invoke(
                app,
                [
                    "pipeline",
                    "compile",
                    "multi_json",
                    "--vars",
                    '{"run_date": "2025-05-16"}',
                ],
            )
            stdout = "\n".join(
                line
                for line in result.stdout.splitlines()
                if not line.startswith("DEBUG:")
            )
            print(f"\nCompile output:\n{stdout}")
            assert (
                result.exit_code == 0
            ), f"Compilation failed with exit code {result.exit_code}\nOutput:\n{stdout}"
            assert (
                "Compiled pipeline 'multi_json'" in stdout
            ), "Pipeline compilation success message not found"
            assert "Found" in stdout, "Pipeline step count not found in output"

            # Run with variable
            print("\nRunning run command...")
            result = runner.invoke(
                app,
                [
                    "pipeline",
                    "run",
                    "multi_json",
                    "--vars",
                    '{"run_date": "2025-05-16"}',
                ],
            )
            stdout = "\n".join(
                line
                for line in result.stdout.splitlines()
                if not line.startswith("DEBUG:")
            )
            print(f"\nRun output:\n{stdout}")
            assert (
                result.exit_code == 0
            ), f"Run failed with exit code {result.exit_code}\nOutput:\n{stdout}"
            assert "Running pipeline" in stdout, "Pipeline run message not found"
            assert "2025-05-16" in stdout, "Run date variable not found in output"
        finally:
            # Change back to original directory no matter what
            os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
