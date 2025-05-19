"""Integration tests for variable substitution in export paths."""

import os
import subprocess
import tempfile

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser


def run_pipeline(pipeline_path, profile="dev", variables=None):
    """Run a pipeline using the CLI."""
    cmd = ["sqlflow", "pipeline", "run", pipeline_path, "--profile", profile]
    if variables:
        vars_str = " ".join([f"{k}={v}" for k, v in variables.items()])
        cmd.extend(["--vars", f"{vars_str}"])

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result


@pytest.fixture
def test_environment():
    """Create a temporary test environment with test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create directories
        data_dir = os.path.join(temp_dir, "data")
        output_dir = os.path.join(temp_dir, "output")
        pipeline_dir = os.path.join(temp_dir, "pipelines")
        profile_dir = os.path.join(temp_dir, "profiles")

        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(pipeline_dir, exist_ok=True)
        os.makedirs(profile_dir, exist_ok=True)

        # Create a sample CSV file with real data
        data_file = os.path.join(data_dir, "sample.csv")
        with open(data_file, "w") as f:
            f.write("id,name,value\n")  # Header
            f.write("1,Alpha,100\n")
            f.write("2,Beta,200\n")
            f.write("3,Gamma,300\n")

        # Ensure the file exists and has data
        assert os.path.exists(data_file), f"Data file not created: {data_file}"
        with open(data_file, "r") as f:
            content = f.read()
            assert "Alpha" in content, "Data file content is incorrect"

        # Create a test profile with variables
        profile_file = os.path.join(profile_dir, "dev.yml")
        with open(profile_file, "w") as f:
            f.write(
                """
engines:
  duckdb:
    mode: memory
    memory_limit: 2GB
variables:
  region: test-region
  date: 2023-10-31
  env: test-env
            """
            )

        # Create a test pipeline with variable substitution in export path
        pipeline_file = os.path.join(pipeline_dir, "test_variable_export.sf")
        with open(pipeline_file, "w") as f:
            f.write(
                f"""
-- Test pipeline for variable substitution in export paths
SOURCE test_source TYPE CSV PARAMS {{
    "path": "{data_file}",
    "has_header": true
}};

LOAD data FROM test_source;

-- Basic transformation
CREATE TABLE processed_data AS
SELECT 
    id,
    name,
    value,
    value * 2 AS double_value
FROM data;

-- Export with variable in path
EXPORT SELECT * FROM processed_data
TO "{output_dir}/${{region|default}}_${{date|2023-10-30}}_${{env|prod}}_report.csv"
TYPE CSV
OPTIONS {{
    "header": true
}};
            """
            )

        yield {
            "temp_dir": temp_dir,
            "data_dir": data_dir,
            "output_dir": output_dir,
            "pipeline_dir": pipeline_dir,
            "profile_dir": profile_dir,
            "data_file": data_file,
            "pipeline_file": pipeline_file,
            "profile_file": profile_file,
        }


@pytest.mark.integration
def test_programmatic_variable_substitution(test_environment):
    """Test variable substitution in export paths using the programmatic API."""
    # Print the data file contents to verify it exists and has data
    print(f"Data file: {test_environment['data_file']}")
    with open(test_environment["data_file"], "r") as f:
        print(f"Data file contents:\n{f.read()}")

    # Parse the pipeline
    print(f"Pipeline file: {test_environment['pipeline_file']}")
    with open(test_environment["pipeline_file"], "r") as f:
        pipeline_text = f.read()
        print(f"Pipeline contents:\n{pipeline_text}")

    parser = Parser(pipeline_text)
    pipeline = parser.parse()

    # Create a plan with variables
    variables = {"region": "us-west", "date": "2023-11-01", "env": "dev"}
    planner = Planner()
    operations = planner.create_plan(pipeline, variables)

    # Execute the plan
    executor = LocalExecutor()
    executor.profile = {"variables": variables}
    results = executor.execute(operations, variables=variables)

    # Print debug information
    print(f"Operations: {len(operations)}")
    for idx, op in enumerate(operations):
        print(f"Operation {idx + 1}: {op['id']} ({op.get('type', 'unknown')})")
        if op.get("type") == "export":
            print(f"  Destination: {op['query'].get('destination_uri', 'N/A')}")
            print(
                f"  After substitution should be: {test_environment['output_dir']}/us-west_2023-11-01_dev_report.csv"
            )

    # Print results
    print(f"Results status: {results.get('status', 'N/A')}")
    print(f"Results error: {results.get('error', 'N/A')}")

    # Check output directory contents
    output_dir = test_environment["output_dir"]
    print(f"Output directory: {output_dir}")
    if os.path.exists(output_dir):
        print(f"Contents: {os.listdir(output_dir)}")
    else:
        print("Output directory doesn't exist")

    # Check that the output file exists with substituted variables
    expected_file = os.path.join(
        test_environment["output_dir"], "us-west_2023-11-01_dev_report.csv"
    )
    assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"

    # Verify file contents
    with open(expected_file, "r") as f:
        content = f.read()
        print(f"Output file contents:\n{content}")
        assert "Alpha" in content, "Output file doesn't contain expected data"


@pytest.mark.integration
def test_variable_substitution_with_defaults(test_environment):
    """Test that default values are used when variables aren't provided."""
    # Parse the pipeline
    with open(test_environment["pipeline_file"], "r") as f:
        pipeline_text = f.read()

    parser = Parser(pipeline_text)
    pipeline = parser.parse()

    # Create a plan with partial variables (missing some)
    variables = {
        "region": "eu-central"
        # Missing date and env - should use defaults
    }
    planner = Planner()
    operations = planner.create_plan(pipeline, variables)

    # Execute the plan
    executor = LocalExecutor()
    executor.profile = {"variables": variables}
    executor.execute(operations, variables=variables)

    # Check that the output file exists with substituted variables and defaults
    expected_file = os.path.join(
        test_environment["output_dir"], "eu-central_2023-10-30_prod_report.csv"
    )
    assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"


@pytest.mark.integration
def test_nested_directory_variable_substitution(test_environment):
    """Test variable substitution in nested directory paths."""
    # Create a pipeline with nested directories in the export path
    nested_pipeline_file = os.path.join(
        test_environment["pipeline_dir"], "nested_variable_export.sf"
    )
    with open(nested_pipeline_file, "w") as f:
        f.write(
            f"""
-- Test pipeline for variable substitution in nested directory paths
SOURCE test_source TYPE CSV PARAMS {{
    "path": "{test_environment['data_file']}",
    "has_header": true
}};

LOAD data FROM test_source;

-- Export with variables in nested directory path
EXPORT SELECT * FROM data
TO "{test_environment['output_dir']}/${{env|prod}}/${{region|default}}/${{date|2023-10-30}}_report.csv"
TYPE CSV
OPTIONS {{
    "header": true
}};
        """
        )

    # Parse the pipeline
    with open(nested_pipeline_file, "r") as f:
        pipeline_text = f.read()

    parser = Parser(pipeline_text)
    pipeline = parser.parse()

    # Create a plan with variables
    variables = {"region": "asia-east", "date": "2023-11-02", "env": "staging"}
    planner = Planner()
    operations = planner.create_plan(pipeline, variables)

    # Execute the plan
    executor = LocalExecutor()
    executor.profile = {"variables": variables}
    executor.execute(operations, variables=variables)

    # Check that the output file exists with substituted variables in nested directories
    expected_file = os.path.join(
        test_environment["output_dir"], "staging/asia-east/2023-11-02_report.csv"
    )
    assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"
