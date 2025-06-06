"""Integration tests for conditional pipeline validation.

Tests the validation behavior for conditional pipelines with real implementations,
covering the bug fix for table reference validation in conditional blocks.

Following testing standards:
- Test behavior, not implementation
- Use real implementations without mocking
- Test multiple components together (parser, planner, validator)
- Cover both positive and error scenarios
- Serve as documentation for conditional pipeline validation
"""

import os
import subprocess
import tempfile
from pathlib import Path


class TestConditionalPipelineValidation:
    """Integration tests for conditional pipeline validation via CLI.

    These tests verify that the validation logic correctly handles:
    1. Tables that exist in conditional blocks (should not fail compilation)
    2. Tables with typos that have close matches (should fail compilation)
    3. Tables that don't exist and don't have close matches (should warn but not fail)
    """

    def setup_method(self):
        """Set up a temporary test project for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.project_dir = Path(self.temp_dir) / "test_project"

        # Create project structure
        self.project_dir.mkdir()
        (self.project_dir / "pipelines").mkdir()
        (self.project_dir / "profiles").mkdir()
        (self.project_dir / "data").mkdir()

        # Create test data files
        self._create_test_data_files()

        # Create a basic profile
        profile_content = """
engines:
  duckdb:
    mode: memory

variables:
  env: dev
"""
        (self.project_dir / "profiles" / "dev.yml").write_text(profile_content)

        # Change to project directory for CLI commands
        self.original_cwd = os.getcwd()
        os.chdir(self.project_dir)

    def teardown_method(self):
        """Clean up after each test."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.temp_dir)

    def _create_test_data_files(self):
        """Create test CSV data files."""
        # Create customers.csv
        customers_content = """customer_id,name,email,region,signup_date,account_type
C001,John Smith,john.smith@example.com,us-east,2022-01-15,premium
C002,Maria Garcia,maria.garcia@example.com,eu,2022-03-22,standard
C003,Wei Zhang,wei.zhang@example.com,asia,2021-11-05,premium
"""
        (self.project_dir / "data" / "customers.csv").write_text(customers_content)

        # Create sales.csv
        sales_content = """order_id,customer_id,product_id,quantity,price,order_date,region,channel
O001,C001,P001,1,699.99,2023-10-01,us-east,online
O002,C002,P002,1,1399.99,2023-10-01,eu,online
O003,C003,P003,2,149.99,2023-10-01,asia,store
"""
        (self.project_dir / "data" / "sales.csv").write_text(sales_content)

    def run_sqlflow_command(self, args: list) -> tuple[int, str, str]:
        """Run a sqlflow CLI command and return exit code, stdout, stderr."""
        import os
        import shutil
        import sys

        # Use the same approach as run_all_examples.sh
        # Check for SQLFLOW_OVERRIDE_PATH environment variable first
        sqlflow_path = os.environ.get("SQLFLOW_OVERRIDE_PATH")

        if not sqlflow_path:
            # Try different locations for SQLFlow (same as examples script)
            repo_root = Path(__file__).parent.parent.parent.parent
            possible_paths = [
                repo_root / ".venv" / "bin" / "sqlflow",  # Local development with venv
                shutil.which("sqlflow"),  # System PATH (CI environments)
                "/usr/local/bin/sqlflow",  # Common system location
                Path.home() / ".local" / "bin" / "sqlflow",  # User-local installation
            ]

            for path in possible_paths:
                if path and Path(path).exists() and Path(path).is_file():
                    sqlflow_path = str(path)
                    break

        if sqlflow_path:
            cmd = [sqlflow_path] + args
        else:
            # Fallback to python -m approach (for development environments)
            try:
                # Try importing to see if module is available
                cmd = [sys.executable, "-m", "sqlflow.cli.main"] + args
            except ImportError:
                # Last resort: assume it's in PATH as 'sqlflow'
                cmd = ["sqlflow"] + args

        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=self.project_dir
        )
        return result.returncode, result.stdout, result.stderr

    def create_pipeline_file(self, name: str, content: str):
        """Create a pipeline file in the test project."""
        pipeline_path = self.project_dir / "pipelines" / f"{name}.sf"
        pipeline_path.write_text(content)
        return pipeline_path

    def test_conditional_pipeline_with_existing_tables_should_pass_compilation(self):
        """Test that conditional pipelines with tables that exist in conditional blocks pass compilation.

        This is the main test case for the bug fix. Previously, this would fail with:
        "Table 'sales_raw' not found. Did you mean 'sales_raw'?" - which was contradictory.
        Now it should pass compilation successfully.
        """
        # Create a conditional pipeline similar to the one that had the bug
        pipeline_content = """
-- Define data sources
SOURCE customers_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

SOURCE sales_source TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

-- Conditional loading based on environment
IF ${env} == 'production' THEN
    LOAD customers FROM customers_source;
    LOAD sales FROM sales_source;
    
    CREATE TABLE processed_customers AS
    SELECT * FROM customers WHERE account_type = 'premium';
    
ELSE
    -- In development, load raw tables first
    LOAD customers_raw FROM customers_source;
    LOAD sales_raw FROM sales_source;
    
    -- Then create processed tables from raw
    CREATE TABLE customers AS
    SELECT * FROM customers_raw LIMIT 5;
    
    CREATE TABLE sales AS
    SELECT * FROM sales_raw LIMIT 10;  -- This should NOT fail validation
    
END IF;

-- Export results
EXPORT SELECT * FROM customers
TO "output/customers.csv"
TYPE CSV
OPTIONS {"header": true};
"""
        self.create_pipeline_file("conditional_existing_tables", pipeline_content)

        # Run compilation (which includes validation)
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "compile", "conditional_existing_tables", "--vars", "env=dev"]
        )

        # Should pass compilation - this is the main assertion for the bug fix
        assert exit_code == 0, f"Compilation failed. stdout: {stdout}, stderr: {stderr}"
        assert "Compilation successful" in stdout

        # Should not contain the contradictory error message
        assert "Table 'sales_raw' not found. Did you mean 'sales_raw'?" not in stdout
        assert "Table 'sales_raw' not found. Did you mean 'sales_raw'?" not in stderr

    def test_conditional_pipeline_with_table_typos_should_fail_compilation(self):
        """Test that conditional pipelines with actual table typos fail compilation.

        This ensures the bug fix doesn't break legitimate error detection.
        Tables with typos that have close matches should still fail compilation.
        """
        pipeline_content = """
SOURCE customers_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

SOURCE sales_source TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

IF ${env} == 'dev' THEN
    LOAD customers_raw FROM customers_source;
    LOAD sales_raw FROM sales_source;
    
    CREATE TABLE customers AS
    SELECT * FROM customers_raw LIMIT 5;
    
    -- This has a typo: sales_raw_typo instead of sales_raw
    CREATE TABLE sales AS
    SELECT * FROM sales_raw_typo LIMIT 10;  -- Should fail validation
    
END IF;
"""
        self.create_pipeline_file("conditional_table_typos", pipeline_content)

        # Run compilation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "compile", "conditional_table_typos", "--vars", "env=dev"]
        )

        # Should fail compilation due to the typo
        assert exit_code == 1
        assert "sales_raw_typo" in stdout or "sales_raw_typo" in stderr
        assert (
            "Did you mean 'sales_raw'" in stdout or "Did you mean 'sales_raw'" in stderr
        )

    def test_conditional_pipeline_with_external_tables_should_warn_but_pass(self):
        """Test that conditional pipelines with external table references warn but pass compilation.

        Tables that don't exist and don't have close matches might be legitimate external
        tables, so they should warn but not fail compilation.
        """
        pipeline_content = """
SOURCE customers_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

IF ${env} == 'dev' THEN
    LOAD customers_raw FROM customers_source;
    
    CREATE TABLE customers AS
    SELECT * FROM customers_raw LIMIT 5;
    
    -- This references an external table that might exist in the database
    CREATE TABLE enriched_customers AS
    SELECT c.*, ext.external_score
    FROM customers c
    LEFT JOIN external_analytics_table ext ON c.customer_id = ext.customer_id;
    
END IF;
"""
        self.create_pipeline_file("conditional_external_tables", pipeline_content)

        # Run compilation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "compile", "conditional_external_tables", "--vars", "env=dev"]
        )

        # Should pass compilation but may show warnings
        assert exit_code == 0
        assert "Compilation successful" in stdout

        # May contain warnings about external table, but should not fail
        if "external_analytics_table" in stdout or "external_analytics_table" in stderr:
            # If warning is shown, it should be a warning, not an error
            assert "might not be defined" in stdout or "might not be defined" in stderr

    def test_nested_conditional_pipeline_validation(self):
        """Test validation of nested conditional blocks with table references.

        This tests more complex conditional logic to ensure the fix works
        with nested IF/ELSE blocks.
        """
        pipeline_content = """
SOURCE customers_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

SOURCE sales_source TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

IF ${env} == 'production' THEN
    LOAD customers FROM customers_source;
    LOAD sales FROM sales_source;
    
    IF ${region} == 'us' THEN
        CREATE TABLE filtered_customers AS
        SELECT * FROM customers WHERE region LIKE 'us-%';
    ELSE
        CREATE TABLE filtered_customers AS
        SELECT * FROM customers WHERE region != 'us-east' AND region != 'us-west';
    END IF;
    
ELSE
    -- Development environment
    LOAD customers_raw FROM customers_source;
    LOAD sales_raw FROM sales_source;
    
    IF ${use_sample} == 'true' THEN
        CREATE TABLE temp_customers AS
        SELECT * FROM customers_raw LIMIT 2;
        
        CREATE TABLE temp_sales AS
        SELECT * FROM sales_raw LIMIT 5;  -- Should not fail validation
        
    ELSE
        CREATE TABLE temp_customers AS
        SELECT * FROM customers_raw LIMIT 10;
        
        CREATE TABLE temp_sales AS
        SELECT * FROM sales_raw LIMIT 20;  -- Should not fail validation
        
    END IF;
    
    -- These should reference the temp tables created above
    CREATE TABLE filtered_customers AS
    SELECT * FROM temp_customers;
    
END IF;
"""
        self.create_pipeline_file("nested_conditional", pipeline_content)

        # Test with development environment
        exit_code, stdout, stderr = self.run_sqlflow_command(
            [
                "pipeline",
                "compile",
                "nested_conditional",
                "--vars",
                "env=dev,use_sample=true",
            ]
        )

        # Should pass compilation
        assert exit_code == 0, f"Compilation failed. stdout: {stdout}, stderr: {stderr}"
        assert "Compilation successful" in stdout

    def test_conditional_pipeline_validation_command(self):
        """Test the validate command specifically with conditional pipelines.

        This tests the validate command (not compile) to ensure it also
        handles conditional table references correctly.
        """
        pipeline_content = """
SOURCE customers_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

SOURCE sales_source TYPE CSV PARAMS {
  "path": "data/sales.csv",
  "has_header": true
};

IF ${env} == 'dev' THEN
    LOAD customers_raw FROM customers_source;
    LOAD sales_raw FROM sales_source;
    
    CREATE TABLE processed_data AS
    SELECT c.customer_id, c.name, s.order_id
    FROM customers_raw c
    JOIN sales_raw s ON c.customer_id = s.customer_id;  -- Should not fail validation
    
END IF;
"""
        self.create_pipeline_file("validate_conditional", pipeline_content)

        # Run validation command
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "validate_conditional"]
        )

        # Should pass validation
        assert exit_code == 0, f"Validation failed. stdout: {stdout}, stderr: {stderr}"
        assert "validation passed" in stdout.lower() or "✅" in stdout

    def test_regression_conditional_table_validation_with_different_variable_values(
        self,
    ):
        """Regression test: Ensure the validation fix works with different variable values.

        This is a comprehensive regression test that validates the fix works correctly
        across different variable combinations that could trigger the bug.
        """
        pipeline_content = """
SOURCE data_source TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

IF ${environment|dev} == 'prod' THEN
    LOAD production_data FROM data_source;
    
    CREATE TABLE final_data AS
    SELECT * FROM production_data;
    
ELSE IF ${environment|dev} == 'staging' THEN
    LOAD staging_data FROM data_source;
    
    CREATE TABLE final_data AS
    SELECT * FROM staging_data;
    
ELSE
    LOAD dev_data FROM data_source;
    
    CREATE TABLE final_data AS
    SELECT * FROM dev_data;  -- This should not fail validation
    
END IF;
"""
        self.create_pipeline_file("regression_test", pipeline_content)

        # Test with various environment values including the default
        test_cases = [
            ("environment=prod", "prod"),
            ("environment=staging", "staging"),
            ("environment=dev", "dev"),
            ("", "dev"),  # Test default value
        ]

        for vars_arg, expected_env in test_cases:
            cmd_args = ["pipeline", "compile", "regression_test"]
            if vars_arg:
                cmd_args.extend(["--vars", vars_arg])

            exit_code, stdout, stderr = self.run_sqlflow_command(cmd_args)

            # Should pass compilation for all cases
            assert (
                exit_code == 0
            ), f"Compilation failed for {vars_arg} (expected env={expected_env}). stdout: {stdout}, stderr: {stderr}"
            assert "Compilation successful" in stdout

            # Should not contain the contradictory error message
            assert "not found. Did you mean" not in stdout
            assert "not found. Did you mean" not in stderr
