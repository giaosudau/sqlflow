"""Integration tests for CLI validation commands.

Tests the actual validation behavior end-to-end with real implementations.
"""

import os
import subprocess
import tempfile
from pathlib import Path

import pytest


class TestValidationIntegration:
    """Integration tests for pipeline validation via CLI."""

    def setup_method(self):
        """Set up a temporary test project for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.project_dir = Path(self.temp_dir) / "test_project"

        # Create project structure
        self.project_dir.mkdir()
        (self.project_dir / "pipelines").mkdir()
        (self.project_dir / "profiles").mkdir()
        (self.project_dir / "data").mkdir()

        # Create a basic profile
        profile_content = """
dev:
  engines:
    duckdb:
      mode: memory
"""
        (self.project_dir / "profiles" / "duckdb.yaml").write_text(profile_content)

        # Change to project directory for CLI commands
        self.original_cwd = os.getcwd()
        os.chdir(self.project_dir)

    def teardown_method(self):
        """Clean up after each test."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.temp_dir)

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

    def test_validate_valid_pipeline_passes(self):
        """Test that validate command passes for a valid pipeline."""
        # Create a valid pipeline
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users_csv;

EXPORT SELECT * FROM users_table
TO "output/users.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        self.create_pipeline_file("valid_pipeline", pipeline_content)

        # Run validation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "valid_pipeline"]
        )

        # Should pass validation
        assert exit_code == 0
        assert "validation passed" in stdout
        assert stderr == ""

    def test_validate_table_reference_error_fails(self):
        """Test that validate command fails for table reference errors (typos)."""
        # Create pipeline with table reference typo
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users_csv;

-- This has a typo: users_table_failed instead of users_table
EXPORT SELECT * FROM users_table_failed
TO "output/users.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        self.create_pipeline_file("invalid_pipeline", pipeline_content)

        # Run validation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "invalid_pipeline"]
        )

        # Should fail validation
        assert exit_code == 1
        assert "validation failed" in stderr
        assert "users_table_failed" in stderr
        assert "might not be defined" in stderr
        assert "Did you mean 'users_table'" in stderr
        assert "Available tables: users_table" in stderr

    def test_validate_external_table_warning_passes(self):
        """Test that validate command passes with warnings for potentially external tables."""
        # Create pipeline that references external table (not similar to any defined table)
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users_csv;

-- This references external_system_table which could be legitimate
EXPORT SELECT * FROM external_system_table
TO "output/external.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        self.create_pipeline_file("external_ref_pipeline", pipeline_content)

        # Run validation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "external_ref_pipeline"]
        )

        # Should pass with warnings (external tables might be legitimate)
        assert exit_code == 0
        assert "validation passed" in stdout

    def test_validate_syntax_error_fails(self):
        """Test that validate command fails for syntax errors."""
        # Create pipeline with syntax error
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv"
  "has_header": true  -- Missing comma
};

LOAD users_table FROM users_csv;
"""
        self.create_pipeline_file("syntax_error_pipeline", pipeline_content)

        # Run validation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "syntax_error_pipeline"]
        )

        # Should fail validation
        assert exit_code == 1
        assert "validation failed" in stderr

    def test_validate_undefined_source_fails(self):
        """Test that validate command fails for undefined SOURCE references."""
        # Create pipeline with undefined source
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

-- This references undefined_source which doesn't exist
LOAD users_table FROM undefined_source;
"""
        self.create_pipeline_file("undefined_source_pipeline", pipeline_content)

        # Run validation
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "undefined_source_pipeline"]
        )

        # Should fail validation
        assert exit_code == 1
        assert "validation failed" in stderr
        assert "undefined_source" in stderr

    def test_validate_all_pipelines(self):
        """Test validating all pipelines in a project."""
        # Create multiple pipelines - some valid, some invalid
        valid_pipeline = """
SOURCE data_csv TYPE CSV PARAMS {
  "path": "data/data.csv",
  "has_header": true
};

LOAD data_table FROM data_csv;

EXPORT SELECT * FROM data_table
TO "output/data.csv"
TYPE CSV
OPTIONS { "header": true };
"""

        invalid_pipeline = """
SOURCE data_csv TYPE CSV PARAMS {
  "path": "data/data.csv",
  "has_header": true
};

LOAD data_table FROM data_csv;

-- Table name typo that's very similar (should fail validation)
EXPORT SELECT * FROM data_tabel
TO "output/data.csv"
TYPE CSV
OPTIONS { "header": true };
"""

        self.create_pipeline_file("valid_one", valid_pipeline)
        self.create_pipeline_file("invalid_one", invalid_pipeline)

        # Run validation on all pipelines (no pipeline name specified)
        exit_code, stdout, stderr = self.run_sqlflow_command(["pipeline", "validate"])

        # Should fail because one pipeline is invalid
        assert exit_code == 1
        assert "validation failed" in stderr  # Should show validation failure
        assert "data_tabel" in stderr  # The typo should be mentioned

    def test_compile_vs_validate_consistency(self):
        """Test that compile and validate give consistent results."""
        # Create pipeline with table reference error
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users_csv;

EXPORT SELECT * FROM users_table_typo
TO "output/users.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        self.create_pipeline_file("consistency_test", pipeline_content)

        # Run validation
        validate_exit_code, validate_stdout, validate_stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "consistency_test"]
        )

        # Run compilation
        compile_exit_code, compile_stdout, compile_stderr = self.run_sqlflow_command(
            ["pipeline", "compile", "consistency_test"]
        )

        # Both should fail for the same reason
        assert validate_exit_code == 1
        assert compile_exit_code == 1
        assert "users_table_typo" in validate_stderr
        assert "users_table_typo" in compile_stderr

    def test_validate_with_verbose_flag(self):
        """Test validate command with verbose flag shows detailed information."""
        # Create pipeline with validation error
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users_csv;

EXPORT SELECT * FROM users_table_wrong
TO "output/users.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        self.create_pipeline_file("verbose_test", pipeline_content)

        # Run validation with verbose flag
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "verbose_test", "--verbose"]
        )

        # Should fail with detailed suggestions
        assert exit_code == 1
        assert "Did you mean 'users_table'" in stderr
        assert "Available tables:" in stderr
        assert "Available sources:" in stderr

    def test_validate_with_quiet_flag(self):
        """Test validate command with quiet flag reduces output."""
        # Create valid pipeline
        pipeline_content = """
SOURCE users_csv TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users_csv;

EXPORT SELECT * FROM users_table
TO "output/users.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        self.create_pipeline_file("quiet_test", pipeline_content)

        # Run validation with quiet flag
        exit_code, stdout, stderr = self.run_sqlflow_command(
            ["pipeline", "validate", "quiet_test", "--quiet"]
        )

        # Should pass with minimal output
        assert exit_code == 0
        assert stdout.strip() == ""  # Quiet mode should suppress success message
        assert stderr == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
