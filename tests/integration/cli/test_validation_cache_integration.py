"""Integration tests for validation caching functionality.

These tests verify the complete functionality of validation caching across
CLI commands, validation helpers, and cache management.

Follows testing standards:
- Real implementations, no mocking
- End-to-end scenarios
- Tests as documentation
- Real data flows
"""

import os
import tempfile
import time
from pathlib import Path

import click
import pytest

from sqlflow.cli.validation_cache import ValidationCache
from sqlflow.cli.validation_helpers import validate_pipeline_with_caching


class TestValidationCacheIntegration:
    """Test validation caching functionality end-to-end."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary project directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create project structure
            project_dir = Path(temp_dir) / "test_project"
            project_dir.mkdir()

            # Create profiles directory and dev profile
            profiles_dir = project_dir / "profiles"
            profiles_dir.mkdir()

            dev_profile = profiles_dir / "dev.yml"
            dev_profile.write_text(
                """
engines:
  duckdb:
    mode: memory
    path: ":memory:"
paths:
  pipelines: "pipelines"
  output: "output"
variables:
  data_dir: "data"
  output_dir: "output"
"""
            )

            # Create pipelines directory
            pipelines_dir = project_dir / "pipelines"
            pipelines_dir.mkdir()

            yield str(project_dir)

    @pytest.fixture
    def sample_pipelines_dir(self, temp_project_dir):
        """Create sample pipeline files for testing."""
        pipelines_dir = Path(temp_project_dir) / "pipelines"

        # Create data directory and required CSV files
        data_dir = Path(temp_project_dir) / "data"
        data_dir.mkdir(exist_ok=True)

        # Create sample data files
        customers_csv = data_dir / "customers.csv"
        customers_csv.write_text(
            "customer_id,name,updated_at\n1,John,2024-01-01\n2,Jane,2024-01-02\n"
        )

        orders_csv = data_dir / "orders.csv"
        orders_csv.write_text(
            "order_id,customer_id,amount,order_date\n1,1,100.00,2024-01-01\n2,2,200.00,2024-01-02\n"
        )

        products_csv = data_dir / "products.csv"
        products_csv.write_text(
            "product_id,name,price\n1,Product A,10.00\n2,Product B,20.00\n"
        )

        # Valid pipeline
        valid_pipeline = pipelines_dir / "valid_pipeline.sf"
        valid_pipeline.write_text(
            """
        SOURCE customers TYPE CSV PARAMS {
            "path": "${data_dir}/customers.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "customer_id",
            "cursor_field": "updated_at"
        };
        
        LOAD customers_table FROM customers;
        
        CREATE TABLE customer_summary AS
        SELECT 
            COUNT(*) as total_customers,
            'processed' as status
        FROM customers_table;
        """
        )

        # Invalid pipeline
        invalid_pipeline = pipelines_dir / "invalid_pipeline.sf"
        invalid_pipeline.write_text(
            """
        SOURCE customers TYPE CSV PARAMS {
            "path": "${data_dir}/customers.csv",
            "has_header": true,
            "sync_mode": "invalid_mode"
        };
        
        LOAD customers_table FROM customers;
        
        CREATE TABLE customer_summary AS
        SELECT 
            COUNT(*) as total_customers
        FROM nonexistent_table;
        """
        )

        # Complex valid pipeline
        complex_pipeline = pipelines_dir / "complex_pipeline.sf"
        complex_pipeline.write_text(
            """
        SOURCE orders TYPE CSV PARAMS {
            "path": "${data_dir}/orders.csv",
            "has_header": true,
            "sync_mode": "incremental",
            "primary_key": "order_id",
            "cursor_field": "order_date"
        };
        
        SOURCE products TYPE CSV PARAMS {
            "path": "${data_dir}/products.csv",
            "has_header": true,
            "sync_mode": "full_refresh"
        };
        
        LOAD orders_table FROM orders;
        LOAD products_table FROM products;
        
        CREATE TABLE order_analytics AS
        SELECT 
            COUNT(*) as total_orders,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value
        FROM orders_table;
        
        CREATE OR REPLACE TABLE product_summary AS
        SELECT 
            COUNT(*) as product_count,
            'active' as status
        FROM products_table;
        """
        )

        return str(pipelines_dir)

    def test_validation_cache_basic_functionality(
        self, temp_project_dir, sample_pipelines_dir
    ):
        """Test basic validation caching functionality."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            ValidationCache(temp_project_dir)
            valid_pipeline_path = os.path.join(
                sample_pipelines_dir, "valid_pipeline.sf"
            )

            # First validation - should not use cache
            start_time = time.time()
            errors1 = validate_pipeline_with_caching(
                valid_pipeline_path, temp_project_dir
            )
            first_duration = time.time() - start_time

            # Should have no errors for valid pipeline
            assert len(errors1) == 0

            # Second validation - should use cache
            start_time = time.time()
            errors2 = validate_pipeline_with_caching(
                valid_pipeline_path, temp_project_dir
            )
            second_duration = time.time() - start_time

            # Results should be identical
            assert len(errors2) == 0
            assert errors1 == errors2

            # Second run should be faster (using cache)
            assert second_duration < first_duration
        finally:
            os.chdir(old_cwd)

    def test_validation_cache_with_errors(self, temp_project_dir, sample_pipelines_dir):
        """Test validation caching with error results."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            invalid_pipeline_path = os.path.join(
                sample_pipelines_dir, "invalid_pipeline.sf"
            )

            # First validation - should detect errors
            errors1 = validate_pipeline_with_caching(
                invalid_pipeline_path, temp_project_dir
            )

            # Should have errors for invalid pipeline
            assert len(errors1) > 0

            # Verify specific error content
            error_messages = [str(error) for error in errors1]
            assert any("sync_mode" in msg for msg in error_messages)

            # Second validation - should use cached errors
            errors2 = validate_pipeline_with_caching(
                invalid_pipeline_path, temp_project_dir
            )

            # Results should be identical
            assert len(errors2) == len(errors1)

            # Error details should match
            for err1, err2 in zip(errors1, errors2):
                assert err1.message == err2.message
                assert err1.error_type == err2.error_type
        finally:
            os.chdir(old_cwd)

    def test_cache_invalidation_on_file_change(
        self, temp_project_dir, sample_pipelines_dir
    ):
        """Test that cache is invalidated when file changes."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            pipeline_path = os.path.join(sample_pipelines_dir, "valid_pipeline.sf")

            # First validation
            errors1 = validate_pipeline_with_caching(pipeline_path, temp_project_dir)
            assert len(errors1) == 0

            # Modify the file to introduce an error
            with open(pipeline_path, "w") as f:
                f.write(
                    """
                SOURCE customers TYPE CSV PARAMS {
                    "path": "${data_dir}/customers.csv",
                    "has_header": true,
                    "sync_mode": "invalid_mode"
                };
                
                LOAD customers_table FROM customers;
                """
                )

            # Validation should detect the change and show errors
            errors2 = validate_pipeline_with_caching(pipeline_path, temp_project_dir)
            assert len(errors2) > 0

            # Should contain sync_mode error
            error_messages = [str(error) for error in errors2]
            assert any("sync_mode" in msg for msg in error_messages)
        finally:
            os.chdir(old_cwd)

    def test_cache_performance_with_complex_pipeline(
        self, temp_project_dir, sample_pipelines_dir
    ):
        """Test caching performance with complex pipeline."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            complex_pipeline_path = os.path.join(
                sample_pipelines_dir, "complex_pipeline.sf"
            )

            # First validation (no cache)
            start_time = time.time()
            errors1 = validate_pipeline_with_caching(
                complex_pipeline_path, temp_project_dir
            )
            first_duration = time.time() - start_time

            # Should be valid
            assert len(errors1) == 0

            # Multiple subsequent validations (using cache)
            cached_durations = []
            for _ in range(3):
                start_time = time.time()
                errors = validate_pipeline_with_caching(
                    complex_pipeline_path, temp_project_dir
                )
                cached_durations.append(time.time() - start_time)
                assert len(errors) == 0

            # Cached validations should be consistently faster
            avg_cached_duration = sum(cached_durations) / len(cached_durations)
            assert avg_cached_duration < first_duration
        finally:
            os.chdir(old_cwd)

    def test_cache_directory_structure(self, temp_project_dir):
        """Test that cache creates proper directory structure."""
        ValidationCache(temp_project_dir)

        # Create data directory and required CSV file
        data_dir = Path(temp_project_dir) / "data"
        data_dir.mkdir(exist_ok=True)

        test_csv = data_dir / "test.csv"
        test_csv.write_text("id,name\n1,test\n2,test2\n")

        # Create a dummy pipeline file to validate
        pipeline_file = Path(temp_project_dir) / "test_pipeline.sf"
        pipeline_file.write_text(
            """
        SOURCE test TYPE CSV PARAMS {
            "path": "data/test.csv",
            "has_header": true
        };
        
        LOAD test_table FROM test;
        """
        )

        # Validate to create cache
        errors = validate_pipeline_with_caching(str(pipeline_file), temp_project_dir)
        assert len(errors) == 0

        # Check cache directory exists
        cache_dir = Path(temp_project_dir) / "target" / "validation"
        assert cache_dir.exists()
        assert cache_dir.is_dir()

        # Check cache files are created
        cache_files = list(cache_dir.glob("*.json"))
        assert len(cache_files) > 0

    def test_cache_clear_functionality(self, temp_project_dir, sample_pipelines_dir):
        """Test cache clearing functionality."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            cache = ValidationCache(temp_project_dir)
            valid_pipeline_path = os.path.join(
                sample_pipelines_dir, "valid_pipeline.sf"
            )

            # Validate to create cache
            errors = validate_pipeline_with_caching(
                valid_pipeline_path, temp_project_dir
            )
            assert len(errors) == 0

            # Verify cache exists
            cache_dir = Path(temp_project_dir) / "target" / "validation"
            assert cache_dir.exists()
            cache_files_before = list(cache_dir.glob("*.json"))
            assert len(cache_files_before) > 0

            # Clear cache
            cache.clear_cache()

            # Verify cache is cleared
            if cache_dir.exists():
                cache_files_after = list(cache_dir.glob("*.json"))
                assert len(cache_files_after) == 0
        finally:
            os.chdir(old_cwd)

    def test_multiple_pipelines_caching(self, temp_project_dir, sample_pipelines_dir):
        """Test caching behavior with multiple pipelines."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            valid_pipeline_path = os.path.join(
                sample_pipelines_dir, "valid_pipeline.sf"
            )
            invalid_pipeline_path = os.path.join(
                sample_pipelines_dir, "invalid_pipeline.sf"
            )

            # Validate both pipelines
            valid_errors = validate_pipeline_with_caching(
                valid_pipeline_path, temp_project_dir
            )
            invalid_errors = validate_pipeline_with_caching(
                invalid_pipeline_path, temp_project_dir
            )

            # Check results
            assert len(valid_errors) == 0
            assert len(invalid_errors) > 0

            # Validate again - should use cache
            valid_errors2 = validate_pipeline_with_caching(
                valid_pipeline_path, temp_project_dir
            )
            invalid_errors2 = validate_pipeline_with_caching(
                invalid_pipeline_path, temp_project_dir
            )

            # Results should be identical
            assert valid_errors == valid_errors2
            assert len(invalid_errors) == len(invalid_errors2)
        finally:
            os.chdir(old_cwd)

    def test_cache_with_variable_substitution(self, temp_project_dir):
        """Test caching with variable substitution."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            # Create data directory and CSV file
            data_dir = Path(temp_project_dir) / "data"
            data_dir.mkdir(exist_ok=True)

            customers_csv = data_dir / "customers.csv"
            customers_csv.write_text(
                "customer_id,name,updated_at\n1,John,2024-01-01\n2,Jane,2024-01-02\n"
            )

            # Create pipeline with variable substitution
            pipeline_file = Path(temp_project_dir) / "variable_pipeline.sf"
            pipeline_file.write_text(
                """
            SOURCE customers TYPE CSV PARAMS {
                "path": "${data_dir}/customers.csv",
                "has_header": true
            };
            
            LOAD customers_table FROM customers;
            """
            )

            # Create profiles directory and profile
            profiles_dir = Path(temp_project_dir) / "profiles"
            profiles_dir.mkdir(exist_ok=True)

            profile_file = profiles_dir / "dev.yaml"
            profile_file.write_text(
                f"""
            variables:
              data_dir: "{data_dir}"
            """
            )

            # Validate with variable substitution
            errors = validate_pipeline_with_caching(
                str(pipeline_file), temp_project_dir
            )
            assert len(errors) == 0

            # Validate again - should use cache
            errors2 = validate_pipeline_with_caching(
                str(pipeline_file), temp_project_dir
            )
            assert len(errors2) == 0
            assert errors == errors2
        finally:
            os.chdir(old_cwd)

    def test_cache_error_handling(self, temp_project_dir):
        """Test cache behavior with file system errors."""
        # Create data directory and required CSV file
        data_dir = Path(temp_project_dir) / "data"
        data_dir.mkdir(exist_ok=True)

        test_csv = data_dir / "test.csv"
        test_csv.write_text("id,name\n1,test\n2,test2\n")

        # Create a pipeline file that will be deleted
        pipeline_file = Path(temp_project_dir) / "temp_pipeline.sf"
        pipeline_file.write_text(
            """
        SOURCE test TYPE CSV PARAMS {
            "path": "data/test.csv",
            "has_header": true
        };
        """
        )

        # Validate first time
        errors1 = validate_pipeline_with_caching(str(pipeline_file), temp_project_dir)
        assert len(errors1) == 0

        # Delete the file
        pipeline_file.unlink()

        # Try to validate deleted file - should handle gracefully
        with pytest.raises(click.exceptions.Exit) as exc_info:
            validate_pipeline_with_caching(str(pipeline_file), temp_project_dir)

        # Should exit with error code 1
        assert exc_info.value.exit_code == 1

    def test_validation_error_formatting(self, temp_project_dir, sample_pipelines_dir):
        """Test that validation errors are properly formatted in cache."""
        old_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            invalid_pipeline_path = os.path.join(
                sample_pipelines_dir, "invalid_pipeline.sf"
            )

            # Validate to get errors
            errors = validate_pipeline_with_caching(
                invalid_pipeline_path, temp_project_dir
            )

            # Should have errors
            assert len(errors) > 0

            # Check error formatting
            for error in errors:
                assert hasattr(error, "message")
                assert hasattr(error, "error_type")
                assert hasattr(error, "line")
                assert hasattr(error, "column")

                # Error message should be non-empty
                assert error.message
                assert error.error_type

                # Line and column should be valid
                assert error.line >= 0
                assert error.column >= 0
        finally:
            os.chdir(old_cwd)
