"""Tests for environment variable support in pipeline validation."""

import os
import tempfile
from pathlib import Path

from sqlflow.cli.validation_helpers import validate_pipeline_with_caching


class TestPipelineValidationWithEnvironmentVariables:
    """Test environment variable support in pipeline validation."""

    def setup_method(self):
        """Setup test environment variables."""
        # Store original environment to restore later
        self.original_env = {}

        # Set test environment variables
        test_vars = {
            "TEST_SHOPIFY_STORE": "test-store.myshopify.com",
            "TEST_SHOPIFY_TOKEN": "shpat_test_token",
            "TEST_DATABASE_URL": "postgresql://user:pass@localhost/db",
            "TEST_S3_BUCKET": "test-bucket",
        }

        for key, value in test_vars.items():
            if key in os.environ:
                self.original_env[key] = os.environ[key]
            os.environ[key] = value

    def teardown_method(self):
        """Clean up test environment variables."""
        # Remove test variables
        test_vars = [
            "TEST_SHOPIFY_STORE",
            "TEST_SHOPIFY_TOKEN",
            "TEST_DATABASE_URL",
            "TEST_S3_BUCKET",
        ]

        for key in test_vars:
            if key in os.environ:
                del os.environ[key]

        # Restore original values
        for key, value in self.original_env.items():
            os.environ[key] = value

    def test_environment_variable_substitution(self):
        """Test that validation works with environment variables in SOURCE PARAMS."""
        pipeline_content = """-- Test pipeline with environment variables
SOURCE test_source TYPE SHOPIFY PARAMS {
    "shop_domain": "${TEST_SHOPIFY_STORE}",
    "access_token": "${TEST_SHOPIFY_TOKEN}",
    "sync_mode": "full_refresh"
};

LOAD orders FROM test_source;

CREATE TABLE order_count AS
SELECT COUNT(*) as total FROM orders;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                # This should pass validation now that environment variables are supported
                errors = validate_pipeline_with_caching(f.name)

                # Should have no validation errors
                assert (
                    len(errors) == 0
                ), f"Validation failed with errors: {[str(e) for e in errors]}"

            finally:
                Path(f.name).unlink()

    def test_environment_variables_with_defaults(self):
        """Test validation with environment variables that have default values."""
        pipeline_content = """-- Test pipeline with environment variables and defaults
SOURCE test_source TYPE POSTGRES PARAMS {
    "host": "${TEST_DB_HOST|localhost}",
    "port": 5432,
    "database": "${TEST_DATABASE_URL}",
    "user": "${TEST_DB_USER|testuser}"
};

LOAD users FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline_with_caching(f.name)

                # Should pass validation even though some variables have defaults
                assert (
                    len(errors) == 0
                ), f"Validation failed with errors: {[str(e) for e in errors]}"

            finally:
                Path(f.name).unlink()

    def test_missing_environment_variables(self):
        """Test validation behavior with missing environment variables (no defaults)."""
        pipeline_content = """-- Test pipeline with missing environment variables
SOURCE test_source TYPE SHOPIFY PARAMS {
    "shop_domain": "${MISSING_SHOPIFY_STORE}",
    "access_token": "${MISSING_SHOPIFY_TOKEN}",
    "sync_mode": "full_refresh"
};

LOAD orders FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline_with_caching(f.name)

                # Missing variables without defaults will cause JSON parsing errors
                # This is expected behavior - the test verifies the error is about JSON parsing
                assert (
                    len(errors) > 0
                ), "Expected validation errors for missing variables"

                # Verify the error is about JSON parsing (which indicates variable substitution was attempted)
                error_messages = [str(e) for e in errors]
                assert any(
                    "Expected JSON object" in msg for msg in error_messages
                ), f"Expected JSON parsing error, got: {error_messages}"

            finally:
                Path(f.name).unlink()

    def test_json_substitution_integrity(self):
        """Test that environment variable substitution maintains JSON integrity."""
        # Test with values that are JSON-safe
        os.environ["TEST_SIMPLE_VALUE"] = "simple_value"
        os.environ["TEST_FORMAT"] = "csv"

        try:
            pipeline_content = """-- Test pipeline with simple environment variables
SOURCE test_source TYPE S3 PARAMS {
    "bucket": "${TEST_S3_BUCKET}",
    "key": "${TEST_SIMPLE_VALUE}",
    "format": "${TEST_FORMAT}"
};

LOAD data FROM test_source;
"""

            with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
                f.write(pipeline_content)
                f.flush()

                try:
                    errors = validate_pipeline_with_caching(f.name)

                    # Should handle simple values without JSON parsing errors
                    assert (
                        len(errors) == 0
                    ), f"Validation failed with simple values: {[str(e) for e in errors]}"

                finally:
                    Path(f.name).unlink()

        finally:
            # Clean up test environment variables
            for var in ["TEST_SIMPLE_VALUE", "TEST_FORMAT"]:
                if var in os.environ:
                    del os.environ[var]

    def test_comprehensive_shopify_scenario(self):
        """Test that demonstrates the fix for environment variable substitution during validation."""
        pipeline_content = """-- Shopify analytics pipeline
SOURCE shopify_store TYPE SHOPIFY PARAMS {
    "shop_domain": "${TEST_SHOPIFY_STORE}",
    "access_token": "${TEST_SHOPIFY_TOKEN}",
    "sync_mode": "full_refresh"
};

LOAD orders FROM shopify_store;
LOAD customers FROM shopify_store;

CREATE TABLE daily_summary AS
SELECT 
    DATE(created_at) as order_date,
    COUNT(*) as order_count
FROM orders
GROUP BY DATE(created_at);

EXPORT SELECT * FROM daily_summary 
TO "output/daily_summary.csv" 
TYPE CSV OPTIONS { "header": true };
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                # This test validates the exact scenario that was failing before the fix:
                # Environment variables in JSON PARAMS causing "Expected JSON object" errors
                errors = validate_pipeline_with_caching(f.name)

                # After the fix, this should pass validation
                assert (
                    len(errors) == 0
                ), f"Environment variable substitution failed during validation: {[str(e) for e in errors]}"

                # Verify that if we had validation errors, they wouldn't be the JSON parsing type
                for error in errors:
                    assert (
                        "Expected JSON object" not in error.message
                    ), "JSON parsing error indicates environment variable substitution failed"

            finally:
                Path(f.name).unlink()

    def test_validation_caching_works_with_environment_variables(self):
        """Test that validation caching works correctly with environment variables."""
        pipeline_content = """-- Test caching with environment variables
SOURCE test_source TYPE SHOPIFY PARAMS {
    "shop_domain": "${TEST_SHOPIFY_STORE}",
    "access_token": "${TEST_SHOPIFY_TOKEN}",
    "sync_mode": "full_refresh"
};

LOAD orders FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                # First validation (should cache the result)
                errors1 = validate_pipeline_with_caching(f.name)
                assert len(errors1) == 0

                # Second validation (should use cached result)
                errors2 = validate_pipeline_with_caching(f.name)
                assert len(errors2) == 0

                # Results should be consistent
                assert errors1 == errors2

            finally:
                Path(f.name).unlink()
