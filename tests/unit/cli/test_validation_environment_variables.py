"""Tests for environment variable support in pipeline validation."""

import os
import tempfile
from pathlib import Path

from sqlflow.cli.validation_helpers import validate_pipeline


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

    def test_validation_with_environment_variables(self):
        """Test that validation passes even when environment variables are missing (V2 behavior)."""
        pipeline_content = """-- Test with environment variables
SOURCE test_source TYPE SHOPIFY PARAMS {
    "shop_domain": "${TEST_SHOPIFY_STORE}",
    "access_token": "${TEST_SHOPIFY_TOKEN}",
    "sync_mode": "full_refresh"
};

LOAD orders FROM test_source;
"""

        # Temporarily remove the environment variables to test missing case
        original_store = os.environ.get("TEST_SHOPIFY_STORE")
        original_token = os.environ.get("TEST_SHOPIFY_TOKEN")

        if "TEST_SHOPIFY_STORE" in os.environ:
            del os.environ["TEST_SHOPIFY_STORE"]
        if "TEST_SHOPIFY_TOKEN" in os.environ:
            del os.environ["TEST_SHOPIFY_TOKEN"]

        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
                f.write(pipeline_content)
                f.flush()

                try:
                    errors = validate_pipeline(f.name)
                    # V2 behavior: Missing env vars don't cause validation to fail
                    # They're just left unsubstituted for runtime resolution
                    assert len(errors) == 0
                finally:
                    Path(f.name).unlink()
        finally:
            # Restore environment variables
            if original_store is not None:
                os.environ["TEST_SHOPIFY_STORE"] = original_store
            if original_token is not None:
                os.environ["TEST_SHOPIFY_TOKEN"] = original_token

    def test_validation_with_invalid_shopify_parameters(self):
        """Test validation with invalid Shopify parameters."""
        pipeline_content = """-- Test with invalid sync_mode
SOURCE test_source TYPE SHOPIFY PARAMS {
    "shop_domain": "test-store.myshopify.com",
    "access_token": "test_token", 
    "sync_mode": "invalid_mode"
};

LOAD orders FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline(f.name)
                # Should have validation errors for invalid sync_mode
                assert len(errors) > 0
                error_messages = [str(error) for error in errors]
                assert any("sync_mode" in msg for msg in error_messages)

            finally:
                Path(f.name).unlink()

    def test_validation_with_missing_required_shopify_params(self):
        """Test validation with missing required Shopify parameters."""
        pipeline_content = """-- Test with missing required parameters
SOURCE test_source TYPE SHOPIFY PARAMS {
    "shop_domain": "test-store.myshopify.com"
};

LOAD orders FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline(f.name)
                # Should have validation errors for missing access_token
                assert len(errors) > 0
                error_messages = [str(error) for error in errors]
                assert any("access_token" in msg for msg in error_messages)

            finally:
                Path(f.name).unlink()

    def test_validation_with_valid_csv_source(self):
        """Test validation with valid CSV source using environment variables."""
        pipeline_content = """-- Test with valid CSV parameters
SOURCE test_source TYPE CSV PARAMS {
    "path": "data/test.csv",
    "has_header": true,
    "sync_mode": "full_refresh"
};

LOAD test_table FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline(f.name)
                # Should pass validation with proper CSV parameters
                assert len(errors) == 0

            finally:
                Path(f.name).unlink()

    def test_validation_with_invalid_csv_sync_mode(self):
        """Test validation with invalid CSV sync mode."""
        pipeline_content = """-- Test with invalid CSV sync_mode  
SOURCE test_source TYPE CSV PARAMS {
    "path": "test.csv",
    "has_header": true,
    "sync_mode": "invalid_sync_mode"
};

LOAD test_table FROM test_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline(f.name)
                # Should have validation errors for invalid sync_mode
                assert len(errors) > 0
                error_messages = [str(error) for error in errors]
                assert any("sync_mode" in msg for msg in error_messages)

            finally:
                Path(f.name).unlink()

    def test_validation_with_mixed_valid_and_invalid_sources(self):
        """Test validation with multiple sources, some valid and some invalid."""
        pipeline_content = """-- Test with mixed source validity
SOURCE valid_source TYPE CSV PARAMS {
    "path": "valid.csv",
    "has_header": true,
    "sync_mode": "full_refresh"
};

SOURCE invalid_source TYPE SHOPIFY PARAMS {
    "shop_domain": "test-store.myshopify.com",
    "access_token": "test_token",
    "sync_mode": "invalid_mode"
};

LOAD valid_table FROM valid_source;
LOAD invalid_table FROM invalid_source;
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sf", delete=False) as f:
            f.write(pipeline_content)
            f.flush()

            try:
                errors = validate_pipeline(f.name)
                # Should have validation errors only for the invalid source
                assert len(errors) > 0
                error_messages = [str(error) for error in errors]
                assert any("sync_mode" in msg for msg in error_messages)
                # Ensure error is specifically about the invalid source
                assert any("invalid_mode" in msg for msg in error_messages)

            finally:
                Path(f.name).unlink()
