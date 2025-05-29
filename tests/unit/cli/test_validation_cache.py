"""Tests for CLI validation cache system."""

import json
import tempfile
import time
from pathlib import Path

from sqlflow.cli.validation_cache import ValidationCache
from sqlflow.validation.errors import ValidationError


class TestValidationCache:
    """Test validation cache functionality."""

    def test_cache_initialization_creates_directory(self):
        """Test that cache initialization creates the cache directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            ValidationCache(str(project_dir))

            expected_cache_dir = project_dir / "target" / "validation"
            assert expected_cache_dir.exists()
            assert expected_cache_dir.is_dir()

    def test_cache_miss_returns_none(self):
        """Test that cache miss returns None for non-existent files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            result = cache.get_cached_errors("nonexistent.sf")
            assert result is None

    def test_cache_hit_returns_cached_errors(self):
        """Test that cache hit returns previously stored errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            # Create a test pipeline file
            pipeline_file = Path(temp_dir) / "test.sf"
            pipeline_file.write_text("SOURCE test TYPE CSV PARAMS {};")

            # Store some validation errors
            errors = [
                ValidationError(
                    message="Test error",
                    line=1,
                    error_type="Test Error",
                    suggestions=["Fix this"],
                )
            ]
            cache.store_errors(str(pipeline_file), errors)

            # Retrieve cached errors
            cached_errors = cache.get_cached_errors(str(pipeline_file))

            assert cached_errors is not None
            assert len(cached_errors) == 1
            assert cached_errors[0].message == "Test error"
            assert cached_errors[0].line == 1
            assert cached_errors[0].error_type == "Test Error"
            assert cached_errors[0].suggestions == ["Fix this"]

    def test_cache_invalidation_on_file_modification(self):
        """Test that cache is invalidated when file is modified."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            # Create a test pipeline file
            pipeline_file = Path(temp_dir) / "test.sf"
            pipeline_file.write_text("SOURCE test TYPE CSV PARAMS {};")

            # Store errors
            errors = [ValidationError("Test error", line=1)]
            cache.store_errors(str(pipeline_file), errors)

            # Verify cache hit
            assert cache.get_cached_errors(str(pipeline_file)) is not None

            # Modify file (simulate newer modification time)
            time.sleep(0.1)  # Ensure different timestamp
            pipeline_file.write_text(
                "SOURCE test TYPE CSV PARAMS {'path': 'test.csv'};"
            )

            # Cache should be invalidated
            assert cache.get_cached_errors(str(pipeline_file)) is None

    def test_store_errors_creates_cache_entry(self):
        """Test that storing errors creates a proper cache entry."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            # Create a test pipeline file
            pipeline_file = Path(temp_dir) / "test.sf"
            pipeline_file.write_text("SOURCE test TYPE CSV PARAMS {};")

            # Store errors
            errors = [
                ValidationError("Error 1", line=1, error_type="Type1"),
                ValidationError(
                    "Error 2", line=2, error_type="Type2", suggestions=["Fix"]
                ),
            ]
            cache.store_errors(str(pipeline_file), errors)

            # Verify cache file exists
            cache_file = cache._get_cache_file(str(pipeline_file))
            assert cache_file.exists()

            # Verify cache file content
            with open(cache_file, "r") as f:
                cached_data = json.load(f)

            assert len(cached_data) == 2
            assert cached_data[0]["message"] == "Error 1"
            assert cached_data[1]["suggestions"] == ["Fix"]

    def test_empty_errors_list_cached_correctly(self):
        """Test that empty errors list (no validation errors) is cached correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            # Create a test pipeline file
            pipeline_file = Path(temp_dir) / "test.sf"
            pipeline_file.write_text(
                "SOURCE test TYPE CSV PARAMS {'path': 'test.csv'};"
            )

            # Store empty errors (valid pipeline)
            cache.store_errors(str(pipeline_file), [])

            # Retrieve cached errors
            cached_errors = cache.get_cached_errors(str(pipeline_file))

            assert cached_errors is not None
            assert len(cached_errors) == 0

    def test_cache_handles_nonexistent_pipeline_file(self):
        """Test that cache gracefully handles nonexistent pipeline files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            # Try to get cache for nonexistent file
            result = cache.get_cached_errors("nonexistent.sf")
            assert result is None

    def test_get_cache_file_path_generation(self):
        """Test that cache file paths are generated correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = ValidationCache(temp_dir)

            # Test various file paths
            test_cases = [
                "pipeline.sf",
                "path/to/pipeline.sf",
                "/absolute/path/pipeline.sf",
                "complex-name_123.sf",
            ]

            for pipeline_path in test_cases:
                cache_file = cache._get_cache_file(pipeline_path)

                # Cache file should be in cache directory
                assert str(cache_file).startswith(str(cache.cache_dir))

                # Cache file should have .json extension
                assert cache_file.suffix == ".json"
