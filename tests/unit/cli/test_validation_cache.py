"""Tests for validation cache functionality."""

import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from sqlflow.cli.validation_cache import ValidationCache
from sqlflow.validation.errors import ValidationError


class TestValidationCache:
    """Test ValidationCache functionality."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary project directory for testing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir

    @pytest.fixture
    def cache(self, temp_project_dir):
        """Create a ValidationCache instance for testing."""
        return ValidationCache(temp_project_dir)

    @pytest.fixture
    def sample_errors(self):
        """Create sample validation errors for testing."""
        return [
            ValidationError(
                message="Missing required parameter",
                line=10,
                column=5,
                error_type="Parameter Error",
                suggestions=["Add required parameter"],
            ),
            ValidationError(
                message="Unknown connector type",
                line=15,
                column=10,
                error_type="Connector Error",
                suggestions=["Use valid connector type"],
            ),
        ]

    @pytest.fixture
    def temp_pipeline_file(self, temp_project_dir):
        """Create a temporary pipeline file for testing."""
        pipeline_path = os.path.join(temp_project_dir, "test_pipeline.sf")
        with open(pipeline_path, "w") as f:
            f.write("SOURCE test TYPE csv PARAMS {'path': 'test.csv'};")
        return pipeline_path

    def test_cache_initialization(self, temp_project_dir):
        """Test cache initialization creates necessary directories."""
        cache = ValidationCache(temp_project_dir)

        assert cache.project_dir == Path(temp_project_dir)
        assert cache.cache_dir == Path(temp_project_dir) / "target" / "validation"
        assert cache.cache_dir.exists()

    def test_store_and_retrieve_errors(self, cache, temp_pipeline_file, sample_errors):
        """Test storing and retrieving validation errors."""
        # Store errors
        cache.store_errors(temp_pipeline_file, sample_errors)

        # Retrieve errors
        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is not None
        assert len(cached_errors) == len(sample_errors)

        # Verify error content
        for original, cached in zip(sample_errors, cached_errors):
            assert cached.message == original.message
            assert cached.line == original.line
            assert cached.column == original.column
            assert cached.error_type == original.error_type
            assert cached.suggestions == original.suggestions

    def test_cache_miss_no_file(self, cache, temp_project_dir):
        """Test cache miss when no cache file exists."""
        nonexistent_pipeline = os.path.join(temp_project_dir, "nonexistent.sf")

        cached_errors = cache.get_cached_errors(nonexistent_pipeline)

        assert cached_errors is None

    def test_cache_miss_stale_cache(self, cache, temp_pipeline_file, sample_errors):
        """Test cache miss when cache is stale (file newer than cache)."""
        # Store errors first
        cache.store_errors(temp_pipeline_file, sample_errors)

        # Wait and modify pipeline file to make it newer
        time.sleep(0.1)
        with open(temp_pipeline_file, "a") as f:
            f.write("\n# Modified")

        # Cache should be stale
        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is None

    def test_cache_hit_fresh_cache(self, cache, temp_pipeline_file, sample_errors):
        """Test cache hit when cache is fresh."""
        # Store errors
        cache.store_errors(temp_pipeline_file, sample_errors)

        # Immediately retrieve (cache should be fresh)
        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is not None
        assert len(cached_errors) == len(sample_errors)

    def test_store_empty_errors_list(self, cache, temp_pipeline_file):
        """Test storing empty errors list."""
        empty_errors = []

        cache.store_errors(temp_pipeline_file, empty_errors)
        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors == []

    def test_clear_cache(self, cache, temp_pipeline_file, sample_errors):
        """Test clearing all cached validation results."""
        # Store some errors first
        cache.store_errors(temp_pipeline_file, sample_errors)

        # Verify cache exists
        assert cache.get_cached_errors(temp_pipeline_file) is not None

        # Clear cache
        cache.clear_cache()

        # Verify cache is cleared
        assert cache.get_cached_errors(temp_pipeline_file) is None

    def test_cache_stats_with_files(self, cache, temp_pipeline_file, sample_errors):
        """Test cache statistics with cached files."""
        # Store some errors
        cache.store_errors(temp_pipeline_file, sample_errors)

        stats = cache.cache_stats()

        assert stats["cache_dir_exists"] is True
        assert stats["cached_files"] == 1
        assert stats["cache_size_mb"] > 0
        assert "cache_dir" in stats

    def test_cache_stats_empty_cache(self, cache):
        """Test cache statistics with empty cache."""
        stats = cache.cache_stats()

        assert stats["cache_dir_exists"] is True
        assert stats["cached_files"] == 0
        assert stats["cache_size_mb"] == 0

    def test_cache_stats_no_cache_dir(self, temp_project_dir):
        """Test cache statistics when cache directory doesn't exist."""
        # Create cache but don't initialize directory
        cache = ValidationCache.__new__(ValidationCache)
        cache.project_dir = Path(temp_project_dir)
        cache.cache_dir = Path(temp_project_dir) / "nonexistent" / "validation"

        stats = cache.cache_stats()

        assert stats["cache_dir_exists"] is False
        assert stats["cached_files"] == 0

    def test_get_cached_errors_nonexistent_pipeline(self, cache):
        """Test getting cached errors for nonexistent pipeline file."""
        nonexistent_path = "/nonexistent/pipeline.sf"

        cached_errors = cache.get_cached_errors(nonexistent_path)

        assert cached_errors is None

    def test_get_cached_errors_corrupted_cache(self, cache, temp_pipeline_file):
        """Test handling corrupted cache file."""
        # Create corrupted cache file
        cache_file = cache._get_cache_file(temp_pipeline_file)
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        with open(cache_file, "w") as f:
            f.write("invalid json content")

        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is None

    def test_get_cached_errors_invalid_error_data(self, cache, temp_pipeline_file):
        """Test handling cache with invalid error data structure."""
        # Create cache with invalid error structure
        cache_file = cache._get_cache_file(temp_pipeline_file)
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        invalid_data = [{"invalid": "structure"}]
        with open(cache_file, "w") as f:
            json.dump(invalid_data, f)

        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is None

    @patch("builtins.open", side_effect=OSError("Permission denied"))
    def test_store_errors_file_permission_error(
        self, mock_open, cache, temp_pipeline_file, sample_errors
    ):
        """Test handling file permission errors when storing errors."""
        # Should not raise exception, just log warning
        cache.store_errors(temp_pipeline_file, sample_errors)

        # Verify it was called (permission error was handled)
        mock_open.assert_called()

    def test_get_cached_errors_file_read_error(self, cache, temp_pipeline_file):
        """Test handling file read errors when getting cached errors."""
        # Create cache file with invalid JSON to trigger read error
        cache_file = cache._get_cache_file(temp_pipeline_file)
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Write invalid JSON that will cause a decode error
        with open(cache_file, "w") as f:
            f.write("{ invalid json content")

        # This should handle the JSON decode error gracefully
        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is None

    def test_clear_cache_permission_error(
        self, cache, temp_pipeline_file, sample_errors
    ):
        """Test handling permission errors when clearing cache."""
        # Store some errors first
        cache.store_errors(temp_pipeline_file, sample_errors)

        with patch("pathlib.Path.unlink", side_effect=OSError("Permission denied")):
            # Should not raise exception, just log warning
            cache.clear_cache()

    def test_cache_file_generation(self, cache):
        """Test cache file path generation."""
        pipeline_path = "/some/long/path/to/pipeline_with_special_chars!@#.sf"

        cache_file = cache._get_cache_file(pipeline_path)

        # Should be in cache directory
        assert cache_file.parent == cache.cache_dir

        # Should contain pipeline name
        assert "pipeline_with_special_chars" in cache_file.name

        # Should be JSON file
        assert cache_file.suffix == ".json"

        # Should contain hash for uniqueness
        assert len(cache_file.stem) > len("pipeline_with_special_chars")

    def test_cache_file_hash_consistency(self, cache):
        """Test that cache file paths are consistent for same pipeline path."""
        pipeline_path = "/test/pipeline.sf"

        cache_file1 = cache._get_cache_file(pipeline_path)
        cache_file2 = cache._get_cache_file(pipeline_path)

        assert cache_file1 == cache_file2

    def test_cache_file_hash_uniqueness(self, cache):
        """Test that different pipeline paths generate different cache files."""
        pipeline_path1 = "/test/pipeline1.sf"
        pipeline_path2 = "/test/pipeline2.sf"

        cache_file1 = cache._get_cache_file(pipeline_path1)
        cache_file2 = cache._get_cache_file(pipeline_path2)

        assert cache_file1 != cache_file2

    def test_multiple_pipeline_caching(self, cache, temp_project_dir, sample_errors):
        """Test caching multiple different pipeline files."""
        # Create multiple pipeline files
        pipeline1 = os.path.join(temp_project_dir, "pipeline1.sf")
        pipeline2 = os.path.join(temp_project_dir, "pipeline2.sf")

        for pipeline in [pipeline1, pipeline2]:
            with open(pipeline, "w") as f:
                f.write("SOURCE test TYPE csv PARAMS {'path': 'test.csv'};")

        # Store errors for both
        cache.store_errors(pipeline1, sample_errors[:1])
        cache.store_errors(pipeline2, sample_errors[1:])

        # Retrieve and verify
        cached1 = cache.get_cached_errors(pipeline1)
        cached2 = cache.get_cached_errors(pipeline2)

        assert len(cached1) == 1
        assert len(cached2) == 1
        assert cached1[0].message != cached2[0].message

    def test_cache_with_validation_errors_with_none_fields(
        self, cache, temp_pipeline_file
    ):
        """Test caching validation errors with None fields."""
        errors_with_none = [
            ValidationError(
                message="Test error",
                line=5,
                column=0,
                error_type="Test Error",
                suggestions=[],
                help_url=None,  # None field
            )
        ]

        cache.store_errors(temp_pipeline_file, errors_with_none)
        cached_errors = cache.get_cached_errors(temp_pipeline_file)

        assert cached_errors is not None
        assert len(cached_errors) == 1
        assert cached_errors[0].help_url is None

    @patch("sqlflow.cli.validation_cache.logger")
    def test_logging_cache_operations(
        self, mock_logger, cache, temp_pipeline_file, sample_errors
    ):
        """Test that cache operations are properly logged."""
        # Test cache miss logging
        cache.get_cached_errors("nonexistent.sf")
        mock_logger.debug.assert_called()

        # Test cache store logging
        cache.store_errors(temp_pipeline_file, sample_errors)
        mock_logger.debug.assert_called()

        # Test cache hit logging
        cache.get_cached_errors(temp_pipeline_file)
        mock_logger.debug.assert_called()
