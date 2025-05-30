"""Validation cache for SQLFlow CLI commands.

Provides file-based caching of validation results to improve CLI performance.
"""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional

from sqlflow.logging import get_logger
from sqlflow.validation.errors import ValidationError

logger = get_logger(__name__)


class ValidationCache:
    """Simple file-based validation cache.

    Caches validation results to avoid re-validating unchanged pipeline files.
    Uses file modification time to detect changes and invalidate cache.
    """

    def __init__(self, project_dir: str):
        """Initialize validation cache.

        Args:
        ----
            project_dir: Project directory path

        """
        self.project_dir = Path(project_dir)
        self.cache_dir = self.project_dir / "target" / "validation"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cached_errors(self, pipeline_path: str) -> Optional[List[ValidationError]]:
        """Get cached validation errors for a pipeline file.

        Args:
        ----
            pipeline_path: Path to the pipeline file

        Returns:
        -------
            List of validation errors if cached and valid, None otherwise

        """
        try:
            cache_file = self._get_cache_file(pipeline_path)
            pipeline_file = Path(pipeline_path)

            # Check if cache file exists
            if not cache_file.exists():
                logger.debug("Cache miss: no cache file for %s", pipeline_path)
                return None

            # Check if pipeline file exists
            if not pipeline_file.exists():
                logger.debug("Pipeline file does not exist: %s", pipeline_path)
                return None

            # Check if cache is stale
            pipeline_mtime = pipeline_file.stat().st_mtime
            cache_mtime = cache_file.stat().st_mtime

            if pipeline_mtime > cache_mtime:
                logger.debug(
                    "Cache stale: pipeline newer than cache for %s", pipeline_path
                )
                return None

            # Load cached errors
            with open(cache_file, "r", encoding="utf-8") as f:
                cached_data = json.load(f)

            # Convert back to ValidationError objects
            errors = [ValidationError(**error_data) for error_data in cached_data]

            logger.debug(
                "Cache hit: loaded %d cached errors for %s", len(errors), pipeline_path
            )
            return errors

        except (OSError, json.JSONDecodeError, TypeError) as e:
            logger.debug("Cache error for %s: %s", pipeline_path, str(e))
            return None

    def store_errors(self, pipeline_path: str, errors: List[ValidationError]) -> None:
        """Store validation errors in cache.

        Args:
        ----
            pipeline_path: Path to the pipeline file
            errors: List of validation errors to cache

        """
        try:
            cache_file = self._get_cache_file(pipeline_path)

            # Convert errors to serializable format
            error_data = [asdict(error) for error in errors]

            # Ensure cache directory exists
            cache_file.parent.mkdir(parents=True, exist_ok=True)

            # Write cache file
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(error_data, f, indent=2)

            logger.debug("Cached %d errors for %s", len(errors), pipeline_path)

        except (OSError, TypeError) as e:
            logger.warning(
                "Failed to cache validation results for %s: %s", pipeline_path, str(e)
            )

    def _get_cache_file(self, pipeline_path: str) -> Path:
        """Generate cache file path for a pipeline.

        Args:
        ----
            pipeline_path: Path to the pipeline file

        Returns:
        -------
            Path to the cache file

        """
        # Create a safe filename from the pipeline path
        # Use hash to handle long paths and special characters
        path_hash = hashlib.md5(pipeline_path.encode("utf-8")).hexdigest()

        # Include pipeline name for readability
        pipeline_name = Path(pipeline_path).stem
        cache_filename = f"{pipeline_name}_{path_hash}.json"

        return self.cache_dir / cache_filename

    def clear_cache(self) -> None:
        """Clear all cached validation results."""
        try:
            if self.cache_dir.exists():
                for cache_file in self.cache_dir.glob("*.json"):
                    cache_file.unlink()
                logger.debug("Cleared validation cache")
        except OSError as e:
            logger.warning("Failed to clear validation cache: %s", str(e))

    def cache_stats(self) -> dict:
        """Get cache statistics for debugging.

        Returns
        -------
            Dictionary with cache statistics

        """
        try:
            if not self.cache_dir.exists():
                return {"cache_dir_exists": False, "cached_files": 0}

            cache_files = list(self.cache_dir.glob("*.json"))
            return {
                "cache_dir_exists": True,
                "cache_dir": str(self.cache_dir),
                "cached_files": len(cache_files),
                "cache_size_mb": sum(f.stat().st_size for f in cache_files)
                / (1024 * 1024),
            }
        except OSError:
            return {"error": "Unable to access cache directory"}
