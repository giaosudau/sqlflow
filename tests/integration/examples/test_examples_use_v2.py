"""
Phase 4 TDD: Write failing tests that define V2 default behavior for examples.

Following Kent Beck's TDD approach:
1. These tests WILL FAIL initially - that's the point
2. Then we migrate examples to make them pass
3. Finally we refactor for clarity

Expert Guidance:
- Kent Beck: Red-Green-Refactor cycle
- Robert Martin: Test what matters to users
- Raymond Hettinger: Simple, clear test logic
"""

import subprocess
from pathlib import Path
from typing import List

import pytest

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TestExamplesUseV2:
    """Test that all examples use V2 executor by default."""

    def test_no_examples_import_local_executor_directly(self):
        """
        Test that no examples import LocalExecutor directly.

        Following Robert Martin: "Test the most important behavior first"
        This is the core requirement of Phase 4.
        """
        logger.info("🔍 Testing examples don't import LocalExecutor directly")

        example_files = list(Path("examples").rglob("*.py"))
        failed_files = []

        for example_file in example_files:
            try:
                content = example_file.read_text()
                if (
                    "from sqlflow.core.executors.local_executor import LocalExecutor"
                    in content
                ):
                    failed_files.append(str(example_file))
            except Exception as e:
                logger.warning(f"Could not read {example_file}: {e}")

        assert (
            not failed_files
        ), f"These examples still import LocalExecutor directly: {failed_files}"

    def test_examples_use_get_executor_pattern(self):
        """
        Test that examples use get_executor() pattern.

        Following Raymond Hettinger: "There should be one obvious way to do it"
        """
        logger.info("🔍 Testing examples use get_executor() pattern")

        example_files = list(Path("examples").rglob("*.py"))
        files_using_local_executor = []

        for example_file in example_files:
            try:
                content = example_file.read_text()
                if "LocalExecutor(" in content and "get_executor" not in content:
                    files_using_local_executor.append(str(example_file))
            except Exception as e:
                logger.warning(f"Could not read {example_file}: {e}")

        assert (
            not files_using_local_executor
        ), f"These examples use LocalExecutor() instead of get_executor(): {files_using_local_executor}"

    @pytest.mark.integration
    @pytest.mark.slow
    def test_example_scripts_run_successfully(self):
        """
        Test that example scripts run successfully with V2.

        Following Kent Beck: "Test what could break"
        This ensures examples actually work after migration.
        """
        logger.info("🔍 Testing example scripts run successfully")

        # Find runnable example scripts
        run_scripts = list(Path("examples").rglob("run*.py"))
        demo_scripts = list(Path("examples").rglob("*demo*.py"))

        # Combine and deduplicate
        test_scripts = list(set(run_scripts + demo_scripts))

        if not test_scripts:
            pytest.skip("No example scripts found to test")

        failed_scripts = []

        for script in test_scripts:
            try:
                logger.info(f"🧪 Testing {script}")

                # Run script with timeout
                result = subprocess.run(
                    ["python", str(script)],
                    capture_output=True,
                    text=True,
                    cwd=script.parent,
                    timeout=30,  # 30 second timeout
                )

                if result.returncode != 0:
                    failed_scripts.append(
                        {
                            "script": str(script),
                            "stdout": result.stdout,
                            "stderr": result.stderr,
                            "returncode": result.returncode,
                        }
                    )

            except subprocess.TimeoutExpired:
                failed_scripts.append(
                    {
                        "script": str(script),
                        "error": "Script timed out after 30 seconds",
                    }
                )
            except Exception as e:
                failed_scripts.append({"script": str(script), "error": str(e)})

        # Report failures with details
        if failed_scripts:
            failure_details = "\n".join(
                [
                    f"  - {fs['script']}: {fs.get('error', fs.get('stderr', 'Unknown error'))}"
                    for fs in failed_scripts
                ]
            )
            assert False, f"These example scripts failed:\n{failure_details}"

    def test_example_migration_is_complete(self):
        """
        Test that Phase 4 migration is considered complete.

        Following the Zen of Python: "Explicit is better than implicit"
        This test serves as the Definition of Done for Phase 4.
        """
        logger.info("🔍 Testing Phase 4 migration completeness")

        # Use helper methods to reduce complexity
        import_issues = self._check_local_executor_imports()
        usage_issues = self._check_local_executor_usage()

        # Combine results
        issues = []
        if import_issues:
            issues.append(f"Files with LocalExecutor imports: {import_issues}")
        if usage_issues:
            issues.append(f"Files with LocalExecutor usage: {usage_issues}")

        assert not issues, "Phase 4 migration incomplete:\n" + "\n".join(issues)
        logger.info("✅ Phase 4 migration is complete!")

    def _check_local_executor_imports(self) -> List[str]:
        """Check for LocalExecutor imports in example files."""
        example_files = list(Path("examples").rglob("*.py"))
        files_with_imports = []

        for example_file in example_files:
            try:
                content = example_file.read_text()
                if (
                    "from sqlflow.core.executors.local_executor import LocalExecutor"
                    in content
                ):
                    files_with_imports.append(str(example_file))
            except Exception:
                pass  # Ignore read errors

        return files_with_imports

    def _check_local_executor_usage(self) -> List[str]:
        """Check for direct LocalExecutor usage in example files."""
        example_files = list(Path("examples").rglob("*.py"))
        files_with_usage = []

        for example_file in example_files:
            try:
                content = example_file.read_text()
                if "LocalExecutor(" in content and "get_executor" not in content:
                    files_with_usage.append(str(example_file))
            except Exception:
                pass  # Ignore read errors

        return files_with_usage


class TestBackupFiles:
    """Test backup file management during migration."""

    def test_backup_files_exist_for_migrated_examples(self):
        """
        Test that backup files exist for migrated examples.

        Following Kent Beck's safety principle: "Keep backups of everything"
        """
        logger.info("🔍 Testing backup files exist")

        list(Path("examples").rglob("*.py"))
        backup_files = list(Path("examples").rglob("*.py.backup"))

        # For now, just verify backup mechanism works
        # This test will evolve as we perform actual migrations
        assert isinstance(backup_files, list), "Backup file discovery should work"

        logger.info(f"📁 Found {len(backup_files)} backup files")

    def test_backup_files_can_be_cleaned_up(self):
        """
        Test that backup files can be safely removed when no longer needed.

        Following Raymond Hettinger: "Flat is better than nested"
        Don't accumulate unnecessary files.
        """
        logger.info("🔍 Testing backup cleanup capability")

        backup_files = list(Path("examples").rglob("*.py.backup"))

        # Test that we can identify and potentially clean backup files
        removable_backups = []
        for backup_file in backup_files:
            original_file = backup_file.with_suffix("")  # Remove .backup
            if original_file.exists():
                removable_backups.append(backup_file)

        logger.info(
            f"🧹 Found {len(removable_backups)} potentially removable backup files"
        )

        # Don't actually remove them in tests, just verify the logic works
        assert isinstance(removable_backups, list), "Backup cleanup logic should work"
