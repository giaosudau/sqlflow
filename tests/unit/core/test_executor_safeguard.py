"""Tests for the executor safeguard."""

import os
from unittest.mock import patch

import pytest
import yaml

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.executors.local_executor import LocalExecutor


class TestExecutorSafeguard:
    """Test cases for the executor safeguard."""

    def test_execution_order_match(self):
        """Test that no warning is logged when execution order matches."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_c", "pipeline_a")
        resolver.add_dependency("pipeline_c", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_c")
        assert order == ["pipeline_a", "pipeline_b", "pipeline_c"]

        plan = [
            {"id": "pipeline_a", "type": "test"},
            {"id": "pipeline_b", "type": "test"},
            {"id": "pipeline_c", "type": "test"},
        ]

        executor = LocalExecutor()

        with (
            patch("sqlflow.core.executors.local_executor.logger") as mock_logger,
            patch.object(
                LocalExecutor, "execute_step", return_value={"status": "success"}
            ) as mock_execute_step,
        ):
            executor.execute(plan, resolver)
            mock_logger.warning.assert_not_called()

    def test_execution_order_mismatch(self):
        """Test that a warning is logged when execution order doesn't match."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_c", "pipeline_a")
        resolver.add_dependency("pipeline_c", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_c")
        assert order == ["pipeline_a", "pipeline_b", "pipeline_c"]

        plan = [
            {"id": "pipeline_a", "type": "test"},
            {"id": "pipeline_c", "type": "test"},  # pipeline_c before pipeline_b
            {"id": "pipeline_b", "type": "test"},
        ]

        executor = LocalExecutor()

        with (
            patch("sqlflow.core.executors.local_executor.logger") as mock_logger,
            patch.object(
                LocalExecutor, "execute_step", return_value={"status": "success"}
            ) as mock_execute_step,
        ):
            executor.execute(plan, resolver)
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args[0]
            assert "Execution order mismatch detected" in call_args[0]
            assert "pipeline_a" in call_args[1]
            assert "pipeline_c" in call_args[1]
            assert "pipeline_b" in call_args[1]
            assert "pipeline_a" in call_args[2]
            assert "pipeline_b" in call_args[2]
            assert "pipeline_c" in call_args[2]

    def test_no_dependency_resolver(self):
        """Test that no warning is logged when no dependency resolver is provided."""
        plan = [
            {"id": "pipeline_a", "type": "test"},
            {"id": "pipeline_b", "type": "test"},
            {"id": "pipeline_c", "type": "test"},
        ]

        executor = LocalExecutor()

        with (
            patch("sqlflow.core.executors.local_executor.logger") as mock_logger,
            patch.object(
                LocalExecutor, "execute_step", return_value={"status": "success"}
            ) as mock_execute_step,
        ):
            executor.execute(plan)
            mock_logger.warning.assert_not_called()


class DummyDuckDBEngine:
    def __init__(self, path):
        self.path = path


# Patch DuckDBEngine for test isolation
def patch_duckdb(monkeypatch):
    monkeypatch.setattr(
        "sqlflow.core.executors.local_executor.DuckDBEngine", DummyDuckDBEngine
    )


def make_profile(tmp_path, name, mode, path=None):
    profile = {"engines": {"duckdb": {"mode": mode}}}
    if path:
        profile["engines"]["duckdb"]["path"] = str(path)
    profile_path = tmp_path / f"{name}.yml"
    with open(profile_path, "w") as f:
        yaml.dump(profile, f)
    return profile_path


def test_local_executor_memory_mode(tmp_path, monkeypatch):
    patch_duckdb(monkeypatch)
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    make_profile(profiles_dir, "dev", "memory")
    cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        exec = LocalExecutor(profile_name="dev")
        assert exec.duckdb_engine.path == ":memory:"
    finally:
        os.chdir(cwd)


def test_local_executor_persistent_mode(tmp_path, monkeypatch):
    patch_duckdb(monkeypatch)
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    db_path = tmp_path / "prod.db"
    make_profile(profiles_dir, "production", "persistent", path=db_path)
    cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        exec = LocalExecutor(profile_name="production")
        assert exec.duckdb_engine.path == str(db_path)
    finally:
        os.chdir(cwd)


def test_local_executor_persistent_mode_missing_path(tmp_path, monkeypatch):
    patch_duckdb(monkeypatch)
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    make_profile(profiles_dir, "broken", "persistent")
    cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        with pytest.raises(ValueError):
            LocalExecutor(profile_name="broken")
    finally:
        os.chdir(cwd)
