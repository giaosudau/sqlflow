"""Tests for the dependency resolver."""

import os
import shutil
import tempfile

import duckdb
import pytest
import yaml

from sqlflow.core.dependencies import DependencyResolver
from sqlflow.core.errors import CircularDependencyError
from sqlflow.core.executors.v2.execution.engines import create_engine_adapter


class TestDependencyResolver:
    """Test cases for the DependencyResolver class."""

    def test_simple_dependency(self):
        """Test that simple dependencies are resolved correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_b")
        assert order == ["pipeline_a", "pipeline_b"]

    def test_multiple_dependencies(self):
        """Test that multiple dependencies are resolved correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_c", "pipeline_a")
        resolver.add_dependency("pipeline_c", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        order = resolver.resolve_dependencies("pipeline_c")
        assert order == ["pipeline_a", "pipeline_b", "pipeline_c"]

    def test_simple_cycle_detection(self):
        """Test that a simple cycle is detected correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_a")

        with pytest.raises(CircularDependencyError) as excinfo:
            resolver.resolve_dependencies("pipeline_a")

        cycle = excinfo.value.cycle
        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert len(cycle) == 3  # [pipeline_a, pipeline_b, pipeline_a]

    def test_complex_cycle_detection(self):
        """Test that a complex cycle is detected correctly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_c")
        resolver.add_dependency("pipeline_c", "pipeline_a")

        with pytest.raises(CircularDependencyError) as excinfo:
            resolver.resolve_dependencies("pipeline_a")

        cycle = excinfo.value.cycle
        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert "pipeline_c" in cycle
        assert len(cycle) == 4  # [pipeline_a, pipeline_b, pipeline_c, pipeline_a]

    def test_cycle_with_multiple_dependencies(self):
        """Test that a cycle is detected correctly when there are multiple dependencies."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_a", "pipeline_c")
        resolver.add_dependency("pipeline_b", "pipeline_d")
        resolver.add_dependency("pipeline_d", "pipeline_a")

        with pytest.raises(CircularDependencyError) as excinfo:
            resolver.resolve_dependencies("pipeline_a")

        cycle = excinfo.value.cycle
        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert "pipeline_d" in cycle
        assert len(cycle) == 4

    def test_no_cycle(self):
        """Test that no cycle is detected when there isn't one."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_a", "pipeline_c")
        resolver.add_dependency("pipeline_b", "pipeline_d")

        order = resolver.resolve_dependencies("pipeline_a")
        assert order == ["pipeline_d", "pipeline_b", "pipeline_c", "pipeline_a"]

    def test_find_cycle_method(self):
        """Test the _find_cycle method directly."""
        resolver = DependencyResolver()
        resolver.add_dependency("pipeline_a", "pipeline_b")
        resolver.add_dependency("pipeline_b", "pipeline_c")
        resolver.add_dependency("pipeline_c", "pipeline_a")

        resolver.temp_visited = {"pipeline_a", "pipeline_b", "pipeline_c"}

        cycle = resolver._find_cycle("pipeline_a")

        assert "pipeline_a" in cycle
        assert "pipeline_b" in cycle
        assert "pipeline_c" in cycle
        assert cycle[-1] == "pipeline_a"


def make_temp_project_with_profile(duckdb_path=None, mode="persistent"):
    temp_dir = tempfile.mkdtemp()
    profiles_dir = os.path.join(temp_dir, "profiles")
    os.makedirs(profiles_dir, exist_ok=True)
    profile = {"engines": {"duckdb": {"mode": mode}}}
    if duckdb_path is not None:
        profile["engines"]["duckdb"]["path"] = duckdb_path
    with open(os.path.join(profiles_dir, "dev.yml"), "w") as f:
        yaml.dump(profile, f)
    return temp_dir


def test_local_executor_duckdb_path_config(tmp_path):
    """Test V2 executor with DuckDB path configuration."""
    # Create a new file path that doesn't exist yet
    temp_db_path = os.path.join(tmp_path, "test.db")

    # Initialize the file as a valid DuckDB database
    conn = duckdb.connect(temp_db_path)
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.close()

    temp_dir = make_temp_project_with_profile(temp_db_path)
    old_cwd = None

    try:
        old_cwd = os.getcwd()
        os.chdir(temp_dir)
    except (FileNotFoundError, OSError):
        os.chdir(str(tmp_path))
        shutil.copytree(temp_dir, str(tmp_path / "temp_project"))
        os.chdir(str(tmp_path / "temp_project"))

    try:
        # Create engine adapter with path
        engine = create_engine_adapter(engine_type="duckdb", database_path=temp_db_path)
        assert engine.native_engine.database_path == temp_db_path
        assert engine.native_engine.is_persistent is True
    finally:
        try:
            os.unlink(temp_db_path)
        except OSError:
            pass
        if old_cwd:
            try:
                os.chdir(old_cwd)
            except (FileNotFoundError, OSError):
                pass


def test_local_executor_duckdb_path_default(tmp_path):
    """Test V2 executor with default DuckDB path configuration."""
    temp_dir = make_temp_project_with_profile(mode="memory")

    old_cwd = None
    try:
        old_cwd = os.getcwd()
        os.chdir(temp_dir)
    except (FileNotFoundError, OSError):
        os.chdir(str(tmp_path))
        shutil.copytree(temp_dir, str(tmp_path / "temp_project"))
        os.chdir(str(tmp_path / "temp_project"))

    try:
        # Create engine adapter in memory mode
        engine = create_engine_adapter(engine_type="duckdb", database_path=":memory:")
        assert engine.native_engine.database_path == ":memory:"
        assert engine.native_engine.is_persistent is False
    finally:
        if old_cwd:
            try:
                os.chdir(old_cwd)
            except (FileNotFoundError, OSError):
                pass
