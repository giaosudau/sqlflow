"""Tests for pipeline commands."""

import os
import tempfile
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

from sqlflow.cli.main import app
from sqlflow.cli.pipeline import pipeline_app


@pytest.fixture
def runner():
    """Create a CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def sample_project():
    """Create a sample project for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pipelines_dir = os.path.join(tmpdir, "pipelines")
        os.makedirs(pipelines_dir)

        with open(os.path.join(pipelines_dir, "test.sf"), "w") as f:
            f.write(
                """
SOURCE sample TYPE CSV PARAMS {
    "path": "data/sample.csv",
    "has_header": true
};

LOAD raw_data FROM sample;
            """
            )

        # Create minimal profiles/dev.yml for profile-driven config
        profiles_dir = os.path.join(tmpdir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        dev_profile = {"engines": {"duckdb": {"mode": "memory"}}}
        with open(os.path.join(profiles_dir, "dev.yml"), "w") as f:
            yaml.dump(dev_profile, f)

        yield tmpdir


def test_list_command(runner, sample_project):
    """Test the list command."""
    with patch("os.getcwd", return_value=sample_project):
        result = runner.invoke(app, ["pipeline", "list"])
        assert result.exit_code == 0
        assert "test" in result.stdout


def test_compile_command(runner, sample_project):
    """Test the compile command."""
    with patch("os.getcwd", return_value=sample_project):
        result = runner.invoke(app, ["pipeline", "compile", "test"])
        assert result.exit_code == 0
        assert "source_sample" in result.stdout
        assert "load_raw_data" in result.stdout


def test_run_command(runner, sample_project):
    """Test the run command."""
    with patch("os.getcwd", return_value=sample_project):
        result = runner.invoke(
            app, ["pipeline", "run", "test", "--vars", '{"date": "2023-10-25"}']
        )
        assert result.exit_code == 0
        assert "test" in result.stdout
        assert "2023-10-25" in result.stdout


def make_profile(tmp_path, name, mode, path=None):
    profile = {"engines": {"duckdb": {"mode": mode}}}
    if path:
        profile["engines"]["duckdb"]["path"] = str(path)
    profile_path = tmp_path / f"{name}.yml"
    with open(profile_path, "w") as f:
        yaml.dump(profile, f)
    return profile_path


def test_cli_run_profile_memory_mode(tmp_path, monkeypatch):
    runner = CliRunner()
    # Patch LocalExecutor to avoid real execution
    monkeypatch.setattr(
        "sqlflow.cli.pipeline.LocalExecutor",
        lambda profile_name, project_dir=None: type(
            "E",
            (),
            {
                "execute": lambda self, plan, variables=None: {"summary": {}},
                "duckdb_engine": type(
                    "D", (), {"path": ":memory:", "database_path": ":memory:"}
                )(),
                "results": {},
                "_generate_step_summary": lambda self, ops: None,
                "profile": {"variables": {}},
                "variables": {},
                "duckdb_mode": "memory",
            },
        )(),
    )
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    make_profile(profiles_dir, "dev", "memory")
    pipeline_path = tmp_path / "pipelines"
    pipeline_path.mkdir()
    test_sf = pipeline_path / "dummy.sf"
    test_sf.write_text("-- dummy pipeline\n")
    os.chdir(tmp_path)
    result = runner.invoke(pipeline_app, ["run", "dummy", "--profile", "dev"])
    assert "[SQLFlow] Using profile: dev" in result.output
    assert "memory mode" in result.output
    assert result.exit_code == 0


def test_cli_run_profile_persistent_mode(tmp_path, monkeypatch):
    runner = CliRunner()
    db_path = tmp_path / "prod.db"
    monkeypatch.setattr(
        "sqlflow.cli.pipeline.LocalExecutor",
        lambda profile_name, project_dir=None: type(
            "E",
            (),
            {
                "execute": lambda self, plan, variables=None: {"summary": {}},
                "duckdb_engine": type(
                    "D", (), {"path": str(db_path), "database_path": str(db_path)}
                )(),
                "results": {},
                "_generate_step_summary": lambda self, ops: None,
                "profile": {"variables": {}},
                "variables": {},
                "duckdb_mode": "persistent",
            },
        )(),
    )
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    make_profile(profiles_dir, "production", "persistent", path=db_path)
    pipeline_path = tmp_path / "pipelines"
    pipeline_path.mkdir()
    test_sf = pipeline_path / "dummy.sf"
    test_sf.write_text("-- dummy pipeline\n")
    os.chdir(tmp_path)
    result = runner.invoke(pipeline_app, ["run", "dummy", "--profile", "production"])
    assert "[SQLFlow] Using profile: production" in result.output
    assert "persistent mode" in result.output
    assert str(db_path) in result.output
    assert result.exit_code == 0
