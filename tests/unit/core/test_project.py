"""Tests for Project functionality."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import yaml

from sqlflow.project import Project


class TestProject:
    """Test Project functionality."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary directory for testing project functionality."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir

    @pytest.fixture
    def sample_profile(self):
        """Create a sample profile configuration."""
        return {
            "engines": {"duckdb": {"mode": "memory", "memory_limit": "2GB"}},
            "log_level": "info",
            "module_log_levels": {
                "sqlflow.core.engines": "debug",
                "sqlflow.connectors": "warning",
            },
            "paths": {"pipelines": "pipelines", "models": "models", "data": "data"},
        }

    @pytest.fixture
    def project_with_profile(self, temp_project_dir, sample_profile):
        """Create a project with a sample profile."""
        # Create profiles directory
        profiles_dir = os.path.join(temp_project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        # Write the sample profile
        profile_path = os.path.join(profiles_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile, f)

        return Project(temp_project_dir, profile_name="test")

    def test_project_init_with_existing_profile(self, project_with_profile):
        """Test Project initialization with an existing profile."""
        project = project_with_profile

        assert project.project_dir is not None
        assert project.profile is not None
        assert project.profile["engines"]["duckdb"]["mode"] == "memory"

    def test_project_init_with_nonexistent_profile(self, temp_project_dir):
        """Test Project initialization with a non-existent profile."""
        # Should handle missing profile gracefully
        project = Project(temp_project_dir, profile_name="nonexistent")

        # Should have empty profile
        assert project.profile == {}

    @patch("sqlflow.project.configure_logging")
    def test_configure_logging_from_profile_debug(
        self, mock_configure_logging, project_with_profile
    ):
        """Test logging configuration with debug level."""
        # Modify profile to use debug logging
        project_with_profile.profile["log_level"] = "debug"

        project_with_profile._configure_logging_from_profile()

        # Should configure logging with verbose=True
        mock_configure_logging.assert_called_with(verbose=True, quiet=False)

    @patch("sqlflow.project.configure_logging")
    def test_configure_logging_from_profile_warning(
        self, mock_configure_logging, project_with_profile
    ):
        """Test logging configuration with warning level."""
        # Modify profile to use warning logging
        project_with_profile.profile["log_level"] = "warning"

        project_with_profile._configure_logging_from_profile()

        # Should configure logging with quiet=True
        mock_configure_logging.assert_called_with(verbose=False, quiet=True)

    @patch("sqlflow.project.configure_logging")
    @patch("logging.getLogger")
    def test_configure_module_log_levels(
        self, mock_get_logger, mock_configure_logging, project_with_profile
    ):
        """Test module-specific log level configuration."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        project_with_profile._configure_logging_from_profile()

        # Should set module-specific log levels
        assert mock_get_logger.call_count >= 2  # Two modules in sample profile
        mock_logger.setLevel.assert_called()

    def test_get_pipeline_path_default(self, project_with_profile):
        """Test getting pipeline path with default pipelines directory."""
        path = project_with_profile.get_pipeline_path("test_pipeline")

        expected_path = os.path.join(
            project_with_profile.project_dir, "pipelines", "test_pipeline.sf"
        )
        assert path == expected_path

    def test_get_pipeline_path_custom_directory(self, temp_project_dir, sample_profile):
        """Test getting pipeline path with custom pipelines directory."""
        # Modify profile to use custom pipelines directory
        sample_profile["paths"]["pipelines"] = "custom_pipelines"

        # Create project with modified profile
        profiles_dir = os.path.join(temp_project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        profile_path = os.path.join(profiles_dir, "custom.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile, f)

        project = Project(temp_project_dir, profile_name="custom")
        path = project.get_pipeline_path("test_pipeline")

        expected_path = os.path.join(
            project.project_dir, "custom_pipelines", "test_pipeline.sf"
        )
        assert path == expected_path

    def test_get_profile(self, project_with_profile):
        """Test getting the loaded profile."""
        profile = project_with_profile.get_profile()

        assert profile is not None
        assert "engines" in profile
        assert profile["engines"]["duckdb"]["mode"] == "memory"

    def test_get_path_existing(self, project_with_profile):
        """Test getting an existing path from profile."""
        path = project_with_profile.get_path("pipelines")
        assert path == "pipelines"

        path = project_with_profile.get_path("models")
        assert path == "models"

    def test_get_path_nonexistent(self, project_with_profile):
        """Test getting a non-existent path from profile."""
        path = project_with_profile.get_path("nonexistent")
        assert path is None

    def test_get_path_no_paths_section(self, temp_project_dir):
        """Test getting path when profile has no paths section."""
        # Create profile without paths section
        profile = {"engines": {"duckdb": {"mode": "memory"}}}

        profiles_dir = os.path.join(temp_project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        profile_path = os.path.join(profiles_dir, "no_paths.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile, f)

        project = Project(temp_project_dir, profile_name="no_paths")
        path = project.get_path("pipelines")

        assert path is None

    def test_get_config(self, project_with_profile):
        """Test getting project configuration."""
        config = project_with_profile.get_config()

        # Should return the same as get_profile()
        assert config == project_with_profile.get_profile()
        assert "engines" in config

    def test_project_init_static_method(self, temp_project_dir):
        """Test static init method for creating new projects."""
        project_name = "test_project"

        project = Project.init(temp_project_dir, project_name)

        # Should create necessary directories
        assert os.path.exists(os.path.join(temp_project_dir, "pipelines"))
        assert os.path.exists(os.path.join(temp_project_dir, "profiles"))
        assert os.path.exists(os.path.join(temp_project_dir, "data"))

        # Should create profile files
        assert os.path.exists(os.path.join(temp_project_dir, "profiles", "dev.yml"))
        assert os.path.exists(os.path.join(temp_project_dir, "profiles", "prod.yml"))
        assert os.path.exists(os.path.join(temp_project_dir, "profiles", "README.md"))

        # Should return a Project instance
        assert isinstance(project, Project)

    def test_project_init_creates_correct_profiles(self, temp_project_dir):
        """Test that init creates profiles with correct structure."""
        Project.init(temp_project_dir, "test_project")

        # Load and verify dev profile
        dev_profile_path = os.path.join(temp_project_dir, "profiles", "dev.yml")
        with open(dev_profile_path, "r") as f:
            dev_profile = yaml.safe_load(f)

        assert dev_profile["engines"]["duckdb"]["mode"] == "memory"
        assert dev_profile["log_level"] == "info"

        # Load and verify prod profile
        prod_profile_path = os.path.join(temp_project_dir, "profiles", "prod.yml")
        with open(prod_profile_path, "r") as f:
            prod_profile = yaml.safe_load(f)

        assert prod_profile["engines"]["duckdb"]["mode"] == "persistent"
        assert "path" in prod_profile["engines"]["duckdb"]
        assert prod_profile["log_level"] == "warning"

    def test_project_init_creates_readme(self, temp_project_dir):
        """Test that init creates a helpful README in profiles directory."""
        Project.init(temp_project_dir, "test_project")

        readme_path = os.path.join(temp_project_dir, "profiles", "README.md")
        assert os.path.exists(readme_path)

        with open(readme_path, "r") as f:
            content = f.read()

        # Should contain helpful information
        assert "Memory Mode" in content
        assert "Persistent Mode" in content
        assert "dev.yml" in content
        assert "prod.yml" in content

    def test_project_init_existing_directories(self, temp_project_dir):
        """Test that init works when directories already exist."""
        # Pre-create some directories
        os.makedirs(os.path.join(temp_project_dir, "pipelines"))
        os.makedirs(os.path.join(temp_project_dir, "profiles"))

        # Should not raise an exception
        project = Project.init(temp_project_dir, "test_project")
        assert isinstance(project, Project)

    def test_load_profile_file_not_found(self, temp_project_dir):
        """Test loading profile when file doesn't exist."""
        project = Project(temp_project_dir, profile_name="missing")

        # Should have empty profile
        assert project.profile == {}

    def test_load_profile_empty_file(self, temp_project_dir):
        """Test loading profile from an empty YAML file."""
        profiles_dir = os.path.join(temp_project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        # Create empty profile file
        profile_path = os.path.join(profiles_dir, "empty.yml")
        with open(profile_path, "w") as f:
            f.write("")  # Empty file

        project = Project(temp_project_dir, profile_name="empty")

        # Should have empty profile
        assert project.profile == {}

    def test_load_profile_malformed_yaml(self, temp_project_dir):
        """Test loading profile with malformed YAML."""
        profiles_dir = os.path.join(temp_project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        # Create malformed YAML file
        profile_path = os.path.join(profiles_dir, "malformed.yml")
        with open(profile_path, "w") as f:
            f.write(
                "engines:\n  duckdb:\n    mode: memory\n  invalid_yaml: [\n"
            )  # Malformed

        # Should raise an exception when loading malformed YAML
        with pytest.raises(yaml.YAMLError):
            Project(temp_project_dir, profile_name="malformed")

    def test_configure_logging_default_level(self, temp_project_dir):
        """Test logging configuration with default level."""
        # Create profile without log_level
        profile = {"engines": {"duckdb": {"mode": "memory"}}}

        profiles_dir = os.path.join(temp_project_dir, "profiles")
        os.makedirs(profiles_dir, exist_ok=True)
        profile_path = os.path.join(profiles_dir, "default_log.yml")
        with open(profile_path, "w") as f:
            yaml.dump(profile, f)

        with patch("sqlflow.project.configure_logging") as mock_configure:
            project = Project(temp_project_dir, profile_name="default_log")

            # Should use default info level (verbose=False, quiet=False)
            mock_configure.assert_called_with(verbose=False, quiet=False)
