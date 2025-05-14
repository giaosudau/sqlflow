"""Tests for CLI utility functions."""

import os
import tempfile
from unittest import TestCase

import pytest

from sqlflow.sqlflow.cli.utils import parse_vars, resolve_pipeline_name


class TestResolvesPipelineName(TestCase):
    """Tests for the resolve_pipeline_name function."""

    def test_resolves_name_with_extension(self):
        """Test resolving a name with .sf extension."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline_file = os.path.join(tmpdir, "test_pipeline.sf")
            with open(pipeline_file, "w") as f:
                f.write("-- Test pipeline")

            resolved = resolve_pipeline_name("test_pipeline.sf", tmpdir)
            assert resolved == pipeline_file

    def test_resolves_name_without_extension(self):
        """Test resolving a name without .sf extension."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline_file = os.path.join(tmpdir, "test_pipeline.sf")
            with open(pipeline_file, "w") as f:
                f.write("-- Test pipeline")

            resolved = resolve_pipeline_name("test_pipeline", tmpdir)
            assert resolved == pipeline_file

    def test_raises_error_if_not_found(self):
        """Test that an error is raised if the pipeline is not found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(FileNotFoundError):
                resolve_pipeline_name("nonexistent", tmpdir)


class TestParseVars(TestCase):
    """Tests for the parse_vars function."""

    def test_parses_json_format(self):
        """Test parsing variables in JSON format."""
        vars_input = '{"date": "2023-10-25", "region": "US"}'
        parsed = parse_vars(vars_input)
        assert parsed == {"date": "2023-10-25", "region": "US"}

    def test_parses_key_value_format(self):
        """Test parsing variables in key=value format."""
        vars_input = "date=2023-10-25 region=US"
        parsed = parse_vars(vars_input)
        assert parsed == {"date": "2023-10-25", "region": "US"}

    def test_returns_empty_dict_for_none(self):
        """Test that None input returns an empty dict."""
        parsed = parse_vars(None)
        assert parsed == {}

    def test_raises_error_for_invalid_input(self):
        """Test that an error is raised for invalid input."""
        with pytest.raises(ValueError):
            parse_vars("invalid input")
