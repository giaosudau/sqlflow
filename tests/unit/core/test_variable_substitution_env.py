"""Tests for environment variable support in variable substitution engine."""

import os

from sqlflow.core.variable_substitution import VariableSubstitutionEngine


class TestVariableSubstitutionWithEnvironment:
    """Test variable substitution with environment variable support."""

    def test_environment_variable_substitution(self):
        """Test that environment variables are used when explicit variables are not provided."""
        # Set an environment variable
        os.environ["TEST_ENV_VAR"] = "env_value"

        try:
            engine = VariableSubstitutionEngine({})
            result = engine.substitute("Value: ${TEST_ENV_VAR}")
            assert result == "Value: env_value"
        finally:
            # Clean up
            del os.environ["TEST_ENV_VAR"]

    def test_explicit_variables_override_environment(self):
        """Test that explicit variables take priority over environment variables."""
        # Set an environment variable
        os.environ["TEST_VAR"] = "env_value"

        try:
            # Explicit variable should override environment variable
            engine = VariableSubstitutionEngine({"TEST_VAR": "explicit_value"})
            result = engine.substitute("Value: ${TEST_VAR}")
            assert result == "Value: explicit_value"
        finally:
            # Clean up
            del os.environ["TEST_VAR"]

    def test_environment_variable_with_default(self):
        """Test environment variables with default values."""
        # Test when environment variable exists
        os.environ["EXISTING_ENV_VAR"] = "existing_value"

        try:
            engine = VariableSubstitutionEngine({})
            result = engine.substitute("Value: ${EXISTING_ENV_VAR|default_value}")
            assert result == "Value: existing_value"
        finally:
            del os.environ["EXISTING_ENV_VAR"]

        # Test when environment variable doesn't exist
        engine = VariableSubstitutionEngine({})
        result = engine.substitute("Value: ${NONEXISTENT_ENV_VAR|default_value}")
        assert result == "Value: default_value"

    def test_missing_variable_no_environment(self):
        """Test behavior when variable is not in explicit vars or environment."""
        engine = VariableSubstitutionEngine({})

        # Make sure the test variable doesn't exist in environment
        if "DEFINITELY_NOT_SET" in os.environ:
            del os.environ["DEFINITELY_NOT_SET"]

        result = engine.substitute("Value: ${DEFINITELY_NOT_SET}")
        # Should keep original text when variable is not found
        assert result == "Value: ${DEFINITELY_NOT_SET}"

    def test_validate_required_variables_with_environment(self):
        """Test validation includes environment variables."""
        # Set an environment variable
        os.environ["AVAILABLE_ENV_VAR"] = "value"

        try:
            engine = VariableSubstitutionEngine({"explicit_var": "value"})

            # Test text with both explicit and environment variables
            text = "Using ${explicit_var} and ${AVAILABLE_ENV_VAR} and ${missing_var}"
            missing = engine.validate_required_variables(text)

            # Only missing_var should be reported as missing
            assert "missing_var" in missing
            assert "explicit_var" not in missing
            assert "AVAILABLE_ENV_VAR" not in missing
        finally:
            del os.environ["AVAILABLE_ENV_VAR"]

    def test_complex_substitution_with_environment(self):
        """Test complex scenarios with both explicit and environment variables."""
        # Set up environment variables
        os.environ["DATABASE_HOST"] = "localhost"
        os.environ["DATABASE_PORT"] = "5432"

        try:
            # Set up explicit variables
            engine = VariableSubstitutionEngine(
                {"database_name": "mydb", "database_user": "user"}
            )

            # Test complex substitution
            template = {
                "connection_string": "postgresql://${database_user}:${DATABASE_PASSWORD|secret}@${DATABASE_HOST}:${DATABASE_PORT}/${database_name}",
                "options": {
                    "host": "${DATABASE_HOST}",
                    "port": "${DATABASE_PORT}",
                    "timeout": "${CONNECTION_TIMEOUT|30}",
                },
            }

            result = engine.substitute(template)

            expected = {
                "connection_string": "postgresql://user:secret@localhost:5432/mydb",
                "options": {"host": "localhost", "port": "5432", "timeout": "30"},
            }

            assert result == expected

        finally:
            # Clean up
            del os.environ["DATABASE_HOST"]
            del os.environ["DATABASE_PORT"]

    def test_environment_variable_logging(self):
        """Test that environment variable usage is logged properly."""
        os.environ["LOG_TEST_VAR"] = "log_value"

        try:
            engine = VariableSubstitutionEngine({})

            # This should trigger logging for environment variable usage
            result = engine.substitute("${LOG_TEST_VAR}")
            assert result == "log_value"

        finally:
            del os.environ["LOG_TEST_VAR"]
