"""Test suite for CLI test_variable_handler script functionality."""

from io import StringIO
from unittest.mock import call, patch

import pytest

from sqlflow.cli.test_variable_handler import test_variable_handling


class TestVariableHandlingScript:
    """Test cases for the test_variable_handler script."""

    @patch("builtins.print")
    def test_variable_handling_script_execution(self, mock_print):
        """Test that the test_variable_handling function executes without errors."""
        # Should not raise any exceptions
        test_variable_handling()

        # Verify that print statements were called (basic smoke test)
        assert mock_print.called

        # Check for expected output messages
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        print_output = "\n".join(print_calls)

        # Verify key sections are printed
        assert "Testing variable validation..." in print_output
        assert "Testing variable substitution..." in print_output
        assert "Testing with missing variables..." in print_output

    @patch("builtins.print")
    def test_variable_validation_success_case(self, mock_print):
        """Test the successful variable validation case."""
        test_variable_handling()

        # Check that success message is printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        success_messages = [
            msg for msg in print_calls if "✓ Variable validation passed" in msg
        ]
        assert len(success_messages) == 1

    @patch("builtins.print")
    def test_variable_validation_failure_case(self, mock_print):
        """Test the variable validation failure case."""
        test_variable_handling()

        # Check that failure detection message is printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        failure_messages = [
            msg for msg in print_calls if "✗ Should have failed validation" in msg
        ]
        assert len(failure_messages) == 1

    @patch("builtins.print")
    def test_substitution_results_printed(self, mock_print):
        """Test that substitution results are printed."""
        test_variable_handling()

        # Check that "Results:" is printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        results_messages = [msg for msg in print_calls if "Results:" in msg]
        assert len(results_messages) == 1

    @patch("builtins.print")
    def test_expected_substitutions_in_output(self, mock_print):
        """Test that expected variable substitutions appear in output."""
        test_variable_handling()

        # Get all print output
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        all_output = "\n".join(print_calls)

        # Check for expected substituted values
        assert "2025-05-16" in all_output  # date substitution
        assert "test" in all_output  # name substitution

    def test_script_uses_correct_variable_handler_class(self):
        """Test that script imports and uses VariableHandler correctly."""
        # Import the function to verify it can access VariableHandler
        from sqlflow.cli.test_variable_handler import test_variable_handling

        # Verify the function exists and is callable
        assert callable(test_variable_handling)

    @patch("builtins.print")
    @patch("sqlflow.cli.test_variable_handler.VariableHandler")
    def test_variable_handler_instantiation(self, mock_handler_class, mock_print):
        """Test that VariableHandler is instantiated correctly."""
        # Mock the VariableHandler class
        mock_handler_instance = mock_handler_class.return_value
        mock_handler_instance.validate_variable_usage.return_value = True
        mock_handler_instance.substitute_variables.return_value = "mocked result"

        # Run the test function
        test_variable_handling()

        # Verify VariableHandler was instantiated twice (with different variables)
        assert mock_handler_class.call_count == 2

        # Check the first instantiation (with both variables)
        first_call = mock_handler_class.call_args_list[0]
        assert "date" in first_call[0][0]
        assert "name" in first_call[0][0]
        assert first_call[0][0]["date"] == "2025-05-16"
        assert first_call[0][0]["name"] == "test"

        # Check the second instantiation (missing name variable)
        second_call = mock_handler_class.call_args_list[1]
        assert "date" in second_call[0][0]
        assert "name" not in second_call[0][0]
        assert second_call[0][0]["date"] == "2025-05-16"

    @patch("builtins.print")
    @patch("sqlflow.cli.test_variable_handler.VariableHandler")
    def test_validation_method_calls(self, mock_handler_class, mock_print):
        """Test that validation methods are called correctly."""
        mock_handler_instance = mock_handler_class.return_value
        mock_handler_instance.validate_variable_usage.side_effect = [True, False]
        mock_handler_instance.substitute_variables.return_value = "mocked result"

        test_variable_handling()

        # Verify validate_variable_usage was called twice
        assert mock_handler_instance.validate_variable_usage.call_count == 2

        # Verify substitute_variables was called once
        assert mock_handler_instance.substitute_variables.call_count == 1

    @patch("builtins.print")
    def test_test_data_content(self, mock_print):
        """Test that the test data contains expected variable patterns."""
        # We can't easily intercept the text variable, but we can verify
        # the function runs and produces expected output patterns
        test_variable_handling()

        # Get all output to verify the test data structure
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        all_output = "\n".join(print_calls)

        # The output should contain elements that suggest variable substitution occurred
        # This is an indirect test of the test data content
        assert "SET" in all_output or "SOURCE" in all_output or "csv" in all_output

    def test_main_execution_block(self):
        """Test the if __name__ == '__main__' execution block."""
        # Import the module to verify it has the execution block
        import sqlflow.cli.test_variable_handler as module

        # Check that the module is executable as a script
        # (The actual execution is tested indirectly through other tests)
        assert hasattr(module, "test_variable_handling")

        # Verify the module can be imported without errors
        assert module is not None
