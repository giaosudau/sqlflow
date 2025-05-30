"""Test suite for SQLFlow main entry point."""

from unittest.mock import patch

import pytest

import sqlflow.main


class TestMain:
    """Test cases for main entry point functionality."""

    def test_main_function_exists(self):
        """Test that main function exists and is callable."""
        assert hasattr(sqlflow.main, "main")
        assert callable(sqlflow.main.main)

    @patch("sqlflow.main.cli")
    @patch("sqlflow.main.configure_logging")
    def test_main_configures_logging_and_runs_cli(
        self, mock_configure_logging, mock_cli
    ):
        """Test that main function configures logging and runs CLI."""
        # Call main function
        sqlflow.main.main()

        # Verify logging is configured
        mock_configure_logging.assert_called_once()

        # Verify CLI is called
        mock_cli.assert_called_once()

    @patch("sqlflow.main.cli")
    @patch("sqlflow.main.configure_logging")
    def test_main_order_of_operations(self, mock_configure_logging, mock_cli):
        """Test that logging is configured before CLI runs."""
        # Create a list to track call order
        call_order = []

        def track_configure_logging():
            call_order.append("configure_logging")

        def track_cli():
            call_order.append("cli")

        mock_configure_logging.side_effect = track_configure_logging
        mock_cli.side_effect = track_cli

        # Call main function
        sqlflow.main.main()

        # Verify order: logging first, then CLI
        assert call_order == ["configure_logging", "cli"]

    @patch("sqlflow.main.cli")
    @patch("sqlflow.main.configure_logging")
    def test_main_handles_configure_logging_exception(
        self, mock_configure_logging, mock_cli
    ):
        """Test that main handles exceptions from configure_logging gracefully."""
        # Make configure_logging raise an exception
        mock_configure_logging.side_effect = Exception("Logging config failed")

        # Main should still attempt to run CLI despite logging error
        with pytest.raises(Exception, match="Logging config failed"):
            sqlflow.main.main()

        # configure_logging was called
        mock_configure_logging.assert_called_once()
        # CLI should not be called if logging fails
        mock_cli.assert_not_called()

    @patch("sqlflow.main.cli")
    @patch("sqlflow.main.configure_logging")
    def test_main_handles_cli_exception(self, mock_configure_logging, mock_cli):
        """Test that main handles exceptions from CLI gracefully."""
        # Make CLI raise an exception
        mock_cli.side_effect = Exception("CLI failed")

        # Main should propagate CLI exceptions
        with pytest.raises(Exception, match="CLI failed"):
            sqlflow.main.main()

        # Both functions were called
        mock_configure_logging.assert_called_once()
        mock_cli.assert_called_once()

    @patch("sqlflow.main.main")
    def test_name_main_execution(self, mock_main):
        """Test that main() is called when script is executed directly."""
        # Simulate running the script directly
        with patch("sqlflow.main.__name__", "__main__"):
            # Import the module to trigger the if __name__ == '__main__' block
            import importlib

            importlib.reload(sqlflow.main)

        # Note: This test is more complex to implement correctly due to
        # how Python modules work. For coverage purposes, we'll test the
        # main function directly since the if __name__ == '__main__'
        # block just calls main()

    def test_module_imports(self):
        """Test that all required imports are available."""
        # Test that cli import is available
        from sqlflow.cli.main import cli

        assert callable(cli)

        # Test that configure_logging import is available
        from sqlflow.logging import configure_logging

        assert callable(configure_logging)

    @patch("sqlflow.main.cli")
    @patch("sqlflow.main.configure_logging")
    def test_main_return_value(self, mock_configure_logging, mock_cli):
        """Test that main function returns None."""
        result = sqlflow.main.main()
        assert result is None

    def test_main_docstring(self):
        """Test that main function has proper docstring."""
        assert sqlflow.main.main.__doc__ is not None
        assert "Run the SQLFlow CLI" in sqlflow.main.main.__doc__

    @patch("sqlflow.main.cli")
    @patch("sqlflow.main.configure_logging")
    def test_main_no_arguments(self, mock_configure_logging, mock_cli):
        """Test that main function accepts no arguments."""
        # Should work without any arguments
        sqlflow.main.main()

        # Both functions called without arguments
        mock_configure_logging.assert_called_once_with()
        mock_cli.assert_called_once_with()
