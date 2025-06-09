"""Tests for the warning tracking in the SQLGenerator class."""

from unittest.mock import patch

from sqlflow.core.sql_generator import SQLGenerator


def test_log_warning_once():
    """Test that _log_warning_once only logs each unique warning once."""
    # Arrange
    generator = SQLGenerator()

    # Act & Assert - with mocked logger
    with patch("sqlflow.core.sql_generator.logger") as mock_logger:
        # First warning with a unique key - should log
        generator._log_warning_once("warning1", "This is warning 1")
        mock_logger.warning.assert_called_once_with("This is warning 1")
        mock_logger.warning.reset_mock()

        # Same warning key again - should not log
        generator._log_warning_once("warning1", "This is warning 1 again")
        mock_logger.warning.assert_not_called()

        # Different warning key - should log
        generator._log_warning_once("warning2", "This is warning 2")
        mock_logger.warning.assert_called_once_with("This is warning 2")


def test_warning_counts():
    """Test that warning counts are tracked correctly."""
    # Arrange
    generator = SQLGenerator()

    # Act
    generator._log_warning_once("warning1", "This is warning 1")
    generator._log_warning_once("warning1", "This is warning 1 again")
    generator._log_warning_once("warning1", "This is warning 1 yet again")
    generator._log_warning_once("warning2", "This is warning 2")

    # Assert
    warning_counts = generator.get_warning_summary()
    assert warning_counts["warning1"] == 3
    assert warning_counts["warning2"] == 1


def test_reset_warning_tracking():
    """Test that reset_warning_tracking clears the warning state."""
    # Arrange
    generator = SQLGenerator()

    # Act - generate some warnings
    generator._log_warning_once("warning1", "This is warning 1")
    generator._log_warning_once("warning2", "This is warning 2")

    # Verify we have warnings
    assert len(generator.get_warning_summary()) == 2

    # Reset the tracker
    generator.reset_warning_tracking()

    # Assert
    assert len(generator.get_warning_summary()) == 0

    # After reset, should be able to log the same warnings again
    with patch("sqlflow.core.sql_generator.logger") as mock_logger:
        generator._log_warning_once("warning1", "This is warning 1")
        mock_logger.warning.assert_called_once()


def test_unknown_source_type_warning():
    """Test warning for unknown source connector types."""
    # Arrange
    generator = SQLGenerator()

    operation = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_source",
        "source_connector_type": "UNKNOWN_TYPE",
        "query": {},
    }

    # Act - with mocked logger
    with patch("sqlflow.core.sql_generator.logger") as mock_logger:
        # Generate SQL for operation
        sql = generator._generate_source_sql(operation, {})

        # Assert
        # Verify warning was logged with debug level
        mock_logger.debug.assert_called()
        warning_msg = mock_logger.debug.call_args[0][0]
        assert "Unknown or unsupported source connector type" in warning_msg
        assert (
            "unknown_type" in warning_msg
        )  # Connector types are normalized to lowercase

        # SQL should have appropriate comment
        assert "-- Unknown source type: unknown_type" in sql
        assert "-- Check your connector configuration" in sql


def test_profile_connector_not_found_warning():
    """Test warning for profile connector not found."""
    # Arrange
    generator = SQLGenerator()

    operation = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_source",
        "source_connector_type": "",
        "is_from_profile": True,
        "profile_connector_name": "nonexistent_connector",
        "query": {},
    }

    context = {"profile": {"connectors": {}}}

    # Act - with mocked logger
    with patch("sqlflow.core.sql_generator.logger") as mock_logger:
        # Generate SQL for operation
        generator._generate_source_sql(operation, context)

        # Assert
        # Should have one warning call for missing connector
        mock_logger.debug.assert_called()
        warning_msg = mock_logger.debug.call_args[0][0]
        assert "Unknown or unsupported source connector type" in warning_msg
        assert (
            "Check that connector 'nonexistent_connector' in your profile has a valid 'type' setting"
            in warning_msg
        )
