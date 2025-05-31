"""Tests for AST SOURCE validation with industry-standard parameters."""

import pytest

from sqlflow.parser.ast import SourceDefinitionStep


class TestSourceDefinitionStepValidation:
    """Test suite for SourceDefinitionStep validation with industry standards."""

    def test_valid_incremental_source(self):
        """Test valid incremental SOURCE with industry-standard parameters."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="postgres",
            params={
                "table": "users",
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
                "primary_key": "user_id"
            }
        )
        
        errors = source.validate()
        assert errors == []

    def test_invalid_incremental_source_missing_cursor(self):
        """Test invalid incremental SOURCE missing cursor field."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="postgres",
            params={
                "table": "users",
                "sync_mode": "incremental"
                # Missing cursor_field
            }
        )
        
        errors = source.validate()
        assert len(errors) == 1
        assert "sync_mode 'incremental' requires cursor_field to be specified" in errors[0]

    def test_invalid_full_refresh_with_cursor(self):
        """Test invalid full refresh SOURCE with cursor field."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="postgres",
            params={
                "table": "users",
                "sync_mode": "full_refresh",
                "cursor_field": "updated_at"  # Should not be used with full_refresh
            }
        )
        
        errors = source.validate()
        assert len(errors) == 1
        assert "sync_mode 'full_refresh' cannot be used with cursor_field" in errors[0]

    def test_invalid_sync_mode(self):
        """Test invalid sync mode value."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="postgres",
            params={
                "table": "users",
                "sync_mode": "invalid_mode"
            }
        )
        
        errors = source.validate()
        assert len(errors) == 1
        assert "Invalid sync_mode 'invalid_mode'" in errors[0]

    def test_migration_suggestions(self):
        """Test migration suggestions for legacy parameters."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="postgres",
            params={
                "table": "users",
                "replication_method": "FULL_TABLE"
            }
        )
        
        suggestions = source.get_migration_suggestions()
        assert len(suggestions) == 1
        assert "Consider using sync_mode: 'full_refresh'" in suggestions[0]

    def test_complex_valid_configuration(self):
        """Test complex but valid configuration."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="postgres",
            params={
                "table": "users",
                "sync_mode": "incremental",
                "cursor_field": "updated_at",
                "cursor_field_datetime_format": "%Y-%m-%d %H:%M:%S",
                "primary_key": ["user_id", "tenant_id"],
                "destination_sync_mode": "append_dedup",
                "lookback_window": 3600,
                "start_date": "2024-01-01"
            }
        )
        
        errors = source.validate()
        assert errors == []

    def test_from_profile_syntax_validation(self):
        """Test FROM profile syntax with industry-standard parameters."""
        source = SourceDefinitionStep(
            name="users",
            connector_type="",  # Empty for FROM syntax
            params={
                "table": "users",
                "sync_mode": "incremental",
                "cursor_field": "updated_at"
            },
            is_from_profile=True,
            profile_connector_name="postgres_prod"
        )
        
        errors = source.validate()
        assert errors == []

    def test_multiple_validation_errors(self):
        """Test SOURCE with multiple validation errors."""
        source = SourceDefinitionStep(
            name="",  # Missing name
            connector_type="postgres",
            params={
                "table": "users",
                "sync_mode": "invalid_mode",  # Invalid sync mode
                "cursor_field": "",  # Empty cursor field
                "primary_key": [],  # Empty primary key list
                "destination_sync_mode": "append_dedup",  # Requires primary key
                "lookback_window": -1  # Negative lookback window
            }
        )
        
        errors = source.validate()
        assert len(errors) >= 5  # Should have multiple validation errors
        
        # Check that we have both basic SOURCE validation and parameter validation errors
        error_text = " ".join(errors)
        assert "SOURCE directive requires a name" in error_text
        assert "Invalid sync_mode" in error_text
        assert "cursor_field cannot be empty" in error_text 