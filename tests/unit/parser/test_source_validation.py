"""Tests for industry-standard SOURCE parameter validation."""

import pytest

from sqlflow.parser.source_validation import SourceParameterValidator


class TestSourceParameterValidator:
    """Test suite for SourceParameterValidator functionality."""

    @pytest.fixture
    def validator(self):
        """Create a SourceParameterValidator instance for testing."""
        return SourceParameterValidator()

    def test_valid_incremental_sync_mode(self, validator):
        """Test valid incremental sync mode with cursor field."""
        params = {
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_valid_full_refresh_sync_mode(self, validator):
        """Test valid full refresh sync mode."""
        params = {
            "sync_mode": "full_refresh",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_valid_cdc_sync_mode(self, validator):
        """Test valid CDC sync mode."""
        params = {
            "sync_mode": "cdc",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_invalid_sync_mode(self, validator):
        """Test invalid sync mode value."""
        params = {
            "sync_mode": "invalid_mode",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "Invalid sync_mode 'invalid_mode'" in errors[0]
        assert "cdc, full_refresh, incremental" in errors[0]

    def test_sync_mode_wrong_type(self, validator):
        """Test sync mode with wrong type."""
        params = {
            "sync_mode": 123,
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "sync_mode must be a string" in errors[0]

    def test_incremental_without_cursor_field(self, validator):
        """Test incremental sync mode without cursor field."""
        params = {
            "sync_mode": "incremental",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "sync_mode 'incremental' requires cursor_field to be specified" in errors[0]

    def test_full_refresh_with_cursor_field(self, validator):
        """Test full refresh with cursor field (should be invalid)."""
        params = {
            "sync_mode": "full_refresh",
            "cursor_field": "updated_at",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "sync_mode 'full_refresh' cannot be used with cursor_field" in errors[0]

    def test_cdc_with_cursor_field(self, validator):
        """Test CDC with cursor field (should be invalid)."""
        params = {
            "sync_mode": "cdc",
            "cursor_field": "updated_at",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "sync_mode 'cdc' cannot be used with cursor_field" in errors[0]

    def test_valid_cursor_field(self, validator):
        """Test valid cursor field."""
        params = {
            "cursor_field": "updated_at",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_empty_cursor_field(self, validator):
        """Test empty cursor field."""
        params = {
            "cursor_field": "",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "cursor_field cannot be empty" in errors[0]

    def test_cursor_field_wrong_type(self, validator):
        """Test cursor field with wrong type."""
        params = {
            "cursor_field": 123,
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "cursor_field must be a string" in errors[0]

    def test_valid_primary_key_string(self, validator):
        """Test valid primary key as string."""
        params = {
            "primary_key": "user_id",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_valid_primary_key_list(self, validator):
        """Test valid primary key as list."""
        params = {
            "primary_key": ["user_id", "tenant_id"],
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_empty_primary_key_string(self, validator):
        """Test empty primary key string."""
        params = {
            "primary_key": "",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "primary_key cannot be empty" in errors[0]

    def test_empty_primary_key_list(self, validator):
        """Test empty primary key list."""
        params = {
            "primary_key": [],
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "primary_key list cannot be empty" in errors[0]

    def test_primary_key_list_with_empty_values(self, validator):
        """Test primary key list with empty values."""
        params = {
            "primary_key": ["user_id", "", "tenant_id"],
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "All primary_key values must be non-empty strings" in errors[0]

    def test_primary_key_wrong_type(self, validator):
        """Test primary key with wrong type."""
        params = {
            "primary_key": 123,
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "primary_key must be a string or list of strings" in errors[0]

    def test_valid_destination_sync_mode(self, validator):
        """Test valid destination sync modes."""
        # Test overwrite and append (no primary key required)
        for mode in ["overwrite", "append"]:
            params = {
                "destination_sync_mode": mode,
                "table": "users"
            }
            
            errors = validator.validate_parameters(params)
            assert errors == []
            
        # Test append_dedup (requires primary key)
        params = {
            "destination_sync_mode": "append_dedup",
            "primary_key": "user_id",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_invalid_destination_sync_mode(self, validator):
        """Test invalid destination sync mode."""
        params = {
            "destination_sync_mode": "invalid_mode",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "Invalid destination_sync_mode 'invalid_mode'" in errors[0]

    def test_append_dedup_without_primary_key(self, validator):
        """Test append_dedup without primary key."""
        params = {
            "destination_sync_mode": "append_dedup",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "destination_sync_mode 'append_dedup' requires primary_key to be specified" in errors[0]

    def test_valid_replication_method(self, validator):
        """Test valid replication methods."""
        for method in ["FULL_TABLE", "INCREMENTAL", "LOG_BASED"]:
            params = {
                "replication_method": method,
                "table": "users"
            }
            
            errors = validator.validate_parameters(params)
            assert errors == []

    def test_invalid_replication_method(self, validator):
        """Test invalid replication method."""
        params = {
            "replication_method": "INVALID_METHOD",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "Invalid replication_method 'INVALID_METHOD'" in errors[0]

    def test_valid_datetime_format(self, validator):
        """Test valid datetime format."""
        params = {
            "cursor_field_datetime_format": "%Y-%m-%d %H:%M:%S",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_empty_datetime_format(self, validator):
        """Test empty datetime format."""
        params = {
            "cursor_field_datetime_format": "",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "cursor_field_datetime_format cannot be empty" in errors[0]

    def test_valid_lookback_window(self, validator):
        """Test valid lookback window."""
        params = {
            "lookback_window": 3600,
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_negative_lookback_window(self, validator):
        """Test negative lookback window."""
        params = {
            "lookback_window": -1,
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "lookback_window must be non-negative" in errors[0]

    def test_lookback_window_wrong_type(self, validator):
        """Test lookback window with wrong type."""
        params = {
            "lookback_window": "3600",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 1
        assert "lookback_window must be an integer" in errors[0]

    def test_valid_date_parameters(self, validator):
        """Test valid date parameters."""
        params = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_empty_date_parameters(self, validator):
        """Test empty date parameters."""
        params = {
            "start_date": "",
            "end_date": "",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) == 2
        assert "start_date cannot be empty" in errors[0]
        assert "end_date cannot be empty" in errors[1]

    def test_migration_suggestions_replication_method(self, validator):
        """Test migration suggestions for replication method."""
        params = {
            "replication_method": "FULL_TABLE",
            "table": "users"
        }
        
        suggestions = validator.get_migration_suggestions(params)
        assert len(suggestions) == 1
        assert "Consider using sync_mode: 'full_refresh'" in suggestions[0]

    def test_migration_suggestions_incremental_lookback(self, validator):
        """Test migration suggestions for incremental without lookback."""
        params = {
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "table": "users"
        }
        
        suggestions = validator.get_migration_suggestions(params)
        assert len(suggestions) == 1
        assert "Consider adding lookback_window" in suggestions[0]

    def test_complex_valid_configuration(self, validator):
        """Test complex but valid configuration."""
        params = {
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "cursor_field_datetime_format": "%Y-%m-%d %H:%M:%S",
            "primary_key": ["user_id", "tenant_id"],
            "destination_sync_mode": "append_dedup",
            "lookback_window": 3600,
            "start_date": "2024-01-01",
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert errors == []

    def test_multiple_errors(self, validator):
        """Test configuration with multiple errors."""
        params = {
            "sync_mode": "invalid_mode",
            "cursor_field": "",
            "primary_key": [],
            "destination_sync_mode": "append_dedup",
            "lookback_window": -1,
            "table": "users"
        }
        
        errors = validator.validate_parameters(params)
        assert len(errors) >= 4  # Should have multiple validation errors 