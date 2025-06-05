"""Tests for SQLFlow Transform Mode AST extensions."""

from sqlflow.parser.ast import SQLBlockStep


class TestSQLBlockStepTransformModes:
    """Test transform mode functionality in SQLBlockStep."""

    def test_standard_sql_block_unchanged(self):
        """Test that standard SQL blocks work unchanged."""
        step = SQLBlockStep(
            table_name="test_table", sql_query="SELECT 1 as id", line_number=1
        )

        assert step.table_name == "test_table"
        assert step.sql_query == "SELECT 1 as id"
        assert step.mode is None
        assert step.time_column is None
        assert step.upsert_keys == []
        assert step.lookback is None
        assert not step.is_transform_mode()

        # Validation should pass for standard SQL blocks
        errors = step.validate()
        assert errors == []

    def test_replace_mode_valid(self):
        """Test valid REPLACE mode transform."""
        step = SQLBlockStep(
            table_name="daily_sales",
            sql_query="SELECT order_date, SUM(amount) FROM orders GROUP BY order_date",
            mode="REPLACE",
            line_number=1,
        )

        assert step.mode == "REPLACE"
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_append_mode_valid(self):
        """Test valid APPEND mode transform."""
        step = SQLBlockStep(
            table_name="event_log",
            sql_query="SELECT event_id, event_type FROM raw_events",
            mode="APPEND",
            line_number=1,
        )

        assert step.mode == "APPEND"
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_upsert_mode_valid_single_key(self):
        """Test valid UPSERT mode with single key."""
        step = SQLBlockStep(
            table_name="customer_summary",
            sql_query="SELECT customer_id, COUNT(*) as orders FROM orders GROUP BY customer_id",
            mode="UPSERT",
            upsert_keys=["customer_id"],
            line_number=1,
        )

        assert step.mode == "UPSERT"
        assert step.upsert_keys == ["customer_id"]
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_upsert_mode_valid_composite_keys(self):
        """Test valid UPSERT mode with composite keys."""
        step = SQLBlockStep(
            table_name="product_metrics",
            sql_query="SELECT product_id, region, SUM(sales) FROM sales GROUP BY product_id, region",
            mode="UPSERT",
            upsert_keys=["product_id", "region"],
            line_number=1,
        )

        assert step.mode == "UPSERT"
        assert step.upsert_keys == ["product_id", "region"]
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_incremental_mode_valid(self):
        """Test valid INCREMENTAL mode."""
        step = SQLBlockStep(
            table_name="daily_metrics",
            sql_query="SELECT event_date, COUNT(*) FROM events WHERE event_date BETWEEN @start_date AND @end_date GROUP BY event_date",
            mode="INCREMENTAL",
            time_column="event_date",
            line_number=1,
        )

        assert step.mode == "INCREMENTAL"
        assert step.time_column == "event_date"
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_incremental_mode_with_lookback_valid(self):
        """Test valid INCREMENTAL mode with LOOKBACK."""
        step = SQLBlockStep(
            table_name="adjusted_metrics",
            sql_query="SELECT product_id, SUM(quantity) FROM sales WHERE updated_at BETWEEN @start_date AND @end_date GROUP BY product_id",
            mode="INCREMENTAL",
            time_column="updated_at",
            lookback="2 DAYS",
            line_number=1,
        )

        assert step.mode == "INCREMENTAL"
        assert step.time_column == "updated_at"
        assert step.lookback == "2 DAYS"
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_invalid_mode(self):
        """Test invalid mode value."""
        step = SQLBlockStep(
            table_name="test_table", sql_query="SELECT 1", mode="INVALID", line_number=1
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "Invalid MODE 'INVALID'" in errors[0]
        assert "REPLACE, APPEND, UPSERT, INCREMENTAL" in errors[0]

    def test_incremental_missing_time_column(self):
        """Test INCREMENTAL mode missing time column."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="INCREMENTAL",
            line_number=1,
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "INCREMENTAL mode requires BY <time_column>" in errors[0]

    def test_incremental_with_upsert_keys_invalid(self):
        """Test INCREMENTAL mode with upsert keys (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="INCREMENTAL",
            time_column="created_at",
            upsert_keys=["id"],
            line_number=1,
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "INCREMENTAL mode cannot use upsert keys" in errors[0]

    def test_upsert_missing_keys(self):
        """Test UPSERT mode missing upsert keys."""
        step = SQLBlockStep(
            table_name="test_table", sql_query="SELECT 1", mode="UPSERT", line_number=1
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "UPSERT mode requires KEY" in errors[0]

    def test_upsert_with_time_column_invalid(self):
        """Test UPSERT mode with time column (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="UPSERT",
            upsert_keys=["id"],
            time_column="created_at",
            line_number=1,
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "UPSERT mode cannot use time column" in errors[0]

    def test_upsert_with_lookback_invalid(self):
        """Test UPSERT mode with lookback (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="UPSERT",
            upsert_keys=["id"],
            lookback="1 DAY",
            line_number=1,
        )

        errors = step.validate()
        assert (
            len(errors) == 2
        )  # Both "UPSERT mode cannot use LOOKBACK" and "LOOKBACK can only be used with INCREMENTAL"
        assert any("UPSERT mode cannot use LOOKBACK" in error for error in errors)
        assert any(
            "LOOKBACK can only be used with INCREMENTAL mode" in error
            for error in errors
        )

    def test_replace_with_time_column_invalid(self):
        """Test REPLACE mode with time column (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="REPLACE",
            time_column="created_at",
            line_number=1,
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "REPLACE mode cannot use time column" in errors[0]

    def test_append_with_upsert_keys_invalid(self):
        """Test APPEND mode with upsert keys (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="APPEND",
            upsert_keys=["id"],
            line_number=1,
        )

        errors = step.validate()
        assert len(errors) == 1
        assert "APPEND mode cannot use upsert keys" in errors[0]

    def test_replace_with_lookback_invalid(self):
        """Test REPLACE mode with lookback (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="REPLACE",
            lookback="1 DAY",
            line_number=1,
        )

        errors = step.validate()
        assert (
            len(errors) == 2
        )  # Both "REPLACE mode cannot use LOOKBACK" and "LOOKBACK can only be used with INCREMENTAL"
        assert any("REPLACE mode cannot use LOOKBACK" in error for error in errors)
        assert any(
            "LOOKBACK can only be used with INCREMENTAL mode" in error
            for error in errors
        )

    def test_lookback_without_incremental_invalid(self):
        """Test LOOKBACK with non-INCREMENTAL mode (invalid)."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="APPEND",
            lookback="1 DAY",
            line_number=1,
        )

        errors = step.validate()
        assert (
            len(errors) == 2
        )  # Both "APPEND mode cannot use LOOKBACK" and "LOOKBACK can only be used with INCREMENTAL"
        assert any("APPEND mode cannot use LOOKBACK" in error for error in errors)
        assert any(
            "LOOKBACK can only be used with INCREMENTAL mode" in error
            for error in errors
        )

    def test_multiple_validation_errors(self):
        """Test multiple validation errors at once."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="UPSERT",
            time_column="created_at",
            lookback="1 DAY",
            line_number=1,
        )

        errors = step.validate()
        assert len(errors) == 4
        assert "UPSERT mode requires KEY" in errors[0]
        assert "UPSERT mode cannot use time column" in errors[1]
        assert "UPSERT mode cannot use LOOKBACK" in errors[2]
        assert "LOOKBACK can only be used with INCREMENTAL mode" in errors[3]

    def test_case_insensitive_mode_validation(self):
        """Test that mode validation is case insensitive."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="replace",  # lowercase
            line_number=1,
        )

        errors = step.validate()
        assert errors == []  # Should be valid

        step_upper = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            mode="INCREMENTAL",  # uppercase
            time_column="created_at",
            line_number=1,
        )

        errors = step_upper.validate()
        assert errors == []  # Should be valid

    def test_backward_compatibility_with_existing_fields(self):
        """Test that existing fields still work with new transform fields."""
        step = SQLBlockStep(
            table_name="test_table",
            sql_query="SELECT 1",
            line_number=5,
            is_replace=True,  # Existing field
            mode="REPLACE",  # New field
        )

        assert step.is_replace is True
        assert step.mode == "REPLACE"
        assert step.line_number == 5
        assert step.is_transform_mode()

        errors = step.validate()
        assert errors == []

    def test_default_values(self):
        """Test that all new fields have proper default values."""
        step = SQLBlockStep(table_name="test_table", sql_query="SELECT 1")

        assert step.mode is None
        assert step.time_column is None
        assert step.upsert_keys == []
        assert step.lookback is None
        assert not step.is_transform_mode()
