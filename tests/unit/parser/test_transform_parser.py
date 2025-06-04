"""Tests for SQLFlow Transform Mode Parser."""

import pytest

from sqlflow.parser.ast import SQLBlockStep
from sqlflow.parser.parser import Parser, ParserError


class TestTransformModeParser:
    """Test context-aware parsing for transform modes."""

    def test_standard_sql_unchanged(self):
        """Test that standard DuckDB SQL passes through unchanged."""
        sql = """
        CREATE TABLE config AS
        SELECT mode FROM settings;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "config"
        assert "SELECT mode FROM settings" in step.sql_query
        assert step.mode is None  # No transform mode
        assert not step.is_transform_mode()

    def test_duckdb_functions_with_mode_column(self):
        """Test DuckDB functions that use 'mode' as column name work correctly."""
        sql = """
        CREATE TABLE sales AS
        SELECT *, json_extract(data, '$.mode') as extraction_mode FROM raw_data;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "sales"
        assert step.mode is None  # No transform mode
        assert not step.is_transform_mode()

    def test_replace_mode_transform(self):
        """Test REPLACE mode transform syntax."""
        sql = """
        CREATE TABLE daily_sales MODE REPLACE AS
        SELECT order_date, SUM(amount) FROM orders GROUP BY order_date;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "daily_sales"
        assert step.mode == "REPLACE"
        assert step.time_column is None
        assert step.merge_keys == []
        assert step.lookback is None
        assert step.is_transform_mode()

    def test_append_mode_transform(self):
        """Test APPEND mode transform syntax."""
        sql = """
        CREATE TABLE event_log MODE APPEND AS
        SELECT event_id, event_type, timestamp FROM raw_events;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "event_log"
        assert step.mode == "APPEND"
        assert step.is_transform_mode()

    def test_merge_mode_single_key(self):
        """Test MERGE mode with single key."""
        sql = """
        CREATE TABLE customer_summary MODE MERGE KEY customer_id AS
        SELECT customer_id, COUNT(*) as orders FROM orders GROUP BY customer_id;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "customer_summary"
        assert step.mode == "MERGE"
        assert step.merge_keys == ["customer_id"]
        assert step.is_transform_mode()

    def test_merge_mode_composite_keys(self):
        """Test MERGE mode with composite keys."""
        sql = """
        CREATE TABLE product_metrics MODE MERGE KEY (product_id, region) AS
        SELECT product_id, region, SUM(sales) FROM sales GROUP BY product_id, region;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "product_metrics"
        assert step.mode == "MERGE"
        assert step.merge_keys == ["product_id", "region"]
        assert step.is_transform_mode()

    def test_incremental_mode_basic(self):
        """Test INCREMENTAL mode with BY column."""
        sql = """
        CREATE TABLE daily_metrics MODE INCREMENTAL BY event_date AS
        SELECT event_date, COUNT(*) FROM events 
        WHERE event_date BETWEEN @start_date AND @end_date 
        GROUP BY event_date;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "daily_metrics"
        assert step.mode == "INCREMENTAL"
        assert step.time_column == "event_date"
        assert step.lookback is None
        assert step.is_transform_mode()

    def test_incremental_mode_with_lookback(self):
        """Test INCREMENTAL mode with LOOKBACK."""
        sql = """
        CREATE TABLE adjusted_metrics MODE INCREMENTAL BY updated_at LOOKBACK 2 DAYS AS
        SELECT product_id, SUM(quantity) FROM sales 
        WHERE updated_at BETWEEN @start_date AND @end_date 
        GROUP BY product_id;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "adjusted_metrics"
        assert step.mode == "INCREMENTAL"
        assert step.time_column == "updated_at"
        assert step.lookback == "2 DAYS"
        assert step.is_transform_mode()

    def test_create_or_replace_with_transform_mode(self):
        """Test CREATE OR REPLACE with transform mode."""
        sql = """
        CREATE OR REPLACE TABLE daily_sales MODE REPLACE AS
        SELECT order_date, SUM(amount) FROM orders GROUP BY order_date;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "daily_sales"
        assert step.mode == "REPLACE"
        assert step.is_replace is True
        assert step.is_transform_mode()

    def test_invalid_mode_error(self):
        """Test error handling for invalid mode."""
        sql = """
        CREATE TABLE test_table MODE INVALID AS
        SELECT 1 as id;
        """

        parser = Parser()
        with pytest.raises(ParserError) as exc_info:
            parser.parse(sql)

        assert "Invalid MODE 'INVALID'" in str(exc_info.value)
        assert "REPLACE, APPEND, MERGE, INCREMENTAL" in str(exc_info.value)

    def test_incremental_missing_by_column_error(self):
        """Test error when INCREMENTAL mode is missing BY column."""
        sql = """
        CREATE TABLE test_table MODE INCREMENTAL AS
        SELECT 1 as id;
        """

        parser = Parser()
        with pytest.raises(ParserError) as exc_info:
            parser.parse(sql)

        assert "INCREMENTAL mode requires BY <time_column>" in str(exc_info.value)

    def test_merge_missing_key_error(self):
        """Test error when MERGE mode is missing KEY."""
        sql = """
        CREATE TABLE test_table MODE MERGE AS
        SELECT 1 as id;
        """

        parser = Parser()
        with pytest.raises(ParserError) as exc_info:
            parser.parse(sql)

        assert "MERGE mode requires KEY" in str(exc_info.value)

    def test_syntax_detection_accuracy(self):
        """Test that syntax detection is accurate and doesn't have false positives/negatives."""
        # Standard SQL with 'mode' in different contexts should not trigger transform parsing
        standard_sql_cases = [
            "CREATE TABLE config AS SELECT mode FROM settings;",
            "CREATE TABLE test AS SELECT * FROM table WHERE mode = 'active';",
            "CREATE TABLE analysis AS SELECT mode, COUNT(*) FROM data GROUP BY mode;",
        ]

        for sql in standard_sql_cases:
            parser = Parser()
            pipeline = parser.parse(sql)
            step = pipeline.steps[0]
            assert not step.is_transform_mode(), f"False positive for: {sql}"

        # Transform SQL should trigger transform parsing
        transform_sql_cases = [
            "CREATE TABLE test MODE REPLACE AS SELECT 1;",
            "CREATE TABLE test MODE APPEND AS SELECT 1;",
            "CREATE TABLE test MODE MERGE KEY id AS SELECT 1;",
            "CREATE TABLE test MODE INCREMENTAL BY date AS SELECT 1;",
        ]

        for sql in transform_sql_cases:
            parser = Parser()
            pipeline = parser.parse(sql)
            step = pipeline.steps[0]
            assert step.is_transform_mode(), f"False negative for: {sql}"

    def test_complex_sql_with_transform_mode(self):
        """Test complex SQL queries with transform modes."""
        sql = """
        CREATE TABLE complex_analysis MODE INCREMENTAL BY event_date LOOKBACK 1 DAY AS
        SELECT 
            event_date,
            customer_id,
            COUNT(*) as event_count,
            SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as revenue,
            AVG(session_duration) as avg_session
        FROM events e
        JOIN customers c ON e.customer_id = c.id
        WHERE event_date BETWEEN @start_date AND @end_date
            AND c.status = 'active'
        GROUP BY event_date, customer_id
        HAVING COUNT(*) > 1;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert isinstance(step, SQLBlockStep)
        assert step.table_name == "complex_analysis"
        assert step.mode == "INCREMENTAL"
        assert step.time_column == "event_date"
        assert step.lookback == "1 DAY"
        assert "JOIN customers" in step.sql_query
        assert "HAVING COUNT(*)" in step.sql_query

    def test_case_insensitive_parsing(self):
        """Test that parsing is case insensitive."""
        sql = """
        create table test_table mode incremental by created_at lookback 2 hours as
        select * from events where created_at between @start_date and @end_date;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 1
        step = pipeline.steps[0]
        assert step.mode == "INCREMENTAL"  # Should be normalized to uppercase
        assert step.time_column == "created_at"
        assert step.lookback == "2 hours"

    def test_multiple_statements_mixed(self):
        """Test parsing multiple statements with mixed standard and transform syntax."""
        sql = """
        CREATE TABLE config AS SELECT mode FROM settings;
        
        CREATE TABLE daily_sales MODE REPLACE AS
        SELECT order_date, SUM(amount) FROM orders GROUP BY order_date;
        
        CREATE TABLE customer_summary MODE MERGE KEY customer_id AS
        SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id;
        """

        parser = Parser()
        pipeline = parser.parse(sql)

        assert len(pipeline.steps) == 3

        # First statement: standard SQL
        step1 = pipeline.steps[0]
        assert step1.table_name == "config"
        assert not step1.is_transform_mode()

        # Second statement: REPLACE transform
        step2 = pipeline.steps[1]
        assert step2.table_name == "daily_sales"
        assert step2.mode == "REPLACE"
        assert step2.is_transform_mode()

        # Third statement: MERGE transform
        step3 = pipeline.steps[2]
        assert step3.table_name == "customer_summary"
        assert step3.mode == "MERGE"
        assert step3.merge_keys == ["customer_id"]
        assert step3.is_transform_mode()
