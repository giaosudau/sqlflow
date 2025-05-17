"""Tests for the validation features in the planner."""

from unittest.mock import MagicMock, patch

import pytest

from sqlflow.core.errors import PlanningError
from sqlflow.core.planner import ExecutionPlanBuilder
from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    ExportStep,
    LoadStep,
    Pipeline,
    SourceDefinitionStep,
    SQLBlockStep,
)


class TestDuplicateTableDetection:
    """Test cases for duplicate table detection."""

    def test_duplicate_table_detection(self):
        """Test that duplicate table definitions are detected."""
        pipeline = Pipeline()

        # Add two steps with the same table name
        load_step = LoadStep(
            table_name="users",
            source_name="users_source",
            line_number=10,
        )
        pipeline.add_step(load_step)

        sql_step = SQLBlockStep(
            table_name="users",
            sql_query="SELECT * FROM other_table",
            line_number=20,
        )
        pipeline.add_step(sql_step)

        builder = ExecutionPlanBuilder()

        # Check that the duplicate table is detected
        with pytest.raises(PlanningError) as excinfo:
            builder._build_table_to_step_mapping(pipeline)

        # Verify the error message contains line numbers and table name
        error_msg = str(excinfo.value)
        assert "Duplicate table definitions" in error_msg
        assert "users" in error_msg
        assert "line 10" in error_msg
        assert "line 20" in error_msg

    def test_no_duplicate_tables(self):
        """Test that valid table definitions are accepted."""
        pipeline = Pipeline()

        # Add steps with different table names
        load_step = LoadStep(
            table_name="users",
            source_name="users_source",
            line_number=10,
        )
        pipeline.add_step(load_step)

        sql_step = SQLBlockStep(
            table_name="filtered_users",
            sql_query="SELECT * FROM users",
            line_number=20,
        )
        pipeline.add_step(sql_step)

        builder = ExecutionPlanBuilder()

        # Check that no exception is raised
        table_map = builder._build_table_to_step_mapping(pipeline)
        assert len(table_map) == 2
        assert "users" in table_map
        assert "filtered_users" in table_map


class TestTableReferenceExtraction:
    """Test cases for SQL table reference extraction."""

    def test_extract_referenced_tables(self):
        """Test extraction of table references from SQL queries."""
        builder = ExecutionPlanBuilder()

        # Test simple FROM clause
        sql = "SELECT * FROM users"
        tables = builder._extract_referenced_tables(sql)
        assert tables == ["users"]

        # Test multiple FROM tables
        sql = "SELECT * FROM users, orders, products"
        tables = builder._extract_referenced_tables(sql)
        assert set(tables) == {"users", "orders", "products"}

        # Test JOIN clause
        sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        tables = builder._extract_referenced_tables(sql)
        assert set(tables) == {"users", "orders"}

        # Test multiple JOINs
        sql = "SELECT * FROM users JOIN orders JOIN products"
        tables = builder._extract_referenced_tables(sql)
        assert set(tables) == {"users", "orders", "products"}

        # Test case insensitivity
        sql = "SELECT * FROM USERS join ORDERS"
        tables = builder._extract_referenced_tables(sql)
        assert set(tables) == {"users", "orders"}

    def test_find_table_references(self):
        """Test dependency creation based on table references."""
        pipeline = Pipeline()

        # Create steps
        users_step = LoadStep(
            table_name="users",
            source_name="users_source",
            line_number=10,
        )

        orders_step = LoadStep(
            table_name="orders",
            source_name="orders_source",
            line_number=15,
        )

        # Step that references both tables
        report_step = SQLBlockStep(
            table_name="user_report",
            sql_query="SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            line_number=20,
        )

        pipeline.add_step(users_step)
        pipeline.add_step(orders_step)
        pipeline.add_step(report_step)

        builder = ExecutionPlanBuilder()
        table_to_step = builder._build_table_to_step_mapping(pipeline)

        # Mock the _add_dependency method to verify it's called correctly
        with patch.object(builder, "_add_dependency") as mock_add_dependency:
            builder._find_table_references(
                report_step, report_step.sql_query.lower(), table_to_step
            )

            # Verify dependencies were added
            assert mock_add_dependency.call_count == 2
            mock_add_dependency.assert_any_call(report_step, users_step)
            mock_add_dependency.assert_any_call(report_step, orders_step)

    def test_undefined_table_warning(self):
        """Test warning for undefined table references."""
        pipeline = Pipeline()

        # Step that references an undefined table
        report_step = SQLBlockStep(
            table_name="user_report",
            sql_query="SELECT * FROM nonexistent_table",
            line_number=20,
        )

        pipeline.add_step(report_step)

        builder = ExecutionPlanBuilder()
        table_to_step = builder._build_table_to_step_mapping(pipeline)

        # Mock the logger to check warnings
        with patch("sqlflow.core.planner.logger") as mock_logger:
            builder._find_table_references(
                report_step, report_step.sql_query.lower(), table_to_step
            )

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "tables that might not be defined" in warning_msg
            assert "nonexistent_table" in warning_msg
            assert "line 20" in warning_msg


class TestSQLSyntaxValidation:
    """Test cases for SQL syntax validation."""

    def test_unmatched_parentheses_warning(self):
        """Test warning for unmatched parentheses in SQL."""
        builder = ExecutionPlanBuilder()

        with patch("sqlflow.core.planner.logger") as mock_logger:
            builder._validate_sql_syntax(
                "SELECT * FROM users WHERE (id > 10", "transform_users", 10
            )

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "Unmatched parentheses" in warning_msg

    def test_missing_select_warning(self):
        """Test warning for SQL without SELECT keyword."""
        builder = ExecutionPlanBuilder()

        with patch("sqlflow.core.planner.logger") as mock_logger:
            builder._validate_sql_syntax(
                "INSERT INTO users VALUES (1, 'test')", "transform_users", 10
            )

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "doesn't contain SELECT keyword" in warning_msg

    def test_incomplete_from_clause_warning(self):
        """Test warning for incomplete FROM clause."""
        builder = ExecutionPlanBuilder()

        with patch("sqlflow.core.planner.logger") as mock_logger:
            builder._validate_sql_syntax("SELECT * FROM ", "transform_users", 10)

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "FROM clause appears to be incomplete" in warning_msg

    def test_unclosed_quotes_warning(self):
        """Test warning for unclosed quotes in SQL."""
        builder = ExecutionPlanBuilder()

        with patch("sqlflow.core.planner.logger") as mock_logger:
            builder._validate_sql_syntax(
                "SELECT * FROM users WHERE name = 'John", "transform_users", 10
            )

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "Unclosed single quotes" in warning_msg

    def test_multiple_statements_info(self):
        """Test info message for multiple SQL statements."""
        builder = ExecutionPlanBuilder()

        with patch("sqlflow.core.planner.logger") as mock_logger:
            builder._validate_sql_syntax(
                "SELECT * FROM users; SELECT * FROM orders", "transform_users", 10
            )

            # Verify info was logged
            mock_logger.info.assert_called_once()
            info_msg = mock_logger.info.call_args[0][0]
            assert "multiple SQL statements" in info_msg


class TestCycleDetection:
    """Test cases for cycle detection in dependencies."""

    def test_detect_simple_cycle(self):
        """Test detection of a simple cycle in the dependency graph."""
        builder = ExecutionPlanBuilder()

        # Create a simple cycle: A -> B -> A
        resolver = MagicMock()
        resolver.dependencies = {"A": ["B"], "B": ["A"]}

        cycles = builder._detect_cycles(resolver)

        assert len(cycles) == 1
        assert cycles[0] == ["A", "B", "A"] or cycles[0] == ["B", "A", "B"]

    def test_detect_complex_cycle(self):
        """Test detection of a more complex cycle."""
        builder = ExecutionPlanBuilder()

        # Create a more complex dependency graph
        resolver = MagicMock()
        resolver.dependencies = {
            "A": ["B"],
            "B": ["C", "D"],
            "C": ["E"],
            "D": ["F"],
            "E": ["A"],  # This creates a cycle: A -> B -> C -> E -> A
            "F": [],
        }

        cycles = builder._detect_cycles(resolver)

        assert len(cycles) == 1
        # The exact cycle order can vary depending on traversal, but should contain these nodes
        cycle = cycles[0]
        assert "A" in cycle
        assert "B" in cycle
        assert "C" in cycle
        assert "E" in cycle

    def test_format_cycle_error(self):
        """Test formatting of cycle error messages."""
        builder = ExecutionPlanBuilder()

        # Create sample cycles
        cycles = [
            [
                "source_users",
                "transform_filtered_users",
                "export_csv_users",
                "source_users",
            ],
            ["load_orders", "transform_order_summary", "load_orders"],
        ]

        error_msg = builder._format_cycle_error(cycles)

        # Check formatting
        assert "Cycle 1:" in error_msg
        assert "Cycle 2:" in error_msg
        assert "→" in error_msg  # Should use arrow notation

        # Check readable names
        assert "SOURCE users" in error_msg or "source_users" in error_msg
        assert (
            "CREATE TABLE filtered_users" in error_msg
            or "transform_filtered_users" in error_msg
        )
        assert "EXPORT users to csv" in error_msg or "export_csv_users" in error_msg
        assert "LOAD orders" in error_msg or "load_orders" in error_msg
        assert (
            "CREATE TABLE order_summary" in error_msg
            or "transform_order_summary" in error_msg
        )

    def test_resolve_execution_order_with_cycles(self):
        """Test that PlanningError is raised for cyclic dependencies."""
        pipeline = Pipeline()

        # Create steps that form a cycle
        step1 = SQLBlockStep(
            table_name="table1",
            sql_query="SELECT * FROM table2",
            line_number=10,
        )

        step2 = SQLBlockStep(
            table_name="table2",
            sql_query="SELECT * FROM table1",
            line_number=20,
        )

        pipeline.add_step(step1)
        pipeline.add_step(step2)

        builder = ExecutionPlanBuilder()

        # Set up the dependency map manually
        builder.step_id_map = {
            id(step1): "transform_table1",
            id(step2): "transform_table2",
        }
        builder.step_dependencies = {
            "transform_table1": ["transform_table2"],
            "transform_table2": ["transform_table1"],
        }

        # Mock the dependency resolver for _resolve_execution_order
        with patch.object(
            builder, "_create_dependency_resolver"
        ) as mock_resolver_factory:
            mock_resolver = MagicMock()
            mock_resolver_factory.return_value = mock_resolver

            # Set up the mock to raise an exception simulating a cycle
            mock_resolver.resolve_dependencies.side_effect = Exception(
                "Cyclic dependency detected"
            )

            # Ensure detect_cycles returns a cycle
            with patch.object(builder, "_detect_cycles") as mock_detect_cycles:
                mock_detect_cycles.return_value = [
                    ["transform_table1", "transform_table2", "transform_table1"]
                ]

                # Check that execution order resolution raises PlanningError
                with pytest.raises(PlanningError) as excinfo:
                    builder._resolve_execution_order()

                error_msg = str(excinfo.value)
                # Accept either message format as the implementation might change
                assert (
                    "Circular dependencies detected" in error_msg
                    or "Failed to resolve execution order" in error_msg
                )


class TestVariableValidation:
    """Test cases for variable reference validation."""

    def test_extract_variable_references(self):
        """Test extracting variable references from text."""
        builder = ExecutionPlanBuilder()
        result = set()

        # Test basic variable references
        builder._extract_variable_references("${var1} and ${var2}", result)
        assert result == {"var1", "var2"}

        # Test with default values
        result.clear()
        builder._extract_variable_references(
            "${var1|default} and ${var2|other}", result
        )
        assert result == {"var1", "var2"}

        # Test with spaces in variable names
        result.clear()
        builder._extract_variable_references("${var1 } and ${ var2}", result)
        assert result == {"var1", "var2"}

    def test_has_default_in_pipeline(self):
        """Test detection of variables with default values."""
        pipeline = Pipeline()

        export_step = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="s3://${bucket|default-bucket}/data.csv",
            connector_type="csv",
            options={},
            line_number=10,
        )
        pipeline.add_step(export_step)

        builder = ExecutionPlanBuilder()

        # Check variable with default
        assert builder._has_default_in_pipeline("bucket", pipeline) is True

        # Check variable without default
        assert builder._has_default_in_pipeline("region", pipeline) is False

    def test_validate_variable_references_all_defined(self):
        """Test validation when all variables are defined."""
        pipeline = Pipeline()

        # Create step with variable references
        source_step = SourceDefinitionStep(
            name="db_source",
            connector_type="postgres",
            params={"host": "${db_host}", "port": 5432, "database": "${db_name}"},
            line_number=10,
        )
        pipeline.add_step(source_step)

        builder = ExecutionPlanBuilder()
        variables = {"db_host": "localhost", "db_name": "test_db"}

        # No exception should be raised
        builder._validate_variable_references(pipeline, variables)

    def test_validate_variable_references_missing(self):
        """Test validation when variables are missing."""
        pipeline = Pipeline()

        # Create conditional step with variable reference
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'",
            steps=[],
            line_number=10,
        )
        conditional_block = ConditionalBlockStep(
            branches=[if_branch],
            else_branch=[],
            line_number=10,
        )
        pipeline.add_step(conditional_block)

        builder = ExecutionPlanBuilder()
        variables = {}  # No variables defined

        # PlanningError should be raised
        with pytest.raises(PlanningError) as excinfo:
            builder._validate_variable_references(pipeline, variables)

        error_msg = str(excinfo.value)
        assert "Pipeline references undefined variables" in error_msg
        assert "${env}" in error_msg
        assert "is used but not defined" in error_msg

    def test_validate_variable_with_default(self):
        """Test that variables with defaults don't trigger errors."""
        pipeline = Pipeline()

        # Create export step with variable references with defaults
        export_step = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="s3://${bucket|default-bucket}/${path|data}/output.csv",
            connector_type="csv",
            options={},
            line_number=10,
        )
        pipeline.add_step(export_step)

        builder = ExecutionPlanBuilder()
        variables = {}  # No variables defined

        # No exception should be raised due to defaults
        builder._validate_variable_references(pipeline, variables)


class TestJSONParsing:
    """Test cases for JSON parsing with helpful errors."""

    def test_parse_valid_json(self):
        """Test parsing valid JSON."""
        builder = ExecutionPlanBuilder()

        valid_json = '{"name": "value", "array": [1, 2, 3]}'
        result = builder._parse_json_token(valid_json, "test context")

        assert result == {"name": "value", "array": [1, 2, 3]}

    def test_parse_invalid_json_property_name(self):
        """Test parsing JSON with invalid property name."""
        builder = ExecutionPlanBuilder()

        # Missing quotes around property name
        invalid_json = '{name: "value"}'

        with pytest.raises(PlanningError) as excinfo:
            builder._parse_json_token(invalid_json, "OPTIONS")

        error_msg = str(excinfo.value)
        assert "Invalid JSON in OPTIONS" in error_msg
        assert "Expecting property name" in error_msg
        assert "Property names must be in double quotes" in error_msg

    def test_parse_invalid_json_missing_comma(self):
        """Test parsing JSON with missing comma."""
        builder = ExecutionPlanBuilder()

        # Missing comma between properties
        invalid_json = '{"prop1": "value1" "prop2": "value2"}'

        with pytest.raises(PlanningError) as excinfo:
            builder._parse_json_token(invalid_json, "PARAMS")

        error_msg = str(excinfo.value)
        assert "Invalid JSON in PARAMS" in error_msg
        assert "delimiter" in error_msg
        assert "Check for missing commas" in error_msg

    def test_parse_invalid_json_expecting_value(self):
        """Test parsing JSON with missing value."""
        builder = ExecutionPlanBuilder()

        # Missing value after colon
        invalid_json = '{"prop1": }'

        with pytest.raises(PlanningError) as excinfo:
            builder._parse_json_token(invalid_json, "source config")

        error_msg = str(excinfo.value)
        assert "Invalid JSON in source config" in error_msg
        assert "Expecting value" in error_msg
        assert "Make sure all property values are valid" in error_msg
