"""Cross-component variable parsing consistency tests.

This module ensures all components parse variables consistently and format them
correctly for their context, as specified in the VARIABLE_SUBSTITUTION_REFACTOR_TECHNICAL_DESIGN.md.
"""

import time

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.evaluator import ConditionEvaluator
from sqlflow.core.sql_generator import SQLGenerator
from sqlflow.core.variables.manager import VariableConfig, VariableManager


class TestVariableParsingConsistency:
    """Ensure all components parse variables consistently and format them correctly for their context."""

    # Use pytest parameterization for clean and extensive test cases
    @pytest.mark.parametrize(
        "template, variables, duckdb_expected, manager_expected, sql_gen_expected, condition_expected",
        [
            ("No vars", {}, "No vars", "No vars", ("No vars", 0), "No vars"),
            ("${simple}", {"simple": "val"}, "'val'", "val", ("'val'", 1), "'val'"),
            ("${dflt|def}", {}, "'def'", "def", ("'def'", 1), "'def'"),
            ("${dflt|'def'}", {}, "'def'", "def", ("'def'", 1), "'def'"),
            (
                "Two: ${v1}, ${v2}",
                {"v1": 1, "v2": 2},
                "Two: 1, 2",
                "Two: 1, 2",
                ("Two: 1, 2", 2),
                "Two: 1, 2",
            ),
            (
                "Repeat: ${v1}, ${v1}",
                {"v1": 1},
                "Repeat: 1, 1",
                "Repeat: 1, 1",
                ("Repeat: 1, 1", 2),
                "Repeat: 1, 1",
            ),
            (
                "Undefined: ${undef}",
                {},
                "Undefined: NULL",
                "Undefined: ${undef}",
                ("Undefined: NULL", 1),
                "Undefined: None",
            ),
            (
                "Boolean: ${flag}",
                {"flag": True},
                "Boolean: true",
                "Boolean: True",
                ("Boolean: true", 1),
                "Boolean: True",
            ),
            (
                "Number: ${count}",
                {"count": 42},
                "Number: 42",
                "Number: 42",
                ("Number: 42", 1),
                "Number: 42",
            ),
            # Test quoted context (critical for SQL)
            (
                "WHERE id = '${user_id}'",
                {"user_id": "123"},
                "WHERE id = '123'",
                "WHERE id = '123'",
                ("WHERE id = '123'", 1),
                "WHERE id = '123'",
            ),
            (
                "AND status = '${status|active}'",
                {},
                "AND status = 'active'",
                "AND status = 'active'",
                ("AND status = 'active'", 1),
                "AND status = 'active'",
            ),
        ],
    )
    def test_all_components_handle_same_patterns(
        self,
        template,
        variables,
        duckdb_expected,
        manager_expected,
        sql_gen_expected,
        condition_expected,
    ):
        """All components should parse consistently but format based on their context."""
        # Setup DuckDB Engine
        duckdb_engine = DuckDBEngine()
        for k, v in variables.items():
            duckdb_engine.register_variable(k, v)

        # Setup VariableManager
        manager = VariableManager(config=VariableConfig(cli_variables=variables))

        # Setup SQLGenerator
        sql_generator = SQLGenerator()

        # Setup ConditionEvaluator
        condition_evaluator = ConditionEvaluator(variables, manager)

        # Assertions - each component formats according to its specific context needs
        assert duckdb_engine.substitute_variables(template) == duckdb_expected
        assert manager.substitute(template) == manager_expected
        assert (
            sql_generator._substitute_variables(template, variables) == sql_gen_expected
        )
        assert condition_evaluator.substitute_variables(template) == condition_expected

    def test_complex_real_world_scenarios(self):
        """Test complex real-world scenarios that combine multiple features."""
        # Test nested quotes with defaults
        self._test_nested_quotes_scenario()

        # Test complex SQL with multiple variables
        self._test_complex_sql_scenario()

        # Test condition evaluation with multiple variables and defaults
        self._test_complex_condition_scenario()

    def _test_nested_quotes_scenario(self):
        """Helper method to test nested quotes scenario."""
        template = "INSERT INTO logs (message) VALUES ('User ${user_name} logged in from ${location|\"unknown\"}')"
        variables = {"user_name": "john.doe"}

        # Expected outputs for nested quotes scenario
        duckdb_expected = (
            "INSERT INTO logs (message) VALUES ('User john.doe logged in from unknown')"
        )
        manager_expected = "INSERT INTO logs (message) VALUES ('User john.doe logged in from unknown')"  # VariableManager strips quotes from defaults
        sql_gen_expected = (
            "INSERT INTO logs (message) VALUES ('User john.doe logged in from unknown')",
            2,
        )
        condition_expected = "INSERT INTO logs (message) VALUES ('User john.doe logged in from unknown')"  # Variables inside quotes don't get additional quotes

        self._assert_component_outputs(
            template,
            variables,
            duckdb_expected,
            manager_expected,
            sql_gen_expected,
            condition_expected,
        )

    def _test_complex_sql_scenario(self):
        """Helper method to test complex SQL scenario."""
        template = "SELECT * FROM ${schema|public}.${table} WHERE ${date_col|created_at} >= '${start_date}' AND status IN (${status_list|'active','pending'})"
        variables = {"table": "users", "start_date": "2023-01-01"}

        # Expected outputs for complex SQL scenario
        duckdb_expected = "SELECT * FROM 'public'.'users' WHERE 'created_at' >= '2023-01-01' AND status IN ('active','pending')"
        manager_expected = "SELECT * FROM public.users WHERE created_at >= '2023-01-01' AND status IN ('active','pending')"
        sql_gen_expected = (
            "SELECT * FROM 'public'.'users' WHERE 'created_at' >= '2023-01-01' AND status IN ('active','pending')",
            5,
        )  # 5 variables: schema|public, table, date_col|created_at, start_date, status_list|'active','pending'
        condition_expected = "SELECT * FROM 'public'.'users' WHERE 'created_at' >= '2023-01-01' AND status IN ('active','pending')"

        self._assert_component_outputs(
            template,
            variables,
            duckdb_expected,
            manager_expected,
            sql_gen_expected,
            condition_expected,
        )

    def _test_complex_condition_scenario(self):
        """Helper method to test complex condition scenario."""
        template = '${env} == "production" and ${debug_mode|false} == False'
        variables = {"env": "production"}

        # Expected outputs for complex condition scenario
        duckdb_expected = "'production' == \"production\" and false == False"  # DuckDB formats booleans as lowercase
        manager_expected = 'production == "production" and false == False'
        sql_gen_expected = (
            "'production' == \"production\" and false == False",
            2,
        )  # SQLGenerator formats booleans as lowercase
        condition_expected = "'production' == \"production\" and False == False"  # ConditionEvaluator converts string 'false' to boolean False

        self._assert_component_outputs(
            template,
            variables,
            duckdb_expected,
            manager_expected,
            sql_gen_expected,
            condition_expected,
        )

    def _assert_component_outputs(
        self,
        template,
        variables,
        duckdb_expected,
        manager_expected,
        sql_gen_expected,
        condition_expected,
    ):
        """Helper method to assert outputs from all components."""
        # Setup components
        duckdb_engine = DuckDBEngine()
        for k, v in variables.items():
            duckdb_engine.register_variable(k, v)

        manager = VariableManager(config=VariableConfig(cli_variables=variables))
        sql_generator = SQLGenerator()
        condition_evaluator = ConditionEvaluator(variables, manager)

        # Assertions
        assert duckdb_engine.substitute_variables(template) == duckdb_expected
        assert manager.substitute(template) == manager_expected
        assert (
            sql_generator._substitute_variables(template, variables) == sql_gen_expected
        )
        assert condition_evaluator.substitute_variables(template) == condition_expected

    def test_edge_cases_consistency(self):
        """Test edge cases that have historically caused issues."""
        edge_cases = [
            {
                "name": "Empty variable name",
                "template": "${}",
                "variables": {},
                "should_be_unchanged": True,
            },
            {
                "name": "Variable with only whitespace",
                "template": "${ }",
                "variables": {},
                "should_be_unchanged": True,
            },
            {
                "name": "Malformed variable syntax",
                "template": "${incomplete",
                "variables": {},
                "should_be_unchanged": True,
            },
            {
                "name": "Multiple pipes in default",
                "template": "${var|default|extra}",
                "variables": {},
                "expected_default": "default|extra",
            },
            {
                "name": "Variable name with special characters",
                "template": "${var-name_123.test}",
                "variables": {"var-name_123.test": "success"},
                "expected_value": "success",
            },
        ]

        for case in edge_cases:
            template = case["template"]
            variables = case["variables"]

            # Setup components
            duckdb_engine = DuckDBEngine()
            for k, v in variables.items():
                duckdb_engine.register_variable(k, v)

            manager = VariableManager(config=VariableConfig(cli_variables=variables))
            sql_generator = SQLGenerator()

            # Test consistency across components
            duckdb_result = duckdb_engine.substitute_variables(template)
            manager_result = manager.substitute(template)
            sql_gen_result, _ = sql_generator._substitute_variables(template, variables)

            if case.get("should_be_unchanged"):
                # All components should leave malformed variables unchanged
                assert template in duckdb_result or "NULL" in duckdb_result
                assert template in manager_result
                assert template in sql_gen_result or "NULL" in sql_gen_result

            elif case.get("expected_default"):
                # Components should correctly extract the default value
                expected = case["expected_default"]
                assert expected in manager_result  # Plain text
                assert (
                    f"'{expected}'" in duckdb_result or expected in duckdb_result
                )  # SQL formatted

            elif case.get("expected_value"):
                # Components should correctly substitute the value
                expected = case["expected_value"]
                assert expected in manager_result  # Plain text
                assert (
                    f"'{expected}'" in duckdb_result or expected in duckdb_result
                )  # SQL formatted

    def test_performance_consistency(self):
        """Test that all components have similar performance characteristics."""
        template = "SELECT * FROM ${table} WHERE ${column} = ${value|'default'} AND region = '${region|us-east-1}'"
        variables = {"table": "users", "column": "name", "value": "test"}

        # Pre-initialize all components to test actual substitution performance, not setup overhead
        duckdb_engine = DuckDBEngine()
        for k, v in variables.items():
            duckdb_engine.register_variable(k, v)

        manager = VariableManager(config=VariableConfig(cli_variables=variables))
        sql_generator = SQLGenerator()
        condition_evaluator = ConditionEvaluator(variables, manager)

        # Test each component's variable substitution performance (not initialization)
        components = [
            ("DuckDBEngine", lambda: duckdb_engine.substitute_variables(template)),
            ("VariableManager", lambda: manager.substitute(template)),
            (
                "SQLGenerator",
                lambda: sql_generator._substitute_variables(template, variables),
            ),
            (
                "ConditionEvaluator",
                lambda: condition_evaluator.substitute_variables(template),
            ),
        ]

        performance_results = {}

        for name, test_func in components:
            start_time = time.time()
            for _ in range(100):  # Run 100 times for better measurement
                test_func()
            end_time = time.time()
            duration = end_time - start_time
            performance_results[name] = duration

            # Performance should be reasonable (< 50ms for 100 iterations)
            assert (
                duration < 0.05
            ), f"{name} took too long: {duration}s for 100 iterations"

        # Ensure no component is more than 10x slower than the fastest (more realistic for DB engine)
        min_time = min(performance_results.values())
        for name, duration in performance_results.items():
            ratio = duration / min_time
            assert ratio < 10.0, f"{name} is {ratio}x slower than the fastest component"

    def test_variable_validation_consistency(self):
        """Test that validation behaves consistently across components."""
        content = """
        Some text with ${valid_var} and ${invalid_var} and ${var_with_default|default_value}.
        Also testing ${another_invalid} variable.
        """

        variables = {"valid_var": "value"}

        # Test VariableManager validation
        manager = VariableManager(config=VariableConfig(cli_variables=variables))
        validation_result = manager.validate(content)

        # Should identify missing variables correctly
        assert not validation_result.is_valid
        expected_missing = {"invalid_var", "another_invalid"}
        actual_missing = set(validation_result.missing_variables)
        assert expected_missing == actual_missing

        # Variables with defaults should not be reported as missing
        assert "var_with_default" not in validation_result.missing_variables

    def test_standard_parser_usage_consistency(self):
        """Verify all components are actually using the StandardVariableParser."""
        from sqlflow.core.variables.parser import StandardVariableParser

        # Test a unique pattern that only StandardVariableParser handles correctly
        template = "${var1|'default with spaces'} and ${var2|default_no_quotes}"
        variables = {}

        # All components should parse this pattern consistently
        duckdb_engine = DuckDBEngine()
        manager = VariableManager(config=VariableConfig(cli_variables=variables))
        sql_generator = SQLGenerator()

        # Parse with StandardVariableParser directly
        parse_result = StandardVariableParser.find_variables(template)
        assert len(parse_result.expressions) == 2

        # First variable should have default "default with spaces" (quotes removed)
        assert parse_result.expressions[0].default_value == "default with spaces"

        # Second variable should have default "default_no_quotes"
        assert parse_result.expressions[1].default_value == "default_no_quotes"

        # All components should produce consistent results based on this parsing
        duckdb_result = duckdb_engine.substitute_variables(template)
        manager_result = manager.substitute(template)
        sql_gen_result, _ = sql_generator._substitute_variables(template, variables)

        # Check that defaults are correctly applied
        assert "default with spaces" in manager_result
        assert "default_no_quotes" in manager_result

        # DuckDB and SQLGenerator should format for SQL context
        assert "'default with spaces'" in duckdb_result
        assert "'default_no_quotes'" in duckdb_result
        assert "'default with spaces'" in sql_gen_result
        assert "'default_no_quotes'" in sql_gen_result
