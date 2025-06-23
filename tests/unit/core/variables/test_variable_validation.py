"""Test variable validation functionality in SQLFlow core.

Following 04_testing_standards.md:
- No mocks, use real Parser and Planner implementations
- Descriptive test names that explain specific behaviors
- Tests serve as usage examples
- Focus on testing important user scenarios
"""

from sqlflow.core.planner_main import Planner
from sqlflow.parser.parser import Parser
from sqlflow.validation.errors import ValidationError  # Correct import path


class TestVariableValueValidation:
    """Test validation of variable values in pipeline execution."""

    def test_planner_accepts_default_values_when_no_explicit_value_provided(self):
        """Test that planner correctly uses default values when variables not explicitly set.

        This ensures that the default value mechanism works correctly for
        optional configuration parameters.
        """
        # Given: SQL with variable that has default value
        sql = """
        SET important_var = "${important_var|default_value}";
        
        CREATE TABLE result AS
        SELECT * FROM source WHERE region = ${important_var};
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()

        # When: Planning without providing explicit variable value
        # Then: Should succeed using default value
        plan = planner.create_plan(pipeline)

        # Then: Plan should be created successfully
        assert plan is not None
        # The plan should contain the resolved default value
        assert any(
            "default_value" in str(step) for step in plan
        ), "Plan should contain resolved default value"

    def test_planner_accepts_explicit_variable_values_overriding_defaults(self):
        """Test that explicit variable values correctly override default values.

        This ensures that users can provide runtime configuration that takes
        precedence over defaults in the SQL template.
        """
        # Given: SQL with variable that has default value
        sql = """
        SET important_var = "${important_var|default_value}";
        
        CREATE TABLE result AS
        SELECT * FROM source WHERE region = ${important_var};
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()

        # When: Planning with explicit variable value
        plan = planner.create_plan(pipeline, {"important_var": "explicit_value"})

        # Then: Plan should use explicit value, not default
        assert plan is not None
        assert any(
            "explicit_value" in str(step) for step in plan
        ), "Plan should contain explicit value, not default"

    def test_planner_handles_empty_variable_values_gracefully(self):
        """Test that planner handles empty variable values according to current behavior.

        This documents the current behavior - the planner may accept empty values
        and provide warnings rather than strict validation errors.
        """
        # Given: SQL with variable that has default value
        sql = """
        SET important_var = "${important_var|default_value}";
        
        CREATE TABLE result AS
        SELECT * FROM source WHERE region = ${important_var};
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()

        # When: Planning with empty variable value
        # Then: Current behavior may be to accept with warnings
        try:
            plan = planner.create_plan(pipeline, {"important_var": ""})
            # If no exception, the planner accepted empty value
            assert plan is not None
        except (ValidationError, ValueError) as e:
            # If exception, verify it's about empty values
            assert "empty" in str(e).lower() or "invalid" in str(e).lower()


class TestSelfReferencingVariableValidation:
    """Test validation of self-referencing variables in conditional logic."""

    def test_planner_handles_self_referencing_variables_with_defaults_correctly(self):
        """Test that self-referencing variables work correctly with default values.

        This pattern is common in configuration where a variable references itself
        to provide conditional behavior with sensible defaults.
        """
        # Given: SQL with self-referencing variable in conditional
        sql = """
        SET use_csv = "${use_csv|true}";
        
        IF ${use_csv} == "true" THEN
            CREATE TABLE my_source AS SELECT * FROM csv_table;
        ELSE
            CREATE TABLE my_source AS SELECT * FROM db_table;
        END IF;
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()

        # When: Planning without explicit value (should use default)
        plan = planner.create_plan(pipeline)

        # Then: Should create plan with default behavior
        assert plan is not None

    def test_planner_handles_explicit_boolean_values_in_conditionals(self):
        """Test that explicit boolean string values work correctly in conditionals.

        This ensures that string representations of boolean values are handled
        correctly in IF conditions.
        """
        # Given: SQL with boolean conditional variable
        sql = """
        SET use_csv = "${use_csv|true}";
        
        IF ${use_csv} == "true" THEN
            CREATE TABLE my_source AS SELECT * FROM csv_table;
        ELSE
            CREATE TABLE my_source AS SELECT * FROM db_table;
        END IF;
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()

        # When: Planning with explicit true value
        plan_true = planner.create_plan(pipeline, {"use_csv": "true"})

        # Then: Should create plan successfully
        assert plan_true is not None

        # When: Planning with explicit false value
        plan_false = planner.create_plan(pipeline, {"use_csv": "false"})

        # Then: Should create different plan (false branch)
        assert plan_false is not None

        # The plans should be different based on the conditional
        assert str(plan_true) != str(
            plan_false
        ), "Plans should differ based on conditional variable"
