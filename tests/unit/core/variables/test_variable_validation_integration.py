"""Tests for Variable Validation Integration with Unified System.

Tests that variable validation works correctly with the unified parser and
maintains robust error handling after Phase 3 optimizations.

Following testing standards:
- Test real validation scenarios with minimal mocking
- Test integration between validation and parsing components
- Verify robust error handling and user-friendly feedback
"""

from sqlflow.core.variables.manager import VariableConfig
from sqlflow.core.variables.substitution_engine import VariableSubstitutionEngine
from sqlflow.core.variables.unified_parser import get_unified_parser
from sqlflow.core.variables.validator import ValidationResult, VariableValidator


class TestVariableValidationBehavior:
    """Test variable validation behavior with unified system."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_variables = {
            "database": "production",
            "table": "users",
            "limit": 100,
            "debug": True,
            "environment": "prod",
        }

        self.validator = VariableValidator(self.test_variables)

    def test_validation_uses_unified_parser_correctly(self):
        """Test that validation uses the unified parser for consistent behavior."""
        content = """
            SELECT * FROM ${database}.${table}
            WHERE active = ${active|true}
            AND created_at > '${start_date|2024-01-01}'
            LIMIT ${limit}
        """

        result = self.validator.validate(content)

        # Should find all variables including those with defaults
        assert isinstance(result, ValidationResult)
        assert result.is_valid  # Should be valid since most variables exist

        # Should identify missing variables correctly
        missing_vars = [
            var for var in result.missing_variables if var not in self.test_variables
        ]
        assert len(missing_vars) == 0  # active and start_date have defaults

    def test_validation_identifies_missing_variables_accurately(self):
        """Test that validation accurately identifies missing variables."""
        content = """
            SELECT * FROM ${database}.${table}
            WHERE status = ${status}  -- No default
            AND region = ${region}    -- No default
            LIMIT ${limit}
        """

        result = self.validator.validate(content)

        assert not result.is_valid  # Should be invalid due to missing variables
        assert "status" in result.missing_variables
        assert "region" in result.missing_variables
        assert "database" not in result.missing_variables  # This exists
        assert "table" not in result.missing_variables  # This exists
        assert "limit" not in result.missing_variables  # This exists

    def test_validation_handles_complex_variable_patterns(self):
        """Test validation with complex variable patterns and edge cases."""
        content = """
            -- Complex SQL with various variable patterns
            SELECT 
                ${field_1|id} as primary_key,
                '${string_with_quotes|"default value"}' as description,
                ${numeric_field|0} + ${another_numeric|1} as calculated,
                CASE 
                    WHEN ${condition|true} THEN '${success_msg|"Success"}'
                    ELSE '${error_msg|"Error"}'
                END as status_message
            FROM ${schema|public}.${table_name}
            WHERE ${filter_condition|1=1}
        """

        result = self.validator.validate(content)

        # Should handle all the complex patterns
        assert isinstance(result, ValidationResult)

        # Should extract variables correctly
        result.context_locations.keys() if result.context_locations else set()

        # Should find variables that exist
        variables_found = set()
        for expr in get_unified_parser().parse(content).expressions:
            variables_found.add(expr.variable_name)

        assert "field_1" in variables_found
        assert "string_with_quotes" in variables_found
        assert "table_name" in variables_found

    def test_quick_validation_provides_efficient_checking(self):
        """Test that quick validation provides efficient missing variable checking."""
        content = "SELECT * FROM ${missing_table} WHERE id = ${missing_id}"

        missing_vars = self.validator.validate_quick(content, self.test_variables)

        assert isinstance(missing_vars, list)
        assert "missing_table" in missing_vars
        assert "missing_id" in missing_vars
        assert len(missing_vars) == 2

    def test_validation_with_variable_config_resolves_priorities(self):
        """Test validation with VariableConfig resolves variable priorities correctly."""
        cli_vars = {"env": "dev", "debug": False}
        env_vars = {"env": "prod", "region": "us-east-1"}

        config = VariableConfig(cli_variables=cli_vars, env_variables=env_vars)

        validator = VariableValidator({})  # No direct variables

        content = "Environment: ${env}, Region: ${region}, Debug: ${debug}"
        validator.validate(content, config)

        # Should resolve variables from config
        resolved_vars = config.resolve_priority()
        assert "env" in resolved_vars
        assert "region" in resolved_vars
        assert "debug" in resolved_vars

        # CLI should take priority over environment
        assert resolved_vars["env"] == "dev"  # CLI wins
        assert resolved_vars["region"] == "us-east-1"  # Only in env
        assert resolved_vars["debug"] is False  # CLI value

    def test_validation_handles_quoted_defaults_correctly(self):
        """Test that validation handles quoted default values correctly."""
        content = """
            SELECT '${name|"John Doe"}' as user_name,
                   '${status|'active'}' as user_status,
                   ${count|0} as record_count
        """

        result = self.validator.validate(content)

        # Should handle quoted defaults without issues
        assert isinstance(result, ValidationResult)

        # Should not report missing variables for those with defaults
        assert "name" not in result.missing_variables
        assert "status" not in result.missing_variables
        assert "count" not in result.missing_variables

    def test_validation_detects_malformed_variable_syntax(self):
        """Test that validation detects and reports malformed variable syntax."""
        content = """
            SELECT * FROM table_name
            WHERE id = ${another_var}
            AND status = ${normal_var}
            AND ${unclosed_var will not be captured
        """

        result = self.validator.validate(content)

        # Should handle malformed syntax gracefully
        assert isinstance(result, ValidationResult)

        # Should find properly formed variables
        found_vars = set()
        for expr in get_unified_parser().parse(content).expressions:
            found_vars.add(expr.variable_name)

        assert "another_var" in found_vars
        assert "normal_var" in found_vars
        # unclosed_var won't be found due to malformed syntax (no closing brace)

    def test_variable_extraction_methods_work_consistently(self):
        """Test that variable extraction methods work consistently."""
        content = """
            SELECT ${field1}, ${field2}, ${field1}  -- field1 appears twice
            FROM ${table}
            WHERE ${condition}
        """

        # Test different extraction methods
        all_vars = self.validator.extract_variables(content)
        var_counts = self.validator.count_variable_references(content)

        # Should extract unique variables
        assert isinstance(all_vars, list)
        assert set(all_vars) == {"field1", "field2", "table", "condition"}

        # Should count references correctly
        assert var_counts["field1"] == 2  # Appears twice
        assert var_counts["field2"] == 1
        assert var_counts["table"] == 1
        assert var_counts["condition"] == 1

    def test_validation_integrates_with_substitution_engine(self):
        """Test that validation integrates correctly with substitution engine."""
        # First validate
        content = "SELECT * FROM ${table} WHERE active = ${active|true}"
        result = self.validator.validate(content)

        # Should be valid (active has default)
        assert result.is_valid

        # Now substitute using same variables
        engine = VariableSubstitutionEngine(self.test_variables)
        substituted = engine.substitute(content, context="sql")

        # Should work without issues
        assert "users" in substituted  # table substituted
        assert "true" in substituted or "TRUE" in substituted  # active default used

    def test_validation_handles_empty_and_edge_cases(self):
        """Test validation handles empty content and edge cases."""
        # Empty content
        result = self.validator.validate("")
        assert result.is_valid
        assert len(result.missing_variables) == 0

        # Content with no variables
        result = self.validator.validate("SELECT 1")
        assert result.is_valid
        assert len(result.missing_variables) == 0

        # Whitespace only
        result = self.validator.validate("   \n\t  ")
        assert result.is_valid

        # Variables with empty names (should be ignored)
        result = self.validator.validate("SELECT ${}")
        assert result.is_valid


class TestValidationErrorHandling:
    """Test validation error handling scenarios."""

    def test_validation_provides_helpful_error_information(self):
        """Test that validation provides helpful error information."""
        variables = {"known_var": "value"}
        validator = VariableValidator(variables)

        content = """
            Line 1: ${known_var} is fine
            Line 2: ${missing_var1} is missing
            Line 3: ${missing_var2} is also missing
        """

        result = validator.validate(content)

        assert not result.is_valid
        assert "missing_var1" in result.missing_variables
        assert "missing_var2" in result.missing_variables
        assert "known_var" not in result.missing_variables

        # Should provide useful context
        assert isinstance(result.missing_variables, list)
        assert len(result.missing_variables) == 2

    def test_validation_handles_complex_nested_scenarios(self):
        """Test validation with complex nested variable scenarios."""
        variables = {"outer": "${inner}", "inner": "value"}
        validator = VariableValidator(variables)

        # This represents a scenario where variables reference other variables
        content = "Value: ${outer}, Direct: ${inner}, Missing: ${missing}"

        result = validator.validate(content)

        # Should identify missing variables correctly
        assert "missing" in result.missing_variables
        assert "outer" not in result.missing_variables
        assert "inner" not in result.missing_variables

    def test_validation_performance_with_large_content(self):
        """Test validation performance with large content."""
        variables = {f"var_{i}": f"value_{i}" for i in range(100)}
        validator = VariableValidator(variables)

        # Create large content with many variables
        content_parts = []
        for i in range(200):
            if i < 100:
                content_parts.append(f"SELECT ${{var_{i}}} FROM table_{i}")
            else:
                content_parts.append(f"SELECT ${{missing_{i}}} FROM table_{i}")

        large_content = "\n".join(content_parts)

        import time

        start_time = time.perf_counter()
        result = validator.validate(large_content)
        validation_time = time.perf_counter() - start_time

        # Should complete within reasonable time (under 100ms)
        assert validation_time < 0.1

        # Should correctly identify missing variables
        assert not result.is_valid
        # Should find missing_100 through missing_199 (100 variables)
        missing_count = len(
            [var for var in result.missing_variables if var.startswith("missing_")]
        )
        assert missing_count == 100


class TestVariableValidationConsistency:
    """Test consistency between validation and actual substitution."""

    def test_validation_and_substitution_agree_on_missing_variables(self):
        """Test that validation and substitution agree on missing variables."""
        variables = {"existing": "value"}
        validator = VariableValidator(variables)
        engine = VariableSubstitutionEngine(variables)

        content = "Existing: ${existing}, Missing: ${missing}"

        # Validate first
        result = validator.validate(content)
        assert "missing" in result.missing_variables
        assert "existing" not in result.missing_variables

        # Substitute - should handle missing variable gracefully
        substituted = engine.substitute(content, context="sql")

        # Should substitute existing variable
        assert "value" in substituted
        # Missing variable should be handled per context (NULL for SQL)
        assert "NULL" in substituted

    def test_validation_quick_mode_matches_full_validation(self):
        """Test that quick validation matches full validation for missing variables."""
        variables = {"var1": "value1", "var2": "value2"}
        validator = VariableValidator(variables)

        content = """
            SELECT ${var1}, ${var2}, ${missing1}, ${missing2}
            FROM ${var3|default_table}
            WHERE ${missing3} = 'test'
        """

        # Full validation
        full_result = validator.validate(content)

        # Quick validation
        quick_missing = validator.validate_quick(content, variables)

        # Should agree on missing variables (those without defaults)
        expected_missing = {"missing1", "missing2", "missing3"}
        assert set(full_result.missing_variables) == expected_missing
        assert set(quick_missing) == expected_missing

    def test_validation_works_with_all_variable_contexts(self):
        """Test that validation works correctly with all variable contexts."""
        variables = {"table": "users", "debug": True, "limit": 100}
        validator = VariableValidator(variables)

        # SQL context content
        sql_content = (
            "SELECT * FROM ${table} WHERE active = ${active|true} LIMIT ${limit}"
        )

        # Text context content
        text_content = "Processing ${table} with debug=${debug} and limit=${limit}"

        # AST context content
        ast_content = "${debug} and ${limit} > 0"

        # All should validate successfully
        for content in [sql_content, text_content, ast_content]:
            result = validator.validate(content)
            assert result.is_valid  # All have defaults or existing variables
            assert len(result.missing_variables) == 0
