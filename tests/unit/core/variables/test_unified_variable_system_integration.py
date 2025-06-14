"""Tests for Unified Variable System Integration.

Tests the complete variable substitution system behavior after Phase 3 optimizations.
Focuses on testing real functionality with minimal mocking, ensuring the unified system
works correctly across all components and contexts.

Following testing standards:
- Test behavior, not implementation details
- Use real implementations with minimal mocking
- Test multiple components working together
- Verify no regressions from Phase 3 optimizations
"""

import time

import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.evaluator import ConditionEvaluator
from sqlflow.core.sql_generator import SQLGenerator
from sqlflow.core.variables.manager import VariableConfig, VariableManager
from sqlflow.core.variables.substitution_engine import (
    VariableSubstitutionEngine,
    get_substitution_engine,
    reset_substitution_engine,
)
from sqlflow.core.variables.unified_parser import (
    get_parser_performance_stats,
    get_unified_parser,
    warm_up_parser,
)


class TestUnifiedVariableSystemBehavior:
    """Test the unified variable system's behavior across all contexts."""

    def setup_method(self):
        """Set up test fixtures with real components."""
        # Reset global state for clean tests
        reset_substitution_engine()

        # Common test variables
        self.test_variables = {
            "table_name": "users",
            "schema": "public",
            "limit": 100,
            "status": "active",
            "debug": True,
            "price": 29.99,
            "description": "A test product with 'quotes' and special chars",
        }

        # Test templates of varying complexity
        self.simple_template = "SELECT * FROM ${table_name}"
        self.complex_template = """
            SELECT u.id, u.name, u.email
            FROM ${schema}.${table_name} u 
            WHERE u.status = ${status|'pending'} 
            AND u.created_at > '${start_date|2024-01-01}'
            LIMIT ${limit|50}
        """
        self.large_template = self._create_large_template()

    def _create_large_template(self) -> str:
        """Create a large template for performance testing."""
        base_query = """
            SELECT 
                t${i}.id,
                t${i}.name,
                t${i}.${field_${i}|default_field}
            FROM ${schema}.${table_${i}|default_table} t${i}
            WHERE t${i}.status = ${status_${i}|'active'}
        """

        parts = []
        for i in range(50):  # Create a template with many variables
            parts.append(base_query.replace("${i}", str(i)))

        return "\nUNION ALL\n".join(parts)

    def test_parser_caching_behavior_improves_performance(self):
        """Test that parser caching significantly improves performance on repeated parsing."""
        parser = get_unified_parser()
        parser.clear_cache()

        # First parse - cache miss
        start_time = time.perf_counter()
        result1 = parser.parse(self.complex_template)
        first_parse_time = time.perf_counter() - start_time

        # Second parse - cache hit
        start_time = time.perf_counter()
        result2 = parser.parse(self.complex_template)
        second_parse_time = time.perf_counter() - start_time

        # Verify results are identical
        assert result1.expressions == result2.expressions
        assert result1.has_variables == result2.has_variables
        assert result1.unique_variables == result2.unique_variables

        # Cache hit should be significantly faster
        assert second_parse_time < first_parse_time * 0.5  # At least 50% faster

        # Verify cache statistics
        stats = parser.get_cache_stats()
        assert stats["cache_hits"] >= 1
        assert stats["cache_hit_rate"] > 0

    def test_substitution_engine_caching_improves_repeated_substitutions(self):
        """Test that substitution result caching improves performance."""
        engine = VariableSubstitutionEngine(self.test_variables)

        # First substitution
        start_time = time.perf_counter()
        result1 = engine.substitute(self.complex_template, context="sql")
        first_time = time.perf_counter() - start_time

        # Second substitution with same parameters - should be cached
        start_time = time.perf_counter()
        result2 = engine.substitute(self.complex_template, context="sql")
        second_time = time.perf_counter() - start_time

        # Results should be identical
        assert result1 == result2

        # Cache hit should be faster
        assert second_time < first_time * 0.8  # At least 20% faster

    def test_large_template_parsing_performance_is_acceptable(self):
        """Test that large templates are parsed efficiently."""
        parser = get_unified_parser()

        start_time = time.perf_counter()
        result = parser.parse(self.large_template)
        parse_time = time.perf_counter() - start_time

        # Should parse large template reasonably quickly (under 50ms)
        assert parse_time < 0.05  # 50ms
        assert result.has_variables
        assert len(result.expressions) > 100  # Many variables in large template

        # Performance metrics should be recorded
        assert result.parse_time_ms > 0

    def test_all_contexts_produce_consistent_behavior(self):
        """Test that all context types work correctly with the unified system."""
        engine = VariableSubstitutionEngine(self.test_variables)

        template = "Value: ${status} (${limit})"

        # Test all context types
        text_result = engine.substitute(template, context="text")
        sql_result = engine.substitute(template, context="sql")
        ast_result = engine.substitute(template, context="ast")

        # All should substitute variables successfully
        assert "${" not in text_result
        assert "${" not in sql_result
        assert "${" not in ast_result

        # Results should be different due to context-specific formatting
        assert text_result != sql_result or sql_result != ast_result

    def test_duckdb_engine_uses_unified_system_correctly(self):
        """Test that DuckDBEngine integrates correctly with unified system."""
        engine = DuckDBEngine(":memory:")

        # Set variables using the register_variable method
        for name, value in self.test_variables.items():
            engine.register_variable(name, value)

        template = "SELECT * FROM ${schema}.${table_name} WHERE status = ${status}"
        result = engine.substitute_variables(template)

        # Should substitute all variables
        assert "${" not in result
        assert (
            "public.users" in result or "'public'.'users'" in result
        )  # May be quoted in SQL context
        assert (
            "active" in result or "'active'" in result
        )  # SQL context should quote strings

    def test_sql_generator_uses_unified_system_correctly(self):
        """Test that SQLGenerator integrates correctly with unified system."""
        generator = SQLGenerator()

        sql_template = """
            INSERT INTO ${schema}.${table_name} (name, status, price)
            VALUES ('${description}', ${status}, ${price})
        """

        result, count = generator._substitute_variables(
            sql_template, self.test_variables
        )

        # Should substitute all variables and count them
        assert "${" not in result
        assert count > 0
        assert (
            "public.users" in result or "'public'.'users'" in result
        )  # May be quoted in SQL context
        assert str(self.test_variables["price"]) in result

    def test_condition_evaluator_uses_unified_system_correctly(self):
        """Test that ConditionEvaluator integrates correctly with unified system."""
        evaluator = ConditionEvaluator(self.test_variables)

        condition = "${debug} == True and ${limit} > 50"
        result = evaluator.substitute_variables(condition)

        # Should substitute variables appropriately for AST evaluation
        assert "${" not in result
        assert "True" in result  # Python boolean
        assert "100" in result

    def test_variable_manager_uses_unified_system_correctly(self):
        """Test that VariableManager integrates correctly with unified system."""
        config = VariableConfig(cli_variables=self.test_variables)
        manager = VariableManager(config)

        template = "Processing ${table_name} with limit ${limit|25}"
        result = manager.substitute(template)

        # Should substitute variables with manager's context detection
        assert "${" not in result
        assert "users" in result
        assert "100" in result

    def test_missing_variables_handled_consistently_across_contexts(self):
        """Test that missing variables are handled consistently."""
        variables = {"known_var": "value"}

        engine = VariableSubstitutionEngine(variables)
        template = "Known: ${known_var}, Unknown: ${unknown_var}"

        # Test different contexts handle missing variables appropriately
        text_result = engine.substitute(template, context="text")
        sql_result = engine.substitute(template, context="sql")
        ast_result = engine.substitute(template, context="ast")

        # All should handle known variable
        assert "value" in text_result
        assert "value" in sql_result
        assert "value" in ast_result

        # Missing variables should be handled per context
        assert "NULL" in sql_result  # SQL context
        assert "None" in ast_result  # AST context

    def test_default_values_work_consistently_across_contexts(self):
        """Test that default values work correctly in all contexts."""
        variables = {}  # No variables, so defaults should be used
        engine = VariableSubstitutionEngine(variables)

        template = "Status: ${status|pending}, Count: ${count|0}"

        text_result = engine.substitute(template, context="text")
        sql_result = engine.substitute(template, context="sql")
        ast_result = engine.substitute(template, context="ast")

        # All should use default values
        assert "pending" in text_result
        assert "0" in text_result
        assert "pending" in sql_result
        assert "0" in sql_result
        assert "pending" in ast_result
        assert "0" in ast_result

    def test_quoted_context_detection_works_correctly(self):
        """Test that quote detection works correctly for SQL formatting."""
        variables = {"table": "users", "status": "active"}
        engine = VariableSubstitutionEngine(variables)

        # Variable inside quotes - should not add extra quotes
        template = "WHERE status = '${status}'"
        result = engine.substitute(template, context="sql", context_detection=True)

        # Should not have double quotes like 'active' -> ''active''
        assert result.count("'") <= 2  # Only the outer quotes

    def test_global_engine_singleton_behavior(self):
        """Test that global engine singleton works correctly."""
        engine1 = get_substitution_engine({"var1": "value1"})
        engine2 = get_substitution_engine({"var2": "value2"})

        # Should be same instance
        assert engine1 is engine2

        # Should have both variables
        assert engine1.has_variable("var1")
        assert engine1.has_variable("var2")

    def test_cache_management_prevents_memory_leaks(self):
        """Test that cache management prevents unlimited memory growth."""
        parser = get_unified_parser()
        parser.clear_cache()

        # Generate many unique templates to test cache size limits
        templates = [
            f"SELECT * FROM table_{i} WHERE id = ${{id_{i}}}" for i in range(1500)
        ]

        for template in templates:
            parser.parse(template)

        stats = parser.get_cache_stats()

        # Cache should be limited to prevent memory leaks
        assert stats["cache_size"] <= stats["max_cache_size"]
        assert stats["cache_size"] > 0  # But should still cache recent items

    def test_parser_warm_up_improves_initial_performance(self):
        """Test that parser warm-up functionality works correctly."""
        common_templates = [
            "SELECT * FROM ${table}",
            "INSERT INTO ${table} VALUES (${values})",
            "UPDATE ${table} SET ${field} = ${value}",
        ]

        parser = get_unified_parser()
        parser.clear_cache()

        # Warm up parser
        warm_up_parser(common_templates)

        # Templates should now be cached
        stats = parser.get_cache_stats()
        assert stats["cache_size"] >= len(common_templates)
        assert stats["cache_hits"] == 0  # No hits yet, just warming up

        # Using templates should now hit cache
        for template in common_templates:
            parser.parse(template)

        stats_after = parser.get_cache_stats()
        assert stats_after["cache_hits"] >= len(common_templates)

    def test_performance_stats_provide_useful_metrics(self):
        """Test that performance statistics provide useful information."""
        # Use the system to generate some stats
        engine = VariableSubstitutionEngine(self.test_variables)
        engine.substitute(self.simple_template, context="sql")
        engine.substitute(self.complex_template, context="text")

        # Get comprehensive stats
        engine_stats = engine.get_stats()
        parser_stats = get_parser_performance_stats()

        # Should have useful information
        assert "variable_count" in engine_stats
        assert "parser_stats" in engine_stats
        assert "cache_hit_rate" in engine_stats["parser_stats"]

        assert "parser_stats" in parser_stats
        assert "pattern_info" in parser_stats
        assert "pattern" in parser_stats["pattern_info"]


class TestVariableSystemRegressionPrevention:
    """Test that Phase 3 optimizations don't break existing functionality."""

    def test_all_examples_still_work_with_optimized_system(self):
        """Test that existing examples still work with the optimized system."""
        # This would typically run actual example files, but we'll test key patterns
        examples = [
            {
                "template": "SELECT * FROM ${schema|public}.${table} WHERE active = ${active|true}",
                "variables": {"table": "users", "active": False},
                "context": "sql",
            },
            {
                "template": "Hello ${name|Anonymous}, you have ${count|0} messages",
                "variables": {"name": "Alice", "count": 5},
                "context": "text",
            },
            {
                "template": "${condition} and ${limit} > 0",
                "variables": {"condition": "True", "limit": 100},
                "context": "ast",
            },
        ]

        for example in examples:
            engine = VariableSubstitutionEngine(example["variables"])
            result = engine.substitute(example["template"], context=example["context"])

            # Should successfully substitute without errors
            assert isinstance(result, str)
            assert len(result) > 0
            # Should not have unresolved variables
            assert (
                "${" not in result or "|" in example["template"]
            )  # Unless defaults exist

    def test_backward_compatibility_with_old_usage_patterns(self):
        """Test that old usage patterns still work correctly."""
        # Test that components still work as they did before optimization

        # DuckDB Engine pattern
        variables = {"table": "test_table", "limit": 50}
        engine = DuckDBEngine(":memory:")

        # Set variables using the register_variable method
        for name, value in variables.items():
            engine.register_variable(name, value)

        result = engine.substitute_variables("SELECT * FROM ${table} LIMIT ${limit}")
        assert "test_table" in result
        assert "50" in result

        # Variable Manager pattern
        var_config = VariableConfig(cli_variables=variables)
        manager = VariableManager(var_config)

        result = manager.substitute("Processing ${table} with limit ${limit}")
        assert "test_table" in result
        assert "50" in result

    def test_error_handling_remains_robust(self):
        """Test that error handling is still robust after optimizations."""
        engine = VariableSubstitutionEngine({})

        # Test various error conditions
        with pytest.raises(TypeError):
            get_unified_parser().parse(None)

        # Empty template should work
        result = engine.substitute("", context="sql")
        assert result == ""

        # Template with no variables should work
        result = engine.substitute("SELECT 1", context="sql")
        assert result == "SELECT 1"

        # Missing variables should be handled gracefully
        result = engine.substitute("${missing}", context="sql")
        assert result == "NULL"  # SQL context default


class TestSystemIntegrationScenarios:
    """Test real-world integration scenarios with the unified system."""

    def test_complete_pipeline_scenario(self):
        """Test a complete pipeline scenario using multiple components."""
        # Simulate a real pipeline with variable substitution at multiple levels
        variables = {
            "source_schema": "raw_data",
            "source_table": "events",
            "target_schema": "processed",
            "target_table": "user_events",
            "batch_date": "2024-01-15",
            "batch_size": 10000,
        }

        # Variable Manager for configuration
        config = VariableConfig(cli_variables=variables)
        manager = VariableManager(config)

        # SQL Generator for query building
        generator = SQLGenerator()

        # DuckDB Engine for execution context
        engine = DuckDBEngine(":memory:")

        # Set variables using the register_variable method
        for name, value in variables.items():
            engine.register_variable(name, value)

        # Complex query with multiple substitution points
        query_template = """
            INSERT INTO ${target_schema}.${target_table}
            SELECT 
                event_id,
                user_id, 
                event_type,
                '${batch_date}' as batch_date
            FROM ${source_schema}.${source_table}
            WHERE event_date = '${batch_date}'
            LIMIT ${batch_size}
        """

        # Each component should handle the template correctly
        manager_result = manager.substitute(query_template)
        generator_result, count = generator._substitute_variables(
            query_template, variables
        )
        engine_result = engine.substitute_variables(query_template)

        # All should produce valid SQL
        for result in [manager_result, generator_result, engine_result]:
            assert (
                "raw_data.events" in result or "'raw_data'.'events'" in result
            )  # May be quoted
            assert (
                "processed.user_events" in result
                or "'processed'.'user_events'" in result
            )  # May be quoted
            assert "2024-01-15" in result
            assert "10000" in result
            assert "${" not in result

        # Generator should count variables
        assert count > 0

    def test_mixed_context_scenario_maintains_correctness(self):
        """Test scenario with mixed contexts maintains correct behavior."""
        variables = {
            "table": "products",
            "condition": "price > 100",
            "debug_mode": True,
            "default_limit": 50,
        }

        engine = VariableSubstitutionEngine(variables)

        # SQL context - should quote strings appropriately
        sql_query = "SELECT * FROM ${table} WHERE ${condition} LIMIT ${default_limit}"
        sql_result = engine.substitute(sql_query, context="sql")

        # Text context - plain formatting
        log_message = "Querying ${table} with condition: ${condition}"
        text_result = engine.substitute(log_message, context="text")

        # AST context - Python evaluation format
        python_expr = "${debug_mode} and ${default_limit} > 0"
        ast_result = engine.substitute(python_expr, context="ast")

        # Verify each context produces appropriate output
        assert "products" in sql_result
        assert "products" in text_result

        assert "True" in ast_result  # Python boolean
        assert "50" in ast_result

        # SQL should handle condition appropriately
        assert "price > 100" in sql_result

    def test_high_load_scenario_performance(self):
        """Test system performance under high load conditions."""
        variables = {"env": "prod", "version": "1.0", "user": "system"}
        engine = VariableSubstitutionEngine(variables)

        # Simulate high load with many substitutions
        template = "Deployment ${version} to ${env} by ${user}"

        start_time = time.perf_counter()

        # Perform many substitutions
        results = []
        for i in range(1000):
            result = engine.substitute(template, context="text")
            results.append(result)

        total_time = time.perf_counter() - start_time

        # Should complete within reasonable time (under 1 second for 1000 operations)
        assert total_time < 1.0

        # All results should be identical and correct
        expected = "Deployment 1.0 to prod by system"
        assert all(result == expected for result in results)

        # Cache should be effective
        stats = engine.get_stats()
        assert stats["substitution_cache_size"] > 0
