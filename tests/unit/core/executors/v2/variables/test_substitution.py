"""Tests for pure variable substitution functions.

Following Kent Beck's testing principles:
- Fast unit tests (<5ms per test)
- Test behavior, not implementation
- Pure functions are easy to test
- Clear examples that serve as documentation
"""

import pytest

from sqlflow.core.executors.v2.variables.substitution import (
    merge_variables,
    substitute_in_dict,
    substitute_in_list,
    substitute_in_step,
    substitute_variables,
    validate_variables,
)


class TestSubstituteVariables:
    """Test pure variable substitution function."""

    def test_simple_substitution(self):
        """Test basic variable substitution."""
        result = substitute_variables("Hello $name", {"name": "World"})
        assert result == "Hello World"

    def test_multiple_variables(self):
        """Test multiple variable substitution."""
        result = substitute_variables(
            "SELECT * FROM $schema.$table WHERE id = $id",
            {"schema": "public", "table": "users", "id": "123"},
        )
        assert result == "SELECT * FROM public.users WHERE id = 123"

    def test_missing_variable_safe(self):
        """Test that missing variables are left as-is (safe_substitute)."""
        result = substitute_variables("Hello $name and $other", {"name": "World"})
        assert result == "Hello World and $other"

    def test_empty_variables(self):
        """Test with empty variables dict."""
        result = substitute_variables("Hello $name", {})
        assert result == "Hello $name"

    def test_none_variables(self):
        """Test with None variables."""
        result = substitute_variables("Hello $name", None)
        assert result == "Hello $name"

    def test_non_string_input(self):
        """Test with non-string input."""
        result = substitute_variables(123, {"name": "World"})
        assert result == "123"

    def test_empty_string(self):
        """Test with empty string."""
        result = substitute_variables("", {"name": "World"})
        assert result == ""

    def test_no_variables_in_text(self):
        """Test text with no variable placeholders."""
        result = substitute_variables("Hello World", {"name": "Test"})
        assert result == "Hello World"


class TestSubstituteInDict:
    """Test recursive dictionary substitution."""

    def test_simple_dict(self):
        """Test simple dictionary substitution."""
        data = {"query": "SELECT * FROM $table"}
        variables = {"table": "users"}
        result = substitute_in_dict(data, variables)

        assert result == {"query": "SELECT * FROM users"}
        # Ensure original is unchanged (immutability)
        assert data == {"query": "SELECT * FROM $table"}

    def test_nested_dict(self):
        """Test nested dictionary substitution."""
        data = {
            "source": {"query": "SELECT * FROM $table"},
            "target": {"table": "$output_table"},
        }
        variables = {"table": "users", "output_table": "processed_users"}
        result = substitute_in_dict(data, variables)

        expected = {
            "source": {"query": "SELECT * FROM users"},
            "target": {"table": "processed_users"},
        }
        assert result == expected

    def test_mixed_value_types(self):
        """Test dict with mixed value types."""
        data = {
            "query": "SELECT * FROM $table",
            "limit": 100,
            "active": True,
            "config": {"timeout": 30},
        }
        variables = {"table": "users"}
        result = substitute_in_dict(data, variables)

        expected = {
            "query": "SELECT * FROM users",
            "limit": 100,
            "active": True,
            "config": {"timeout": 30},
        }
        assert result == expected

    def test_empty_dict(self):
        """Test with empty dictionary."""
        result = substitute_in_dict({}, {"table": "users"})
        assert result == {}

    def test_non_dict_input(self):
        """Test with non-dict input."""
        result = substitute_in_dict("not a dict", {"table": "users"})
        assert result == "not a dict"


class TestSubstituteInList:
    """Test list substitution."""

    def test_simple_list(self):
        """Test simple list substitution."""
        data = ["SELECT * FROM $table", "INSERT INTO $table"]
        variables = {"table": "users"}
        result = substitute_in_list(data, variables)

        assert result == ["SELECT * FROM users", "INSERT INTO users"]
        # Ensure original is unchanged
        assert data == ["SELECT * FROM $table", "INSERT INTO $table"]

    def test_mixed_list(self):
        """Test list with mixed types."""
        data = ["SELECT * FROM $table", 123, {"query": "$sql"}]
        variables = {"table": "users", "sql": "SELECT 1"}
        result = substitute_in_list(data, variables)

        expected = ["SELECT * FROM users", 123, {"query": "SELECT 1"}]
        assert result == expected

    def test_nested_list(self):
        """Test nested lists."""
        data = [["$table1", "$table2"], ["$table3"]]
        variables = {"table1": "users", "table2": "orders", "table3": "products"}
        result = substitute_in_list(data, variables)

        expected = [["users", "orders"], ["products"]]
        assert result == expected

    def test_empty_list(self):
        """Test with empty list."""
        result = substitute_in_list([], {"table": "users"})
        assert result == []

    def test_non_list_input(self):
        """Test with non-list input."""
        result = substitute_in_list("not a list", {"table": "users"})
        assert result == "not a list"


class TestSubstituteInStep:
    """Test step-specific substitution."""

    def test_pipeline_step(self):
        """Test typical pipeline step substitution."""
        step = {
            "type": "load",
            "id": "load_$source",
            "source": {"path": "/data/$table.csv"},
            "target": {"table": "$target_table"},
        }
        variables = {
            "source": "users",
            "table": "customer",
            "target_table": "customers",
        }
        result = substitute_in_step(step, variables)

        expected = {
            "type": "load",
            "id": "load_users",
            "source": {"path": "/data/customer.csv"},
            "target": {"table": "customers"},
        }
        assert result == expected

    def test_no_variables(self):
        """Test step with no variables."""
        step = {"type": "load", "id": "test"}
        result = substitute_in_step(step, {})
        assert result == step

    def test_none_variables(self):
        """Test step with None variables."""
        step = {"type": "load", "id": "test"}
        result = substitute_in_step(step, None)
        assert result == step


class TestMergeVariables:
    """Test variable merging."""

    def test_simple_merge(self):
        """Test simple variable merge."""
        result = merge_variables({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_override_merge(self):
        """Test that later variables override earlier ones."""
        result = merge_variables({"a": 1, "b": 2}, {"a": 3, "c": 4})
        assert result == {"a": 3, "b": 2, "c": 4}

    def test_multiple_merges(self):
        """Test merging multiple dictionaries."""
        result = merge_variables({"a": 1}, {"b": 2}, {"a": 3}, {"c": 4})
        assert result == {"a": 3, "b": 2, "c": 4}

    def test_empty_merge(self):
        """Test merging with empty dicts."""
        result = merge_variables({}, {"a": 1}, {})
        assert result == {"a": 1}

    def test_non_dict_ignored(self):
        """Test that non-dict arguments are ignored."""
        result = merge_variables({"a": 1}, None, "not dict", {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_no_args(self):
        """Test with no arguments."""
        result = merge_variables()
        assert result == {}


class TestValidateVariables:
    """Test variable validation."""

    def test_valid_dict(self):
        """Test with valid dictionary."""
        variables = {"a": 1, "b": "test"}
        result = validate_variables(variables)
        assert result == variables

    def test_none_variables(self):
        """Test with None returns empty dict."""
        result = validate_variables(None)
        assert result == {}

    def test_non_string_keys_converted(self):
        """Test that non-string keys are converted to strings."""
        variables = {1: "one", 2: "two"}
        result = validate_variables(variables)
        assert result == {"1": "one", "2": "two"}

    def test_invalid_type_raises(self):
        """Test that invalid types raise TypeError."""
        with pytest.raises(TypeError, match="Variables must be dict or None"):
            validate_variables("not a dict")

    def test_invalid_type_list_raises(self):
        """Test that list raises TypeError."""
        with pytest.raises(TypeError, match="Variables must be dict or None"):
            validate_variables([1, 2, 3])


class TestPerformanceRequirements:
    """Test performance requirements for pure functions."""

    def test_large_dict_substitution_fast(self):
        """Test that large dictionary substitution is fast."""
        import time

        # Create large test data
        large_dict = {f"key_{i}": f"value_$var_{i}" for i in range(1000)}
        variables = {f"var_{i}": f"substituted_{i}" for i in range(1000)}

        start_time = time.time()
        result = substitute_in_dict(large_dict, variables)
        end_time = time.time()

        # Should complete in reasonable time (< 100ms for 1000 items)
        assert (end_time - start_time) < 0.1
        assert len(result) == 1000

    def test_immutability_preserved(self):
        """Test that original data structures are never modified."""
        original_dict = {"query": "SELECT * FROM $table", "nested": {"key": "$value"}}
        original_list = ["$item1", {"nested": "$item2"}]
        variables = {"table": "users", "value": "test", "item1": "one", "item2": "two"}

        # Store original state
        dict_copy = original_dict.copy()
        list_copy = original_list.copy()

        # Perform substitutions
        substitute_in_dict(original_dict, variables)
        substitute_in_list(original_list, variables)

        # Verify originals are unchanged
        assert original_dict == dict_copy
        assert original_list == list_copy
