"""Tests for the condition evaluator."""

import pytest

from sqlflow.core.evaluator import ConditionEvaluator, EvaluationError


class TestConditionEvaluator:
    """Test cases for the ConditionEvaluator class."""

    def test_basic_comparison_equality(self):
        """Test that basic equality comparisons work."""
        # Setup
        variables = {"env": "production", "debug": True, "count": 42}
        evaluator = ConditionEvaluator(variables)

        # Test string comparison with exact string match
        assert evaluator.evaluate("${env} == 'production'") is True
        assert evaluator.evaluate("${env} == 'development'") is False

        # Test string comparison with variable string
        assert evaluator.evaluate("'production' == ${env}") is True
        assert evaluator.evaluate("'development' == ${env}") is False

        # Test numeric comparison
        assert evaluator.evaluate("${count} == 42") is True
        assert evaluator.evaluate("${count} == 100") is False

        # Test boolean comparison
        assert evaluator.evaluate("${debug} == true") is True
        assert evaluator.evaluate("${debug} == false") is False

    def test_basic_comparison_inequality(self):
        """Test that basic inequality comparisons work."""
        # Setup
        variables = {"env": "production", "debug": True, "count": 42}
        evaluator = ConditionEvaluator(variables)

        # Test string comparison
        assert evaluator.evaluate("${env} != 'development'") is True
        assert evaluator.evaluate("${env} != 'production'") is False

        # Test numeric comparison
        assert evaluator.evaluate("${count} != 100") is True
        assert evaluator.evaluate("${count} != 42") is False

        # Test boolean comparison
        assert evaluator.evaluate("${debug} != false") is True
        assert evaluator.evaluate("${debug} != true") is False

    def test_numeric_comparisons(self):
        """Test that numeric comparisons work."""
        # Setup
        variables = {"count": 42, "threshold": 50, "small": 10}
        evaluator = ConditionEvaluator(variables)

        # Test less than
        assert evaluator.evaluate("${count} < ${threshold}") is True
        assert evaluator.evaluate("${count} < ${small}") is False

        # Test less than or equal
        assert evaluator.evaluate("${count} <= ${threshold}") is True
        assert evaluator.evaluate("${count} <= ${count}") is True
        assert evaluator.evaluate("${count} <= ${small}") is False

        # Test greater than
        assert evaluator.evaluate("${count} > ${small}") is True
        assert evaluator.evaluate("${count} > ${threshold}") is False

        # Test greater than or equal
        assert evaluator.evaluate("${count} >= ${small}") is True
        assert evaluator.evaluate("${count} >= ${count}") is True
        assert evaluator.evaluate("${count} >= ${threshold}") is False

    def test_logical_operators(self):
        """Test that logical operators work."""
        # Setup
        variables = {"env": "production", "debug": True, "count": 42}
        evaluator = ConditionEvaluator(variables)

        # Test AND
        assert evaluator.evaluate("${env} == 'production' and ${debug} == true") is True
        assert evaluator.evaluate("${env} == 'production' and ${count} > 100") is False

        # Test OR
        assert evaluator.evaluate("${env} == 'development' or ${debug} == true") is True
        assert evaluator.evaluate("${env} == 'development' or ${count} > 100") is False

        # Test NOT
        assert evaluator.evaluate("not ${debug} == false") is True
        assert evaluator.evaluate("not ${env} == 'production'") is False

        # Test complex expressions
        assert (
            evaluator.evaluate(
                "(${env} == 'production' or ${env} == 'staging') and ${count} < 100"
            )
            is True
        )
        assert (
            evaluator.evaluate(
                "not (${env} == 'development' or ${count} > 100) and ${debug} == true"
            )
            is True
        )

    def test_variable_default_values(self):
        """Test that default values for variables work."""
        # Setup
        variables = {"env": "production"}
        evaluator = ConditionEvaluator(variables)

        # Test variable with default that's not used
        assert evaluator.evaluate("${env|development} == 'production'") is True

        # Test variable with default that is used
        assert evaluator.evaluate("${region|us-east-1} == 'us-east-1'") is True
        assert evaluator.evaluate("${count|10} > 5") is True
        assert evaluator.evaluate("${debug|false} == false") is True

    def test_missing_variables(self):
        """Test that missing variables are handled gracefully."""
        # Setup
        variables = {}
        evaluator = ConditionEvaluator(variables)

        # Test missing variable becomes None
        assert evaluator.evaluate("${env} == None") is True
        assert evaluator.evaluate("${env} == 'production'") is False

    def test_invalid_expressions(self):
        """Test that invalid expressions raise appropriate errors."""
        # Setup
        variables = {"env": "production", "count": 42}
        evaluator = ConditionEvaluator(variables)

        # Test unsupported operations
        with pytest.raises(EvaluationError):
            evaluator.evaluate("${count} + 10 == 52")  # Arithmetic not supported

        # Test invalid syntax
        with pytest.raises(EvaluationError):
            evaluator.evaluate("${env} == ")  # Incomplete expression

        # Test non-boolean result
        with pytest.raises(EvaluationError):
            evaluator.evaluate("${env}")  # Just returning a string

    def test_single_equals_raises_syntax_error(self):
        """Test that using '=' instead of '==' raises a syntax error with a helpful message."""
        variables = {"use_csv": "true"}
        evaluator = ConditionEvaluator(variables)
        with pytest.raises(EvaluationError) as excinfo:
            evaluator.evaluate("${use_csv} = 'true'")
        assert "Syntax error in condition" in str(excinfo.value)
        assert "Use '==' for equality" in str(excinfo.value)
