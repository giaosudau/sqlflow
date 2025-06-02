"""Tests for SQL concatenation operator (||) parsing.

Following TDD principles: test behavior, not implementation.
Tests cover lexer tokenization and parser formatting of SQL concatenation.
"""

from sqlflow.parser.lexer import Lexer, TokenType
from sqlflow.parser.parser import Parser


class TestSqlConcatenationOperator:
    """Test SQL concatenation operator (||) parsing and formatting."""

    def test_lexer_tokenizes_concatenation_operator(self):
        """Test that lexer correctly tokenizes || as concatenation operator."""
        # Arrange
        sql = "first_name || last_name"
        lexer = Lexer(sql)

        # Act
        tokens = lexer.tokenize()

        # Assert
        token_types = [token.type for token in tokens if token.type != TokenType.EOF]
        expected_types = [TokenType.IDENTIFIER, TokenType.CONCAT, TokenType.IDENTIFIER]
        assert token_types == expected_types

        # Verify token values
        assert tokens[0].value == "first_name"
        assert tokens[1].value == "||"
        assert tokens[2].value == "last_name"

    def test_lexer_distinguishes_pipe_from_concatenation(self):
        """Test that lexer distinguishes single pipe | from concatenation ||."""
        # Arrange
        sql = "${var|default} || 'text'"
        lexer = Lexer(sql)

        # Act
        tokens = lexer.tokenize()

        # Assert
        # Should have VARIABLE (with internal |), CONCAT, STRING
        non_whitespace_tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.EOF)
        ]
        assert len(non_whitespace_tokens) == 3

        assert non_whitespace_tokens[0].type == TokenType.VARIABLE
        assert non_whitespace_tokens[0].value == "${var|default}"
        assert non_whitespace_tokens[1].type == TokenType.CONCAT
        assert non_whitespace_tokens[1].value == "||"
        assert non_whitespace_tokens[2].type == TokenType.STRING
        assert non_whitespace_tokens[2].value == "'text'"

    def test_parser_formats_simple_concatenation(self):
        """Test parser correctly formats simple string concatenation."""
        # Arrange
        sql = """
        CREATE TABLE full_names AS
        SELECT first_name || ' ' || last_name as full_name
        FROM customers;
        """
        parser = Parser(sql)

        # Act
        pipeline = parser.parse()

        # Assert
        assert len(pipeline.steps) == 1
        sql_step = pipeline.steps[0]

        # Should contain proper concatenation without extra spaces around ||
        assert "first_name || ' ' || last_name" in sql_step.sql_query
        # Should not have spaces around the || operator
        assert "first_name | | " not in sql_step.sql_query
        assert " | | last_name" not in sql_step.sql_query

    def test_parser_formats_complex_concatenation_with_functions(self):
        """Test parser handles concatenation with SQL functions."""
        # Arrange
        sql = """
        CREATE TABLE formatted_data AS
        SELECT 
            UPPER(first_name) || ' - ' || LOWER(last_name) as formatted_name,
            city || ', ' || state as location
        FROM customers;
        """
        parser = Parser(sql)

        # Act
        pipeline = parser.parse()

        # Assert
        assert len(pipeline.steps) == 1
        sql_step = pipeline.steps[0]

        # Check function calls are properly formatted (no space before parentheses)
        assert "UPPER(first_name)" in sql_step.sql_query
        assert "LOWER(last_name)" in sql_step.sql_query

        # Check concatenation is properly formatted
        assert "UPPER(first_name) || ' - ' || LOWER(last_name)" in sql_step.sql_query
        assert "city || ', ' || state" in sql_step.sql_query

    def test_parser_handles_concatenation_in_case_expressions(self):
        """Test parser handles concatenation within CASE expressions."""
        # Arrange
        sql = """
        CREATE TABLE conditional_names AS
        SELECT 
            CASE 
                WHEN middle_name IS NOT NULL THEN first_name || ' ' || middle_name || ' ' || last_name
                ELSE first_name || ' ' || last_name
            END as full_name
        FROM customers;
        """
        parser = Parser(sql)

        # Act
        pipeline = parser.parse()

        # Assert
        assert len(pipeline.steps) == 1
        sql_step = pipeline.steps[0]

        # Verify concatenation expressions are preserved
        assert (
            "first_name || ' ' || middle_name || ' ' || last_name" in sql_step.sql_query
        )
        assert "first_name || ' ' || last_name" in sql_step.sql_query

    def test_parser_formats_concatenation_with_table_prefixes(self):
        """Test parser handles concatenation with table.column references."""
        # Arrange
        sql = """
        CREATE TABLE customer_details AS
        SELECT 
            c.first_name || ' ' || c.last_name as customer_name,
            o.order_id || '-' || c.customer_id as reference_code
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id;
        """
        parser = Parser(sql)

        # Act
        pipeline = parser.parse()

        # Assert
        assert len(pipeline.steps) == 1
        sql_step = pipeline.steps[0]

        # Verify table.column references have no spaces around dots
        assert "c.first_name" in sql_step.sql_query
        assert "c.last_name" in sql_step.sql_query
        assert "o.order_id" in sql_step.sql_query
        assert "c.customer_id" in sql_step.sql_query

        # Verify concatenation is preserved
        assert "c.first_name || ' ' || c.last_name" in sql_step.sql_query
        assert "o.order_id || '-' || c.customer_id" in sql_step.sql_query

    def test_lexer_handles_multiple_consecutive_concatenations(self):
        """Test lexer correctly handles multiple || operators in sequence."""
        # Arrange
        sql = "a || b || c || d"
        lexer = Lexer(sql)

        # Act
        tokens = lexer.tokenize()

        # Assert
        non_whitespace_tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.EOF)
        ]
        expected_pattern = [
            TokenType.IDENTIFIER,  # a
            TokenType.CONCAT,  # ||
            TokenType.IDENTIFIER,  # b
            TokenType.CONCAT,  # ||
            TokenType.IDENTIFIER,  # c
            TokenType.CONCAT,  # ||
            TokenType.IDENTIFIER,  # d
        ]

        actual_types = [token.type for token in non_whitespace_tokens]
        assert actual_types == expected_pattern

    def test_parser_handles_concatenation_in_export_statements(self):
        """Test parser handles concatenation in EXPORT SELECT statements."""
        # Arrange
        sql = """
        EXPORT SELECT 
            customer_id,
            first_name || ' ' || last_name as customer_name,
            city || ', ' || state as location
        FROM customers 
        TO 'output/customers.csv' 
        TYPE CSV 
        OPTIONS { "header": true };
        """
        parser = Parser(sql)

        # Act
        pipeline = parser.parse()

        # Assert
        assert len(pipeline.steps) == 1
        export_step = pipeline.steps[0]

        # Verify concatenation is preserved in the SQL query
        assert "first_name || ' ' || last_name" in export_step.sql_query
        assert "city || ', ' || state" in export_step.sql_query

    def test_concatenation_operator_precedence_with_arithmetic(self):
        """Test concatenation operator works correctly with arithmetic operators."""
        # Arrange
        sql = """
        CREATE TABLE mixed_operations AS
        SELECT 
            'Order ' || order_id || ': $' || (price * quantity) as description
        FROM order_items;
        """
        parser = Parser(sql)

        # Act
        pipeline = parser.parse()

        # Assert
        assert len(pipeline.steps) == 1
        sql_step = pipeline.steps[0]

        # Verify both concatenation and arithmetic are preserved
        assert (
            "'Order ' || order_id || ': $' || (price * quantity)" in sql_step.sql_query
        )
