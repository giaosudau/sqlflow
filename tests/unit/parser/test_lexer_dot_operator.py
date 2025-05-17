"""Tests for SQL dot operator tokenization and formatting."""

import unittest

from sqlflow.parser.lexer import Lexer, TokenType
from sqlflow.parser.parser import Parser


class TestLexerDotOperator(unittest.TestCase):
    """Test that SQL dot operators are properly tokenized and formatted."""

    def test_tokenize_dot_operator(self):
        """Test that dots in SQL are properly tokenized."""
        sql = "a.b"
        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        # Should have 4 tokens: IDENTIFIER, DOT, IDENTIFIER, EOF
        self.assertEqual(4, len(tokens))
        self.assertEqual(TokenType.IDENTIFIER, tokens[0].type)
        self.assertEqual("a", tokens[0].value)
        self.assertEqual(TokenType.DOT, tokens[1].type)
        self.assertEqual(".", tokens[1].value)
        self.assertEqual(TokenType.IDENTIFIER, tokens[2].type)
        self.assertEqual("b", tokens[2].value)
        self.assertEqual(TokenType.EOF, tokens[3].type)

    def test_parse_sql_with_dot_operator(self):
        """Test that SQL with dot operators is properly parsed and formatted."""
        sql = """
        CREATE TABLE test_table AS
        SELECT t.id, t.name, u.email
        FROM users u
        JOIN transactions t ON u.id = t.user_id;
        """

        parser = Parser(sql)
        pipeline = parser.parse()

        # Should have 1 step
        self.assertEqual(1, len(pipeline.steps))
        sql_step = pipeline.steps[0]

        # Check that dots are not surrounded by spaces
        self.assertIn("t.id", sql_step.sql_query)
        self.assertIn("t.name", sql_step.sql_query)
        self.assertIn("u.email", sql_step.sql_query)
        self.assertIn("u.id", sql_step.sql_query)
        self.assertIn("t.user_id", sql_step.sql_query)

        # These should not be found (incorrect spacing)
        self.assertNotIn("t . id", sql_step.sql_query)
        self.assertNotIn("t . name", sql_step.sql_query)
        self.assertNotIn("u . email", sql_step.sql_query)
        self.assertNotIn("u . id", sql_step.sql_query)
        self.assertNotIn("t . user_id", sql_step.sql_query)

    def test_sql_function_call_formatting(self):
        """Test that SQL function calls are properly formatted without spaces before parentheses."""
        sql = """
        CREATE TABLE aggregated_data AS
        SELECT 
            region,
            COUNT (DISTINCT user_id) as user_count,
            SUM (amount) as total_amount,
            AVG (amount) as average_amount
        FROM transactions
        GROUP BY region;
        """

        parser = Parser(sql)
        pipeline = parser.parse()

        # Should have 1 step
        self.assertEqual(1, len(pipeline.steps))
        sql_step = pipeline.steps[0]

        # Check that function calls don't have spaces between name and parenthesis
        self.assertIn("COUNT(DISTINCT", sql_step.sql_query)
        self.assertIn("SUM(amount", sql_step.sql_query)
        self.assertIn("AVG(amount", sql_step.sql_query)

        # These should not be found (incorrect spacing)
        self.assertNotIn("COUNT (", sql_step.sql_query)
        self.assertNotIn("SUM (", sql_step.sql_query)
        self.assertNotIn("AVG (", sql_step.sql_query)


if __name__ == "__main__":
    unittest.main()
