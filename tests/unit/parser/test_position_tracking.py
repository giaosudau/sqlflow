"""Tests for position tracking in the lexer."""

from sqlflow.parser.lexer import Lexer, TokenType


class TestPositionTracking:
    """Test precise position tracking in the lexer."""

    def test_simple_tokens_position_tracking(self):
        """Test that simple tokens have correct line, column, and char_position."""
        text = "SOURCE users TYPE csv"
        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Filter out EOF token
        tokens = [t for t in tokens if t.type != TokenType.EOF]

        expected_positions = [
            (TokenType.SOURCE, "SOURCE", 1, 1, 0),
            (TokenType.IDENTIFIER, "users", 1, 8, 7),
            (TokenType.TYPE, "TYPE", 1, 14, 13),
            (TokenType.IDENTIFIER, "csv", 1, 19, 18),
        ]

        assert len(tokens) == len(expected_positions)

        for token, (exp_type, exp_value, exp_line, exp_column, exp_char_pos) in zip(
            tokens, expected_positions
        ):
            assert token.type == exp_type
            assert token.value == exp_value
            assert token.line == exp_line
            assert token.column == exp_column
            assert token.char_position == exp_char_pos

    def test_multiline_position_tracking(self):
        """Test position tracking across multiple lines."""
        text = "SOURCE users\nTYPE csv\nPARAMS {}"
        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Filter out EOF token
        tokens = [t for t in tokens if t.type != TokenType.EOF]

        expected_positions = [
            (TokenType.SOURCE, "SOURCE", 1, 1, 0),
            (TokenType.IDENTIFIER, "users", 1, 8, 7),
            (TokenType.TYPE, "TYPE", 2, 1, 13),
            (TokenType.IDENTIFIER, "csv", 2, 6, 18),
            (TokenType.PARAMS, "PARAMS", 3, 1, 22),
            (TokenType.JSON_OBJECT, "{}", 3, 8, 29),
        ]

        assert len(tokens) == len(expected_positions)

        for token, (exp_type, exp_value, exp_line, exp_column, exp_char_pos) in zip(
            tokens, expected_positions
        ):
            assert token.type == exp_type
            assert token.value == exp_value
            assert token.line == exp_line
            assert token.column == exp_column
            assert token.char_position == exp_char_pos

    def test_json_object_position_tracking(self):
        """Test position tracking for JSON objects."""
        text = 'SOURCE users TYPE csv PARAMS {"file": "users.csv"}'
        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Find the JSON object token
        json_token = next(t for t in tokens if t.type == TokenType.JSON_OBJECT)

        assert json_token.line == 1
        assert json_token.column == 30  # Position where JSON starts
        assert json_token.char_position == 29  # 0-indexed char position
        assert json_token.value == '{"file": "users.csv"}'

    def test_error_token_position_tracking(self):
        """Test position tracking for error tokens."""
        text = "SOURCE @ users"  # @ is an invalid character
        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Find the error token
        error_token = next(t for t in tokens if t.type == TokenType.ERROR)

        assert error_token.line == 1
        assert error_token.column == 8  # Position of the @ character
        assert error_token.char_position == 7  # 0-indexed char position
        assert error_token.value == "@"
