"""Lexer for SQLFlow DSL."""

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Pattern, Tuple


class TokenType(Enum):
    """Token types for SQLFlow DSL lexer."""

    SOURCE = auto()
    TYPE = auto()
    PARAMS = auto()
    LOAD = auto()
    FROM = auto()
    EXPORT = auto()
    TO = auto()
    OPTIONS = auto()
    SELECT = auto()
    INCLUDE = auto()
    AS = auto()
    SET = auto()
    CREATE = auto()
    TABLE = auto()

    IDENTIFIER = auto()
    STRING = auto()
    NUMBER = auto()
    JSON_OBJECT = auto()

    SEMICOLON = auto()

    SQL_BLOCK = auto()

    WHITESPACE = auto()
    COMMENT = auto()
    ERROR = auto()
    EOF = auto()


@dataclass
class Token:
    """Token in the SQLFlow DSL."""

    type: TokenType
    value: str
    line: int
    column: int


class Lexer:
    """Lexer for SQLFlow DSL.

    The lexer tokenizes the input text into a sequence of tokens.
    """

    def __init__(self, text: str):
        """Initialize the lexer with input text.

        Args:
            text: The input text to tokenize
        """
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []

        self.patterns: List[Tuple[TokenType, Pattern]] = [
            (TokenType.WHITESPACE, re.compile(r"\s+")),
            (TokenType.COMMENT, re.compile(r"--.*?(?:\n|$)")),
            (TokenType.SOURCE, re.compile(r"SOURCE\b", re.IGNORECASE)),
            (TokenType.TYPE, re.compile(r"TYPE\b", re.IGNORECASE)),
            (TokenType.PARAMS, re.compile(r"PARAMS\b", re.IGNORECASE)),
            (TokenType.LOAD, re.compile(r"LOAD\b", re.IGNORECASE)),
            (TokenType.FROM, re.compile(r"FROM\b", re.IGNORECASE)),
            (TokenType.EXPORT, re.compile(r"EXPORT\b", re.IGNORECASE)),
            (TokenType.TO, re.compile(r"TO\b", re.IGNORECASE)),
            (TokenType.OPTIONS, re.compile(r"OPTIONS\b", re.IGNORECASE)),
            (TokenType.SELECT, re.compile(r"SELECT\b", re.IGNORECASE)),
            (TokenType.INCLUDE, re.compile(r"INCLUDE\b", re.IGNORECASE)),
            (TokenType.AS, re.compile(r"AS\b", re.IGNORECASE)),
            (TokenType.SET, re.compile(r"SET\b", re.IGNORECASE)),
            (TokenType.CREATE, re.compile(r"CREATE\b", re.IGNORECASE)),
            (TokenType.TABLE, re.compile(r"TABLE\b", re.IGNORECASE)),
            (TokenType.SEMICOLON, re.compile(r";")),
            (TokenType.IDENTIFIER, re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")),
            (TokenType.STRING, re.compile(r'"(?:[^"\\]|\\.)*"')),
            (TokenType.NUMBER, re.compile(r"\d+(?:\.\d+)?")),
        ]

    def tokenize(self) -> List[Token]:
        """Tokenize the input text.

        Returns:
            List of tokens
        """
        while self.pos < len(self.text):
            self._tokenize_next()

        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens

    def _tokenize_next(self) -> None:
        """Tokenize the next token in the input text."""
        for token_type, pattern in self.patterns:
            match = pattern.match(self.text[self.pos :])
            if match:
                value = match.group(0)

                if token_type not in (TokenType.WHITESPACE, TokenType.COMMENT):
                    self.tokens.append(Token(token_type, value, self.line, self.column))

                for char in value:
                    if char == "\n":
                        self.line += 1
                        self.column = 1
                    else:
                        self.column += 1

                self.pos += len(value)
                return

        if self.text[self.pos] == "{":
            json_value, success = self._extract_json_object()
            if success:
                self.tokens.append(
                    Token(TokenType.JSON_OBJECT, json_value, self.line, self.column)
                )

                for char in json_value:
                    if char == "\n":
                        self.line += 1
                        self.column = 1
                    else:
                        self.column += 1

                self.pos += len(json_value)
                return

        self.tokens.append(
            Token(TokenType.ERROR, self.text[self.pos], self.line, self.column)
        )
        self.column += 1
        self.pos += 1

    def _extract_json_object(self) -> Tuple[str, bool]:
        """Extract a JSON object from the input text.

        Returns:
            Tuple of (json_text, success)
        """
        if self.text[self.pos] != "{":
            return "", False

        depth = 0
        in_string = False
        escape_next = False
        start_pos = self.pos

        for i in range(start_pos, len(self.text)):
            char = self.text[i]

            if escape_next:
                escape_next = False
                continue

            if char == "\\" and in_string:
                escape_next = True
                continue

            if char == '"' and not escape_next:
                in_string = not in_string
                continue

            if not in_string:
                if char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        return self.text[start_pos : i + 1], True

        return "", False
