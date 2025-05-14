"""Parser for SQLFlow DSL."""

import json
from typing import List, Optional

from sqlflow.sqlflow.parser.ast import (
    ExportStep,
    IncludeStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)
from sqlflow.sqlflow.parser.lexer import Lexer, Token, TokenType


class ParserError(Exception):
    """Exception raised for parser errors."""

    def __init__(self, message: str, line: int, column: int):
        """Initialize a ParserError.

        Args:
            message: Error message
            line: Line number where the error occurred
            column: Column number where the error occurred
        """
        self.message = message
        self.line = line
        self.column = column
        super().__init__(f"{message} at line {line}, column {column}")


class Parser:
    """Parser for SQLFlow DSL.

    The parser converts a sequence of tokens into an AST.
    """

    def __init__(self, text: str):
        """Initialize the parser with input text.

        Args:
            text: The input text to parse
        """
        self.lexer = Lexer(text)
        self.tokens: List[Token] = []
        self.current = 0
        self.pipeline = Pipeline()

    def parse(self) -> Pipeline:
        """Parse the input text into a Pipeline AST.

        Returns:
            Pipeline AST

        Raises:
            ParserError: If the input text cannot be parsed
        """
        self.tokens = self.lexer.tokenize()

        while not self._is_at_end():
            try:
                step = self._parse_statement()
                if step:
                    self.pipeline.add_step(step)
            except ParserError as e:
                self._synchronize()
                raise e

        return self.pipeline

    def _parse_statement(self) -> Optional[PipelineStep]:
        """Parse a statement in the SQLFlow DSL.

        Returns:
            PipelineStep or None if the statement is not recognized

        Raises:
            ParserError: If the statement cannot be parsed
        """
        token = self._peek()

        if token.type == TokenType.SOURCE:
            return self._parse_source_statement()
        elif token.type == TokenType.LOAD:
            return self._parse_load_statement()
        elif token.type == TokenType.EXPORT:
            return self._parse_export_statement()
        elif token.type == TokenType.INCLUDE:
            return self._parse_include_statement()
        elif token.type == TokenType.SET:
            return self._parse_set_statement()
        elif token.type == TokenType.CREATE:
            return self._parse_sql_block_statement()

        self._advance()
        return None

    def _parse_source_statement(self) -> SourceDefinitionStep:
        """Parse a SOURCE statement.

        Returns:
            SourceDefinitionStep

        Raises:
            ParserError: If the SOURCE statement cannot be parsed
        """
        source_token = self._consume(TokenType.SOURCE, "Expected 'SOURCE'")

        name_token = self._consume(
            TokenType.IDENTIFIER, "Expected source name after 'SOURCE'"
        )

        self._consume(TokenType.TYPE, "Expected 'TYPE' after source name")

        type_token = self._consume(
            TokenType.IDENTIFIER, "Expected connector type after 'TYPE'"
        )

        self._consume(
            TokenType.PARAMS, "Expected 'PARAMS' after connector type"
        )

        params_token = self._consume(
            TokenType.JSON_OBJECT, "Expected JSON object after 'PARAMS'"
        )

        try:
            params = json.loads(params_token.value)
        except json.JSONDecodeError:
            raise ParserError(
                "Invalid JSON in PARAMS", params_token.line, params_token.column
            )

        self._consume(TokenType.SEMICOLON, "Expected ';' after SOURCE statement")

        return SourceDefinitionStep(
            name=name_token.value,
            connector_type=type_token.value,
            params=params,
            line_number=source_token.line,
        )

    def _advance(self) -> Token:
        """Advance to the next token.

        Returns:
            The current token before advancing
        """
        if not self._is_at_end():
            self.current += 1
        return self._previous()

    def _consume(self, type: TokenType, error_message: str) -> Token:
        """Consume a token of the expected type.

        Args:
            type: Expected token type
            error_message: Error message if the token is not of the expected
                type

        Returns:
            The consumed token

        Raises:
            ParserError: If the token is not of the expected type
        """
        if self._check(type):
            return self._advance()

        token = self._peek()
        raise ParserError(error_message, token.line, token.column)

    def _check(self, type: TokenType) -> bool:
        """Check if the current token is of the expected type.

        Args:
            type: Expected token type

        Returns:
            True if the current token is of the expected type, False otherwise
        """
        if self._is_at_end():
            return False
        return self._peek().type == type

    def _is_at_end(self) -> bool:
        """Check if we have reached the end of the token stream.

        Returns:
            True if we have reached the end, False otherwise
        """
        return self._peek().type == TokenType.EOF

    def _peek(self) -> Token:
        """Peek at the current token.

        Returns:
            The current token
        """
        return self.tokens[self.current]

    def _previous(self) -> Token:
        """Get the previous token.

        Returns:
            The previous token
        """
        return self.tokens[self.current - 1]

    def _parse_load_statement(self) -> LoadStep:
        """Parse a LOAD statement.

        Returns:
            LoadStep

        Raises:
            ParserError: If the LOAD statement cannot be parsed
        """
        load_token = self._consume(TokenType.LOAD, "Expected 'LOAD'")

        table_name_token = self._consume(
            TokenType.IDENTIFIER, "Expected table name after 'LOAD'"
        )

        self._consume(TokenType.FROM, "Expected 'FROM' after table name")

        source_name_token = self._consume(
            TokenType.IDENTIFIER, "Expected source name after 'FROM'"
        )

        self._consume(TokenType.SEMICOLON, "Expected ';' after LOAD statement")

        return LoadStep(
            table_name=table_name_token.value,
            source_name=source_name_token.value,
            line_number=load_token.line,
        )

    def _parse_export_statement(self) -> ExportStep:
        """Parse an EXPORT statement.

        Returns:
            ExportStep

        Raises:
            ParserError: If the EXPORT statement cannot be parsed
        """
        export_token = self._consume(TokenType.EXPORT, "Expected 'EXPORT'")

        self._consume(TokenType.SELECT, "Expected 'SELECT' after 'EXPORT'")

        sql_query_tokens = ["SELECT"]
        while not self._check(TokenType.TO) and not self._is_at_end():
            sql_query_tokens.append(self._advance().value)

        sql_query = " ".join(sql_query_tokens)

        self._consume(TokenType.TO, "Expected 'TO' after SQL query")

        destination_uri_token = self._consume(
            TokenType.STRING, "Expected destination URI string after 'TO'"
        )
        destination_uri = destination_uri_token.value.strip('"')

        self._consume(TokenType.TYPE, "Expected 'TYPE' after destination URI")

        connector_type_token = self._consume(
            TokenType.IDENTIFIER, "Expected connector type after 'TYPE'"
        )

        self._consume(
            TokenType.OPTIONS, "Expected 'OPTIONS' after connector type"
        )

        options_token = self._consume(
            TokenType.JSON_OBJECT, "Expected JSON object after 'OPTIONS'"
        )

        try:
            options = json.loads(options_token.value)
        except json.JSONDecodeError:
            raise ParserError(
                "Invalid JSON in OPTIONS", options_token.line, options_token.column
            )

        self._consume(
            TokenType.SEMICOLON, "Expected ';' after EXPORT statement"
        )

        return ExportStep(
            sql_query=sql_query,
            destination_uri=destination_uri,
            connector_type=connector_type_token.value,
            options=options,
            line_number=export_token.line,
        )

    def _parse_include_statement(self) -> IncludeStep:
        """Parse an INCLUDE statement.

        Returns:
            IncludeStep

        Raises:
            ParserError: If the INCLUDE statement cannot be parsed
        """
        include_token = self._consume(TokenType.INCLUDE, "Expected 'INCLUDE'")

        file_path_token = self._consume(
            TokenType.STRING, "Expected file path string after 'INCLUDE'"
        )
        file_path = file_path_token.value.strip('"')

        self._consume(TokenType.AS, "Expected 'AS' after file path")

        alias_token = self._consume(TokenType.IDENTIFIER, "Expected alias after 'AS'")

        self._consume(TokenType.SEMICOLON, "Expected ';' after INCLUDE statement")

        return IncludeStep(
            file_path=file_path, alias=alias_token.value, line_number=include_token.line
        )

    def _parse_set_statement(self) -> SetStep:
        """Parse a SET statement.

        Returns:
            SetStep

        Raises:
            ParserError: If the SET statement cannot be parsed
        """
        set_token = self._consume(TokenType.SET, "Expected 'SET'")

        variable_name_token = self._consume(
            TokenType.IDENTIFIER, "Expected variable name after 'SET'"
        )

        equals_token = self._advance()
        if equals_token.value != "=":
            raise ParserError(
                "Expected '=' after variable name",
                equals_token.line,
                equals_token.column,
            )

        value_token = self._advance()
        if value_token.type not in (
            TokenType.STRING,
            TokenType.NUMBER,
            TokenType.IDENTIFIER,
        ):
            raise ParserError(
                "Expected value after '='", value_token.line, value_token.column
            )

        variable_value = value_token.value
        if value_token.type == TokenType.STRING:
            variable_value = variable_value.strip('"')

        self._consume(TokenType.SEMICOLON, "Expected ';' after SET statement")

        return SetStep(
            variable_name=variable_name_token.value,
            variable_value=variable_value,
            line_number=set_token.line,
        )

    def _parse_sql_block_statement(self) -> SQLBlockStep:
        """Parse a CREATE TABLE statement.

        Returns:
            SQLBlockStep

        Raises:
            ParserError: If the CREATE TABLE statement cannot be parsed
        """
        create_token = self._consume(TokenType.CREATE, "Expected 'CREATE'")

        self._consume(TokenType.TABLE, "Expected 'TABLE' after 'CREATE'")

        table_name_token = self._consume(
            TokenType.IDENTIFIER, "Expected table name after 'TABLE'"
        )

        self._consume(TokenType.AS, "Expected 'AS' after table name")

        sql_query_tokens = ["SELECT"]
        self._consume(TokenType.SELECT, "Expected 'SELECT' after 'AS'")

        while not self._check(TokenType.SEMICOLON) and not self._is_at_end():
            sql_query_tokens.append(self._advance().value)

        sql_query = " ".join(sql_query_tokens)

        self._consume(TokenType.SEMICOLON, "Expected ';' after SQL query")

        return SQLBlockStep(
            table_name=table_name_token.value,
            sql_query=sql_query,
            line_number=create_token.line,
        )

    def _synchronize(self) -> None:
        """Synchronize the parser after an error.

        This skips tokens until the beginning of the next statement.
        """
        self._advance()

        while not self._is_at_end():
            if self._previous().type == TokenType.SEMICOLON:
                return

            if self._peek().type in (
                TokenType.SOURCE,
                TokenType.LOAD,
                TokenType.EXPORT,
                TokenType.INCLUDE,
                TokenType.SET,
                TokenType.CREATE,
            ):
                return

            self._advance()
