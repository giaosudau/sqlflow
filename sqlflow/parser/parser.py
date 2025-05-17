"""Parser for SQLFlow DSL."""

import json
from typing import List, Optional

from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    ExportStep,
    IncludeStep,
    LoadStep,
    Pipeline,
    PipelineStep,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)
from sqlflow.parser.lexer import Lexer, Token, TokenType


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

    def __init__(self, text: Optional[str] = None):
        """Initialize the parser with input text.

        Args:
            text: The input text to parse (optional)
        """
        if text is not None:
            self.lexer = Lexer(text)
        else:
            self.lexer = None

        self.tokens = []
        self.current = 0
        self.pipeline = Pipeline()
        self._previous_tokens = []  # Track previous tokens for context

    def _tokenize_input(self, text: Optional[str] = None) -> None:
        """Tokenize the input text and set up the lexer if needed.

        Args:
            text: Input text to tokenize (optional)

        Raises:
            ValueError: If no text is provided
            ParserError: If lexer encounters an error
        """
        # If text is provided, create a new lexer
        if text is not None:
            self.lexer = Lexer(text)
        elif self.lexer is None:
            raise ValueError("No text provided to parse")

        # Tokenize input and handle lexer errors
        try:
            self.tokens = self.lexer.tokenize()
        except Exception as e:
            raise ParserError(f"Lexer error: {str(e)}", 0, 0) from e

    def _parse_all_statements(self) -> list:
        """Parse all statements in the token stream.

        Returns:
            List of parsing errors, empty if successful

        Side effect:
            Adds parsed steps to self.pipeline
        """
        parsing_errors = []

        while not self._is_at_end():
            try:
                step = self._parse_statement()
                if step:
                    self.pipeline.add_step(step)
            except ParserError as e:
                # Record the error and continue parsing
                parsing_errors.append(e)
                self._synchronize()
            except Exception as e:
                # Convert unexpected errors to ParserError
                err = ParserError(
                    f"Unexpected error: {str(e)}",
                    self._peek().line,
                    self._peek().column,
                )
                parsing_errors.append(err)
                self._synchronize()

        return parsing_errors

    def _format_error_message(self, errors: list) -> str:
        """Format multiple parsing errors into a single error message.

        Args:
            errors: List of ParserError objects

        Returns:
            Formatted error message
        """
        error_messages = [
            f"{e.message} at line {e.line}, column {e.column}" for e in errors
        ]
        return "\n".join(error_messages)

    def parse(self, text: Optional[str] = None) -> Pipeline:
        """Parse the input text into a Pipeline AST.

        Args:
            text: The input text to parse (optional if provided in constructor)

        Returns:
            Pipeline AST

        Raises:
            ParserError: If the input text cannot be parsed
            ValueError: If no text is provided
        """
        # Reset parser state
        self.current = 0
        self.pipeline = Pipeline()

        # Set up and tokenize the input
        self._tokenize_input(text)

        # Parse all statements and collect any errors
        parsing_errors = self._parse_all_statements()

        # If we encountered any errors, report them all
        if parsing_errors:
            error_message = self._format_error_message(parsing_errors)
            raise ParserError(f"Multiple errors found:\n{error_message}", 0, 0)

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
        elif token.type == TokenType.IF:
            return self._parse_conditional_block()

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

        self._consume(TokenType.PARAMS, "Expected 'PARAMS' after connector type")

        # Use the _parse_json_token method to handle JSON parsing with variable substitution
        params = self._parse_json_token()

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
        token = self.tokens[self.current]
        if not self._is_at_end():
            self.current += 1
        self._previous_tokens.append(token)  # Track the previous token
        return token

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

        self._consume(TokenType.OPTIONS, "Expected 'OPTIONS' after connector type")

        # Use the _parse_json_token method to handle JSON parsing with variable substitution
        options = self._parse_json_token()

        self._consume(TokenType.SEMICOLON, "Expected ';' after EXPORT statement")

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

        # Consume tokens until we find a semicolon
        value_tokens = []
        while not self._check(TokenType.SEMICOLON) and not self._is_at_end():
            token = self._advance()
            value_tokens.append(token)

        if not value_tokens:
            token = self._peek()
            raise ParserError("Expected value after '='", token.line, token.column)

        # Join the tokens to form the complete value
        variable_value = " ".join(token.value for token in value_tokens)
        # Remove outer quotes if present
        variable_value = variable_value.strip("'\"")

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

        This skips tokens until the beginning of the next valid statement.
        Errors in previous statements don't prevent parsing of later statements.
        """
        # If we are at the end of a statement, advance past it
        if self._peek().type == TokenType.SEMICOLON:
            self._advance()

        while not self._is_at_end():
            # We found the end of a statement, prepare for the next one
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

    def _match(self, type: TokenType) -> Token:
        """Match a token of the expected type and advance.
        Similar to _consume but returns the token without raising an error.

        Args:
            type: Expected token type

        Returns:
            The matched token if it matches the expected type,
            otherwise None
        """
        if self._check(type):
            return self._advance()
        return None

    def _parse_json_token(self) -> dict:
        """Parse a JSON token.

        Returns:
            Parsed JSON value
        """
        json_token = self._consume(TokenType.JSON_OBJECT, "Expected JSON object")
        try:
            from sqlflow.parser.lexer import replace_variables_for_validation

            # Pre-process the JSON to handle variables and trailing commas
            json_text = json_token.value
            json_text_for_validation = replace_variables_for_validation(json_text)

            # Try to parse the JSON
            return json.loads(json_text_for_validation)
        except json.JSONDecodeError as e:
            # More specific error messages for common directives
            if self._previous_tokens and len(self._previous_tokens) >= 2:
                prev_token = self._previous_tokens[-2]
                if prev_token.type == TokenType.PARAMS:
                    raise ParserError(
                        f"Invalid JSON in PARAMS: {str(e)}",
                        json_token.line,
                        json_token.column,
                    )
                elif prev_token.type == TokenType.OPTIONS:
                    raise ParserError(
                        f"Invalid JSON in OPTIONS: {str(e)}",
                        json_token.line,
                        json_token.column,
                    )

            # Generic error if we can't determine the context
            raise ParserError(
                f"Invalid JSON: {str(e)}", json_token.line, json_token.column
            )

    def _parse_conditional_block(self) -> ConditionalBlockStep:
        """Parse an IF/ELSEIF/ELSE/ENDIF block.

        Returns:
            ConditionalBlockStep AST node

        Raises:
            ParserError: If the conditional block cannot be parsed
        """
        start_line = self._peek().line
        branches = []
        else_branch = None

        # Parse initial IF branch
        self._consume(TokenType.IF, "Expected 'IF'")
        condition = self._parse_condition_expression()
        self._consume(TokenType.THEN, "Expected 'THEN' after condition")
        if_branch_steps = self._parse_branch_statements(
            [TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF]
        )
        branches.append(ConditionalBranchStep(condition, if_branch_steps, start_line))

        # Parse ELSEIF branches
        while self._check(TokenType.ELSE_IF):
            self._consume(TokenType.ELSE_IF, "Expected 'ELSE IF'")
            elseif_line = self._peek().line
            condition = self._parse_condition_expression()
            self._consume(TokenType.THEN, "Expected 'THEN' after condition")
            elseif_branch_steps = self._parse_branch_statements(
                [TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF]
            )
            branches.append(
                ConditionalBranchStep(condition, elseif_branch_steps, elseif_line)
            )

        # Parse optional ELSE branch
        if self._check(TokenType.ELSE):
            self._consume(TokenType.ELSE, "Expected 'ELSE'")
            else_branch = self._parse_branch_statements([TokenType.END_IF])

        # Consume END IF
        self._consume(TokenType.END_IF, "Expected 'END IF'")
        self._consume(TokenType.SEMICOLON, "Expected ';' after 'END IF'")

        return ConditionalBlockStep(branches, else_branch, start_line)

    def _parse_condition_expression(self) -> str:
        """Parse a condition expression until THEN.

        Returns:
            String containing the condition expression

        Raises:
            ParserError: If the condition expression cannot be parsed
        """
        condition_tokens = []
        while not self._check(TokenType.THEN) and not self._is_at_end():
            token = self._advance()

            # Special handling for variable expressions
            if token.type == TokenType.VARIABLE:
                condition_tokens.append(token.value)
            # Handle equality operator to ensure "==" stays together
            elif (
                token.type == TokenType.EQUALS
                and condition_tokens
                and condition_tokens[-1] == "="
            ):
                # Replace the last "=" with "=="
                condition_tokens[-1] = "=="
            else:
                condition_tokens.append(token.value)

        # Join tokens and normalize spaces
        condition = " ".join(condition_tokens).strip()
        # Replace multiple spaces with single space
        condition = " ".join(condition.split())

        return condition

    def _parse_branch_statements(
        self, terminator_tokens: List[TokenType]
    ) -> List[PipelineStep]:
        """Parse statements until reaching one of the terminator tokens.

        Args:
            terminator_tokens: List of token types that terminate the branch

        Returns:
            List of parsed pipeline steps

        Raises:
            ParserError: If the branch statements cannot be parsed
        """
        branch_steps = []
        while not self._check_any(terminator_tokens) and not self._is_at_end():
            step = self._parse_statement()
            if step:
                branch_steps.append(step)
            else:
                # If we didn't recognize the statement, advance to avoid infinite loop
                self._advance()

        return branch_steps

    def _check_any(self, token_types: List[TokenType]) -> bool:
        """Check if the current token is any of the given types.

        Args:
            token_types: List of token types to check

        Returns:
            True if the current token is any of the given types, False otherwise
        """
        return any(self._check(token_type) for token_type in token_types)
