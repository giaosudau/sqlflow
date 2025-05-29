"""Parser for SQLFlow DSL."""

import json
import re
from typing import List, Optional

from sqlflow.logging import get_logger
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

logger = get_logger(__name__)


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
            logger.debug("Parser initialized with provided text")
        else:
            self.lexer = None
            logger.debug("Parser initialized without text")

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
            logger.debug("Created new lexer with provided text")
        elif self.lexer is None:
            logger.error("No text provided to parse")
            raise ValueError("No text provided to parse")

        # Tokenize input and handle lexer errors
        try:
            self.tokens = self.lexer.tokenize()
            logger.debug(f"Tokenized input: {len(self.tokens)} tokens generated")
        except Exception as e:
            logger.error(f"Lexer error: {str(e)}")
            raise ParserError(f"Lexer error: {str(e)}", 0, 0) from e

    def _parse_all_statements(self) -> list:
        """Parse all statements in the token stream.

        Returns:
            List of parsing errors, empty if successful

        Side effect:
            Adds parsed steps to self.pipeline
        """
        parsing_errors = []
        logger.debug("Starting to parse all statements")

        while not self._is_at_end():
            try:
                step = self._parse_statement()
                if step:
                    logger.debug(
                        f"Added step of type {type(step).__name__} to pipeline"
                    )
                    self.pipeline.add_step(step)
            except ParserError as e:
                # Record the error and continue parsing
                parsing_errors.append(e)
                # Log at debug level to avoid duplicate output when CLI formats errors
                logger.debug(
                    f"Parser error: {e.message} at line {e.line}, column {e.column}"
                )
                self._synchronize()
            except Exception as e:
                # Convert unexpected errors to ParserError
                err = ParserError(
                    f"Unexpected error: {str(e)}",
                    self._peek().line,
                    self._peek().column,
                )
                parsing_errors.append(err)
                # Log at debug level to avoid duplicate output when CLI formats errors
                logger.debug(
                    f"Unexpected error: {str(e)} at line {self._peek().line}, column {self._peek().column}"
                )
                self._synchronize()

        logger.debug(
            f"Completed parsing: {len(parsing_errors)} errors, {len(self.pipeline.steps)} steps"
        )
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

    def parse(self, text: Optional[str] = None, validate: bool = True) -> Pipeline:
        """Parse the input text into a Pipeline AST.

        Args:
            text: The input text to parse (optional if provided in constructor)
            validate: Whether to run validation after parsing (default: True)

        Returns:
            Pipeline AST

        Raises:
            ValidationError: If the pipeline has validation errors and validate=True
            ParserError: If the input text cannot be parsed
            ValueError: If no text is provided
        """
        # Reset parser state
        self.current = 0
        self.pipeline = Pipeline()
        logger.info("Starting parsing pipeline")

        # Set up and tokenize the input
        self._tokenize_input(text)

        # Parse all statements and collect any errors
        parsing_errors = self._parse_all_statements()

        # If we encountered any errors, report them all
        if parsing_errors:
            error_message = self._format_error_message(parsing_errors)
            # Log at debug level to avoid duplicate output when CLI formats errors
            logger.debug(f"Parsing failed: {len(parsing_errors)} errors found")
            raise ParserError(f"Multiple errors found:\n{error_message}", 0, 0)

        # Validate the parsed pipeline if requested
        if validate:
            validation_errors = self._validate_pipeline()
            if validation_errors:
                # Raise the first validation error (they all have proper formatting)
                logger.debug(
                    f"Validation failed: {len(validation_errors)} errors found"
                )
                raise validation_errors[0]

        logger.info(
            f"Successfully parsed {'and validated ' if validate else ''}pipeline with {len(self.pipeline.steps)} steps"
        )
        return self.pipeline

    def _validate_pipeline(self) -> List:
        """Validate the parsed pipeline using the validation module.

        Returns:
            List of ValidationError objects, empty if valid
        """
        try:
            from sqlflow.validation import ValidationError, validate_pipeline

            return validate_pipeline(self.pipeline)
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            # Return a basic error if validation module fails
            from sqlflow.validation import ValidationError

            return [
                ValidationError(
                    message=f"Validation failed: {str(e)}",
                    line=1,
                    column=0,
                    error_type="validation_error",
                    suggestions=["Check the pipeline syntax"],
                    help_url="",
                )
            ]

    def _parse_statement(self) -> Optional[PipelineStep]:
        """Parse a statement in the SQLFlow DSL.

        Returns:
            PipelineStep or None if the statement is not recognized

        Raises:
            ParserError: If the statement cannot be parsed
        """
        token = self._peek()

        logger.debug(
            f"Parsing statement, next token is: {token.type.name} at line {token.line}"
        )

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
        logger.debug(f"Unknown statement type: {token.type.name}, skipping")
        return None

    def _parse_source_statement(self) -> SourceDefinitionStep:
        """Parse a SOURCE statement.

        Two supported syntaxes:
        1. SOURCE name TYPE connector_type PARAMS {...};
        2. SOURCE name FROM "connector_name" OPTIONS {...};

        Returns:
            SourceDefinitionStep

        Raises:
            ParserError: If the SOURCE statement cannot be parsed
        """
        source_token = self._consume(TokenType.SOURCE, "Expected 'SOURCE'")

        name_token = self._consume(
            TokenType.IDENTIFIER, "Expected source name after 'SOURCE'"
        )

        # Look ahead to see if this is a FROM or TYPE based syntax
        next_token = self._peek()

        # Track tokens for syntax validation and error messages

        if next_token.type == TokenType.FROM:
            # Handle FROM-based syntax (getting connector from profile)
            self._advance()  # Consume FROM token

            # The connector name is a string token
            connector_name_token = self._consume(
                TokenType.STRING, "Expected connector name string after 'FROM'"
            )
            # Remove quotes from the connector name
            connector_name = connector_name_token.value.strip("\"'")

            next_token = self._peek()

            # Check if user mistakenly used TYPE after FROM
            if next_token.type == TokenType.TYPE:
                self._advance()  # Consume TYPE token
                # This is a syntax error - mixing FROM and TYPE
                error_message = (
                    f"Invalid SOURCE syntax at line {next_token.line}: Cannot mix FROM and TYPE keywords.\n\n"
                    "Choose one of these formats:\n"
                    '1. SOURCE name FROM "connector_name" OPTIONS { ... };\n'
                    "2. SOURCE name TYPE connector_type PARAMS { ... };\n"
                )
                raise ParserError(error_message, next_token.line, next_token.column)

            # Check for OPTIONS keyword
            if next_token.type == TokenType.OPTIONS:
                self._advance()  # Consume OPTIONS token
            else:
                # Produce a helpful error message with the expected syntax
                error_message = (
                    f"Expected 'OPTIONS' after connector name at line {next_token.line}.\n\n"
                    "Correct syntax:\n"
                    'SOURCE name FROM "connector_name" OPTIONS { ... };\n'
                )
                raise ParserError(error_message, next_token.line, next_token.column)

            # Parse options JSON
            options = self._parse_json_token()

            # Look ahead to check for PARAMS (which would be incorrect)
            next_token = self._peek()
            if next_token.type == TokenType.PARAMS:
                self._advance()  # Consume PARAMS token
                # This is a syntax error - mixing OPTIONS and PARAMS
                error_message = (
                    f"Invalid SOURCE syntax at line {next_token.line}: Cannot use PARAMS with FROM-based syntax.\n\n"
                    "Correct syntax:\n"
                    'SOURCE name FROM "connector_name" OPTIONS { ... };\n'
                )
                raise ParserError(error_message, next_token.line, next_token.column)

            self._consume(TokenType.SEMICOLON, "Expected ';' after SOURCE statement")

            return SourceDefinitionStep(
                name=name_token.value,
                connector_type="",  # This will be filled from the profile
                params=options,
                is_from_profile=True,
                profile_connector_name=connector_name,
                line_number=source_token.line,
            )
        elif next_token.type == TokenType.TYPE:
            # Handle traditional TYPE-based syntax
            self._advance()  # Consume TYPE token

            type_token = self._consume(
                TokenType.IDENTIFIER, "Expected connector type after 'TYPE'"
            )

            next_token = self._peek()

            # Check if user mistakenly used FROM after TYPE
            if next_token.type == TokenType.FROM:
                self._advance()  # Consume FROM token
                # This is a syntax error - mixing TYPE and FROM
                error_message = (
                    f"Invalid SOURCE syntax at line {next_token.line}: Cannot mix TYPE and FROM keywords.\n\n"
                    "Choose one of these formats:\n"
                    '1. SOURCE name FROM "connector_name" OPTIONS { ... };\n'
                    "2. SOURCE name TYPE connector_type PARAMS { ... };\n"
                )
                raise ParserError(error_message, next_token.line, next_token.column)

            # Check for PARAMS keyword
            if next_token.type == TokenType.PARAMS:
                self._advance()  # Consume PARAMS token
            else:
                # Check if user mistakenly used OPTIONS with TYPE-based syntax
                if next_token.type == TokenType.OPTIONS:
                    self._advance()  # Consume OPTIONS token
                    # This is a syntax error - mixing TYPE and OPTIONS
                    error_message = (
                        f"Invalid SOURCE syntax at line {next_token.line}: Cannot use OPTIONS with TYPE-based syntax.\n\n"
                        "Correct syntax:\n"
                        "SOURCE name TYPE connector_type PARAMS { ... };\n"
                    )
                    raise ParserError(error_message, next_token.line, next_token.column)
                else:
                    # Generic error message for missing PARAMS
                    error_message = (
                        f"Expected 'PARAMS' after connector type at line {next_token.line}.\n\n"
                        "Correct syntax:\n"
                        "SOURCE name TYPE connector_type PARAMS { ... };\n"
                    )
                    raise ParserError(error_message, next_token.line, next_token.column)

            # Use the _parse_json_token method to handle JSON parsing with variable substitution
            params = self._parse_json_token()

            self._consume(TokenType.SEMICOLON, "Expected ';' after SOURCE statement")

            return SourceDefinitionStep(
                name=name_token.value,
                connector_type=type_token.value,
                params=params,
                is_from_profile=False,
                line_number=source_token.line,
            )
        else:
            # Neither FROM nor TYPE - provide error with both syntax options
            error_message = (
                f"Invalid SOURCE syntax at line {next_token.line}: Expected FROM or TYPE after source name.\n\n"
                "Choose one of these formats:\n"
                '1. SOURCE name FROM "connector_name" OPTIONS { ... };\n'
                "2. SOURCE name TYPE connector_type PARAMS { ... };\n"
            )
            raise ParserError(error_message, next_token.line, next_token.column)

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

        Supports three formats:
        1. LOAD table_name FROM source_name;  (default MODE is REPLACE)
        2. LOAD table_name FROM source_name MODE mode_type;
        3. LOAD table_name FROM source_name MODE MERGE MERGE_KEYS key1, key2, ...;

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

        # Default mode is REPLACE if not specified
        mode = "REPLACE"
        merge_keys = []

        # Check if MODE is specified
        if self._check(TokenType.MODE):
            mode, merge_keys = self._parse_load_mode()

        self._consume(TokenType.SEMICOLON, "Expected ';' after LOAD statement")

        return LoadStep(
            table_name=table_name_token.value,
            source_name=source_name_token.value,
            mode=mode,
            merge_keys=merge_keys,
            line_number=load_token.line,
        )

    def _parse_load_mode(self) -> tuple[str, list[str]]:
        """Parse the MODE clause of a LOAD statement.

        Returns:
            Tuple of (mode, merge_keys)

        Raises:
            ParserError: If the MODE clause cannot be parsed
        """
        self._advance()  # Consume MODE token

        # Parse the mode type
        if self._check(TokenType.REPLACE):
            self._advance()  # Consume REPLACE token
            return "REPLACE", []
        elif self._check(TokenType.APPEND):
            self._advance()  # Consume APPEND token
            return "APPEND", []
        elif self._check(TokenType.MERGE):
            self._advance()  # Consume MERGE token
            merge_keys = self._parse_merge_keys()
            return "MERGE", merge_keys
        else:
            token = self._peek()
            raise ParserError(
                "Expected 'REPLACE', 'APPEND', or 'MERGE' after 'MODE'",
                token.line,
                token.column,
            )

    def _parse_merge_keys(self) -> list[str]:
        """Parse MERGE_KEYS clause with optional parentheses.

        Supports both:
        - MERGE_KEYS key1, key2
        - MERGE_KEYS (key1, key2)

        Returns:
            List of merge key column names

        Raises:
            ParserError: If the MERGE_KEYS clause cannot be parsed
        """
        # For MERGE mode, MERGE_KEYS is required
        if not self._check(TokenType.MERGE_KEYS):
            token = self._peek()
            raise ParserError(
                "Expected 'MERGE_KEYS' after 'MERGE'",
                token.line,
                token.column,
            )

        self._advance()  # Consume MERGE_KEYS token

        # Check for optional opening parenthesis
        has_parentheses = False
        if self._check(TokenType.LEFT_PAREN):
            self._advance()  # Consume '(' token
            has_parentheses = True

        merge_keys = []

        # Parse comma-separated list of merge keys
        while (
            not self._check(TokenType.SEMICOLON)
            and not (has_parentheses and self._check(TokenType.RIGHT_PAREN))
            and not self._is_at_end()
        ):
            key_token = self._consume(
                TokenType.IDENTIFIER, "Expected column name for MERGE_KEYS"
            )
            merge_keys.append(key_token.value)

            # Check if there's a comma after the key
            if self._check(TokenType.COMMA):
                self._advance()  # Consume comma
            elif not self._check(TokenType.SEMICOLON) and not (
                has_parentheses and self._check(TokenType.RIGHT_PAREN)
            ):
                # If we don't see a semicolon or closing paren, we expect a comma
                token = self._peek()
                expected_tokens = "comma between merge keys"
                if has_parentheses:
                    expected_tokens += " or ')'"
                raise ParserError(
                    f"Expected {expected_tokens}",
                    token.line,
                    token.column,
                )

        # If we started with a parenthesis, we must close it
        if has_parentheses:
            self._consume(
                TokenType.RIGHT_PAREN,
                "Expected ')' to close merge keys list",
            )

        return merge_keys

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
            token = self._advance()
            sql_query_tokens.append(token)

        # Properly handle SQL query tokens (especially DOT tokens)
        sql_query = self._format_sql_query(sql_query_tokens)

        self._consume(TokenType.TO, "Expected 'TO' after SQL query")

        destination_uri_token = self._consume(
            TokenType.STRING, "Expected destination URI string after 'TO'"
        )
        destination_uri = destination_uri_token.value.strip('"')

        # Fix variable references in the destination URI
        destination_uri = self._fix_variable_references(destination_uri)

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
            token = self._advance()
            sql_query_tokens.append(token)

        # Properly handle SQL query tokens (especially DOT tokens)
        sql_query = self._format_sql_query(sql_query_tokens)

        self._consume(TokenType.SEMICOLON, "Expected ';' after SQL query")

        return SQLBlockStep(
            table_name=table_name_token.value,
            sql_query=sql_query,
            line_number=create_token.line,
        )

    def _format_sql_query(self, tokens) -> str:
        """Format SQL query tokens with proper handling of operators like DOT.

        This method ensures SQL table.column references are formatted correctly
        without spaces around the dot operator. This is critical for SQL syntax
        validity and prevents errors during execution.

        It also handles:
        - SQL function calls: no spaces between function name and opening parenthesis
        - Compound operators: >= <= != etc.
        - Variable references in different contexts

        Args:
            tokens: List of tokens or token values

        Returns:
            Properly formatted SQL query string
        """
        formatted_parts = []
        i = 0

        while i < len(tokens):
            current = tokens[i]

            # Handle first token (typically "SELECT")
            if i == 0 and isinstance(current, str):
                formatted_parts.append(current)
                i += 1
                continue

            # Get token value and type
            token_value = current.value if hasattr(current, "value") else str(current)
            token_type = current.type if hasattr(current, "type") else None

            # Handle compound operators
            if token_type in (
                TokenType.GREATER_EQUAL,
                TokenType.LESS_EQUAL,
                TokenType.NOT_EQUAL,
            ):
                # Remove any trailing space from previous token
                if formatted_parts:
                    formatted_parts[-1] = formatted_parts[-1].rstrip()
                formatted_parts.append(token_value)
            # Handle dot operators by joining without spaces
            elif token_type == TokenType.DOT:
                # Append without space before
                formatted_parts[-1] = formatted_parts[-1].rstrip()
                formatted_parts.append(token_value)
            elif (
                i > 0
                and hasattr(tokens[i - 1], "type")
                and tokens[i - 1].type == TokenType.DOT
            ):
                # Append without space after
                formatted_parts.append(token_value)
            # Check for function calls (IDENTIFIER followed by LEFT_PAREN)
            elif token_type == TokenType.IDENTIFIER and i + 1 < len(tokens):
                next_token = tokens[i + 1]
                next_value = (
                    next_token.value
                    if hasattr(next_token, "value")
                    else str(next_token)
                )
                if next_value == "(":
                    # This is likely a function call, don't add space after function name
                    formatted_parts.append(token_value)
                    # Don't add space between function name and opening parenthesis
                    i += 1  # Skip to the parenthesis
                    formatted_parts.append(next_value)
                else:
                    # Regular token with space
                    formatted_parts.append(token_value)
            else:
                # Regular token with space
                formatted_parts.append(token_value)

            i += 1

        # Join parts, then normalize whitespace
        raw_sql = " ".join(formatted_parts)

        # Replace any remaining spaces around dots
        sql = raw_sql.replace(" . ", ".").replace(" .", ".").replace(". ", ".")

        # Fix any remaining function call spacing issues
        # Match common SQL functions followed by space and parenthesis
        sql_functions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT"]
        for func in sql_functions:
            sql = re.sub(rf"{func}\s+\(", f"{func}(", sql, flags=re.IGNORECASE)

        # Fix spaces between opening parenthesis and content, and between content and closing parenthesis
        sql = re.sub(r"\(\s+", "(", sql)  # Remove space after opening parenthesis
        sql = re.sub(r"\s+\)", ")", sql)  # Remove space before closing parenthesis

        # Fix variable references in SQL context
        sql = self._fix_sql_variable_references(sql)

        # Normalize whitespace
        return " ".join(sql.split())

    def _fix_sql_variable_references(self, sql: str) -> str:
        """Fix variable references in SQL context.

        This method handles variable references differently based on their context:
        - String comparisons: Variables should be quoted ('${var}')
        - Numeric comparisons: Variables should not be quoted (${var})

        Args:
            sql: SQL query containing variable references

        Returns:
            SQL query with properly formatted variable references
        """
        # First fix any spacing issues in variable references
        sql = self._fix_variable_references(sql)

        # Find all variable references
        var_pattern = r"\$\{[^}]+\}"
        var_matches = list(re.finditer(var_pattern, sql))

        # Track positions to avoid modifying the same region twice
        modifications = []

        # Track BETWEEN clauses to handle both parts
        between_clauses = []

        # First pass - identify BETWEEN clauses
        for i, match in enumerate(var_matches):
            var_ref = match.group(0)
            start_pos = match.start()
            end_pos = match.end()

            # Look before and after the variable reference
            before_context = sql[:start_pos].rstrip()
            after_context = sql[end_pos:].lstrip()

            # Check for BETWEEN operator
            if "BETWEEN" in before_context[-20:]:  # First part of BETWEEN
                # Look for the AND part
                for j in range(i + 1, len(var_matches)):
                    next_match = var_matches[j]
                    between_text = sql[end_pos : next_match.start()]
                    if "AND" in between_text:
                        between_clauses.append((match, next_match))
                        break

        # Second pass - handle all variables
        for i, match in enumerate(var_matches):
            var_ref = match.group(0)
            start_pos = match.start()
            end_pos = match.end()

            # Skip if this is part of a BETWEEN clause
            is_between_var = any(
                match in (between_pair[0], between_pair[1])
                for between_pair in between_clauses
            )
            if is_between_var:
                continue

            # Look before and after the variable reference
            before_context = sql[:start_pos].rstrip()
            after_context = sql[end_pos:].lstrip()

            # Check if this is a numeric comparison
            numeric_operators = [">=", "<=", ">", "<", "BETWEEN", "IN"]
            is_numeric = any(
                before_context.endswith(f" {op} ")
                or before_context.endswith(f" {op}")
                or after_context.startswith(f" {op} ")
                or after_context.startswith(f" {op}")
                for op in numeric_operators
            )

            # Also check if it's part of a numeric function or calculation
            numeric_functions = ["SUM", "AVG", "COUNT", "MIN", "MAX"]
            is_numeric = is_numeric or any(
                f"{func}(" in before_context for func in numeric_functions
            )

            # Check for arithmetic operators
            arithmetic_operators = ["+", "-", "*", "/", "%"]
            is_numeric = is_numeric or any(
                op in before_context[-2:] or op in after_context[:2]
                for op in arithmetic_operators
            )

            if not is_numeric:
                # For string comparisons, add quotes if not already quoted
                if not (before_context.endswith("'") and after_context.startswith("'")):
                    modifications.append((start_pos, end_pos, f"'{var_ref}'"))

        # Apply modifications in reverse order to maintain correct positions
        for start_pos, end_pos, replacement in sorted(modifications, reverse=True):
            sql = sql[:start_pos] + replacement + sql[end_pos:]

        return sql

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
            ConditionalBlockStep

        Raises:
            ParserError: If the conditional block cannot be parsed
        """
        logger.debug("Parsing conditional block")
        start_line = self._peek().line
        branches = []
        else_branch = None

        # Parse initial IF branch
        self._consume(TokenType.IF, "Expected 'IF'")
        condition = self._parse_condition_expression()
        logger.debug(f"Parsed IF condition: {condition}")
        self._consume(TokenType.THEN, "Expected 'THEN' after condition")
        if_branch_steps = self._parse_branch_statements(
            [TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF]
        )
        branches.append(ConditionalBranchStep(condition, if_branch_steps, start_line))

        # Parse ELSEIF branches
        while self._check(TokenType.ELSE_IF):
            elseif_line = self._peek().line
            self._consume(TokenType.ELSE_IF, "Expected 'ELSEIF'")
            condition = self._parse_condition_expression()
            logger.debug(f"Parsed ELSEIF condition: {condition}")
            self._consume(TokenType.THEN, "Expected 'THEN' after condition")
            elseif_branch_steps = self._parse_branch_statements(
                [TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF]
            )
            branches.append(
                ConditionalBranchStep(condition, elseif_branch_steps, elseif_line)
            )

        # Parse optional ELSE branch
        if self._check(TokenType.ELSE):
            logger.debug("Parsing ELSE branch")
            self._consume(TokenType.ELSE, "Expected 'ELSE'")
            else_branch = self._parse_branch_statements([TokenType.END_IF])

        # Consume END IF
        self._consume(TokenType.END_IF, "Expected 'END IF'")
        self._consume(TokenType.SEMICOLON, "Expected ';' after 'END IF'")

        logger.debug(
            f"Completed parsing conditional block with {len(branches)} branches, else_branch: {else_branch is not None}"
        )
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

        # Fix variable references
        condition = self._fix_variable_references(condition)

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

    def _fix_variable_references(self, text: str) -> str:
        """Fix variable references by removing spaces within ${} syntax.

        Converts '$ { var_name | default }' to '${var_name|default}'
        Also handles spaces around pipes:
        - '${var | default}' to '${var|default}'

        Args:
            text: Text containing variable references

        Returns:
            Text with properly formatted variable references
        """
        # Step 1: Fix the outer spaces - Replace ${ var_name } with ${var_name}
        fixed = re.sub(r"\$\s*{\s*([^}]+?)\s*}", r"${\1}", text)

        # Step 2: Find all variable references
        var_pattern = r"\$\{([^}]+)\}"
        var_matches = re.findall(var_pattern, fixed)

        # For each variable reference, fix internal formatting
        for var_expr in var_matches:
            old_var_expr = var_expr
            new_var_expr = var_expr

            # Fix spaces around pipes: var | default -> var|default
            if "|" in new_var_expr:
                new_var_expr = re.sub(r"\s*\|\s*", "|", new_var_expr)

            # Replace the old variable expression with the new one
            if old_var_expr != new_var_expr:
                fixed = fixed.replace(f"${{{old_var_expr}}}", f"${{{new_var_expr}}}")

        return fixed
