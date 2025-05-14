"""Parser for SQLFlow DSL."""

import json
from typing import Dict, List, Optional, Any, Tuple, Union

from sqlflow.sqlflow.parser.ast import Pipeline, PipelineStep, SourceDefinitionStep
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
        
        name_token = self._consume(TokenType.IDENTIFIER, "Expected source name after 'SOURCE'")
        
        self._consume(TokenType.TYPE, "Expected 'TYPE' after source name")
        
        type_token = self._consume(TokenType.IDENTIFIER, "Expected connector type after 'TYPE'")
        
        self._consume(TokenType.PARAMS, "Expected 'PARAMS' after connector type")
        
        params_token = self._consume(TokenType.JSON_OBJECT, "Expected JSON object after 'PARAMS'")
        
        try:
            params = json.loads(params_token.value)
        except json.JSONDecodeError:
            raise ParserError("Invalid JSON in PARAMS", params_token.line, params_token.column)
        
        self._consume(TokenType.SEMICOLON, "Expected ';' after SOURCE statement")
        
        return SourceDefinitionStep(
            name=name_token.value,
            connector_type=type_token.value,
            params=params,
            line_number=source_token.line
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
            error_message: Error message if the token is not of the expected type
            
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
            ):
                return
                
            self._advance()
