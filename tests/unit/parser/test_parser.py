"""Tests for the SQLFlow parser."""

import json
import pytest

from sqlflow.sqlflow.parser.ast import Pipeline, SourceDefinitionStep, LoadStep
from sqlflow.sqlflow.parser.lexer import Lexer, TokenType
from sqlflow.sqlflow.parser.parser import Parser, ParserError


class TestLexer:
    """Test cases for the SQLFlow lexer."""

    def test_source_directive_tokens(self):
        """Test that the lexer correctly tokenizes a SOURCE directive."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        };"""
        
        lexer = Lexer(text)
        tokens = lexer.tokenize()
        
        tokens = [t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)]
        
        assert tokens[0].type == TokenType.SOURCE
        assert tokens[1].type == TokenType.IDENTIFIER
        assert tokens[1].value == "users"
        assert tokens[2].type == TokenType.TYPE
        assert tokens[3].type == TokenType.IDENTIFIER
        assert tokens[3].value == "POSTGRES"
        assert tokens[4].type == TokenType.PARAMS
        assert tokens[5].type == TokenType.JSON_OBJECT
        assert tokens[6].type == TokenType.SEMICOLON
        assert tokens[7].type == TokenType.EOF
        
    def test_load_directive_tokens(self):
        """Test that the lexer correctly tokenizes a LOAD directive."""
        text = """LOAD users_table FROM users;"""
        
        lexer = Lexer(text)
        tokens = lexer.tokenize()
        
        tokens = [t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)]
        
        assert tokens[0].type == TokenType.LOAD
        assert tokens[1].type == TokenType.IDENTIFIER
        assert tokens[1].value == "users_table"
        assert tokens[2].type == TokenType.FROM
        assert tokens[3].type == TokenType.IDENTIFIER
        assert tokens[3].value == "users"
        assert tokens[4].type == TokenType.SEMICOLON
        assert tokens[5].type == TokenType.EOF


class TestParser:
    """Test cases for the SQLFlow parser."""

    def test_parse_source_directive(self):
        """Test that the parser correctly parses a SOURCE directive."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        };"""
        
        parser = Parser(text)
        pipeline = parser.parse()
        
        assert len(pipeline.steps) == 1
        assert isinstance(pipeline.steps[0], SourceDefinitionStep)
        
        source_step = pipeline.steps[0]
        assert source_step.name == "users"
        assert source_step.connector_type == "POSTGRES"
        assert source_step.params["connection"] == "postgresql://user:pass@localhost:5432/db"
        assert source_step.params["table"] == "users"
        
    def test_parse_load_directive(self):
        """Test that the parser correctly parses a LOAD directive."""
        text = """LOAD users_table FROM users;"""
        
        parser = Parser(text)
        pipeline = parser.parse()
        
        assert len(pipeline.steps) == 1
        assert isinstance(pipeline.steps[0], LoadStep)
        
        load_step = pipeline.steps[0]
        assert load_step.table_name == "users_table"
        assert load_step.source_name == "users"

    def test_parse_multiple_source_directives(self):
        """Test that the parser correctly parses multiple SOURCE directives."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        };
        
        SOURCE sales TYPE CSV PARAMS {
            "path": "data/sales.csv",
            "has_header": true
        };"""
        
        parser = Parser(text)
        pipeline = parser.parse()
        
        assert len(pipeline.steps) == 2
        assert isinstance(pipeline.steps[0], SourceDefinitionStep)
        assert isinstance(pipeline.steps[1], SourceDefinitionStep)
        
        users_step = pipeline.steps[0]
        assert users_step.name == "users"
        assert users_step.connector_type == "POSTGRES"
        assert users_step.params["connection"] == "postgresql://user:pass@localhost:5432/db"
        assert users_step.params["table"] == "users"
        
        sales_step = pipeline.steps[1]
        assert sales_step.name == "sales"
        assert sales_step.connector_type == "CSV"
        assert sales_step.params["path"] == "data/sales.csv"
        assert sales_step.params["has_header"] is True
        
    def test_parse_source_and_load_directives(self):
        """Test that the parser correctly parses SOURCE and LOAD directives together."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        };
        
        LOAD users_table FROM users;"""
        
        parser = Parser(text)
        pipeline = parser.parse()
        
        assert len(pipeline.steps) == 2
        assert isinstance(pipeline.steps[0], SourceDefinitionStep)
        assert isinstance(pipeline.steps[1], LoadStep)
        
        source_step = pipeline.steps[0]
        assert source_step.name == "users"
        
        load_step = pipeline.steps[1]
        assert load_step.table_name == "users_table"
        assert load_step.source_name == "users"

    def test_parse_source_directive_missing_semicolon(self):
        """Test that the parser raises an error for a SOURCE directive missing a semicolon."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        }"""
        
        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse()
        
        assert "Expected ';' after SOURCE statement" in str(excinfo.value)
        
    def test_parse_load_directive_missing_semicolon(self):
        """Test that the parser raises an error for a LOAD directive missing a semicolon."""
        text = """LOAD users_table FROM users"""
        
        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse()
        
        assert "Expected ';' after LOAD statement" in str(excinfo.value)
        
    def test_parse_load_directive_missing_from(self):
        """Test that the parser raises an error for a LOAD directive missing FROM."""
        text = """LOAD users_table users;"""
        
        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse()
        
        assert "Expected 'FROM' after table name" in str(excinfo.value)

    def test_parse_source_directive_missing_type(self):
        """Test that the parser raises an error for a SOURCE directive missing a TYPE."""
        text = """SOURCE users PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        };"""
        
        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse()
        
        assert "Expected 'TYPE' after source name" in str(excinfo.value)

    def test_parse_source_directive_missing_params(self):
        """Test that the parser raises an error for a SOURCE directive missing PARAMS."""
        text = """SOURCE users TYPE POSTGRES;"""
        
        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse()
        
        assert "Expected 'PARAMS' after connector type" in str(excinfo.value)

    def test_parse_source_directive_invalid_json(self):
        """Test that the parser raises an error for a SOURCE directive with invalid JSON."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users",
        };"""
        
        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse()
        
        assert "Invalid JSON in PARAMS" in str(excinfo.value)

    def test_source_step_validation(self):
        """Test that SourceDefinitionStep validation works correctly."""
        valid_step = SourceDefinitionStep(
            name="users",
            connector_type="POSTGRES",
            params={"connection": "postgresql://user:pass@localhost:5432/db", "table": "users"},
            line_number=1
        )
        assert valid_step.validate() == []
        
        invalid_step1 = SourceDefinitionStep(
            name="",
            connector_type="POSTGRES",
            params={"connection": "postgresql://user:pass@localhost:5432/db", "table": "users"},
            line_number=1
        )
        assert "SOURCE directive requires a name" in invalid_step1.validate()
        
        invalid_step2 = SourceDefinitionStep(
            name="users",
            connector_type="",
            params={"connection": "postgresql://user:pass@localhost:5432/db", "table": "users"},
            line_number=1
        )
        assert "SOURCE directive requires a TYPE" in invalid_step2.validate()
        
        invalid_step3 = SourceDefinitionStep(
            name="users",
            connector_type="POSTGRES",
            params={},
            line_number=1
        )
        assert "SOURCE directive requires PARAMS" in invalid_step3.validate()
        
    def test_load_step_validation(self):
        """Test that LoadStep validation works correctly."""
        valid_step = LoadStep(
            table_name="users_table",
            source_name="users",
            line_number=1
        )
        assert valid_step.validate() == []
        
        invalid_step1 = LoadStep(
            table_name="",
            source_name="users",
            line_number=1
        )
        assert "LOAD directive requires a table name" in invalid_step1.validate()
        
        invalid_step2 = LoadStep(
            table_name="users_table",
            source_name="",
            line_number=1
        )
        assert "LOAD directive requires a source name" in invalid_step2.validate()
