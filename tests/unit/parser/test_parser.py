"""Tests for the SQLFlow parser."""

import pytest

from sqlflow.parser.ast import ExportStep, LoadStep, SourceDefinitionStep
from sqlflow.parser.lexer import Lexer, TokenType
from sqlflow.parser.parser import Parser, ParserError


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

        tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
        ]

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

        tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
        ]

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
        pipeline = parser.parse(validate=False)

        assert len(pipeline.steps) == 1
        assert isinstance(pipeline.steps[0], SourceDefinitionStep)

        source_step = pipeline.steps[0]
        assert source_step.name == "users"
        assert source_step.connector_type == "POSTGRES"
        assert (
            source_step.params["connection"]
            == "postgresql://user:pass@localhost:5432/db"
        )
        assert source_step.params["table"] == "users"

    def test_parse_load_directive(self):
        """Test that the parser correctly parses a LOAD directive."""
        text = """LOAD users_table FROM users;"""

        parser = Parser(text)
        pipeline = parser.parse(validate=False)

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
        pipeline = parser.parse(validate=False)

        assert len(pipeline.steps) == 2
        assert isinstance(pipeline.steps[0], SourceDefinitionStep)
        assert isinstance(pipeline.steps[1], SourceDefinitionStep)

        users_step = pipeline.steps[0]
        assert users_step.name == "users"
        assert users_step.connector_type == "POSTGRES"
        assert (
            users_step.params["connection"]
            == "postgresql://user:pass@localhost:5432/db"
        )
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
        pipeline = parser.parse(validate=False)

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
            parser.parse(validate=False)

        assert "Expected ';' after SOURCE statement" in str(excinfo.value)

    def test_parse_load_directive_missing_semicolon(self):
        """Test that the parser can handle a LOAD directive without a semicolon."""
        text = """LOAD users_table FROM users"""

        parser = Parser(text)
        # This should parse successfully now (semicolons are optional for simple statements)
        pipeline = parser.parse(validate=False)

        assert len(pipeline.steps) == 1
        from sqlflow.parser.ast import LoadStep

        assert isinstance(pipeline.steps[0], LoadStep)
        assert pipeline.steps[0].table_name == "users_table"
        assert pipeline.steps[0].source_name == "users"

    def test_parse_load_directive_missing_from(self):
        """Test that the parser raises an error for a LOAD directive missing FROM."""
        text = """LOAD users_table users;"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        # Check for the error message, accounting for "Multiple errors found:" prefix
        error_msg = str(excinfo.value)
        assert "Expected FROM after table name" in error_msg

    def test_parse_source_directive_missing_type(self):
        """Test that the parser raises an error for a SOURCE directive missing a TYPE."""
        text = """SOURCE users PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        error_message = str(excinfo.value)
        assert "Expected FROM or TYPE after source name" in error_message

    def test_parse_source_directive_missing_params(self):
        """Test that the parser raises an error for a SOURCE directive missing PARAMS."""
        text = """SOURCE users TYPE POSTGRES;"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'PARAMS' after connector type" in str(excinfo.value)

    def test_parse_source_directive_invalid_json(self):
        """Test that the parser raises an error for a SOURCE directive with invalid JSON."""
        text = """SOURCE users TYPE POSTGRES PARAMS {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users",,  # Double comma makes this invalid
            "invalid_syntax": : ,
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        error_message = str(excinfo.value)
        assert (
            "Invalid JSON in PARAMS" in error_message
            or "Expected JSON object" in error_message
        )

    def test_source_step_validation(self):
        """Test that SourceDefinitionStep validation works correctly."""
        valid_step = SourceDefinitionStep(
            name="users",
            connector_type="POSTGRES",
            params={
                "connection": "postgresql://user:pass@localhost:5432/db",
                "table": "users",
            },
            line_number=1,
        )
        assert valid_step.validate() == []

        invalid_step1 = SourceDefinitionStep(
            name="",
            connector_type="POSTGRES",
            params={
                "connection": "postgresql://user:pass@localhost:5432/db",
                "table": "users",
            },
            line_number=1,
        )
        assert "SOURCE directive requires a name" in invalid_step1.validate()

        invalid_step2 = SourceDefinitionStep(
            name="users",
            connector_type="",
            params={
                "connection": "postgresql://user:pass@localhost:5432/db",
                "table": "users",
            },
            line_number=1,
        )
        errors = invalid_step2.validate()
        assert any(
            "SOURCE directive with TYPE syntax requires a connector type" in error
            for error in errors
        )

        invalid_step3 = SourceDefinitionStep(
            name="users", connector_type="POSTGRES", params={}, line_number=1
        )
        errors = invalid_step3.validate()
        assert any(
            "SOURCE directive with TYPE syntax requires PARAMS" in error
            for error in errors
        )

    def test_load_step_validation(self):
        """Test that LoadStep validation works correctly."""
        valid_step = LoadStep(
            table_name="users_table", source_name="users", line_number=1
        )
        assert valid_step.validate() == []

        invalid_step1 = LoadStep(table_name="", source_name="users", line_number=1)
        assert "LOAD directive requires a table name" in invalid_step1.validate()

        invalid_step2 = LoadStep(
            table_name="users_table", source_name="", line_number=1
        )
        assert "LOAD directive requires a source name" in invalid_step2.validate()

    def test_export_directive_tokens(self):
        """Test that the lexer correctly tokenizes an EXPORT directive."""
        text = """EXPORT
            SELECT * FROM users
        TO "s3://bucket/users.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true
        };"""

        lexer = Lexer(text)
        tokens = lexer.tokenize()

        tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
        ]

        assert tokens[0].type == TokenType.EXPORT
        assert tokens[1].type == TokenType.SELECT
        assert tokens[5].type == TokenType.TO
        assert tokens[6].type == TokenType.STRING
        assert tokens[7].type == TokenType.TYPE
        assert tokens[8].type == TokenType.IDENTIFIER
        assert tokens[9].type == TokenType.OPTIONS
        assert tokens[10].type == TokenType.JSON_OBJECT
        assert tokens[11].type == TokenType.SEMICOLON

    def test_parse_export_directive(self):
        """Test that the parser correctly parses an EXPORT directive."""
        text = """EXPORT
            SELECT * FROM users
        TO "s3://bucket/users.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true
        };"""

        parser = Parser(text)
        pipeline = parser.parse(validate=False)

        assert len(pipeline.steps) == 1
        assert isinstance(pipeline.steps[0], ExportStep)

        export_step = pipeline.steps[0]
        assert export_step.sql_query == "SELECT * FROM users"
        assert export_step.destination_uri == "s3://bucket/users.csv"
        assert export_step.connector_type == "CSV"
        assert export_step.options["delimiter"] == ","
        assert export_step.options["header"] is True

    def test_parse_export_directive_missing_options(self):
        """Test that the parser raises an error for an EXPORT directive missing OPTIONS."""
        text = """EXPORT
            SELECT * FROM users
        TO "s3://bucket/users.csv"
        TYPE CSV;"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'OPTIONS' after connector type" in str(excinfo.value)

    def test_parse_export_directive_missing_type(self):
        """Test that the parser raises an error for an EXPORT directive missing TYPE."""
        text = """EXPORT
            SELECT * FROM users
        TO "s3://bucket/users.csv"
        OPTIONS {
            "delimiter": ",",
            "header": true
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'TYPE' after destination URI" in str(excinfo.value)

    def test_parse_export_directive_missing_to(self):
        """Test that the parser raises an error for an EXPORT directive missing TO."""
        text = """EXPORT
            SELECT * FROM users
        "s3://bucket/users.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'TO' after SQL query" in str(excinfo.value)

    def test_parse_export_directive_invalid_json(self):
        """Test that the parser raises an error for an EXPORT directive with invalid JSON."""
        text = """EXPORT
            SELECT * FROM users
        TO "s3://bucket/users.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true,,  # Double comma makes this invalid 
            "invalid": : 
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        error_message = str(excinfo.value)
        assert (
            "Invalid JSON in OPTIONS" in error_message
            or "Expected JSON object" in error_message
        )

    def test_export_step_validation(self):
        """Test that ExportStep validation works correctly."""
        valid_step = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="s3://bucket/users.csv",
            connector_type="CSV",
            options={"delimiter": ",", "header": True},
            line_number=1,
        )
        assert valid_step.validate() == []

        invalid_step1 = ExportStep(
            sql_query="",
            destination_uri="s3://bucket/users.csv",
            connector_type="CSV",
            options={"delimiter": ",", "header": True},
            line_number=1,
        )
        assert "EXPORT directive requires a SQL query" in invalid_step1.validate()

        invalid_step2 = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="",
            connector_type="CSV",
            options={"delimiter": ",", "header": True},
            line_number=1,
        )
        assert "EXPORT directive requires a destination URI" in invalid_step2.validate()

        invalid_step3 = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="s3://bucket/users.csv",
            connector_type="",
            options={"delimiter": ",", "header": True},
            line_number=1,
        )
        assert "EXPORT directive requires a connector TYPE" in invalid_step3.validate()

        invalid_step4 = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="s3://bucket/users.csv",
            connector_type="CSV",
            options={},
            line_number=1,
        )
        assert "EXPORT directive requires OPTIONS" in invalid_step4.validate()

    def test_source_syntax_error_handling(self):
        """Test that the parser correctly handles invalid SOURCE syntax patterns."""
        # Test mixing FROM and TYPE keywords
        text = """SOURCE users FROM "postgres" TYPE POSTGRES OPTIONS {
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Cannot mix FROM and TYPE keywords" in str(excinfo.value)

        # Test TYPE followed by FROM
        text = """SOURCE users TYPE POSTGRES FROM "postgres" PARAMS {
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Cannot mix TYPE and FROM keywords" in str(excinfo.value)

        # Test missing OPTIONS after FROM
        text = """SOURCE users FROM "postgres" {
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'OPTIONS' after connector name" in str(excinfo.value)

        # Test missing PARAMS after TYPE
        text = """SOURCE users TYPE POSTGRES {
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'PARAMS' after connector type" in str(excinfo.value)

        # Test using OPTIONS with TYPE syntax
        text = """SOURCE users TYPE POSTGRES OPTIONS {
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Cannot use OPTIONS with TYPE-based syntax" in str(excinfo.value)

        # Test using PARAMS with FROM syntax
        text = """SOURCE users FROM "postgres" PARAMS {
            "table": "users"
        };"""

        parser = Parser(text)
        with pytest.raises(ParserError) as excinfo:
            parser.parse(validate=False)

        assert "Expected 'OPTIONS' after connector name" in str(excinfo.value)
