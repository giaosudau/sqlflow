"""Tests for INCLUDE and SET directives in the SQLFlow parser."""

import pytest

from sqlflow.sqlflow.parser.ast import IncludeStep, SetStep
from sqlflow.sqlflow.parser.parser import Parser, ParserError


def test_include_directive_valid():
    """Test parsing a valid INCLUDE directive."""
    input_text = 'INCLUDE "common/utils.sf" AS utils;'

    parser = Parser(input_text)
    pipeline = parser.parse()

    assert len(pipeline.steps) == 1
    step = pipeline.steps[0]
    assert isinstance(step, IncludeStep)
    assert step.file_path == "common/utils.sf"
    assert step.alias == "utils"
    assert step.validate() == []


def test_include_directive_missing_as():
    """Test parsing an INCLUDE directive with missing AS keyword."""
    input_text = 'INCLUDE "common/utils.sf" utils;'

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected 'AS'" in str(excinfo.value)


def test_include_directive_missing_semicolon():
    """Test parsing an INCLUDE directive with missing semicolon."""
    input_text = 'INCLUDE "common/utils.sf" AS utils'

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected ';'" in str(excinfo.value)


def test_include_directive_missing_file_path():
    """Test parsing an INCLUDE directive with missing file path."""
    input_text = "INCLUDE AS utils;"

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected file path string" in str(excinfo.value)


def test_include_directive_missing_alias():
    """Test parsing an INCLUDE directive with missing alias."""
    input_text = 'INCLUDE "common/utils.sf" AS;'

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected alias" in str(excinfo.value)


def test_include_directive_validation():
    """Test validation of IncludeStep."""
    step = IncludeStep(file_path="", alias="utils", line_number=1)

    errors = step.validate()

    assert len(errors) == 2
    assert "INCLUDE directive requires a file path" in errors
    assert "INCLUDE file path must have an extension" in errors

    step = IncludeStep(file_path="common/utils.sf", alias="", line_number=1)

    errors = step.validate()

    assert len(errors) == 1
    assert "INCLUDE directive requires an alias (AS keyword)" in errors


def test_set_directive_valid():
    """Test parsing a valid SET directive."""
    input_text = 'SET table_name = "users";'

    parser = Parser(input_text)
    pipeline = parser.parse()

    assert len(pipeline.steps) == 1
    step = pipeline.steps[0]
    assert isinstance(step, SetStep)
    assert step.variable_name == "table_name"
    assert step.variable_value == "users"
    assert step.validate() == []


def test_set_directive_with_number():
    """Test parsing a SET directive with a number value."""
    input_text = "SET max_rows = 1000;"

    parser = Parser(input_text)
    pipeline = parser.parse()

    assert len(pipeline.steps) == 1
    step = pipeline.steps[0]
    assert isinstance(step, SetStep)
    assert step.variable_name == "max_rows"
    assert step.variable_value == "1000"
    assert step.validate() == []


def test_set_directive_with_identifier():
    """Test parsing a SET directive with an identifier value."""
    input_text = "SET source_table = users;"

    parser = Parser(input_text)
    pipeline = parser.parse()

    assert len(pipeline.steps) == 1
    step = pipeline.steps[0]
    assert isinstance(step, SetStep)
    assert step.variable_name == "source_table"
    assert step.variable_value == "users"
    assert step.validate() == []


def test_set_directive_missing_equals():
    """Test parsing a SET directive with missing equals sign."""
    input_text = 'SET table_name "users";'

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected '='" in str(excinfo.value)


def test_set_directive_missing_value():
    """Test parsing a SET directive with missing value."""
    input_text = "SET table_name =;"

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected value" in str(excinfo.value)


def test_set_directive_missing_semicolon():
    """Test parsing a SET directive with missing semicolon."""
    input_text = 'SET table_name = "users"'

    parser = Parser(input_text)
    with pytest.raises(ParserError) as excinfo:
        parser.parse()

    assert "Expected ';'" in str(excinfo.value)


def test_set_directive_validation():
    """Test validation of SetStep."""
    step = SetStep(variable_name="", variable_value="users", line_number=1)

    errors = step.validate()

    assert len(errors) == 1
    assert "SET directive requires a variable name" in errors

    step = SetStep(variable_name="table_name", variable_value="", line_number=1)

    errors = step.validate()

    assert len(errors) == 1
    assert "SET directive requires a variable value" in errors


def test_multiple_directives():
    """Test parsing multiple directives including INCLUDE and SET."""
    input_text = """
    INCLUDE "common/utils.sf" AS utils;
    SET table_name = "users";
    """

    parser = Parser(input_text)
    pipeline = parser.parse()

    assert len(pipeline.steps) == 2

    include_step = pipeline.steps[0]
    assert isinstance(include_step, IncludeStep)
    assert include_step.file_path == "common/utils.sf"
    assert include_step.alias == "utils"

    set_step = pipeline.steps[1]
    assert isinstance(set_step, SetStep)
    assert set_step.variable_name == "table_name"
    assert set_step.variable_value == "users"
