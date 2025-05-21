"""Tests for improved SOURCE statement error handling."""

import pytest

from sqlflow.parser.ast import SourceDefinitionStep
from sqlflow.parser.parser import Parser, ParserError


def test_source_from_missing_options():
    """Test error when OPTIONS is missing from FROM-based SOURCE statement."""
    pipeline_text = 'SOURCE test_source FROM "postgres";'

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Expected 'OPTIONS'" in error_message
    assert 'SOURCE name FROM "connector_name" OPTIONS' in error_message


def test_source_type_missing_params():
    """Test error when PARAMS is missing from TYPE-based SOURCE statement."""
    pipeline_text = "SOURCE test_source TYPE POSTGRES;"

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Expected 'PARAMS'" in error_message
    assert "SOURCE name TYPE connector_type PARAMS" in error_message


def test_source_missing_from_or_type():
    """Test error when neither FROM nor TYPE is provided after source name."""
    pipeline_text = "SOURCE test_source;"

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Expected FROM or TYPE" in error_message
    assert "SOURCE name FROM" in error_message
    assert "SOURCE name TYPE" in error_message


def test_source_mixing_from_type():
    """Test error when FROM and TYPE syntaxes are mixed."""
    pipeline_text = 'SOURCE test_source FROM "postgres" TYPE POSTGRES OPTIONS {};'

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Cannot mix FROM and TYPE keywords" in error_message


def test_source_mixing_type_from():
    """Test error when TYPE and FROM syntaxes are mixed."""
    pipeline_text = 'SOURCE test_source TYPE POSTGRES FROM "postgres" PARAMS {};'

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Cannot mix TYPE and FROM keywords" in error_message


def test_source_from_with_params():
    """Test error when PARAMS is used with FROM-based syntax."""
    pipeline_text = 'SOURCE test_source FROM "postgres" OPTIONS {} PARAMS {};'

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Cannot use PARAMS with FROM-based syntax" in error_message


def test_source_type_with_options():
    """Test error when OPTIONS is used with TYPE-based syntax."""
    pipeline_text = "SOURCE test_source TYPE POSTGRES OPTIONS {};"

    parser = Parser()
    with pytest.raises(ParserError) as excinfo:
        parser.parse(pipeline_text)

    error_message = str(excinfo.value)
    assert "Cannot use OPTIONS with TYPE-based syntax" in error_message
    assert "SOURCE name TYPE connector_type PARAMS" in error_message


def test_sourcedef_step_validation():
    """Test validation in SourceDefinitionStep class."""
    # Test mixing of FROM and TYPE patterns
    step = SourceDefinitionStep(
        name="test",
        connector_type="POSTGRES",
        params={},
        is_from_profile=True,
        profile_connector_name="postgres",
    )
    errors = step.validate()
    assert any("Invalid SOURCE syntax" in error for error in errors)

    # Test missing connector type with TYPE syntax
    step = SourceDefinitionStep(
        name="test",
        connector_type="",
        params={},
        is_from_profile=False,
    )
    errors = step.validate()
    assert any("requires a connector type" in error for error in errors)

    # Test missing PARAMS with TYPE syntax
    step = SourceDefinitionStep(
        name="test",
        connector_type="POSTGRES",
        params={},
        is_from_profile=False,
    )
    errors = step.validate()
    assert any("requires PARAMS" in error for error in errors)

    # Test missing connector name with FROM syntax
    step = SourceDefinitionStep(
        name="test",
        connector_type="",
        params={},
        is_from_profile=True,
        profile_connector_name=None,
    )
    errors = step.validate()
    assert any("requires a connector name in quotes" in error for error in errors)

    # Test OPTIONS with TYPE syntax (shouldn't have OPTIONS as a param key)
    step = SourceDefinitionStep(
        name="test",
        connector_type="POSTGRES",
        params={"OPTIONS": {}},
        is_from_profile=False,
    )
    errors = step.validate()
    assert any("Cannot use OPTIONS with TYPE-based syntax" in error for error in errors)

    # Test PARAMS with FROM syntax (shouldn't have PARAMS as a param key)
    step = SourceDefinitionStep(
        name="test",
        connector_type="",
        params={"PARAMS": {}},
        is_from_profile=True,
        profile_connector_name="postgres",
    )
    errors = step.validate()
    assert any("Cannot use PARAMS with FROM-based syntax" in error for error in errors)
