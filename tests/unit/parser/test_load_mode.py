"""Tests for LOAD with MODE parameter in SQLFlow parser."""

import pytest

from sqlflow.parser.ast import LoadStep
from sqlflow.parser.parser import Parser, ParserError


def test_load_default_mode():
    """Test default mode (REPLACE) is used when MODE is not specified."""
    parser = Parser("LOAD users FROM users_source;")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "REPLACE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert not pipeline.steps[0].upsert_keys


def test_load_with_replace_mode():
    """Test LOAD with MODE REPLACE."""
    parser = Parser("LOAD users FROM users_source MODE REPLACE;")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "REPLACE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert not pipeline.steps[0].upsert_keys


def test_load_with_append_mode():
    """Test LOAD with MODE APPEND."""
    parser = Parser("LOAD users FROM users_source MODE APPEND;")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "APPEND"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert not pipeline.steps[0].upsert_keys


def test_load_with_upsert_mode():
    """Test LOAD with MODE UPSERT and KEY."""
    parser = Parser("LOAD users FROM users_source MODE UPSERT KEY user_id;")
    pipeline = parser.parse(validate=False)

    load_step = pipeline.steps[0]
    assert isinstance(load_step, LoadStep)
    assert load_step.mode == "UPSERT"
    assert load_step.upsert_keys == ["user_id"]


def test_load_with_upsert_mode_multiple_keys():
    """Test LOAD with MODE UPSERT and multiple KEY."""
    parser = Parser("LOAD users FROM users_source MODE UPSERT KEY user_id, email;")
    pipeline = parser.parse(validate=False)

    load_step = pipeline.steps[0]
    assert isinstance(load_step, LoadStep)
    assert load_step.mode == "UPSERT"
    assert load_step.upsert_keys == ["user_id", "email"]


def test_load_with_upsert_mode_without_keys():
    """Test LOAD with MODE UPSERT but without KEY (should raise error)."""
    parser = Parser("LOAD users FROM users_source MODE UPSERT;")

    with pytest.raises(ParserError) as excinfo:
        parser.parse(validate=False)

    # Should fail with appropriate error message about missing keys
    assert "KEY" in str(excinfo.value) or "upsert" in str(excinfo.value).lower()


def test_load_with_invalid_mode():
    """Test LOAD with invalid MODE (should raise error)."""
    parser = Parser("LOAD users FROM users_source MODE UPDATE;")

    with pytest.raises(ParserError) as excinfo:
        parser.parse(validate=False)

    assert "Expected mode type (REPLACE, APPEND, UPSERT) after MODE" in str(
        excinfo.value
    )


def test_load_with_upsert_mode_validation():
    """Test validation for LOAD with MODE UPSERT without KEY."""
    # Create a LoadStep with UPSERT mode but no upsert_keys
    load_step = LoadStep(
        table_name="users",
        source_name="users_source",
        mode="UPSERT",
        upsert_keys=[],
        line_number=1,
    )

    # Validate should return an error
    errors = load_step.validate()
    assert "UPSERT mode requires KEY to be specified" in errors


def test_load_with_upsert_mode_parentheses():
    """Test LOAD with MODE UPSERT and KEY with parentheses."""
    parser = Parser("LOAD users FROM users_source MODE UPSERT KEY (user_id);")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "UPSERT"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert pipeline.steps[0].upsert_keys == ["user_id"]


def test_load_with_upsert_mode_multiple_keys_parentheses():
    """Test LOAD with MODE UPSERT and multiple KEY with parentheses."""
    parser = Parser("LOAD users FROM users_source MODE UPSERT KEY (user_id, email);")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "UPSERT"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert pipeline.steps[0].upsert_keys == ["user_id", "email"]


def test_load_with_upsert_mode_missing_closing_paren():
    """Test LOAD with MODE UPSERT and KEY with missing closing parenthesis."""
    parser = Parser("LOAD users FROM users_source MODE UPSERT KEY (user_id;")

    with pytest.raises(ParserError) as excinfo:
        parser.parse(validate=False)

    # The error might include "Multiple errors found:" prefix
    error_msg = str(excinfo.value)
    assert (
        "Expected ')' after upsert keys" in error_msg
        or "Expected ',' or ')' in upsert keys list" in error_msg
    )
