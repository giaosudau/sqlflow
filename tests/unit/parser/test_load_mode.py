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
    assert not pipeline.steps[0].merge_keys


def test_load_with_replace_mode():
    """Test LOAD with MODE REPLACE."""
    parser = Parser("LOAD users FROM users_source MODE REPLACE;")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "REPLACE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert not pipeline.steps[0].merge_keys


def test_load_with_append_mode():
    """Test LOAD with MODE APPEND."""
    parser = Parser("LOAD users FROM users_source MODE APPEND;")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "APPEND"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert not pipeline.steps[0].merge_keys


def test_load_with_merge_mode():
    """Test LOAD with MODE MERGE and MERGE_KEYS."""
    parser = Parser("LOAD users FROM users_source MODE MERGE MERGE_KEYS user_id;")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "MERGE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert pipeline.steps[0].merge_keys == ["user_id"]


def test_load_with_merge_mode_multiple_keys():
    """Test LOAD with MODE MERGE and multiple MERGE_KEYS."""
    parser = Parser(
        "LOAD users FROM users_source MODE MERGE MERGE_KEYS user_id, email;"
    )
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "MERGE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert pipeline.steps[0].merge_keys == ["user_id", "email"]


def test_load_with_merge_mode_without_keys():
    """Test LOAD with MODE MERGE but without MERGE_KEYS (should raise error)."""
    parser = Parser("LOAD users FROM users_source MODE MERGE;")

    with pytest.raises(ParserError) as excinfo:
        parser.parse(validate=False)

    assert "Expected 'MERGE_KEYS' after 'MERGE'" in str(excinfo.value)


def test_load_with_invalid_mode():
    """Test LOAD with invalid MODE (should raise error)."""
    parser = Parser("LOAD users FROM users_source MODE UPDATE;")

    with pytest.raises(ParserError) as excinfo:
        parser.parse(validate=False)

    assert "Expected 'REPLACE', 'APPEND', or 'MERGE' after 'MODE'" in str(excinfo.value)


def test_load_with_merge_mode_validation():
    """Test validation for LOAD with MODE MERGE without MERGE_KEYS."""
    # Create a LoadStep with MERGE mode but no merge_keys
    load_step = LoadStep(
        table_name="users",
        source_name="users_source",
        mode="MERGE",
        merge_keys=[],
        line_number=1,
    )

    # Validate should return an error
    errors = load_step.validate()
    assert "MERGE mode requires MERGE_KEYS to be specified" in errors


def test_load_with_merge_mode_parentheses():
    """Test LOAD with MODE MERGE and MERGE_KEYS with parentheses."""
    parser = Parser("LOAD users FROM users_source MODE MERGE MERGE_KEYS (user_id);")
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "MERGE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert pipeline.steps[0].merge_keys == ["user_id"]


def test_load_with_merge_mode_multiple_keys_parentheses():
    """Test LOAD with MODE MERGE and multiple MERGE_KEYS with parentheses."""
    parser = Parser(
        "LOAD users FROM users_source MODE MERGE MERGE_KEYS (user_id, email);"
    )
    pipeline = parser.parse(validate=False)

    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], LoadStep)
    assert pipeline.steps[0].mode == "MERGE"
    assert pipeline.steps[0].table_name == "users"
    assert pipeline.steps[0].source_name == "users_source"
    assert pipeline.steps[0].merge_keys == ["user_id", "email"]


def test_load_with_merge_mode_missing_closing_paren():
    """Test LOAD with MODE MERGE and MERGE_KEYS with missing closing parenthesis."""
    parser = Parser("LOAD users FROM users_source MODE MERGE MERGE_KEYS (user_id;")

    with pytest.raises(ParserError) as excinfo:
        parser.parse(validate=False)

    assert "Expected ')' to close merge keys list" in str(excinfo.value)
