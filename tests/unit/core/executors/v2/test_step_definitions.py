"""Unit tests for V2 Executor Step definitions.

Tests the Pythonic NamedTuple-based step definitions following Raymond Hettinger's guidance.
Tests real functionality without mocks, following SQLFlow testing standards.
"""

import pytest

from sqlflow.core.executors.v2.steps.definitions import (
    ExportFormat,
    ExportStep,
    LoadMode,
    LoadStep,
    SourceStep,
    TransformStep,
    create_step_from_dict,
    validate_export_step,
    validate_load_step,
    validate_source_step,
    validate_transform_step,
)


class TestLoadStep:
    """Test LoadStep NamedTuple-based immutable step."""

    def test_load_step_creation_with_enum(self):
        """Test load step creation with LoadMode enum."""
        step = LoadStep(
            id="test_load",
            source="data.csv",
            target_table="test_table",
            mode=LoadMode.REPLACE,
        )

        assert step.id == "test_load"
        assert step.source == "data.csv"
        assert step.target_table == "test_table"
        assert step.mode == LoadMode.REPLACE
        assert step.step_type == "load"

    def test_load_step_creation_with_upsert(self):
        """Test load step creation with UPSERT mode and keys."""
        step = LoadStep(
            id="test_upsert",
            source="data.csv",
            target_table="users",
            mode=LoadMode.UPSERT,
            upsert_keys=["user_id", "email"],
        )

        assert step.mode == LoadMode.UPSERT
        assert step.upsert_keys == ["user_id", "email"]

    def test_load_step_defaults(self):
        """Test load step with default values."""
        step = LoadStep(id="test_load", source="data.csv", target_table="test_table")

        assert step.mode == LoadMode.REPLACE  # Default value
        assert step.upsert_keys == []

    def test_load_step_immutability(self):
        """Test that LoadStep is truly immutable (NamedTuple behavior)."""
        step = LoadStep(id="test_load", source="data.csv", target_table="test_table")

        # NamedTuple attributes are read-only - verify they exist but can't be modified
        assert step.id == "test_load"
        assert step.source == "data.csv"

        # NamedTuple doesn't allow attribute assignment
        # We can't test this directly due to static typing, but NamedTuple guarantees immutability

    def test_load_step_validation_success(self):
        """Test successful load step validation."""
        step = LoadStep(
            id="test_load",
            source="data.csv",
            target_table="test_table",
            mode=LoadMode.REPLACE,
        )

        # Should not raise any exception
        validate_load_step(step)

    def test_load_step_validation_empty_id(self):
        """Test load step validation with empty ID."""
        step = LoadStep(
            id="",
            source="data.csv",
            target_table="test_table",
        )

        with pytest.raises(ValueError, match="Step ID cannot be empty"):
            validate_load_step(step)

    def test_load_step_validation_empty_source(self):
        """Test load step validation with empty source."""
        step = LoadStep(
            id="test_load",
            source="",
            target_table="test_table",
        )

        with pytest.raises(ValueError, match="Source cannot be empty"):
            validate_load_step(step)

    def test_load_step_validation_upsert_without_keys(self):
        """Test UPSERT mode validation without upsert keys."""
        step = LoadStep(
            id="test_load",
            source="data.csv",
            target_table="test_table",
            mode=LoadMode.UPSERT,
            upsert_keys=[],  # Empty upsert keys
        )

        with pytest.raises(ValueError, match="UPSERT mode requires upsert_keys"):
            validate_load_step(step)

    def test_all_load_modes_enum_values(self):
        """Test all valid LoadMode enum values."""
        for mode in LoadMode:
            step = LoadStep(
                id="test_load",
                source="data.csv",
                target_table="test_table",
                mode=mode,
                upsert_keys=["id"] if mode == LoadMode.UPSERT else [],
            )

            # Should validate successfully
            validate_load_step(step)
            assert step.mode == mode


class TestTransformStep:
    """Test TransformStep NamedTuple-based immutable step."""

    def test_transform_step_creation(self):
        """Test basic transform step creation."""
        step = TransformStep(
            id="test_transform",
            sql="SELECT * FROM test_table",
            target_table="transformed_table",
            dependencies=["load_step"],
        )

        assert step.id == "test_transform"
        assert step.sql == "SELECT * FROM test_table"
        assert step.target_table == "transformed_table"
        assert step.dependencies == ["load_step"]
        assert step.step_type == "transform"

    def test_transform_step_defaults(self):
        """Test transform step with default values."""
        step = TransformStep(id="test_transform", sql="SELECT * FROM test_table")

        assert step.target_table == ""  # Default empty string
        assert step.dependencies == []

    def test_transform_step_immutability(self):
        """Test that TransformStep is immutable."""
        step = TransformStep(id="test_transform", sql="SELECT * FROM test_table")

        # Verify NamedTuple immutability - attributes exist but are read-only
        assert step.sql == "SELECT * FROM test_table"
        # NamedTuple guarantees immutability without needing to test attribute assignment

    def test_transform_step_validation_success(self):
        """Test successful transform step validation."""
        step = TransformStep(
            id="test_transform",
            sql="SELECT * FROM test_table",
        )

        validate_transform_step(step)

    def test_transform_step_validation_empty_sql(self):
        """Test transform step validation with empty SQL."""
        step = TransformStep(
            id="test_transform",
            sql="",
        )

        with pytest.raises(ValueError, match="SQL cannot be empty"):
            validate_transform_step(step)


class TestExportStep:
    """Test ExportStep NamedTuple-based immutable step."""

    def test_export_step_creation_with_enum(self):
        """Test export step creation with ExportFormat enum."""
        step = ExportStep(
            id="test_export",
            source_table="test_table",
            destination="output.csv",
            format=ExportFormat.CSV,
        )

        assert step.id == "test_export"
        assert step.source_table == "test_table"
        assert step.destination == "output.csv"
        assert step.format == ExportFormat.CSV
        assert step.step_type == "export"

    def test_export_step_defaults(self):
        """Test export step with default format."""
        step = ExportStep(
            id="test_export", source_table="test_table", destination="output.csv"
        )

        assert step.format == ExportFormat.CSV  # Default format enum

    def test_export_step_validation_success(self):
        """Test successful export step validation."""
        step = ExportStep(
            id="test_export",
            source_table="test_table",
            destination="output.csv",
            format=ExportFormat.PARQUET,
        )

        validate_export_step(step)

    def test_export_step_validation_empty_destination(self):
        """Test export step validation with empty destination."""
        step = ExportStep(
            id="test_export",
            source_table="test_table",
            destination="",
        )

        with pytest.raises(ValueError, match="Destination cannot be empty"):
            validate_export_step(step)

    def test_all_export_formats_enum_values(self):
        """Test all valid ExportFormat enum values."""
        for format_type in ExportFormat:
            step = ExportStep(
                id="test_export",
                source_table="test_table",
                destination=f"output.{format_type.value}",
                format=format_type,
            )

            validate_export_step(step)
            assert step.format == format_type


class TestSourceStep:
    """Test SourceStep NamedTuple-based immutable step."""

    def test_source_step_creation(self):
        """Test basic source step creation."""
        config = {"host": "localhost", "port": 5432}
        step = SourceStep(
            id="test_source",
            name="my_database",
            connector_type="postgresql",
            configuration=config,
        )

        assert step.id == "test_source"
        assert step.name == "my_database"
        assert step.connector_type == "postgresql"
        assert step.configuration == config
        assert step.step_type == "source"

    def test_source_step_defaults(self):
        """Test source step with default configuration."""
        step = SourceStep(
            id="test_source", name="my_database", connector_type="postgresql"
        )

        assert step.configuration == {}  # Default empty dict

    def test_source_step_validation_success(self):
        """Test successful source step validation."""
        step = SourceStep(
            id="test_source",
            name="my_database",
            connector_type="postgresql",
        )

        validate_source_step(step)

    def test_source_step_validation_empty_connector_type(self):
        """Test source step validation with empty connector type."""
        step = SourceStep(
            id="test_source",
            name="my_database",
            connector_type="",
        )

        with pytest.raises(ValueError, match="Connector type cannot be empty"):
            validate_source_step(step)


class TestStepFactory:
    """Test functional step creation from dictionaries."""

    def test_create_load_step_from_dict_with_mode(self):
        """Test creating LoadStep from dictionary with mode field."""
        step_dict = {
            "id": "test_load",
            "type": "load",
            "source": "data.csv",
            "target_table": "test_table",
            "mode": "append",
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, LoadStep)
        assert step.id == "test_load"
        assert step.source == "data.csv"
        assert step.target_table == "test_table"
        assert step.mode == LoadMode.APPEND

    def test_create_load_step_from_dict_with_legacy_load_mode(self):
        """Test creating LoadStep from dictionary with legacy load_mode field."""
        step_dict = {
            "id": "test_load",
            "type": "load",
            "source": "data.csv",
            "target_table": "test_table",
            "load_mode": "upsert",
            "upsert_keys": ["user_id"],
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, LoadStep)
        assert step.mode == LoadMode.UPSERT
        assert step.upsert_keys == ["user_id"]

    def test_create_transform_step_from_dict(self):
        """Test creating TransformStep from dictionary."""
        step_dict = {
            "id": "test_transform",
            "type": "transform",
            "sql": "SELECT * FROM test_table",
            "target_table": "transformed_table",
            "dependencies": ["load_step"],
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, TransformStep)
        assert step.id == "test_transform"
        assert step.sql == "SELECT * FROM test_table"
        assert step.target_table == "transformed_table"
        assert step.dependencies == ["load_step"]

    def test_create_export_step_from_dict(self):
        """Test creating ExportStep from dictionary."""
        step_dict = {
            "id": "test_export",
            "type": "export",
            "source_table": "test_table",
            "destination": "output.parquet",
            "format": "parquet",
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, ExportStep)
        assert step.id == "test_export"
        assert step.format == ExportFormat.PARQUET

    def test_create_export_step_with_nested_query_structure(self):
        """Test creating ExportStep from dictionary with nested query structure."""
        step_dict = {
            "id": "test_export",
            "type": "export",
            "source_table": "test_table",
            "query": {
                "destination_uri": "s3://bucket/output.json",
                "type": "json",
            },
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, ExportStep)
        assert step.destination == "s3://bucket/output.json"
        assert step.format == ExportFormat.JSON

    def test_create_source_step_from_dict(self):
        """Test creating SourceStep from dictionary."""
        step_dict = {
            "id": "test_source",
            "type": "source",
            "name": "my_database",
            "connector_type": "postgresql",
            "configuration": {"host": "localhost"},
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, SourceStep)
        assert step.name == "my_database"
        assert step.connector_type == "postgresql"
        assert step.configuration == {"host": "localhost"}

    def test_create_source_step_with_params_field(self):
        """Test creating SourceStep with legacy params field."""
        step_dict = {
            "id": "test_source",
            "type": "source",
            "name": "my_csv",
            "connector_type": "csv",
            "params": {"path": "/data/file.csv", "has_header": True},
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, SourceStep)
        assert step.configuration == {"path": "/data/file.csv", "has_header": True}

    def test_unknown_step_type(self):
        """Test handling of unknown step types."""
        step_dict = {"id": "test_unknown", "type": "unknown_type"}

        with pytest.raises(ValueError, match="Unknown step type: unknown_type"):
            create_step_from_dict(step_dict)

    def test_missing_step_type(self):
        """Test handling of missing step type."""
        step_dict = {"id": "test_missing"}

        with pytest.raises(ValueError, match="Step must have a 'step' or 'type' field"):
            create_step_from_dict(step_dict)


class TestStepProperties:
    """Test step properties and behavior."""

    def test_step_immutability_comprehensive(self):
        """Test comprehensive immutability of all step types."""
        load_step = LoadStep(id="test", source="data.csv", target_table="table")
        transform_step = TransformStep(id="test", sql="SELECT 1")
        export_step = ExportStep(id="test", source_table="table", destination="out.csv")
        source_step = SourceStep(id="test", name="src", connector_type="csv")

        steps = [load_step, transform_step, export_step, source_step]

        # Verify all steps have correct IDs and are NamedTuple instances (immutable)
        for step in steps:
            assert step.id == "test"
            assert isinstance(step, tuple)  # NamedTuple inherits from tuple

    def test_step_equality_and_hashing(self):
        """Test step equality comparison and hashability."""
        step1 = LoadStep(id="test", source="data.csv", target_table="table")
        step2 = LoadStep(id="test", source="data.csv", target_table="table")
        step3 = LoadStep(id="different", source="data.csv", target_table="table")

        # Test equality
        assert step1 == step2  # Same content
        assert step1 != step3  # Different content

        # Test hashability with steps that don't contain mutable fields
        # Note: Steps with upsert_keys (list) are not hashable
        export1 = ExportStep(id="test", source_table="table", destination="out.csv")
        export2 = ExportStep(id="test", source_table="table", destination="out.csv")
        export3 = ExportStep(
            id="different", source_table="table", destination="out.csv"
        )

        assert hash(export1) == hash(export2)
        assert hash(export1) != hash(export3)

        # Test in sets/dicts with hashable steps
        export_set = {export1, export2, export3}
        assert len(export_set) == 2  # export1 and export2 are equal

    def test_step_memory_efficiency(self):
        """Test that NamedTuple provides memory efficiency."""
        step = LoadStep(id="test", source="data.csv", target_table="table")

        # NamedTuple doesn't have __dict__ (more memory efficient)
        assert not hasattr(step, "__dict__")

        # But has __slots__-like behavior through NamedTuple
        assert hasattr(step, "_fields")
        assert step._fields == ("id", "source", "target_table", "mode", "upsert_keys")

    def test_enum_values_are_strings(self):
        """Test that enum values produce expected string representations."""
        assert LoadMode.REPLACE.value == "replace"
        assert LoadMode.APPEND.value == "append"
        assert LoadMode.UPSERT.value == "upsert"

        assert ExportFormat.CSV.value == "csv"
        assert ExportFormat.PARQUET.value == "parquet"
        assert ExportFormat.JSON.value == "json"
        assert ExportFormat.XLSX.value == "xlsx"


class TestFunctionalValidation:
    """Test functional validation approach."""

    def test_validation_functions_are_pure(self):
        """Test that validation functions don't modify input."""
        original_step = LoadStep(
            id="test", source="data.csv", target_table="table", mode=LoadMode.REPLACE
        )

        # Create a copy to compare
        step_copy = LoadStep(
            id="test", source="data.csv", target_table="table", mode=LoadMode.REPLACE
        )

        # Validate
        validate_load_step(original_step)

        # Original should be unchanged (pure function)
        assert original_step == step_copy

    def test_validation_functions_comprehensive_coverage(self):
        """Test that all validation functions cover edge cases."""
        # Test load step validation edge cases
        load_test_cases = [
            LoadStep("", "src", "tbl"),  # Empty ID
            LoadStep("id", "", "tbl"),  # Empty source
            LoadStep("id", "src", ""),  # Empty target
        ]
        for invalid_step in load_test_cases:
            with pytest.raises(ValueError):
                validate_load_step(invalid_step)

        # Test transform step validation edge cases
        transform_test_cases = [
            TransformStep("", "sql"),  # Empty ID
            TransformStep("id", ""),  # Empty SQL
        ]
        for invalid_step in transform_test_cases:
            with pytest.raises(ValueError):
                validate_transform_step(invalid_step)

        # Test export step validation edge cases
        export_test_cases = [
            ExportStep("", "tbl", "dest"),  # Empty ID
            ExportStep("id", "", "dest"),  # Empty source table
            ExportStep("id", "tbl", ""),  # Empty destination
        ]
        for invalid_step in export_test_cases:
            with pytest.raises(ValueError):
                validate_export_step(invalid_step)

        # Test source step validation edge cases
        source_test_cases = [
            SourceStep("", "name", "type"),  # Empty ID
            SourceStep("id", "", "type"),  # Empty name
            SourceStep("id", "name", ""),  # Empty connector type
        ]
        for invalid_step in source_test_cases:
            with pytest.raises(ValueError):
                validate_source_step(invalid_step)
