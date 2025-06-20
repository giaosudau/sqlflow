"""Unit tests for V2 Executor Step data classes."""

from datetime import datetime

import pytest

from sqlflow.core.executors.v2.steps import (
    BaseStep,
    ExportStep,
    LoadStep,
    SetVariableStep,
    SourceDefinitionStep,
    TransformStep,
    create_step_from_dict,
)


class TestBaseStep:
    """Test BaseStep functionality."""

    def test_base_step_creation(self):
        """Test basic step creation."""
        step = BaseStep(
            id="test_step",
            type="test",
            depends_on=["step1", "step2"],
            expected_duration_ms=1000.0,
            criticality="high",
        )

        assert step.id == "test_step"
        assert step.type == "test"
        assert step.depends_on == ["step1", "step2"]
        assert step.expected_duration_ms == 1000.0
        assert step.criticality == "high"
        assert isinstance(step.created_at, datetime)
        assert step.metadata == {}
        assert step.retry_policy is None

    def test_base_step_defaults(self):
        """Test step creation with defaults."""
        step = BaseStep(id="test", type="test")

        assert step.depends_on == []
        assert step.expected_duration_ms is None
        assert step.criticality == "normal"
        assert step.retry_policy is None
        assert step.metadata == {}

    def test_base_step_validation(self):
        """Test step validation."""
        # Empty ID should raise ValueError
        with pytest.raises(ValueError, match="Step ID cannot be empty"):
            BaseStep(id="", type="test")

        # Empty type should raise ValueError
        with pytest.raises(ValueError, match="Step type cannot be empty"):
            BaseStep(id="test", type="")

        # Negative duration should raise ValueError
        with pytest.raises(ValueError, match="Expected duration must be non-negative"):
            BaseStep(id="test", type="test", expected_duration_ms=-100)

    def test_base_step_immutability(self):
        """Test that BaseStep is immutable."""
        step = BaseStep(id="test", type="test")

        # Should not be able to modify attributes
        with pytest.raises(AttributeError):
            step.id = "new_id"

        with pytest.raises(AttributeError):
            step.type = "new_type"


class TestLoadStep:
    """Test LoadStep functionality."""

    def test_load_step_creation(self):
        """Test LoadStep creation."""
        step = LoadStep(
            id="load_test",
            source="source_table",
            target_table="target_table",
            load_mode="upsert",
            options={"key": "value"},
            schema_options={"infer": True},
            incremental_config={"upsert_keys": ["id"]},  # Required for UPSERT mode
        )

        assert step.id == "load_test"
        assert step.type == "load"
        assert step.source == "source_table"
        assert step.target_table == "target_table"
        assert step.load_mode == "upsert"
        assert step.options == {"key": "value"}
        assert step.schema_options == {"infer": True}
        assert step.incremental_config == {"upsert_keys": ["id"]}

    def test_load_step_defaults(self):
        """Test LoadStep with defaults."""
        step = LoadStep(
            id="load_test", source="source_table", target_table="target_table"
        )

        assert step.load_mode == "replace"
        assert step.options == {}
        assert step.schema_options == {}
        assert step.incremental_config is None

    def test_load_step_validation(self):
        """Test LoadStep validation."""
        # Empty source should raise ValueError
        with pytest.raises(ValueError, match="Load step source cannot be empty"):
            LoadStep(id="test", source="", target_table="target")

        # Empty target_table should raise ValueError
        with pytest.raises(ValueError, match="Load step target_table cannot be empty"):
            LoadStep(id="test", source="source", target_table="")


class TestTransformStep:
    """Test TransformStep functionality."""

    def test_transform_step_creation(self):
        """Test TransformStep creation."""
        step = TransformStep(
            id="transform_test",
            sql="SELECT * FROM table",
            target_table="result_table",
            operation_type="create_table",
            udf_dependencies=["udf1", "udf2"],
            schema_evolution=True,
            materialization="view",
        )

        assert step.id == "transform_test"
        assert step.type == "transform"
        assert step.sql == "SELECT * FROM table"
        assert step.target_table == "result_table"
        assert step.operation_type == "create_table"
        assert step.udf_dependencies == ["udf1", "udf2"]
        assert step.schema_evolution is True
        assert step.materialization == "view"

    def test_transform_step_defaults(self):
        """Test TransformStep with defaults."""
        step = TransformStep(id="transform_test", sql="SELECT * FROM table")

        assert step.target_table is None
        assert step.operation_type == "select"
        assert step.udf_dependencies == []
        assert step.schema_evolution is False
        assert step.materialization == "table"

    def test_transform_step_validation(self):
        """Test TransformStep validation."""
        # Empty SQL should raise ValueError
        with pytest.raises(ValueError, match="Transform step SQL cannot be empty"):
            TransformStep(id="test", sql="")

        with pytest.raises(ValueError, match="Transform step SQL cannot be empty"):
            TransformStep(id="test", sql="   ")

        # Operations requiring target_table should validate
        with pytest.raises(ValueError, match="requires target_table"):
            TransformStep(
                id="test", sql="SELECT * FROM table", operation_type="create_table"
            )

        with pytest.raises(ValueError, match="requires target_table"):
            TransformStep(id="test", sql="SELECT * FROM table", operation_type="insert")


class TestExportStep:
    """Test ExportStep functionality."""

    def test_export_step_creation(self):
        """Test ExportStep creation."""
        step = ExportStep(
            id="export_test",
            source_table="source_table",
            target="output.csv",
            export_format="parquet",
            options={"compression": "gzip"},
            partitioning={"by": "date"},
            compression="snappy",
        )

        assert step.id == "export_test"
        assert step.type == "export"
        assert step.source_table == "source_table"
        assert step.target == "output.csv"
        assert step.export_format == "parquet"
        assert step.options == {"compression": "gzip"}
        assert step.partitioning == {"by": "date"}
        assert step.compression == "snappy"

    def test_export_step_defaults(self):
        """Test ExportStep with defaults."""
        step = ExportStep(
            id="export_test", source_table="source_table", target="output.csv"
        )

        assert step.export_format == "csv"
        assert step.options == {}
        assert step.partitioning is None
        assert step.compression is None

    def test_export_step_validation(self):
        """Test ExportStep validation."""
        # Empty source_table should raise ValueError
        with pytest.raises(
            ValueError, match="Export step source_table cannot be empty"
        ):
            ExportStep(id="test", source_table="", target="output.csv")

        # Empty target should raise ValueError
        with pytest.raises(ValueError, match="Export step target cannot be empty"):
            ExportStep(id="test", source_table="source", target="")


class TestSourceDefinitionStep:
    """Test SourceDefinitionStep functionality."""

    def test_source_definition_step_creation(self):
        """Test SourceDefinitionStep creation."""
        step = SourceDefinitionStep(
            id="source_def_test",
            source_name="my_source",
            source_config={"type": "postgres", "host": "localhost"},
            profile_name="dev",
            validation_rules=[{"rule": "not_null"}],
            setup_sql="CREATE SCHEMA test",
        )

        assert step.id == "source_def_test"
        assert step.type == "source_definition"
        assert step.source_name == "my_source"
        assert step.source_config == {"type": "postgres", "host": "localhost"}
        assert step.profile_name == "dev"
        assert step.validation_rules == [{"rule": "not_null"}]
        assert step.setup_sql == "CREATE SCHEMA test"

    def test_source_definition_step_defaults(self):
        """Test SourceDefinitionStep with defaults."""
        step = SourceDefinitionStep(
            id="source_def_test",
            source_name="my_source",
            source_config={"type": "postgres"},
        )

        assert step.profile_name is None
        assert step.validation_rules == []
        assert step.setup_sql is None

    def test_source_definition_step_validation(self):
        """Test SourceDefinitionStep validation."""
        # Empty source_name should raise ValueError
        with pytest.raises(
            ValueError, match="Source definition step source_name cannot be empty"
        ):
            SourceDefinitionStep(
                id="test", source_name="", source_config={"type": "postgres"}
            )

        # Empty source_config should raise ValueError
        with pytest.raises(
            ValueError, match="Source definition step source_config cannot be empty"
        ):
            SourceDefinitionStep(id="test", source_name="source", source_config={})


class TestSetVariableStep:
    """Test SetVariableStep functionality."""

    def test_set_variable_step_with_value(self):
        """Test SetVariableStep with static value."""
        step = SetVariableStep(
            id="var_test", variable_name="my_var", value="test_value", scope="global"
        )

        assert step.id == "var_test"
        assert step.type == "set_variable"
        assert step.variable_name == "my_var"
        assert step.value == "test_value"
        assert step.sql_expression is None
        assert step.scope == "global"

    def test_set_variable_step_with_sql(self):
        """Test SetVariableStep with SQL expression."""
        step = SetVariableStep(
            id="var_test",
            variable_name="my_var",
            sql_expression="SELECT MAX(id) FROM table",
        )

        assert step.variable_name == "my_var"
        assert step.value is None
        assert step.sql_expression == "SELECT MAX(id) FROM table"
        assert step.scope == "pipeline"

    def test_set_variable_step_validation(self):
        """Test SetVariableStep validation."""
        # Empty variable_name should raise ValueError
        with pytest.raises(
            ValueError, match="Set variable step variable_name cannot be empty"
        ):
            SetVariableStep(id="test", variable_name="", value="test")

        # Must have either value or sql_expression
        with pytest.raises(
            ValueError, match="must have either value or sql_expression"
        ):
            SetVariableStep(id="test", variable_name="var")

        # Cannot have both value and sql_expression
        with pytest.raises(
            ValueError, match="cannot have both value and sql_expression"
        ):
            SetVariableStep(
                id="test", variable_name="var", value="test", sql_expression="SELECT 1"
            )


class TestCreateStepFromDict:
    """Test create_step_from_dict function."""

    def test_create_load_step_from_dict(self):
        """Test creating LoadStep from dictionary."""
        step_dict = {
            "id": "load_test",
            "type": "load",
            "source": "source_table",
            "target_table": "target_table",
            "load_mode": "append",
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, LoadStep)
        assert step.id == "load_test"
        assert step.source == "source_table"
        assert step.target_table == "target_table"
        assert step.load_mode == "append"

    def test_create_transform_step_from_dict(self):
        """Test creating TransformStep from dictionary."""
        step_dict = {
            "id": "transform_test",
            "type": "transform",
            "sql": "SELECT * FROM table",
            "target_table": "result",
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, TransformStep)
        assert step.id == "transform_test"
        assert step.sql == "SELECT * FROM table"
        assert step.target_table == "result"

    def test_create_step_with_auto_id(self):
        """Test creating step with auto-generated ID."""
        step_dict = {
            "type": "load",
            "source": "source_table",
            "target_table": "target_table",
        }

        step = create_step_from_dict(step_dict)

        assert isinstance(step, LoadStep)
        assert step.id.startswith("load_")
        assert len(step.id) > 5  # Should have UUID suffix

    def test_create_step_validation_errors(self):
        """Test validation errors in create_step_from_dict."""
        # Non-dict input should raise ValueError
        with pytest.raises(ValueError, match="step_dict must be a dictionary"):
            create_step_from_dict("not_a_dict")

        # Missing type should raise ValueError
        with pytest.raises(ValueError, match="step_dict must contain a 'type' field"):
            create_step_from_dict({"id": "test"})

        # Unknown step type should raise ValueError
        with pytest.raises(ValueError, match="Unknown step type: unknown"):
            create_step_from_dict({"type": "unknown", "id": "test"})

        # Invalid configuration should raise ValueError
        with pytest.raises(ValueError, match="Invalid step configuration"):
            create_step_from_dict(
                {"type": "load", "id": "test", "invalid_field": "value"}
            )


class TestStepHierarchy:
    """Test step inheritance and polymorphism."""

    def test_all_steps_are_base_steps(self):
        """Test that all step types inherit from BaseStep."""
        load_step = LoadStep(id="load", source="src", target_table="tgt")
        transform_step = TransformStep(id="transform", sql="SELECT 1")
        export_step = ExportStep(id="export", source_table="src", target="dst")

        assert isinstance(load_step, BaseStep)
        assert isinstance(transform_step, BaseStep)
        assert isinstance(export_step, BaseStep)

    def test_step_type_consistency(self):
        """Test that step types are consistent."""
        load_step = LoadStep(id="load", source="src", target_table="tgt")
        transform_step = TransformStep(id="transform", sql="SELECT 1")
        export_step = ExportStep(id="export", source_table="src", target="dst")
        source_step = SourceDefinitionStep(
            id="source", source_name="src", source_config={"type": "test"}
        )
        var_step = SetVariableStep(id="var", variable_name="var", value="test")

        assert load_step.type == "load"
        assert transform_step.type == "transform"
        assert export_step.type == "export"
        assert source_step.type == "source_definition"
        assert var_step.type == "set_variable"


if __name__ == "__main__":
    pytest.main([__file__])
