"""Tests for AST to DAG converter."""

from sqlflow.parser.ast import (
    ExportStep,
    IncludeStep,
    LoadStep,
    Pipeline,
    SetStep,
    SourceDefinitionStep,
    SQLBlockStep,
)
from sqlflow.parser.ast_to_dag import ASTToDAGConverter
from sqlflow.visualizer.dag_builder import PipelineDAG


class TestASTToDAGConverter:
    """Test AST to DAG conversion functionality."""

    def test_initialization(self):
        """Test converter initialization."""
        converter = ASTToDAGConverter()
        assert converter.dag_builder is not None

    def test_convert_empty_pipeline(self):
        """Test converting an empty pipeline."""
        converter = ASTToDAGConverter()
        pipeline = Pipeline(steps=[])

        dag = converter.convert(pipeline)
        assert isinstance(dag, PipelineDAG)

    def test_convert_source_definition_step(self):
        """Test converting a source definition step."""
        converter = ASTToDAGConverter()

        source_step = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"file_path": "data/users.csv"},
            line_number=1,
        )

        pipeline = Pipeline(steps=[source_step])
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)

    def test_convert_load_step(self):
        """Test converting a load step."""
        converter = ASTToDAGConverter()

        load_step = LoadStep(
            table_name="users_table", source_name="users", line_number=2
        )

        pipeline = Pipeline(steps=[load_step])
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)

    def test_convert_export_step(self):
        """Test converting an export step."""
        converter = ASTToDAGConverter()

        export_step = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="output/users.csv",
            connector_type="CSV",
            options={"header": True},
            line_number=3,
        )

        pipeline = Pipeline(steps=[export_step])
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)

    def test_convert_include_step(self):
        """Test converting an include step."""
        converter = ASTToDAGConverter()

        include_step = IncludeStep(
            file_path="shared/common.sf", alias="common", line_number=4
        )

        pipeline = Pipeline(steps=[include_step])
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)

    def test_convert_set_step(self):
        """Test converting a set step."""
        converter = ASTToDAGConverter()

        set_step = SetStep(
            variable_name="env", variable_value="production", line_number=5
        )

        pipeline = Pipeline(steps=[set_step])
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)

    def test_convert_sql_block_step(self):
        """Test converting a SQL block step."""
        converter = ASTToDAGConverter()

        sql_step = SQLBlockStep(
            table_name="processed_users",
            sql_query="SELECT id, name FROM users WHERE active = true",
            line_number=6,
        )

        pipeline = Pipeline(steps=[sql_step])
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)

    def test_convert_step_to_dict_source_definition(self):
        """Test converting source definition step to dictionary."""
        converter = ASTToDAGConverter()

        step = SourceDefinitionStep(
            name="users",
            connector_type="CSV",
            params={"file_path": "data/users.csv"},
            line_number=1,
        )

        result = converter._convert_step_to_dict(step, 0)

        assert result is not None
        assert result["id"] == "step_0"
        assert result["name"] == "users"
        assert result["type"] == "SOURCE"
        assert result["connector_type"] == "CSV"
        assert result["params"] == {"file_path": "data/users.csv"}
        assert result["line_number"] == 1

    def test_convert_step_to_dict_load_step(self):
        """Test converting load step to dictionary."""
        converter = ASTToDAGConverter()

        step = LoadStep(table_name="users_table", source_name="users", line_number=2)

        result = converter._convert_step_to_dict(step, 1)

        assert result is not None
        assert result["id"] == "step_1"
        assert result["table_name"] == "users_table"
        assert result["source_name"] == "users"
        assert result["type"] == "LOAD"
        assert result["line_number"] == 2

    def test_convert_step_to_dict_export_step(self):
        """Test converting export step to dictionary."""
        converter = ASTToDAGConverter()

        step = ExportStep(
            sql_query="SELECT * FROM users",
            destination_uri="output/users.csv",
            connector_type="CSV",
            options={"header": True},
            line_number=3,
        )

        result = converter._convert_step_to_dict(step, 2)

        assert result is not None
        assert result["id"] == "step_2"
        assert result["sql_query"] == "SELECT * FROM users"
        assert result["destination_uri"] == "output/users.csv"
        assert result["connector_type"] == "CSV"
        assert result["options"] == {"header": True}
        assert result["type"] == "EXPORT"
        assert result["line_number"] == 3

    def test_convert_step_to_dict_include_step(self):
        """Test converting include step to dictionary."""
        converter = ASTToDAGConverter()

        step = IncludeStep(file_path="shared/common.sf", alias="common", line_number=4)

        result = converter._convert_step_to_dict(step, 3)

        assert result is not None
        assert result["id"] == "step_3"
        assert result["file_path"] == "shared/common.sf"
        assert result["alias"] == "common"
        assert result["type"] == "INCLUDE"
        assert result["line_number"] == 4

    def test_convert_step_to_dict_set_step(self):
        """Test converting set step to dictionary."""
        converter = ASTToDAGConverter()

        step = SetStep(variable_name="env", variable_value="production", line_number=5)

        result = converter._convert_step_to_dict(step, 4)

        assert result is not None
        assert result["id"] == "step_4"
        assert result["variable_name"] == "env"
        assert result["variable_value"] == "production"
        assert result["type"] == "SET"
        assert result["line_number"] == 5

    def test_convert_step_to_dict_sql_block_step(self):
        """Test converting SQL block step to dictionary."""
        converter = ASTToDAGConverter()

        step = SQLBlockStep(
            table_name="processed_users",
            sql_query="SELECT id, name FROM users WHERE active = true",
            line_number=6,
        )

        result = converter._convert_step_to_dict(step, 5)

        assert result is not None
        assert result["id"] == "step_5"
        assert result["table_name"] == "processed_users"
        assert result["sql_query"] == "SELECT id, name FROM users WHERE active = true"
        assert result["type"] == "SQL_BLOCK"
        assert result["line_number"] == 6

    def test_convert_step_to_dict_unknown_step(self):
        """Test converting unknown step type returns None."""
        converter = ASTToDAGConverter()

        # Test with None instead of creating unknown step
        result = converter._convert_step_to_dict(None, 0)

        assert result is None

    def test_build_source_map(self):
        """Test building source name to step ID map."""
        converter = ASTToDAGConverter()

        pipeline_steps = [
            {"id": "step_0", "name": "users", "type": "SOURCE"},
            {"id": "step_1", "name": "orders", "type": "SOURCE"},
            {"id": "step_2", "table_name": "users_table", "type": "LOAD"},
        ]

        source_map = converter._build_source_map(pipeline_steps)

        assert source_map == {"users": "step_0", "orders": "step_1"}

    def test_build_table_map(self):
        """Test building table name to step ID map."""
        converter = ASTToDAGConverter()

        pipeline_steps = [
            {"id": "step_0", "name": "users", "type": "SOURCE"},
            {"id": "step_1", "table_name": "users_table", "type": "LOAD"},
            {"id": "step_2", "table_name": "processed_users", "type": "SQL_BLOCK"},
        ]

        table_map = converter._build_table_map(pipeline_steps)

        assert table_map == {"users_table": "step_1", "processed_users": "step_2"}

    def test_add_load_dependencies(self):
        """Test adding dependencies for load steps."""
        converter = ASTToDAGConverter()

        step = {
            "id": "step_1",
            "source_name": "users",
            "type": "LOAD",
            "depends_on": [],
        }

        source_map = {"users": "step_0", "orders": "step_2"}

        converter._add_load_dependencies(step, source_map)

        assert step["depends_on"] == ["step_0"]

    def test_add_sql_dependencies(self):
        """Test adding dependencies for SQL-based steps."""
        converter = ASTToDAGConverter()

        step = {
            "id": "step_2",
            "sql_query": "SELECT * FROM users_table JOIN orders_table ON users_table.id = orders_table.user_id",
            "type": "SQL_BLOCK",
            "depends_on": [],
        }

        table_map = {"users_table": "step_0", "orders_table": "step_1"}

        converter._add_sql_dependencies(step, table_map)

        assert "step_0" in step["depends_on"]  # users_table dependency
        assert "step_1" in step["depends_on"]  # orders_table dependency

    def test_add_dependencies_integration(self):
        """Test complete dependency resolution."""
        converter = ASTToDAGConverter()

        pipeline_steps = [
            {"id": "step_0", "name": "users", "type": "SOURCE"},
            {
                "id": "step_1",
                "table_name": "users_table",
                "source_name": "users",
                "type": "LOAD",
            },
            {
                "id": "step_2",
                "table_name": "processed_users",
                "sql_query": "SELECT * FROM users_table",
                "type": "SQL_BLOCK",
            },
        ]

        converter._add_dependencies(pipeline_steps)

        # Check that dependencies were added
        assert pipeline_steps[0]["depends_on"] == []  # SOURCE has no dependencies
        assert pipeline_steps[1]["depends_on"] == ["step_0"]  # LOAD depends on SOURCE
        assert pipeline_steps[2]["depends_on"] == [
            "step_1"
        ]  # SQL_BLOCK depends on LOAD

    def test_complex_pipeline_conversion(self):
        """Test converting a complex pipeline with multiple step types."""
        converter = ASTToDAGConverter()

        steps = [
            SourceDefinitionStep(
                name="users", connector_type="CSV", params={}, line_number=1
            ),
            LoadStep(table_name="users_table", source_name="users", line_number=2),
            SQLBlockStep(
                table_name="active_users",
                sql_query="SELECT * FROM users_table WHERE active = true",
                line_number=3,
            ),
            ExportStep(
                sql_query="SELECT * FROM active_users",
                destination_uri="output.csv",
                connector_type="CSV",
                options={},
                line_number=4,
            ),
        ]

        pipeline = Pipeline(steps=steps)
        dag = converter.convert(pipeline)

        assert isinstance(dag, PipelineDAG)
