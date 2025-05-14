"""Tests for the operation planner."""

import json
from unittest.mock import patch

import pytest

from sqlflow.core.errors import PlanningError
from sqlflow.core.planner import ExecutionPlanBuilder, OperationPlanner
from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    SourceDefinitionStep,
    SQLBlockStep,
)


class TestExecutionPlanBuilder:
    """Test cases for the ExecutionPlanBuilder class."""

    def test_build_plan_empty_pipeline(self):
        """Test building a plan from an empty pipeline."""
        pipeline = Pipeline()
        builder = ExecutionPlanBuilder()
        plan = builder.build_plan(pipeline)
        assert plan == []

    def test_build_plan_simple_pipeline(self):
        """Test building a plan from a simple pipeline."""
        pipeline = Pipeline()

        source_step = SourceDefinitionStep(
            name="users_source",
            connector_type="csv",
            params={"path": "users.csv"},
        )
        pipeline.add_step(source_step)

        load_step = LoadStep(
            table_name="users_table",
            source_name="users_source",
        )
        pipeline.add_step(load_step)

        sql_step = SQLBlockStep(
            table_name="users_summary",
            sql_query="SELECT COUNT(*) FROM users_table",
        )
        pipeline.add_step(sql_step)

        export_step = ExportStep(
            sql_query="SELECT * FROM users_summary",
            destination_uri="s3://bucket/users_summary.csv",
            connector_type="csv",
            options={"delimiter": ","},
        )
        pipeline.add_step(export_step)

        builder = ExecutionPlanBuilder()
        plan = builder.build_plan(pipeline)

        assert len(plan) == 4

        assert plan[0]["id"] == "source_users_source"
        assert plan[0]["type"] == "source_definition"
        assert plan[0]["name"] == "users_source"
        assert plan[0]["source_connector_type"] == "csv"
        assert plan[0]["query"] == {"path": "users.csv"}

        assert plan[1]["id"] == "load_users_table"
        assert plan[1]["type"] == "load"
        assert plan[1]["name"] == "users_table"
        assert plan[1]["query"]["source_name"] == "users_source"
        assert plan[1]["query"]["table_name"] == "users_table"

        assert plan[2]["id"] == "transform_users_summary"
        assert plan[2]["type"] == "transform"
        assert plan[2]["name"] == "users_summary"
        assert plan[2]["query"] == "SELECT COUNT(*) FROM users_table"
        assert "load_users_table" in plan[2]["depends_on"]

        assert plan[3]["id"].startswith("export_")
        assert plan[3]["type"] == "export"
        assert plan[3]["source_connector_type"] == "csv"
        assert plan[3]["query"]["destination_uri"] == "s3://bucket/users_summary.csv"
        assert plan[3]["query"]["options"] == {"delimiter": ","}
        assert (
            "transform_users_summary" in plan[3]["depends_on"]
            or "load_users_table" in plan[3]["depends_on"]
        )

    def test_build_plan_dependency_resolution(self):
        """Test that dependencies are correctly resolved in the plan."""
        pipeline = Pipeline()

        load_step = LoadStep(
            table_name="users_table",
            source_name="users_source",
        )
        pipeline.add_step(load_step)

        sql_step1 = SQLBlockStep(
            table_name="users_filtered",
            sql_query="SELECT * FROM users_table WHERE active = true",
        )
        pipeline.add_step(sql_step1)

        sql_step2 = SQLBlockStep(
            table_name="users_summary",
            sql_query="SELECT COUNT(*) FROM users_filtered",
        )
        pipeline.add_step(sql_step2)

        builder = ExecutionPlanBuilder()
        plan = builder.build_plan(pipeline)

        assert len(plan) == 3

        assert plan[0]["id"] == "load_users_table"
        assert plan[0]["type"] == "load"

        assert plan[1]["id"] == "transform_users_filtered"
        assert plan[1]["type"] == "transform"
        assert "load_users_table" in plan[1]["depends_on"]

        assert plan[2]["id"] == "transform_users_summary"
        assert plan[2]["type"] == "transform"
        assert "transform_users_filtered" in plan[2]["depends_on"]

    def test_generate_step_id(self):
        """Test generating step IDs for different step types."""
        builder = ExecutionPlanBuilder()

        source_step = SourceDefinitionStep(
            name="users_source",
            connector_type="csv",
            params={"path": "users.csv"},
        )
        assert builder._generate_step_id(source_step, 0) == "source_users_source"

        load_step = LoadStep(
            table_name="users_table",
            source_name="users_source",
        )
        assert builder._generate_step_id(load_step, 1) == "load_users_table"

        sql_step = SQLBlockStep(
            table_name="users_summary",
            sql_query="SELECT COUNT(*) FROM users_table",
        )
        assert builder._generate_step_id(sql_step, 2) == "transform_users_summary"

        export_step = ExportStep(
            sql_query="SELECT * FROM users_summary",
            destination_uri="s3://bucket/users_summary.csv",
            connector_type="csv",
            options={"delimiter": ","},
        )
        assert builder._generate_step_id(export_step, 3) == "export_3"


class TestOperationPlanner:
    """Test cases for the OperationPlanner class."""

    def test_plan(self):
        """Test planning operations for a pipeline."""
        pipeline = Pipeline()

        load_step = LoadStep(
            table_name="users_table",
            source_name="users_source",
        )
        pipeline.add_step(load_step)

        sql_step = SQLBlockStep(
            table_name="users_summary",
            sql_query="SELECT COUNT(*) FROM users_table",
        )
        pipeline.add_step(sql_step)

        planner = OperationPlanner()
        plan = planner.plan(pipeline)

        assert len(plan) == 2
        assert plan[0]["id"] == "load_users_table"
        assert plan[1]["id"] == "transform_users_summary"

    def test_to_json(self):
        """Test converting a plan to JSON."""
        plan = [
            {
                "id": "load_users_table",
                "type": "load",
                "name": "users_table",
                "source_connector_type": "csv",
                "query": {"source_name": "users_source", "table_name": "users_table"},
                "depends_on": [],
            },
            {
                "id": "transform_users_summary",
                "type": "transform",
                "name": "users_summary",
                "query": "SELECT COUNT(*) FROM users_table",
                "depends_on": ["load_users_table"],
            },
        ]

        planner = OperationPlanner()
        json_str = planner.to_json(plan)

        assert isinstance(json_str, str)
        parsed_plan = json.loads(json_str)
        assert len(parsed_plan) == 2
        assert parsed_plan[0]["id"] == "load_users_table"
        assert parsed_plan[1]["id"] == "transform_users_summary"

    def test_from_json(self):
        """Test converting JSON to a plan."""
        json_str = """
        [
            {
                "id": "load_users_table",
                "type": "load",
                "name": "users_table",
                "source_connector_type": "csv",
                "query": {"source_name": "users_source", "table_name": "users_table"},
                "depends_on": []
            },
            {
                "id": "transform_users_summary",
                "type": "transform",
                "name": "users_summary",
                "query": "SELECT COUNT(*) FROM users_table",
                "depends_on": ["load_users_table"]
            }
        ]
        """

        planner = OperationPlanner()
        plan = planner.from_json(json_str)

        assert len(plan) == 2
        assert plan[0]["id"] == "load_users_table"
        assert plan[1]["id"] == "transform_users_summary"

    def test_planning_error(self):
        """Test that a PlanningError is raised when planning fails."""
        pipeline = Pipeline()

        with patch.object(
            ExecutionPlanBuilder, "build_plan", side_effect=Exception("Test error")
        ):
            planner = OperationPlanner()

            with pytest.raises(PlanningError) as excinfo:
                planner.plan(pipeline)

            assert "Test error" in str(excinfo.value)
