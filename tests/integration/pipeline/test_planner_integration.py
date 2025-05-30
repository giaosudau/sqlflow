"""Integration tests for the operation planner."""

import json
import os
import tempfile

from sqlflow.core.planner import OperationPlanner
from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    SourceDefinitionStep,
    SQLBlockStep,
)


def test_planner_integration():
    """Test that the planner correctly maps DAG nodes to execution plan items."""
    pipeline = Pipeline()

    source_step1 = SourceDefinitionStep(
        name="users_source",
        connector_type="csv",
        params={"path": "users.csv"},
    )
    pipeline.add_step(source_step1)

    source_step2 = SourceDefinitionStep(
        name="orders_source",
        connector_type="csv",
        params={"path": "orders.csv"},
    )
    pipeline.add_step(source_step2)

    load_step1 = LoadStep(
        table_name="users_table",
        source_name="users_source",
    )
    pipeline.add_step(load_step1)

    load_step2 = LoadStep(
        table_name="orders_table",
        source_name="orders_source",
    )
    pipeline.add_step(load_step2)

    sql_step1 = SQLBlockStep(
        table_name="active_users",
        sql_query="SELECT * FROM users_table WHERE active = true",
    )
    pipeline.add_step(sql_step1)

    sql_step2 = SQLBlockStep(
        table_name="user_orders",
        sql_query="SELECT u.*, o.* FROM active_users u JOIN orders_table o ON u.id = o.user_id",
    )
    pipeline.add_step(sql_step2)

    export_step = ExportStep(
        sql_query="SELECT * FROM user_orders",
        destination_uri="s3://bucket/user_orders.csv",
        connector_type="csv",
        options={"delimiter": ","},
    )
    pipeline.add_step(export_step)

    planner = OperationPlanner()
    plan = planner.plan(pipeline)

    json_plan = planner.to_json(plan)

    assert len(plan) == 7

    source_steps = [step for step in plan if step["type"] == "source_definition"]
    assert len(source_steps) == 2
    assert any(step["name"] == "users_source" for step in source_steps)
    assert any(step["name"] == "orders_source" for step in source_steps)

    load_steps = [step for step in plan if step["type"] == "load"]
    assert len(load_steps) == 2
    assert any(step["name"] == "users_table" for step in load_steps)
    assert any(step["name"] == "orders_table" for step in load_steps)

    sql_steps = [step for step in plan if step["type"] == "transform"]
    assert len(sql_steps) == 2
    assert any(step["name"] == "active_users" for step in sql_steps)
    assert any(step["name"] == "user_orders" for step in sql_steps)

    export_steps = [step for step in plan if step["type"] == "export"]
    assert len(export_steps) == 1
    assert export_steps[0]["source_connector_type"] == "csv"

    active_users_step = next(
        step for step in sql_steps if step["name"] == "active_users"
    )
    user_orders_step = next(step for step in sql_steps if step["name"] == "user_orders")

    users_table_id = next(
        step["id"] for step in load_steps if step["name"] == "users_table"
    )
    assert users_table_id in active_users_step["depends_on"]

    active_users_id = active_users_step["id"]
    orders_table_id = next(
        step["id"] for step in load_steps if step["name"] == "orders_table"
    )
    assert active_users_id in user_orders_step["depends_on"]
    assert orders_table_id in user_orders_step["depends_on"]

    user_orders_id = user_orders_step["id"]
    assert user_orders_id in export_steps[0]["depends_on"]

    parsed_plan = json.loads(json_plan)
    assert len(parsed_plan) == 7
    assert isinstance(parsed_plan, list)

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        f.write(json_plan)
        snapshot_path = f.name

    with open(snapshot_path, "r") as f:
        loaded_plan = json.load(f)
        assert len(loaded_plan) == 7

    os.unlink(snapshot_path)


def test_plan_order_matches_dependencies():
    """Test that the plan order matches dependencies."""
    pipeline = Pipeline()

    load_step = LoadStep(
        table_name="users_table",
        source_name="users_source",
    )
    pipeline.add_step(load_step)

    sql_step1 = SQLBlockStep(
        table_name="filtered_users",
        sql_query="SELECT * FROM users_table WHERE active = true",
    )
    pipeline.add_step(sql_step1)

    sql_step2 = SQLBlockStep(
        table_name="user_stats",
        sql_query="SELECT COUNT(*) FROM filtered_users",
    )
    pipeline.add_step(sql_step2)

    planner = OperationPlanner()
    plan = planner.plan(pipeline)

    assert len(plan) == 3

    step_ids = [step["id"] for step in plan]

    load_step_id = next(
        i for i, step in enumerate(step_ids) if step.startswith("load_")
    )
    sql_step1_id = next(
        i for i, step in enumerate(step_ids) if step == "transform_filtered_users"
    )
    sql_step2_id = next(
        i for i, step in enumerate(step_ids) if step == "transform_user_stats"
    )

    assert load_step_id < sql_step1_id < sql_step2_id

    assert plan[sql_step1_id]["depends_on"] == [step_ids[load_step_id]]
    assert plan[sql_step2_id]["depends_on"] == [step_ids[sql_step1_id]]
