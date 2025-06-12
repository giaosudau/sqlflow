"""Tests for extracted planner components.

This module tests the individual components extracted from ExecutionPlanBuilder
to verify they follow the Single Responsibility Principle and work correctly.

Following Zen of Python:
- Simple is better than complex: Clear test scenarios for each component
- Explicit is better than implicit: Test component interfaces explicitly
- Practicality beats purity: Focus on real-world usage scenarios
"""

import pytest

from sqlflow.core.errors import PlanningError
from sqlflow.core.planner import DependencyAnalyzer, ExecutionOrderResolver, StepBuilder
from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    SourceDefinitionStep,
    SQLBlockStep,
)


class TestDependencyAnalyzer:
    """Test cases for the DependencyAnalyzer component.

    Following Zen of Python: Simple is better than complex.
    Each test focuses on a specific dependency analysis behavior.
    """

    def test_analyze_simple_pipeline(self):
        """Test dependency analysis for a simple pipeline."""
        pipeline = Pipeline()

        # Create a simple pipeline: SOURCE -> LOAD -> TRANSFORM
        source_step = SourceDefinitionStep(
            name="users_source", connector_type="csv", params={"path": "users.csv"}
        )
        load_step = LoadStep(table_name="users", source_name="users_source")
        transform_step = SQLBlockStep(
            table_name="users_summary", sql_query="SELECT COUNT(*) FROM users"
        )

        pipeline.add_step(source_step)
        pipeline.add_step(load_step)
        pipeline.add_step(transform_step)

        # Create step ID mapping
        step_id_map = {
            id(source_step): "source_users_source",
            id(load_step): "load_users",
            id(transform_step): "transform_users_summary",
        }

        analyzer = DependencyAnalyzer()
        dependencies = analyzer.analyze(pipeline, step_id_map)

        # Verify dependencies
        assert "source_users_source" in dependencies
        assert "load_users" in dependencies
        assert "transform_users_summary" in dependencies

        # Verify dependency relationships
        assert "source_users_source" in dependencies["load_users"]
        assert "load_users" in dependencies["transform_users_summary"]
        assert dependencies["source_users_source"] == []  # No dependencies

    def test_analyze_sql_dependencies(self):
        """Test SQL table reference dependency analysis."""
        pipeline = Pipeline()

        # Create SQL steps with table references
        step1 = SQLBlockStep(
            table_name="table1", sql_query="SELECT * FROM source_table"
        )
        step2 = SQLBlockStep(
            table_name="table2", sql_query="SELECT * FROM table1 JOIN other_table"
        )

        pipeline.add_step(step1)
        pipeline.add_step(step2)

        step_id_map = {id(step1): "transform_table1", id(step2): "transform_table2"}

        analyzer = DependencyAnalyzer()
        dependencies = analyzer.analyze(pipeline, step_id_map)

        # table2 should depend on table1
        assert "transform_table1" in dependencies["transform_table2"]

    def test_analyze_export_dependencies(self):
        """Test export step dependency analysis."""
        pipeline = Pipeline()

        # Create transform and export steps
        transform_step = SQLBlockStep(
            table_name="summary", sql_query="SELECT COUNT(*) FROM users"
        )
        export_step = ExportStep(
            sql_query="SELECT * FROM summary",
            destination_uri="s3://bucket/summary.csv",
            connector_type="csv",
            options={},
        )

        pipeline.add_step(transform_step)
        pipeline.add_step(export_step)

        step_id_map = {
            id(transform_step): "transform_summary",
            id(export_step): "export_csv_summary",
        }

        analyzer = DependencyAnalyzer()
        dependencies = analyzer.analyze(pipeline, step_id_map)

        # Export should depend on transform
        assert "transform_summary" in dependencies["export_csv_summary"]

    def test_duplicate_table_detection(self):
        """Test duplicate table detection raises PlanningError."""
        pipeline = Pipeline()

        # Create duplicate table definitions
        step1 = SQLBlockStep(
            table_name="users", sql_query="CREATE TABLE users AS SELECT 1"
        )
        step2 = SQLBlockStep(
            table_name="users", sql_query="CREATE TABLE users AS SELECT 2"
        )
        step1.line_number = 10
        step2.line_number = 20

        pipeline.add_step(step1)
        pipeline.add_step(step2)

        step_id_map = {id(step1): "transform_users_1", id(step2): "transform_users_2"}

        analyzer = DependencyAnalyzer()

        with pytest.raises(PlanningError) as exc_info:
            analyzer.analyze(pipeline, step_id_map)

        assert "Duplicate table definitions found" in str(exc_info.value)
        assert "line 20" in str(exc_info.value)
        assert "line 10" in str(exc_info.value)


class TestExecutionOrderResolver:
    """Test cases for the ExecutionOrderResolver component.

    Following Zen of Python: Explicit is better than implicit.
    Tests verify clear dependency-based ordering.
    """

    def test_resolve_simple_order(self):
        """Test simple execution order resolution."""
        step_dependencies = {"step_a": [], "step_b": ["step_a"], "step_c": ["step_b"]}

        resolver = ExecutionOrderResolver()
        order = resolver.resolve(step_dependencies)

        # Verify proper order
        assert order.index("step_a") < order.index("step_b")
        assert order.index("step_b") < order.index("step_c")
        assert len(order) == 3

    def test_resolve_parallel_steps(self):
        """Test resolution with parallel steps."""
        step_dependencies = {"step_a": [], "step_b": [], "step_c": ["step_a", "step_b"]}

        resolver = ExecutionOrderResolver()
        order = resolver.resolve(step_dependencies)

        # Both step_a and step_b should come before step_c
        assert order.index("step_a") < order.index("step_c")
        assert order.index("step_b") < order.index("step_c")
        assert len(order) == 3

    def test_detect_circular_dependency(self):
        """Test circular dependency detection."""
        step_dependencies = {
            "step_a": ["step_b"],
            "step_b": ["step_c"],
            "step_c": ["step_a"],  # Creates a cycle
        }

        resolver = ExecutionOrderResolver()

        with pytest.raises(PlanningError) as exc_info:
            resolver.resolve(step_dependencies)

        assert "Circular dependencies detected" in str(exc_info.value)

    def test_empty_dependencies(self):
        """Test resolution with empty dependencies."""
        step_dependencies = {}

        resolver = ExecutionOrderResolver()
        order = resolver.resolve(step_dependencies)

        assert order == []

    def test_single_step(self):
        """Test resolution with single step."""
        step_dependencies = {"single_step": []}

        resolver = ExecutionOrderResolver()
        order = resolver.resolve(step_dependencies)

        assert order == ["single_step"]


class TestStepBuilder:
    """Test cases for the StepBuilder component.

    Following Zen of Python: Simple is better than complex.
    Tests verify clear step building for each step type.
    """

    def test_build_source_definition_step(self):
        """Test building source definition execution steps."""
        pipeline = Pipeline()
        source_step = SourceDefinitionStep(
            name="users_source",
            connector_type="csv",
            params={"path": "users.csv", "sync_mode": "incremental"},
        )
        pipeline.add_step(source_step)

        step_id_map = {id(source_step): "source_users_source"}
        step_dependencies = {"source_users_source": []}

        builder = StepBuilder()
        execution_steps = builder.build_steps(
            pipeline, ["source_users_source"], step_id_map, step_dependencies
        )

        assert len(execution_steps) == 1
        step = execution_steps[0]
        assert step["id"] == "source_users_source"
        assert step["type"] == "source_definition"
        assert step["name"] == "users_source"
        assert step["source_connector_type"] == "csv"
        assert step["sync_mode"] == "incremental"

    def test_build_load_step(self):
        """Test building load execution steps."""
        pipeline = Pipeline()

        # Add source first
        source_step = SourceDefinitionStep(
            name="users_source", connector_type="postgres", params={"host": "localhost"}
        )
        load_step = LoadStep(
            table_name="users", source_name="users_source", mode="APPEND"
        )

        pipeline.add_step(source_step)
        pipeline.add_step(load_step)

        step_id_map = {
            id(source_step): "source_users_source",
            id(load_step): "load_users",
        }
        step_dependencies = {
            "source_users_source": [],
            "load_users": ["source_users_source"],
        }

        builder = StepBuilder()
        execution_steps = builder.build_steps(
            pipeline,
            ["source_users_source", "load_users"],
            step_id_map,
            step_dependencies,
        )

        # Find the load step
        load_exec_step = next(
            step for step in execution_steps if step["type"] == "load"
        )

        assert load_exec_step["id"] == "load_users"
        assert load_exec_step["name"] == "users"
        assert load_exec_step["source_name"] == "users_source"
        assert load_exec_step["target_table"] == "users"
        assert load_exec_step["source_connector_type"] == "postgres"
        assert load_exec_step["mode"] == "APPEND"
        assert "source_users_source" in load_exec_step["depends_on"]

    def test_build_sql_block_step(self):
        """Test building SQL block execution steps."""
        pipeline = Pipeline()
        sql_step = SQLBlockStep(
            table_name="users_summary",
            sql_query="SELECT COUNT(*) as total FROM users",
            is_replace=True,
        )
        sql_step.line_number = 15

        pipeline.add_step(sql_step)

        step_id_map = {id(sql_step): "transform_users_summary"}
        step_dependencies = {"transform_users_summary": []}

        builder = StepBuilder()
        execution_steps = builder.build_steps(
            pipeline, ["transform_users_summary"], step_id_map, step_dependencies
        )

        assert len(execution_steps) == 1
        step = execution_steps[0]
        assert step["id"] == "transform_users_summary"
        assert step["type"] == "transform"
        assert step["name"] == "users_summary"
        assert step["query"] == "SELECT COUNT(*) as total FROM users"
        assert step["is_replace"] is True

    def test_build_export_step(self):
        """Test building export execution steps."""
        pipeline = Pipeline()
        export_step = ExportStep(
            sql_query="SELECT * FROM users_summary",
            destination_uri="s3://bucket/summary.csv",
            connector_type="csv",
            options={"delimiter": ","},
        )

        pipeline.add_step(export_step)

        step_id_map = {id(export_step): "export_csv_summary"}
        step_dependencies = {"export_csv_summary": []}

        builder = StepBuilder()
        execution_steps = builder.build_steps(
            pipeline, ["export_csv_summary"], step_id_map, step_dependencies
        )

        assert len(execution_steps) == 1
        step = execution_steps[0]
        assert step["id"] == "export_csv_summary"
        assert step["type"] == "export"
        assert step["source_connector_type"] == "csv"
        assert step["query"]["destination_uri"] == "s3://bucket/summary.csv"
        assert step["query"]["options"] == {"delimiter": ","}

    def test_build_mixed_pipeline(self):
        """Test building a complete pipeline with multiple step types."""
        pipeline = Pipeline()

        # Create a complete pipeline
        source_step = SourceDefinitionStep(
            name="users_source", connector_type="csv", params={"path": "users.csv"}
        )
        load_step = LoadStep(table_name="users", source_name="users_source")
        transform_step = SQLBlockStep(
            table_name="users_summary", sql_query="SELECT COUNT(*) FROM users"
        )
        export_step = ExportStep(
            sql_query="SELECT * FROM users_summary",
            destination_uri="output.csv",
            connector_type="csv",
            options={},
        )

        pipeline.add_step(source_step)
        pipeline.add_step(load_step)
        pipeline.add_step(transform_step)
        pipeline.add_step(export_step)

        step_id_map = {
            id(source_step): "source_users_source",
            id(load_step): "load_users",
            id(transform_step): "transform_users_summary",
            id(export_step): "export_csv_summary",
        }

        step_dependencies = {
            "source_users_source": [],
            "load_users": ["source_users_source"],
            "transform_users_summary": ["load_users"],
            "export_csv_summary": ["transform_users_summary"],
        }

        execution_order = [
            "source_users_source",
            "load_users",
            "transform_users_summary",
            "export_csv_summary",
        ]

        builder = StepBuilder()
        execution_steps = builder.build_steps(
            pipeline, execution_order, step_id_map, step_dependencies
        )

        # Verify all steps are built
        assert len(execution_steps) == 4

        # Verify step types
        step_types = [step["type"] for step in execution_steps]
        assert "source_definition" in step_types
        assert "load" in step_types
        assert "transform" in step_types
        assert "export" in step_types

        # Verify execution order is preserved
        step_ids = [step["id"] for step in execution_steps]
        assert step_ids == execution_order


class TestIntegratedComponents:
    """Integration tests for all extracted components working together.

    Following Zen of Python: Practicality beats purity.
    Tests verify components work together like the original ExecutionPlanBuilder.
    """

    def test_full_pipeline_processing(self):
        """Test all components working together for a complete pipeline."""
        pipeline = Pipeline()

        # Create a realistic pipeline
        source_step = SourceDefinitionStep(
            name="sales_source",
            connector_type="postgres",
            params={"host": "localhost", "database": "sales"},
        )
        load_step = LoadStep(table_name="sales", source_name="sales_source")
        transform_step = SQLBlockStep(
            table_name="monthly_sales",
            sql_query="SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total FROM sales GROUP BY month",
        )
        export_step = ExportStep(
            sql_query="SELECT * FROM monthly_sales ORDER BY month",
            destination_uri="s3://reports/monthly_sales.csv",
            connector_type="s3",
            options={},
        )

        pipeline.add_step(source_step)
        pipeline.add_step(load_step)
        pipeline.add_step(transform_step)
        pipeline.add_step(export_step)

        # Simulate step ID generation
        step_id_map = {
            id(source_step): "source_sales_source",
            id(load_step): "load_sales",
            id(transform_step): "transform_monthly_sales",
            id(export_step): "export_s3_monthly_sales",
        }

        # Step 1: Analyze dependencies
        analyzer = DependencyAnalyzer()
        dependencies = analyzer.analyze(pipeline, step_id_map)

        # Step 2: Resolve execution order
        resolver = ExecutionOrderResolver()
        execution_order = resolver.resolve(dependencies)

        # Step 3: Build execution steps
        builder = StepBuilder()
        execution_steps = builder.build_steps(
            pipeline, execution_order, step_id_map, dependencies
        )

        # Verify the complete pipeline
        assert len(execution_steps) == 4

        # Verify proper execution order
        step_ids = [step["id"] for step in execution_steps]
        assert step_ids.index("source_sales_source") < step_ids.index("load_sales")
        assert step_ids.index("load_sales") < step_ids.index("transform_monthly_sales")
        assert step_ids.index("transform_monthly_sales") < step_ids.index(
            "export_s3_monthly_sales"
        )

        # Verify step dependencies are correctly set
        export_step_exec = next(
            step for step in execution_steps if step["type"] == "export"
        )
        assert "transform_monthly_sales" in export_step_exec["depends_on"]

        transform_step_exec = next(
            step for step in execution_steps if step["type"] == "transform"
        )
        assert "load_sales" in transform_step_exec["depends_on"]
