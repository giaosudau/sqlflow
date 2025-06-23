"""Integration tests for CREATE OR REPLACE TABLE functionality.

These tests verify the complete functionality of CREATE OR REPLACE support across
the entire SQLFlow pipeline: parsing, planning, SQL generation, and execution.

Follows testing standards:
- Real implementations, no mocking
- End-to-end scenarios
- Tests as documentation
- Real data flows
"""

import os
import tempfile
from pathlib import Path

import pytest

from sqlflow.core.planner_main import Planner
from sqlflow.core.sql_generator import SQLGenerator
from sqlflow.parser.ast import SQLBlockStep
from sqlflow.parser.parser import Parser


class TestCreateOrReplaceTableFunctionality:
    """Test CREATE OR REPLACE TABLE functionality end-to-end."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary project directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create project structure
            project_dir = Path(temp_dir) / "test_project"
            project_dir.mkdir()

            # Create profiles directory and dev profile
            profiles_dir = project_dir / "profiles"
            profiles_dir.mkdir()

            dev_profile = profiles_dir / "dev.yml"
            dev_profile.write_text(
                """
engines:
  duckdb:
    mode: memory
    path: ":memory:"
paths:
  pipelines: "pipelines"
  output: "output"
variables:
  test_var: "test_value"
"""
            )

            # Create pipelines directory
            pipelines_dir = project_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create output directory
            output_dir = project_dir / "output"
            output_dir.mkdir()

            yield str(project_dir)

    @pytest.fixture
    def sample_data_dir(self, temp_project_dir):
        """Create sample data files for testing."""
        data_dir = Path(temp_project_dir) / "data"
        data_dir.mkdir()

        # Create sample CSV file
        sample_csv = data_dir / "sample_data.csv"
        sample_csv.write_text(
            """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
"""
        )

        return str(data_dir)

    def test_create_or_replace_parsing(self):
        """Test that CREATE OR REPLACE TABLE is parsed correctly."""
        parser = Parser()

        # Test CREATE TABLE (without OR REPLACE)
        create_sql = """
        CREATE TABLE test_table AS
        SELECT id, name FROM source_table;
        """

        pipeline = parser.parse(create_sql)
        sql_step = pipeline.steps[0]

        assert isinstance(sql_step, SQLBlockStep)
        assert sql_step.table_name == "test_table"
        assert sql_step.is_replace is False

        # Test CREATE OR REPLACE TABLE
        create_or_replace_sql = """
        CREATE OR REPLACE TABLE test_table AS
        SELECT id, name, value FROM source_table;
        """

        pipeline = parser.parse(create_or_replace_sql)
        sql_step = pipeline.steps[0]

        assert isinstance(sql_step, SQLBlockStep)
        assert sql_step.table_name == "test_table"
        assert sql_step.is_replace is True

    def test_create_or_replace_sql_generation(self):
        """Test that SQLGenerator correctly generates CREATE and CREATE OR REPLACE statements."""
        sql_generator = SQLGenerator()

        # Test default behavior - should now use CREATE OR REPLACE TABLE
        default_operation = {
            "type": "transform",
            "id": "test_default",
            "name": "test_table",
            "query": "SELECT id, name FROM source_table",
            # No is_replace specified - should default to True now
        }

        context = {"variables": {}}
        sql = sql_generator.generate_operation_sql(default_operation, context)

        # Should generate CREATE OR REPLACE TABLE by default
        assert "CREATE OR REPLACE TABLE test_table AS" in sql

        # Test explicit CREATE operation (backward compatibility)
        create_operation = {
            "type": "transform",
            "id": "test_create",
            "name": "test_table",
            "query": "SELECT id, name FROM source_table",
            "is_replace": False,
        }

        sql = sql_generator.generate_operation_sql(create_operation, context)

        # Should generate standard CREATE TABLE when explicitly requested
        assert "CREATE TABLE test_table AS" in sql
        assert "CREATE OR REPLACE" not in sql

        # Test explicit CREATE OR REPLACE operation
        replace_operation = {
            "type": "transform",
            "id": "test_replace",
            "name": "test_table",
            "query": "SELECT id, name, value FROM source_table",
            "is_replace": True,
        }

        sql = sql_generator.generate_operation_sql(replace_operation, context)

        # Should generate CREATE OR REPLACE TABLE
        assert "CREATE OR REPLACE TABLE test_table AS" in sql

    def test_create_or_replace_planner_integration(self):
        """Test that planner correctly handles CREATE OR REPLACE."""
        parser = Parser()
        planner = Planner()

        pipeline_text = """
        SOURCE test_source TYPE CSV PARAMS {
            "path": "data/test.csv",
            "has_header": true
        };
        
        LOAD test_table FROM test_source;
        
        CREATE OR REPLACE TABLE summary_table AS
        SELECT 
            COUNT(*) as total_rows,
            AVG(value) as avg_value
        FROM test_table;
        """

        pipeline = parser.parse(pipeline_text)
        operations = planner.create_plan(pipeline)

        # Find the CREATE OR REPLACE operation
        transform_ops = [op for op in operations if op["type"] == "transform"]
        assert len(transform_ops) == 1

        transform_op = transform_ops[0]
        assert transform_op["name"] == "summary_table"
        assert transform_op["is_replace"] is True

    def test_create_or_replace_duplicate_table_validation(self):
        """Test that CREATE OR REPLACE allows redefining tables."""
        parser = Parser()
        planner = Planner()

        # Pipeline with duplicate table definitions using CREATE OR REPLACE
        pipeline_text = """
        SOURCE test_source TYPE CSV PARAMS {
            "path": "data/test.csv",
            "has_header": true
        };
        
        LOAD test_table FROM test_source;
        
        -- First definition
        CREATE TABLE metrics AS
        SELECT COUNT(*) as count FROM test_table;
        
        -- Second definition with CREATE OR REPLACE - should be allowed
        CREATE OR REPLACE TABLE metrics AS
        SELECT 
            COUNT(*) as total_count,
            AVG(value) as avg_value
        FROM test_table;
        """

        pipeline = parser.parse(pipeline_text)

        # This should not raise an error because CREATE OR REPLACE allows redefinition
        operations = planner.create_plan(pipeline)

        # Verify both operations are in the plan
        transform_ops = [op for op in operations if op["type"] == "transform"]
        assert len(transform_ops) == 2

        # First should be regular CREATE
        assert transform_ops[0]["is_replace"] is False
        # Second should be CREATE OR REPLACE
        assert transform_ops[1]["is_replace"] is True

    def test_create_or_replace_end_to_end_execution(
        self, temp_project_dir, sample_data_dir, v2_pipeline_runner
    ):
        """Test complete CREATE OR REPLACE functionality end-to-end."""
        os.chdir(temp_project_dir)

        # Create pipeline file
        pipeline_file = (
            Path(temp_project_dir) / "pipelines" / "test_create_or_replace.sf"
        )
        pipeline_content = f"""
        SOURCE sample_data TYPE CSV PARAMS {{
            "path": "{sample_data_dir}/sample_data.csv",
            "has_header": true
        }};

        LOAD raw_data FROM sample_data;

        -- First create a summary table
        CREATE TABLE data_summary AS
        SELECT
            COUNT(*) as row_count,
            'initial' as version
        FROM raw_data;

        -- Replace it with enhanced summary
        CREATE OR REPLACE TABLE data_summary AS
        SELECT
            COUNT(*) as row_count,
            AVG(CAST(value AS INTEGER)) as avg_value,
            MAX(name) as max_name,
            'updated' as version
        FROM raw_data;
        """

        pipeline_file.write_text(pipeline_content)

        # Parse and plan
        parser = Parser()
        planner = Planner()

        pipeline = parser.parse(pipeline_content)
        operations = planner.create_plan(pipeline)

        # Execute
        coordinator = v2_pipeline_runner(operations, project_dir=temp_project_dir)
        result = coordinator.result

        assert result.success is True

        # Verify results
        engine = coordinator.context.engine
        final_summary = engine.execute_query("SELECT * FROM data_summary").df()

        assert len(final_summary) == 1
        assert final_summary["version"][0] == "updated"
        assert final_summary["row_count"][0] == 3
        assert final_summary["avg_value"][0] == 200.0

    def test_create_or_replace_error_handling(self):
        """Test error handling for invalid CREATE OR REPLACE syntax."""
        parser = Parser()

        # Test invalid syntax combinations
        invalid_syntaxes = [
            "CREATE OR REPLACE AS SELECT * FROM table;",  # Missing TABLE
            "CREATE REPLACE TABLE test AS SELECT * FROM table;",  # Missing OR
            "CREATE OR TABLE test AS SELECT * FROM table;",  # Missing REPLACE
        ]

        for invalid_syntax in invalid_syntaxes:
            with pytest.raises(Exception):
                parser.parse(invalid_syntax)

    def test_create_or_replace_with_complex_queries(
        self, temp_project_dir, sample_data_dir, v2_pipeline_runner
    ):
        """Test CREATE OR REPLACE with complex SQL queries."""
        os.chdir(temp_project_dir)

        # Create pipeline with complex queries
        pipeline_content = f"""
        SOURCE sample_data TYPE CSV PARAMS {{
            "path": "{sample_data_dir}/sample_data.csv",
            "has_header": true
        }};

        LOAD raw_data FROM sample_data;

        -- Complex query with aggregations and case statements
        CREATE OR REPLACE TABLE complex_analysis AS
        SELECT
            id,
            name,
            CAST(value AS INTEGER) as value,
            CASE
                WHEN CAST(value AS INTEGER) > 200 THEN 'high'
                WHEN CAST(value AS INTEGER) > 100 THEN 'medium'
                ELSE 'low'
            END as category
        FROM raw_data;
        """

        # Parse and execute
        parser = Parser()
        planner = Planner()

        pipeline = parser.parse(pipeline_content)
        operations = planner.create_plan(pipeline)

        # Execute
        coordinator = v2_pipeline_runner(operations, project_dir=temp_project_dir)
        result = coordinator.result

        assert result.success is True

        # Verify results
        engine = coordinator.context.engine
        df = engine.execute_query("SELECT * FROM complex_analysis").df()

        assert len(df) == 3
        assert "category" in df.columns
        assert df["category"].tolist() == ["low", "medium", "high"]

    def test_create_or_replace_dependency_resolution(self):
        """Test that CREATE OR REPLACE works correctly with dependency resolution."""
        parser = Parser()
        planner = Planner()

        pipeline_text = """
        SOURCE data_source TYPE CSV PARAMS {
            "path": "data/test.csv",
            "has_header": true
        };
        
        LOAD base_table FROM data_source;
        
        CREATE TABLE intermediate AS
        SELECT * FROM base_table WHERE value > 0;
        
        CREATE OR REPLACE TABLE final_result AS
        SELECT 
            COUNT(*) as count,
            'replaced' as status
        FROM intermediate;
        
        CREATE TABLE dependent_on_final AS
        SELECT status FROM final_result;
        """

        pipeline = parser.parse(pipeline_text)
        operations = planner.create_plan(pipeline)

        # Verify dependency order is maintained
        [op["id"] for op in operations]

        # Find positions of relevant operations using 'name' field
        final_result_idx = None
        dependent_idx = None

        for i, op in enumerate(operations):
            if op.get("name") == "final_result":
                final_result_idx = i
            elif op.get("name") == "dependent_on_final":
                dependent_idx = i

        # Verify we found both operations
        assert final_result_idx is not None, "final_result operation not found"
        assert dependent_idx is not None, "dependent_on_final operation not found"

        # dependent_on_final should come after final_result
        assert dependent_idx > final_result_idx

        # Verify the CREATE OR REPLACE operation has correct dependencies
        final_result_op = operations[final_result_idx]
        assert final_result_op["is_replace"] is True
        assert "intermediate" in str(final_result_op.get("depends_on", []))
