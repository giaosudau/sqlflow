"""Tests for V2 LocalOrchestrator.

Following Kent Beck's testing principles:
- Use real implementations wherever possible
- Test behavior, not implementation
- Tests as documentation
- Simple tests that clearly show how the system works

No mocks except at system boundaries.
Real components working together.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator


@pytest.fixture
def temp_csv_file():
    """Create a real CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name,age\n")
        f.write("1,Alice,30\n")
        f.write("2,Bob,25\n")
        f.write("3,Charlie,35\n")
        f.flush()
        yield Path(f.name)
        Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestLocalOrchestratorBehavior:
    """Test orchestrator behavior with real components."""

    def test_empty_pipeline_succeeds(self):
        """Empty pipeline should succeed gracefully."""
        orchestrator = LocalOrchestrator()
        result = orchestrator.execute([])

        assert result["status"] == "success"
        assert result["executed_steps"] == []
        assert result["total_steps"] == 0

    def test_single_step_pipeline_real_execution(self, temp_csv_file):
        """Test single step pipeline with real CSV loading using V2 architecture."""
        orchestrator = LocalOrchestrator()

        # V2 Architecture: First define the source, then load it
        plan = [
            # Step 1: Define the source
            {
                "type": "source_definition",
                "id": "define_customers_source",
                "name": "customers_source",
                "source_connector_type": "csv",
                "query": {"path": str(temp_csv_file), "has_header": True},
            },
            # Step 2: Load from the defined source
            {
                "type": "load",
                "id": "load_customers",
                "source_name": "customers_source",
                "target_table": "customers",
                "load_mode": "replace",
            },
        ]

        result = orchestrator.execute(plan)

        assert result["status"] == "success"
        assert "load_customers" in result["executed_steps"]
        assert result["total_steps"] == 2
        assert "performance_summary" in result

    def test_multi_step_pipeline_real_flow(self, temp_csv_file, temp_output_dir):
        """Test multi-step pipeline with real data flow."""
        orchestrator = LocalOrchestrator()

        output_file = temp_output_dir / "processed_customers.csv"

        # Real pipeline: load -> transform -> export
        plan = [
            {
                "type": "load",
                "id": "load_step",
                "source": str(temp_csv_file),
                "target_table": "customers",
                "load_mode": "replace",
            },
            {
                "type": "transform",
                "id": "transform_step",
                "sql": "SELECT name, age FROM customers WHERE age >= 25",
                "target_table": "adult_customers",
            },
            {
                "type": "export",
                "id": "export_step",
                "source_table": "adult_customers",
                "target": str(output_file),
            },
        ]

        result = orchestrator.execute(plan)

        # Verify complete pipeline execution
        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 3
        assert result["total_steps"] == 3

        # Verify actual output file was created
        assert output_file.exists()

        # Verify data lineage tracking
        lineage = result["data_lineage"]
        assert len(lineage["steps"]) == 3
        assert "customers" in lineage.get("tables_created", [])
        assert "adult_customers" in lineage.get("tables_created", [])

    def test_variable_substitution_real_behavior(self, temp_csv_file, temp_output_dir):
        """Test variable substitution with real execution."""
        orchestrator = LocalOrchestrator()

        output_file = temp_output_dir / "customers_filtered.csv"
        variables = {"min_age": "25", "output_path": str(output_file)}

        plan = [
            {
                "type": "load",
                "id": "load_step",
                "source": str(temp_csv_file),
                "target_table": "customers",
                "load_mode": "replace",
            },
            {
                "type": "transform",
                "id": "filter_step",
                "sql": "SELECT * FROM customers WHERE age >= ${min_age}",
                "target_table": "filtered_customers",
            },
            {
                "type": "export",
                "id": "export_step",
                "source_table": "filtered_customers",
                "target": "${output_path}",
            },
        ]

        result = orchestrator.execute(plan, variables=variables)

        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 3
        assert output_file.exists()


class TestOrchestratorAsDocumentation:
    """These tests serve as documentation showing how to use the orchestrator."""

    def test_basic_etl_pipeline_example(self, temp_csv_file, temp_output_dir):
        """
        Example: Basic ETL pipeline.

        This shows how users would typically use the orchestrator
        for a simple Extract-Transform-Load workflow.
        """
        # Create orchestrator
        orchestrator = LocalOrchestrator()

        # Define output location
        output_file = temp_output_dir / "transformed_data.csv"

        # Define ETL pipeline
        etl_pipeline = [
            # Extract: Load data from CSV
            {
                "type": "load",
                "id": "extract_customers",
                "source": str(temp_csv_file),
                "target_table": "raw_customers",
                "load_mode": "replace",
            },
            # Transform: Clean and filter data
            {
                "type": "transform",
                "id": "transform_customers",
                "sql": """
                    SELECT
                        id,
                        UPPER(name) as name_upper,
                        age,
                        CASE WHEN age >= 25 THEN 'Adult' ELSE 'Young' END as age_group
                    FROM raw_customers
                    WHERE age IS NOT NULL
                """,
                "target_table": "clean_customers",
            },
            # Load: Export to new CSV
            {
                "type": "export",
                "id": "load_clean_data",
                "source_table": "clean_customers",
                "target": str(output_file),
            },
        ]

        # Execute pipeline
        result = orchestrator.execute(etl_pipeline)

        # Verify success
        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 3
        assert output_file.exists()

        # Verify data was actually transformed
        df = pd.read_csv(output_file)
        assert "name_upper" in df.columns
        assert "age_group" in df.columns

    def test_pipeline_with_variables_example(self, temp_csv_file, temp_output_dir):
        """
        Example: Pipeline with variable substitution.

        Shows how to parameterize pipelines for different environments.
        """
        orchestrator = LocalOrchestrator()

        # Pipeline variables (could come from config file, CLI, etc.)
        pipeline_vars = {
            "input_file": str(temp_csv_file),
            "min_age": "25",
            "output_dir": str(temp_output_dir),
            "env": "test",
        }

        # Parameterized pipeline
        pipeline = [
            {
                "type": "load",
                "id": "load_data",
                "source": "${input_file}",
                "target_table": "customers_${env}",
                "load_mode": "replace",
            },
            {
                "type": "transform",
                "id": "filter_adults",
                "sql": "SELECT * FROM customers_${env} WHERE age >= ${min_age}",
                "target_table": "adult_customers_${env}",
            },
            {
                "type": "export",
                "id": "export_results",
                "source_table": "adult_customers_${env}",
                "target": "${output_dir}/adults_${env}.csv",
            },
        ]

        result = orchestrator.execute(pipeline, variables=pipeline_vars)

        assert result["status"] == "success"
        assert len(result["executed_steps"]) == 3

        # Verify variable substitution worked
        output_file = temp_output_dir / "adults_test.csv"
        assert output_file.exists()
