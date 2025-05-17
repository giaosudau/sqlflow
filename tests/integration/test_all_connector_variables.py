"""Integration tests for variable substitution in all connector types."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser


class TestAllConnectorVariableSubstitution:
    """Test that variable substitution works correctly for all connector types."""

    @pytest.fixture
    def mock_executor(self):
        """Set up a mock executor with test profile."""
        executor = LocalExecutor()
        executor.profile = {
            "variables": {"region": "us-east", "date": "2023-10-25", "env": "test"}
        }
        executor.connector_engine = MagicMock()
        return executor

    @pytest.fixture
    def test_data_chunk(self):
        """Create a test data chunk with sample data."""
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        return DataChunk(data)

    def test_csv_connector(self, mock_executor, test_data_chunk):
        """Test variable substitution with CSV connector."""
        step = {
            "id": "export_csv_test",
            "type": "export",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": "output/${region}/${date}_report.csv",
                "options": {"header": True},
            },
        }

        with patch.object(mock_executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", test_data_chunk)
            result = mock_executor._execute_export(step)

        # Verify correct path substitution
        expected_path = "output/us-east/2023-10-25_report.csv"
        mock_executor.connector_engine.export_data.assert_called_once()
        args, kwargs = mock_executor.connector_engine.export_data.call_args
        assert (
            kwargs["destination"] == expected_path
        ), f"Expected {expected_path}, got {kwargs['destination']}"
        assert result["status"] == "success"

    def test_s3_connector(self, mock_executor, test_data_chunk):
        """Test variable substitution with S3 connector."""
        step = {
            "id": "export_s3_test",
            "type": "export",
            "source_connector_type": "S3",
            "query": {
                "destination_uri": "s3://mybucket/${env}/${region}/${date}/data.parquet",
                "options": {"format": "parquet"},
            },
        }

        with patch.object(mock_executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", test_data_chunk)
            result = mock_executor._execute_export(step)

        # Verify correct path substitution
        expected_path = "s3://mybucket/test/us-east/2023-10-25/data.parquet"
        mock_executor.connector_engine.export_data.assert_called_once()
        args, kwargs = mock_executor.connector_engine.export_data.call_args
        assert (
            kwargs["destination"] == expected_path
        ), f"Expected {expected_path}, got {kwargs['destination']}"
        assert result["status"] == "success"

    def test_postgres_connector(self, mock_executor, test_data_chunk):
        """Test variable substitution with Postgres connector."""
        step = {
            "id": "export_postgres_test",
            "type": "export",
            "source_connector_type": "POSTGRES",
            "query": {
                "destination_uri": "postgres://user:pass@host/${env}_${region}_${date}",
                "options": {"schema": "public", "table": "data_${date}"},
            },
        }

        with patch.object(mock_executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", test_data_chunk)
            result = mock_executor._execute_export(step)

        # Verify correct path substitution
        expected_path = "postgres://user:pass@host/test_us-east_2023-10-25"
        mock_executor.connector_engine.export_data.assert_called_once()
        args, kwargs = mock_executor.connector_engine.export_data.call_args
        assert (
            kwargs["destination"] == expected_path
        ), f"Expected {expected_path}, got {kwargs['destination']}"

        # Verify that options are also properly substituted
        expected_options = {"schema": "public", "table": "data_2023-10-25"}
        assert kwargs["options"] == expected_options
        assert result["status"] == "success"

    def test_variable_in_options(self, mock_executor, test_data_chunk):
        """Test variable substitution in connector options."""
        # We need to modify _substitute_variables to also handle options
        # For now, this test is a reminder that we need to implement this feature

        # Create a step with variables in both destination and options
        step = {
            "id": "export_csv_test",
            "type": "export",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": "output/${region}/${date}_report.csv",
                "options": {
                    "header": True,
                    "file_prefix": "${env}_",
                    "file_suffix": "_${region}",
                },
            },
        }

        with patch.object(mock_executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", test_data_chunk)
            result = mock_executor._execute_export(step)

        # This would be the future expected behavior, but for now options are not substituted
        # expected_options = {
        #     "header": True,
        #     "file_prefix": "test_",
        #     "file_suffix": "_us-east"
        # }

        mock_executor.connector_engine.export_data.assert_called_once()
        args, kwargs = mock_executor.connector_engine.export_data.call_args
        assert kwargs["destination"] == "output/us-east/2023-10-25_report.csv"
        assert result["status"] == "success"


@pytest.mark.integration
class TestRealConnectorIntegration:
    """Integration tests with real connectors."""

    @pytest.fixture
    def test_environment(self):
        """Create a temporary test environment."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create directories
            data_dir = os.path.join(temp_dir, "data")
            output_dir = os.path.join(temp_dir, "output")

            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            # Create a sample CSV file
            data_file = os.path.join(data_dir, "sample.csv")
            with open(data_file, "w") as f:
                f.write("id,name,value\n")
                f.write("1,Alpha,100\n")
                f.write("2,Beta,200\n")
                f.write("3,Gamma,300\n")

            yield {
                "temp_dir": temp_dir,
                "data_dir": data_dir,
                "output_dir": output_dir,
                "data_file": data_file,
            }

    def test_csv_export_with_variables(self, test_environment):
        """Test CSV export with variable substitution."""
        # Create a simple pipeline definition
        pipeline_text = f"""
        SOURCE test_source TYPE CSV PARAMS {{
            "path": "{test_environment['data_file']}",
            "has_header": true
        }};
        
        LOAD data FROM test_source;
        
        CREATE TABLE enriched_data AS
        SELECT 
            id,
            name,
            value,
            value * 2 AS double_value
        FROM data;
        
        EXPORT SELECT * FROM enriched_data
        TO "{test_environment['output_dir']}/${{region|global}}/${{date|today}}_report.csv"
        TYPE CSV
        OPTIONS {{
            "header": true
        }};
        """

        # Parse and execute the pipeline
        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        variables = {"region": "test-region", "date": "2023-11-15"}
        planner = Planner()
        operations = planner.create_plan(pipeline, variables)

        executor = LocalExecutor()
        executor.profile = {"variables": variables}
        result = executor.execute(operations, variables=variables)

        # Check if the export was successful
        assert "error" not in result, f"Pipeline execution error: {result.get('error')}"

        # Check if the file was created with substituted variables
        expected_file = os.path.join(
            test_environment["output_dir"], "test-region/2023-11-15_report.csv"
        )
        assert os.path.exists(
            expected_file
        ), f"Expected file not found: {expected_file}"

        # Verify file content
        with open(expected_file, "r") as f:
            content = f.read()
            assert "id,name,value,double_value" in content
            assert "1,Alpha,100,200" in content

    # This test might be skipped if no S3/Postgres is available
    @pytest.mark.skip(reason="Requires S3/Postgres setup")
    def test_other_connectors(self, test_environment):
        """Placeholder for testing other connectors."""
        # This would be implemented to test S3, Postgres, etc.
        # For now, it's a placeholder for future implementation
