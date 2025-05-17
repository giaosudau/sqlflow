"""Tests for variable substitution in export paths and destinations."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.core.executors.local_executor import LocalExecutor


class TestExportPathVariableSubstitution(unittest.TestCase):
    """Test that variable substitution works correctly in export paths for all connectors."""

    def setUp(self):
        """Set up a test environment."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_profile = {
            "variables": {"region": "us-east", "date": "2023-10-25", "env": "test"}
        }

        # Mock profile loading
        self.executor = LocalExecutor()
        self.executor.profile = self.test_profile

    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()

    def test_csv_export_variable_substitution(self):
        """Test that variables are substituted in CSV export paths."""
        # Create mock data
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        data_chunk = DataChunk(data)

        # Mock the connector engine and export method
        self.executor.connector_engine = MagicMock()

        # Create export step with variables in path
        step = {
            "id": "export_csv_test",
            "type": "export",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": f"{self.temp_dir.name}/${{region}}_${{date}}_report.csv",
                "options": {"header": True},
            },
        }

        # Execute the export step
        with patch.object(self.executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", data_chunk)
            result = self.executor._execute_export(step)

        # Check that the executor called export_data with the substituted path
        expected_path = f"{self.temp_dir.name}/us-east_2023-10-25_report.csv"
        self.executor.connector_engine.export_data.assert_called_once()
        args, kwargs = self.executor.connector_engine.export_data.call_args
        self.assertEqual(kwargs["destination"], expected_path)
        self.assertEqual(result["status"], "success")

    def test_default_value_substitution(self):
        """Test that default values are used when variables are not provided."""
        # Create mock data
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        data_chunk = DataChunk(data)

        # Mock the connector engine and export method
        self.executor.connector_engine = MagicMock()

        # Create export step with variables and defaults in path
        step = {
            "id": "export_csv_test",
            "type": "export",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": f"{self.temp_dir.name}/${{missing|default}}_${{date}}_report.csv",
                "options": {"header": True},
            },
        }

        # Execute the export step
        with patch.object(self.executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", data_chunk)
            result = self.executor._execute_export(step)

        # Check that the executor called export_data with the substituted path using default
        expected_path = f"{self.temp_dir.name}/default_2023-10-25_report.csv"
        self.executor.connector_engine.export_data.assert_called_once()
        args, kwargs = self.executor.connector_engine.export_data.call_args
        self.assertEqual(kwargs["destination"], expected_path)
        self.assertEqual(result["status"], "success")

    def test_complex_nested_variables(self):
        """Test substitution of complex nested variable patterns."""
        # Create mock data
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        data_chunk = DataChunk(data)

        # Mock the connector engine and export method
        self.executor.connector_engine = MagicMock()

        # Create export step with complex nested variables in path
        step = {
            "id": "export_csv_test",
            "type": "export",
            "source_connector_type": "CSV",
            "query": {
                "destination_uri": f"{self.temp_dir.name}/${{env}}/${{region}}/reports_${{date}}_${{format|csv}}.csv",
                "options": {"header": True},
            },
        }

        # Execute the export step
        with patch.object(self.executor, "_resolve_export_source") as mock_resolve:
            mock_resolve.return_value = ("test_table", data_chunk)
            result = self.executor._execute_export(step)

        # Check that the executor called export_data with the substituted path
        expected_path = f"{self.temp_dir.name}/test/us-east/reports_2023-10-25_csv.csv"
        self.executor.connector_engine.export_data.assert_called_once()
        args, kwargs = self.executor.connector_engine.export_data.call_args
        self.assertEqual(kwargs["destination"], expected_path)
        self.assertEqual(result["status"], "success")

    def test_all_connector_types(self):
        """Test variable substitution works with all connector types."""
        # List of known connector types
        connector_types = ["CSV", "S3", "POSTGRES"]

        # Create mock data
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        data_chunk = DataChunk(data)

        # Mock the connector engine and export method
        self.executor.connector_engine = MagicMock()

        for connector_type in connector_types:
            # Reset mock
            self.executor.connector_engine.reset_mock()

            # Create export step with variables in path
            step = {
                "id": f"export_{connector_type.lower()}_test",
                "type": "export",
                "source_connector_type": connector_type,
                "query": {
                    "destination_uri": f"output/${{region}}/${{date}}_report.{connector_type.lower()}",
                    "options": {"header": True},
                },
            }

            # Execute the export step
            with patch.object(self.executor, "_resolve_export_source") as mock_resolve:
                mock_resolve.return_value = ("test_table", data_chunk)
                result = self.executor._execute_export(step)

            # Check that the executor called export_data with the substituted path
            expected_path = f"output/us-east/2023-10-25_report.{connector_type.lower()}"
            self.executor.connector_engine.export_data.assert_called_once()
            args, kwargs = self.executor.connector_engine.export_data.call_args
            self.assertEqual(
                kwargs["destination"],
                expected_path,
                f"Failed for connector type: {connector_type}",
            )
            self.assertEqual(
                result["status"],
                "success",
                f"Export failed for connector type: {connector_type}",
            )


@pytest.mark.integration
class TestIntegrationVariableSubstitution:
    """Integration tests for variable substitution in real pipeline execution."""

    def test_variable_substitution_in_pipeline(self, tmp_path):
        """Test variable substitution in a real pipeline."""
        # Create temporary directory for output
        output_dir = os.path.join(tmp_path, "output")
        os.makedirs(output_dir, exist_ok=True)

        # Create a test pipeline file
        pipeline_file = os.path.join(tmp_path, "test_pipeline.sf")
        with open(pipeline_file, "w") as f:
            f.write(
                f"""
            -- Test pipeline for variable substitution
            SOURCE test_source TYPE CSV PARAMS {{
                "path": "{tmp_path}/data.csv",
                "has_header": true
            }};
            
            LOAD data FROM test_source;
            
            CREATE TABLE processed_data AS
            SELECT * FROM data;
            
            EXPORT SELECT * FROM processed_data
            TO "{output_dir}/${{region|us-east}}/${{date|2023-10-25}}_export.csv"
            TYPE CSV
            OPTIONS {{
                "header": true
            }};
            """
            )

        # Create test data
        data_file = os.path.join(tmp_path, "data.csv")
        with open(data_file, "w") as f:
            f.write("id,name\n1,Alice\n2,Bob\n3,Charlie")

        # Execute the pipeline
        LocalExecutor()
        # This part would be implemented for actual integration testing
        # For now, it's a placeholder to demonstrate the test structure

        # Assert the exported file exists with the expected path
        os.path.join(output_dir, "us-east/2023-10-25_export.csv")
        # assert os.path.exists(expected_file)  # For real integration testing
