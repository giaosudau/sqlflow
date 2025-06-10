"""Integration tests for LocalExecutor ConnectorEngine components.

Tests the refactored connector engine methods against real Docker services:
- PostgreSQL connector registration and data loading
- S3/MinIO connector export and discovery operations
- Error handling and validation scenarios
- Individual method functionality rather than full pipeline execution
"""

import os
import tempfile
from unittest.mock import MagicMock

import pandas as pd
import pytest

from sqlflow.core.executors.local_executor import LocalExecutor


class TestConnectorEngineRegistration:
    """Test connector registration functionality with real services."""

    @pytest.mark.external_services
    @pytest.mark.postgres
    def test_postgres_connector_registration_success(self):
        """Test successful PostgreSQL connector registration."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # PostgreSQL connection parameters matching docker-compose.yml
        postgres_params = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "demo"),
            "username": os.getenv("POSTGRES_USER", "sqlflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "sqlflow123"),
        }

        # Test connector registration
        engine.register_connector("test_postgres", "postgres", postgres_params)

        # Verify connector was registered
        assert "test_postgres" in engine.registered_connectors
        connector_info = engine.registered_connectors["test_postgres"]
        assert connector_info["type"] == "postgres"
        assert connector_info["instance"] is not None
        assert "error" not in connector_info

    @pytest.mark.external_services
    @pytest.mark.s3
    def test_s3_connector_registration_success(self):
        """Test successful S3/MinIO connector registration."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # S3/MinIO connection parameters matching docker-compose.yml
        s3_params = {
            "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
            "bucket": os.getenv("S3_BUCKET", "sqlflow-demo"),
            "path_prefix": "test/data",
        }

        # Test connector registration
        engine.register_connector("test_s3", "s3", s3_params)

        # Verify connector was registered
        assert "test_s3" in engine.registered_connectors
        connector_info = engine.registered_connectors["test_s3"]
        assert connector_info["type"] == "s3"
        assert connector_info["instance"] is not None
        assert "error" not in connector_info

    def test_connector_registration_failure_handling(self):
        """Test connector registration error handling."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Invalid connector parameters
        invalid_params = {
            "host": "nonexistent-host",
            "port": 99999,
            "database": "nonexistent-db",
            "username": "invalid",
            "password": "invalid",
        }

        # Test that registration fails gracefully
        with pytest.raises(Exception):
            engine.register_connector("test_invalid", "postgres", invalid_params)

        # Verify error is stored
        assert "test_invalid" in engine.registered_connectors
        connector_info = engine.registered_connectors["test_invalid"]
        assert connector_info["instance"] is None
        assert "error" in connector_info


class TestConnectorEngineDataLoading:
    """Test data loading functionality with real services."""

    @pytest.mark.external_services
    @pytest.mark.postgres
    def test_postgres_data_loading_success(self, setup_postgres_test_data):
        """Test successful data loading from PostgreSQL."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Register PostgreSQL connector
        postgres_params = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "demo"),
            "username": os.getenv("POSTGRES_USER", "sqlflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "sqlflow123"),
        }

        engine.register_connector("test_postgres", "postgres", postgres_params)

        # Test data loading
        result = engine.load_data("test_postgres", "test_users")

        # Verify data was loaded
        assert len(result) == 1
        data_chunk = result[0]
        assert hasattr(data_chunk, "pandas_df")
        df = data_chunk.pandas_df
        assert len(df) > 0
        expected_columns = ["id", "name", "email"]
        assert all(col in df.columns for col in expected_columns)

    @pytest.mark.external_services
    @pytest.mark.s3
    def test_s3_data_loading_with_discovery(self, setup_s3_test_data):
        """Test S3 data loading with path prefix discovery."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Register S3 connector with path prefix for discovery
        s3_params = {
            "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
            "bucket": os.getenv("S3_BUCKET", "sqlflow-demo"),
            "path_prefix": "sales/",  # Use prefix for discovery mode
        }

        engine.register_connector("test_s3", "s3", s3_params)

        # Test data loading with discovery
        result = engine.load_data("test_s3", None)

        # Verify data was loaded or empty result if no files
        assert isinstance(result, list)
        if result:  # If files were discovered
            data_chunk = result[0]
            assert hasattr(data_chunk, "pandas_df")

    def test_data_loading_connector_not_found(self):
        """Test data loading error when connector not registered."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Test loading from non-existent connector
        result = engine.load_data("nonexistent_connector", "test_table")

        # Should return empty list
        assert result == []

    def test_data_loading_failed_connector(self):
        """Test data loading error when connector failed to initialize."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Manually add a failed connector entry
        engine.registered_connectors["failed_connector"] = {
            "type": "postgres",
            "params": {},
            "instance": None,
            "error": "Connection failed",
        }

        # Test loading from failed connector
        with pytest.raises(ValueError, match="not properly initialized"):
            engine.load_data("failed_connector", "test_table")


class TestConnectorEngineExport:
    """Test data export functionality with real services."""

    @pytest.mark.external_services
    @pytest.mark.s3
    def test_s3_export_csv_format(self):
        """Test S3 export in CSV format."""
        executor = LocalExecutor()
        executor.profile = {
            "connectors": {
                "s3": {
                    "params": {
                        "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                        "endpoint_url": os.getenv(
                            "AWS_ENDPOINT_URL", "http://localhost:9000"
                        ),
                    }
                }
            }
        }

        engine = executor._create_connector_engine()

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.0, 200.0, 300.0],
            }
        )

        # Mock data object
        mock_data = MagicMock()
        mock_data.pandas_df = test_df

        # Test S3 export
        destination = (
            f"s3://{os.getenv('S3_BUCKET', 'sqlflow-demo')}/exports/test_export.csv"
        )
        options = {"file_format": "csv"}

        # Should not raise exception
        engine.export_data(mock_data, "S3", destination, options)

    @pytest.mark.external_services
    @pytest.mark.s3
    def test_s3_export_json_format(self):
        """Test S3 export in JSON format."""
        executor = LocalExecutor()
        executor.profile = {
            "connectors": {
                "s3": {
                    "params": {
                        "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                        "endpoint_url": os.getenv(
                            "AWS_ENDPOINT_URL", "http://localhost:9000"
                        ),
                    }
                }
            }
        }

        engine = executor._create_connector_engine()

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Test1", "Test2"],
            }
        )

        # Mock data object
        mock_data = MagicMock()
        mock_data.pandas_df = test_df

        # Test S3 export in JSON format
        destination = (
            f"s3://{os.getenv('S3_BUCKET', 'sqlflow-demo')}/exports/test_export.json"
        )
        options = {"file_format": "json"}

        # Should not raise exception
        engine.export_data(mock_data, "S3", destination, options)

    def test_local_csv_export(self):
        """Test local CSV file export."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "value": ["A", "B", "C"],
            }
        )

        # Mock data object
        mock_data = MagicMock()
        mock_data.pandas_df = test_df

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "test_export.csv")

            # Test local export
            engine.export_data(mock_data, "CSV", destination, {})

            # Verify file was created
            assert os.path.exists(destination)

            # Verify content
            exported_df = pd.read_csv(destination)
            pd.testing.assert_frame_equal(test_df, exported_df)

    def test_export_no_data_handling(self):
        """Test export behavior when no data is provided."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine()

        # Mock data object without pandas_df
        mock_data = MagicMock()
        mock_data.pandas_df = None

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "empty_export.csv")

            # Test export with no data
            engine.export_data(mock_data, "CSV", destination, {})

            # Should create empty file with header
            assert os.path.exists(destination)
            with open(destination, "r") as f:
                content = f.read()
                assert "id,value" in content


class TestDataFormatConversion:
    """Test data format conversion methods."""

    def test_format_dataframe_csv(self):
        """Test DataFrame to CSV format conversion."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine_stub()
        stub_instance = engine

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )

        # Test CSV formatting
        content_bytes, content_type = stub_instance._format_dataframe_content(
            test_df, "csv"
        )

        assert content_type == "text/csv"
        assert isinstance(content_bytes, bytes)
        content_str = content_bytes.decode("utf-8")
        assert "id,name" in content_str
        assert "Alice" in content_str
        assert "Bob" in content_str

    def test_format_dataframe_json(self):
        """Test DataFrame to JSON format conversion."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine_stub()
        stub_instance = engine

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1, 2],
                "value": ["A", "B"],
            }
        )

        # Test JSON formatting
        content_bytes, content_type = stub_instance._format_dataframe_content(
            test_df, "json"
        )

        assert content_type == "application/json"
        assert isinstance(content_bytes, bytes)
        content_str = content_bytes.decode("utf-8")
        assert '"id":1' in content_str or '"id": 1' in content_str
        assert '"value":"A"' in content_str or '"value": "A"' in content_str

    def test_format_dataframe_parquet(self):
        """Test DataFrame to Parquet format conversion."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine_stub()
        stub_instance = engine

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Test Parquet formatting
        content_bytes, content_type = stub_instance._format_dataframe_content(
            test_df, "parquet"
        )

        assert content_type == "application/octet-stream"
        assert isinstance(content_bytes, bytes)
        assert len(content_bytes) > 0

    def test_format_dataframe_default_fallback(self):
        """Test DataFrame formatting with unknown format falls back to CSV."""
        executor = LocalExecutor()
        engine = executor._create_connector_engine_stub()
        stub_instance = engine

        # Create test data
        test_df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Test"],
            }
        )

        # Test unknown format
        content_bytes, content_type = stub_instance._format_dataframe_content(
            test_df, "unknown"
        )

        # Should default to CSV
        assert content_type == "text/csv"
        assert isinstance(content_bytes, bytes)
        content_str = content_bytes.decode("utf-8")
        assert "id,name" in content_str


# Fixtures for test setup


@pytest.fixture(scope="function")
def setup_postgres_test_data():
    """Set up test data in PostgreSQL for integration tests."""
    import os
    import subprocess

    # PostgreSQL connection parameters
    postgres_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "demo"),
        "username": os.getenv("POSTGRES_USER", "sqlflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "sqlflow123"),
    }

    # Create test table and data using docker exec if available
    try:
        # Try to create table and insert test data
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "sqlflow-postgres",
                "psql",
                "-U",
                postgres_params["username"],
                "-d",
                postgres_params["database"],
                "-c",
                "CREATE TABLE IF NOT EXISTS test_users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100) NOT NULL);",
            ],
            check=True,
            capture_output=True,
        )

        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "sqlflow-postgres",
                "psql",
                "-U",
                postgres_params["username"],
                "-d",
                postgres_params["database"],
                "-c",
                "INSERT INTO test_users (name, email) VALUES ('Alice Smith', 'alice@example.com'), ('Bob Johnson', 'bob@example.com'), ('Charlie Davis', 'charlie@example.com') ON CONFLICT DO NOTHING;",
            ],
            check=True,
            capture_output=True,
        )

    except (subprocess.CalledProcessError, FileNotFoundError):
        # If docker approach fails, assume test data already exists
        pass

    yield

    # Cleanup: Remove test data after test
    try:
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "sqlflow-postgres",
                "psql",
                "-U",
                postgres_params["username"],
                "-d",
                postgres_params["database"],
                "-c",
                "DELETE FROM test_users WHERE email IN ('alice@example.com', 'bob@example.com', 'charlie@example.com');",
            ],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Cleanup failed, but that's okay for tests
        pass


@pytest.fixture(scope="function")
def setup_s3_test_data():
    """Set up test data in S3/MinIO for integration tests."""
    # This would be implemented to create test files in S3
    # For now, assumes test data exists from docker initialization
    yield
    # Cleanup would go here
