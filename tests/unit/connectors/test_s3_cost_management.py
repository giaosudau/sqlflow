"""Tests for S3 connector cost management features."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from sqlflow.connectors.base import ConnectorState
from sqlflow.connectors.s3_connector import (
    CostLimitError,
    S3Connector,
    S3CostManager,
    S3Object,
)


class TestS3CostManager:
    """Test S3 cost management functionality."""

    def test_cost_manager_initialization(self):
        """Test cost manager initialization."""
        manager = S3CostManager(cost_limit_usd=10.0)
        assert manager.cost_limit_usd == 10.0
        assert manager.current_cost == 0.0
        assert manager.data_scanned_gb == 0.0
        assert manager.requests_made == 0

    def test_cost_estimation_accuracy(self):
        """Test cost estimation accuracy."""
        manager = S3CostManager(cost_limit_usd=100.0)

        # Create test files with known sizes
        files = [
            S3Object(
                key="file1.csv",
                size=1024 * 1024 * 1024,  # 1 GB
                last_modified=datetime.now(),
                e_tag="etag1",
            ),
            S3Object(
                key="file2.csv",
                size=512 * 1024 * 1024,  # 0.5 GB
                last_modified=datetime.now(),
                e_tag="etag2",
            ),
        ]

        cost_breakdown = manager.estimate_cost(files)

        # Verify cost calculation
        expected_data_cost = 1.5 * S3CostManager.COST_PER_GB_REQUEST  # 1.5 GB
        expected_request_cost = (
            2 * S3CostManager.COST_PER_1000_REQUESTS / 1000
        )  # 2 requests
        expected_total = expected_data_cost + expected_request_cost

        assert abs(cost_breakdown["data_cost_usd"] - expected_data_cost) < 0.0001
        assert abs(cost_breakdown["request_cost_usd"] - expected_request_cost) < 0.0001
        assert abs(cost_breakdown["estimated_cost_usd"] - expected_total) < 0.0001
        assert cost_breakdown["data_size_gb"] == 1.5
        assert cost_breakdown["num_requests"] == 2

    def test_cost_limit_enforcement(self):
        """Test cost limit enforcement."""
        manager = S3CostManager(cost_limit_usd=1.0)

        # Test within limit
        assert manager.check_cost_limit(0.5) is True

        # Test exactly at limit
        assert manager.check_cost_limit(1.0) is True

        # Test exceeding limit
        assert manager.check_cost_limit(1.5) is False

    def test_cost_tracking(self):
        """Test real-time cost tracking."""
        manager = S3CostManager(cost_limit_usd=10.0)

        # Track first operation
        manager.track_operation(
            data_size_bytes=1024**3, num_requests=1
        )  # 1 GB, 1 request

        metrics = manager.get_metrics()
        assert metrics["data_scanned_gb"] == 1.0
        assert metrics["requests_made"] == 1
        assert metrics["current_cost_usd"] > 0

        # Track second operation
        manager.track_operation(
            data_size_bytes=512 * 1024**2, num_requests=2
        )  # 0.5 GB, 2 requests

        metrics = manager.get_metrics()
        assert metrics["data_scanned_gb"] == 1.5
        assert metrics["requests_made"] == 3
        assert metrics["current_cost_usd"] > 0

    def test_cost_metrics(self):
        """Test cost metrics reporting."""
        manager = S3CostManager(cost_limit_usd=50.0)

        # Track some operations
        manager.track_operation(data_size_bytes=2 * 1024**3, num_requests=5)

        metrics = manager.get_metrics()

        required_fields = [
            "cost_limit_usd",
            "current_cost_usd",
            "remaining_budget_usd",
            "data_scanned_gb",
            "requests_made",
            "cost_per_gb",
        ]

        for field in required_fields:
            assert field in metrics

        assert metrics["cost_limit_usd"] == 50.0
        assert metrics["data_scanned_gb"] == 2.0
        assert metrics["requests_made"] == 5
        assert metrics["remaining_budget_usd"] > 0


class TestS3ConnectorCostManagement:
    """Test S3 connector cost management integration."""

    @pytest.fixture
    def s3_connector(self):
        """Create S3 connector with cost management."""
        connector = S3Connector()
        return connector

    @pytest.fixture
    def cost_config(self):
        """Cost management configuration."""
        return {
            "bucket": "test-bucket",
            "cost_limit_usd": 5.0,
            "max_files_per_run": 100,
            "max_data_size_gb": 10.0,
            "dev_sampling": 0.1,
            "dev_max_files": 10,
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
        }

    def test_cost_management_configuration(self, s3_connector, cost_config):
        """Test cost management configuration."""
        s3_connector.configure(cost_config)

        assert s3_connector.state == ConnectorState.CONFIGURED
        assert s3_connector.cost_manager.cost_limit_usd == 5.0
        assert hasattr(s3_connector, "params")
        assert s3_connector.params["cost_limit_usd"] == 5.0

    @patch("boto3.Session")
    def test_cost_limit_enforcement_in_discovery(
        self, mock_session, s3_connector, cost_config
    ):
        """Test cost limit enforcement during discovery."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client

        # Mock large files that would exceed cost limit
        large_objects = [
            {
                "Key": f"large_file_{i}.csv",
                "Size": 10 * 1024**3,  # 10 GB each
                "LastModified": datetime.now(),
                "ETag": f"etag{i}",
            }
            for i in range(10)  # 100 GB total
        ]

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": large_objects}]
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_s3_client.head_bucket.return_value = {}

        s3_connector.configure(cost_config)

        # Discovery should stop early due to cost/size limits and apply sampling
        discovered_objects = s3_connector.discover()

        # Should discover fewer objects due to limits and sampling
        assert len(discovered_objects) < len(large_objects)
        assert len(discovered_objects) <= cost_config.get("dev_max_files", 10)

    @patch("boto3.Session")
    def test_development_sampling(self, mock_session, s3_connector, cost_config):
        """Test development sampling reduces costs."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client

        # Mock 100 small files
        objects = [
            {
                "Key": f"file_{i}.csv",
                "Size": 1024 * 1024,  # 1 MB each
                "LastModified": datetime.now(),
                "ETag": f"etag{i}",
            }
            for i in range(100)
        ]

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_s3_client.head_bucket.return_value = {}

        s3_connector.configure(cost_config)
        discovered_objects = s3_connector.discover()

        # With 10% sampling and max 10 files, should get at most 10 files
        assert len(discovered_objects) <= 10
        assert len(discovered_objects) <= len(objects) * 0.15  # Allow some variance

    @patch("boto3.Session")
    def test_cost_estimation_before_processing(
        self, mock_session, s3_connector, cost_config
    ):
        """Test cost estimation before processing."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.head_bucket.return_value = {}

        s3_connector.configure(cost_config)

        # Create test files for cost estimation
        files = [
            S3Object(
                key="test1.csv",
                size=100 * 1024 * 1024,  # 100 MB
                last_modified=datetime.now(),
                e_tag="etag1",
            ),
            S3Object(
                key="test2.csv",
                size=200 * 1024 * 1024,  # 200 MB
                last_modified=datetime.now(),
                e_tag="etag2",
            ),
        ]

        cost_breakdown = s3_connector.cost_manager.estimate_cost(files)

        # Verify estimation provides detailed breakdown
        assert "estimated_cost_usd" in cost_breakdown
        assert "data_cost_usd" in cost_breakdown
        assert "request_cost_usd" in cost_breakdown
        assert "data_size_gb" in cost_breakdown
        assert abs(cost_breakdown["data_size_gb"] - 0.3) < 0.01  # ~300 MB = ~0.3 GB

    def test_parameter_validation_for_cost_management(self, s3_connector):
        """Test parameter validation for cost management features."""
        # Test valid cost parameters
        valid_config = {
            "bucket": "test-bucket",
            "cost_limit_usd": "10.5",  # String should be converted
            "max_files_per_run": "1000",  # String should be converted
            "max_data_size_gb": "50.0",  # String should be converted
        }

        s3_connector.configure(valid_config)
        assert s3_connector.cost_manager.cost_limit_usd == 10.5

        # Test invalid cost parameters
        invalid_configs = [
            {"bucket": "test", "cost_limit_usd": "invalid"},
            {"bucket": "test", "max_files_per_run": "not_a_number"},
            {"bucket": "test", "dev_sampling": "2.0"},  # > 1.0
            {"bucket": "test", "dev_sampling": "0"},  # <= 0
        ]

        for invalid_config in invalid_configs:
            with pytest.raises(Exception):  # Should raise ParameterError
                fresh_connector = S3Connector()
                fresh_connector.configure(invalid_config)

    @patch("boto3.Session")
    def test_cost_tracking_integration(self, mock_session, s3_connector, cost_config):
        """Test cost tracking integration with actual operations."""
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.head_bucket.return_value = {}

        # Mock file content for reading
        mock_body = MagicMock()
        mock_body.read.return_value = b"id,name\n1,Alice\n2,Bob"
        mock_s3_client.get_object.return_value = {"Body": mock_body}
        mock_s3_client.head_object.return_value = {"ContentLength": 1024}

        s3_connector.configure(cost_config)

        # Track initial cost
        initial_metrics = s3_connector.cost_manager.get_metrics()
        initial_cost = initial_metrics["current_cost_usd"]

        # Perform read operation (should track costs)
        list(s3_connector.read("test.csv"))

        # Verify cost was tracked
        final_metrics = s3_connector.cost_manager.get_metrics()
        assert final_metrics["current_cost_usd"] > initial_cost
        assert final_metrics["requests_made"] > 0
        assert final_metrics["data_scanned_gb"] > 0

    def test_cost_management_error_messages(self, s3_connector):
        """Test clear error messages for cost management violations."""
        config = {
            "bucket": "test-bucket",
            "cost_limit_usd": 0.001,  # Very low limit
        }

        s3_connector.configure(config)

        # Create files that would exceed cost limit
        large_files = [
            S3Object(
                key="huge_file.csv",
                size=100 * 1024**3,  # 100 GB
                last_modified=datetime.now(),
                e_tag="etag1",
            )
        ]

        try:
            cost_breakdown = s3_connector.cost_manager.estimate_cost(large_files)
            s3_connector.cost_manager.check_cost_limit(
                cost_breakdown["estimated_cost_usd"]
            )
        except CostLimitError as e:
            assert "exceeds limit" in str(e)
            assert "$" in str(e)  # Should show dollar amounts


if __name__ == "__main__":
    pytest.main([__file__])
