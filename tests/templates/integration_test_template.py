"""Template for integration tests following SQLFlow testing standards.

Use this template when creating new integration tests that verify
component interactions and real system behavior.

Integration tests should:
- Test multiple components working together
- Use real implementations (minimize mocking)
- Verify end-to-end scenarios users will perform
- Test actual data flows between components
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqlflow.core.executors import get_executor


class TestComponentIntegration:
    """Integration tests for [COMPONENT_NAME] functionality.

    These tests verify that [COMPONENT_NAME] correctly integrates with
    the rest of the SQLFlow system and handles real user scenarios.
    """

    def test_basic_functionality_integration(self, integration_executor):
        """Test basic [COMPONENT_NAME] functionality in realistic scenario.

        This test verifies the core behavior that users depend on:
        - [KEY_BEHAVIOR_1]
        - [KEY_BEHAVIOR_2]
        - [KEY_BEHAVIOR_3]
        """
        # Arrange: Set up realistic test data
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.0, 200.0, 300.0],
            }
        )

        # Act: Execute the functionality being tested
        plan = [
            {
                "type": "transform",
                "id": "test_operation",
                "name": "result_table",
                "query": "SELECT * FROM test_data WHERE value > 150",
            }
        ]

        result = integration_executor.execute(plan)

        # Assert: Verify expected behavior
        assert result["status"] == "success"
        result_data = integration_executor.table_data["result_table"]
        assert len(result_data) == 2  # Bob and Charlie
        assert "Alice" not in result_data["name"].values

    def test_error_handling_integration(self, integration_executor):
        """Test error handling in realistic failure scenarios.

        Users should receive clear, actionable error messages when
        operations fail in predictable ways.
        """
        # Arrange: Set up scenario that should fail
        plan = [
            {
                "type": "transform",
                "id": "failing_operation",
                "name": "result",
                "query": "SELECT * FROM nonexistent_table",
            }
        ]

        # Act: Execute operation that should fail
        result = integration_executor.execute(plan)

        # Assert: Verify graceful error handling
        assert result["status"] == "failed"
        assert "error" in result
        assert "nonexistent_table" in str(result["error"]).lower()

    def test_complex_scenario_integration(self, integration_executor):
        """Test complex multi-step scenario that mirrors real usage.

        This test validates the complete workflow that users would
        perform in production scenarios.
        """
        # Arrange: Set up complex multi-table scenario
        customers = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "tier": ["premium", "standard", "premium"],
            }
        )

        orders = pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104],
                "customer_id": [1, 1, 2, 3],
                "amount": [250.0, 150.0, 300.0, 100.0],
            }
        )

        integration_executor.table_data["customers"] = customers
        integration_executor.table_data["orders"] = orders

        # Act: Execute multi-step pipeline
        plan = [
            {
                "type": "transform",
                "id": "step1_customer_summary",
                "name": "customer_summary",
                "query": """
                    SELECT 
                        c.customer_id,
                        c.name,
                        c.tier,
                        COUNT(o.order_id) as order_count,
                        SUM(o.amount) as total_amount
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id  
                    GROUP BY c.customer_id, c.name, c.tier
                """,
            },
            {
                "type": "transform",
                "id": "step2_premium_analysis",
                "name": "premium_analysis",
                "query": """
                    SELECT 
                        name,
                        order_count,
                        total_amount,
                        CASE 
                            WHEN total_amount > 300 THEN 'high_value'
                            WHEN total_amount > 100 THEN 'medium_value' 
                            ELSE 'low_value'
                        END as value_segment
                    FROM customer_summary
                    WHERE tier = 'premium'
                """,
            },
        ]

        result = integration_executor.execute(plan)

        # Assert: Verify complex operation results
        assert result["status"] == "success"

        # Verify step 1 results
        summary = integration_executor.table_data["customer_summary"]
        assert len(summary) == 3
        alice_summary = summary[summary["name"] == "Alice"].iloc[0]
        assert alice_summary["order_count"] == 2
        assert alice_summary["total_amount"] == 400.0

        # Verify step 2 results
        premium_analysis = integration_executor.table_data["premium_analysis"]
        assert len(premium_analysis) == 2  # Alice and Charlie are premium
        alice_analysis = premium_analysis[premium_analysis["name"] == "Alice"].iloc[0]
        assert alice_analysis["value_segment"] == "high_value"


@pytest.fixture
def integration_executor():
    """Provide a LocalExecutor configured for integration testing.

    Returns a clean executor instance for each test with proper
    isolation and realistic configuration.
    """
    return get_executor()


@pytest.fixture
def temp_project_structure():
    """Create a temporary project directory structure for testing.

    Provides a realistic project layout that matches what users
    would have in production environments.
    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_integration_test_") as tmp_dir:
        project_paths = {
            "project_dir": tmp_dir,
            "data_dir": Path(tmp_dir) / "data",
            "output_dir": Path(tmp_dir) / "output",
            "pipelines_dir": Path(tmp_dir) / "pipelines",
            "profiles_dir": Path(tmp_dir) / "profiles",
        }

        # Create directory structure
        for path in project_paths.values():
            if isinstance(path, Path):
                path.mkdir(exist_ok=True)

        yield project_paths


@pytest.fixture
def sample_dataset():
    """Provide realistic sample data for testing.

    Data should mirror the types and patterns found in real
    production scenarios.
    """
    return {
        "customers": pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "name": [
                    "Alice Johnson",
                    "Bob Smith",
                    "Charlie Brown",
                    "Diana Prince",
                    "Eve Wilson",
                ],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                    "diana@example.com",
                    "eve@example.com",
                ],
                "registration_date": pd.to_datetime(
                    [
                        "2023-01-15",
                        "2023-02-10",
                        "2023-03-05",
                        "2023-04-20",
                        "2023-05-12",
                    ]
                ),
                "status": ["active", "active", "inactive", "active", "active"],
            }
        ),
        "orders": pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104, 105, 106],
                "customer_id": [1, 1, 2, 3, 4, 5],
                "amount": [250.50, 175.25, 300.00, 125.75, 450.00, 200.00],
                "order_date": pd.to_datetime(
                    [
                        "2023-06-01",
                        "2023-06-15",
                        "2023-06-10",
                        "2023-06-20",
                        "2023-06-25",
                        "2023-06-30",
                    ]
                ),
                "status": [
                    "completed",
                    "completed",
                    "pending",
                    "completed",
                    "completed",
                    "cancelled",
                ],
            }
        ),
    }


# Template Usage Instructions:
#
# 1. Copy this template to create new integration test files
# 2. Replace [COMPONENT_NAME] with the actual component being tested
# 3. Replace [KEY_BEHAVIOR_X] with specific behaviors being verified
# 4. Customize test data to match your component's requirements
# 5. Focus on testing real user scenarios and component interactions
# 6. Use descriptive test names that explain the behavior being tested
# 7. Ensure tests are self-contained and don't depend on external state
# 8. Follow the Arrange-Act-Assert pattern for clarity
# 9. Test both positive scenarios and error conditions
# 10. Document any special setup or assumptions in test docstrings
