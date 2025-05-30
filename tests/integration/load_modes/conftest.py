"""Load modes specific fixtures for integration tests."""

import pandas as pd
import pytest

from sqlflow.parser.ast import LoadStep


@pytest.fixture(scope="session")
def sample_users_updates() -> pd.DataFrame:
    """Create sample user update data for merge testing.

    Returns:
        DataFrame with user updates for testing merge operations
    """
    return pd.DataFrame(
        {
            "user_id": [2, 3, 4],  # 2,3 exist, 4 is new
            "name": ["Jane Smith Updated", "Bob Johnson Updated", "New User"],
            "email": [
                "jane.updated@example.com",
                "bob.updated@example.com",
                "new@example.com",
            ],
            "is_active": [True, True, True],
            "age": [31, 36, 25],
            "signup_date": ["2023-02-20", "2023-03-10", "2023-06-15"],
        }
    )


@pytest.fixture(scope="session")
def sample_orders_updates() -> pd.DataFrame:
    """Create sample order update data for merge testing.

    Returns:
        DataFrame with order updates for testing merge operations
    """
    return pd.DataFrame(
        {
            "order_id": [103, 104, 106],  # 103,104 exist, 106 is new
            "user_id": [1, 3, 5],
            "product_name": ["Keyboard Updated", "Monitor Updated", "New Product"],
            "price": [85.00, 319.99, 199.99],
            "quantity": [2, 1, 3],
            "order_date": ["2023-06-03", "2023-06-04", "2023-06-06"],
        }
    )


@pytest.fixture(scope="function")
def load_step_replace() -> LoadStep:
    """Create a REPLACE load step for testing.

    Returns:
        LoadStep configured for REPLACE mode
    """
    return LoadStep(
        table_name="target_table",
        source_name="source_table",
        mode="REPLACE",
    )


@pytest.fixture(scope="function")
def load_step_append() -> LoadStep:
    """Create an APPEND load step for testing.

    Returns:
        LoadStep configured for APPEND mode
    """
    return LoadStep(
        table_name="target_table",
        source_name="source_table",
        mode="APPEND",
    )


@pytest.fixture(scope="function")
def load_step_merge() -> LoadStep:
    """Create a MERGE load step for testing.

    Returns:
        LoadStep configured for MERGE mode with user_id as merge key
    """
    return LoadStep(
        table_name="target_table",
        source_name="source_table",
        mode="MERGE",
        merge_keys=["user_id"],
    )
