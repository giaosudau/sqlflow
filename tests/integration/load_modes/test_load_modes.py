"""Integration tests for load modes (REPLACE, APPEND, UPSERT) in SQLFlow."""

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    users_data = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "name": ["John Doe", "Jane Smith", "Bob Johnson"],
            "email": ["john@example.com", "jane@example.com", "bob@example.com"],
            "is_active": [True, True, False],
        }
    )

    users_updates = pd.DataFrame(
        {
            "user_id": [2, 3, 4],  # 2,3 exist, 4 is new
            "name": ["Jane Updated", "Bob Updated", "Alice New"],
            "email": [
                "jane.new@example.com",
                "bob.new@example.com",
                "alice@example.com",
            ],
            "is_active": [False, True, True],
        }
    )

    users_incompatible = pd.DataFrame(
        {
            "user_id": [5, 6],
            "name": ["Charlie", "Diana"],
            "email": ["charlie@example.com", "diana@example.com"],
            "is_active": [True, False],
            "extra_column": [
                "extra1",
                "extra2",
            ],
        }
    )

    return {
        "users_data": users_data,
        "users_updates": users_updates,
        "users_incompatible": users_incompatible,
    }


def test_upsert_mode(v2_pipeline_runner, sample_data, tmp_path: Path):
    """Test V2 coordinator executes UPSERT mode load operation successfully."""
    users_csv_path = tmp_path / "users_data.csv"
    updates_csv_path = tmp_path / "updates.csv"
    sample_data["users_data"].to_csv(users_csv_path, index=False)
    sample_data["users_updates"].to_csv(updates_csv_path, index=False)

    pipeline = {
        "steps": [
            {
                "type": "load",
                "id": "initial_load",
                "source": str(users_csv_path),
                "target_table": "users_target",
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "upsert_load",
                "source": str(updates_csv_path),
                "target_table": "users_target",
                "load_mode": "upsert",
                "upsert_keys": ["user_id"],
            },
        ]
    }

    coordinator = v2_pipeline_runner(pipeline["steps"])

    assert coordinator.result.success
    result_df = coordinator.context.engine.execute_query(
        "SELECT * FROM users_target ORDER BY user_id"
    ).df()
    assert len(result_df) == 4
    assert result_df[result_df.user_id == 2]["name"].iloc[0] == "Jane Updated"
    assert result_df[result_df.user_id == 4]["name"].iloc[0] == "Alice New"


def test_schema_compatibility_validation(
    v2_pipeline_runner, sample_data, tmp_path: Path
):
    """Test schema compatibility validation in V2 executor."""
    users_csv_path = tmp_path / "users.csv"
    incompatible_csv_path = tmp_path / "incompatible.csv"
    sample_data["users_data"].to_csv(users_csv_path, index=False)
    sample_data["users_incompatible"].to_csv(incompatible_csv_path, index=False)

    pipeline = {
        "steps": [
            {
                "type": "load",
                "id": "initial_load",
                "source": str(users_csv_path),
                "target_table": "users_target",
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "append_incompatible",
                "source": str(incompatible_csv_path),
                "target_table": "users_target",
                "load_mode": "append",
            },
        ]
    }
    coordinator = v2_pipeline_runner(pipeline["steps"])
    assert not coordinator.result.success
    assert (
        "binder error" in str(coordinator.result.step_results[1].error_message).lower()
    )


def test_upsert_key_validation(v2_pipeline_runner, sample_data, tmp_path: Path):
    """Test validation for missing upsert keys in V2 executor."""
    users_csv_path = tmp_path / "users.csv"
    sample_data["users_data"].to_csv(users_csv_path, index=False)

    pipeline = {
        "steps": [
            {
                "type": "load",
                "id": "upsert_missing_keys",
                "source": str(users_csv_path),
                "target_table": "users_target",
                "load_mode": "upsert",
            }
        ]
    }
    with pytest.raises(ValueError, match="UPSERT mode requires upsert_keys"):
        v2_pipeline_runner(pipeline["steps"])


def test_full_pipeline_with_load_modes(v2_pipeline_runner, sample_data, tmp_path: Path):
    """Test a full V2 pipeline with various load modes."""
    users_path = tmp_path / "users.csv"
    updates_path = tmp_path / "updates.csv"
    new_data_path = tmp_path / "new_data.csv"

    sample_data["users_data"].to_csv(users_path, index=False)
    sample_data["users_updates"].to_csv(updates_path, index=False)
    upsert_data = pd.DataFrame(
        {
            "user_id": [3, 4, 5],
            "name": ["Bob Upserted", "Alice Upserted", "Chris New"],
            "email": [
                "bob.upsert@example.com",
                "alice.upsert@example.com",
                "chris@example.com",
            ],
            "is_active": [True, False, True],
        }
    )
    upsert_data.to_csv(new_data_path, index=False)

    pipeline = {
        "steps": [
            {
                "type": "load",
                "id": "replace_step",
                "target_table": "users_target",
                "source": str(users_path),
                "load_mode": "replace",
            },
            {
                "type": "load",
                "id": "upsert_step",
                "target_table": "users_target",
                "source": str(updates_path),
                "load_mode": "upsert",
                "upsert_keys": ["user_id"],
            },
            {
                "type": "load",
                "id": "upsert_step_2",
                "target_table": "users_target",
                "source": str(new_data_path),
                "load_mode": "upsert",
                "upsert_keys": ["user_id"],
            },
        ]
    }
    coordinator = v2_pipeline_runner(pipeline["steps"])

    assert coordinator.result.success

    final_df = coordinator.context.engine.execute_query(
        "SELECT * FROM users_target ORDER BY user_id"
    ).df()
    assert len(final_df) == 5
    assert final_df["user_id"].tolist() == [1, 2, 3, 4, 5]
    assert final_df[final_df.user_id == 1]["name"].iloc[0] == "John Doe"
    assert final_df[final_df.user_id == 3]["name"].iloc[0] == "Bob Upserted"
    assert final_df[final_df.user_id == 5]["name"].iloc[0] == "Chris New"
