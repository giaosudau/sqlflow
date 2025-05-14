"""DuckDB state backend for SQLFlow execution state persistence."""

import json
import logging
import os
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import duckdb

from sqlflow.core.executors.task_status import TaskState, TaskStatus

logger = logging.getLogger(__name__)


class DuckDBStateBackend:
    """DuckDB state backend for SQLFlow execution state persistence."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize a DuckDBStateBackend.

        Args:
            db_path: Path to the DuckDB database file.
                     Defaults to ~/.sqlflow/state.db.
        """
        if db_path is None:
            home_dir = os.path.expanduser("~")
            sqlflow_dir = os.path.join(home_dir, ".sqlflow")
            os.makedirs(sqlflow_dir, exist_ok=True)
            db_path = os.path.join(sqlflow_dir, "state.db")

        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_tables()

    def _initialize_tables(self) -> None:
        """Initialize the database tables."""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_runs (
                run_id VARCHAR PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status VARCHAR,
                metadata JSON
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_state (
                run_id VARCHAR,
                task_id VARCHAR,
                state VARCHAR,
                unmet_dependencies INTEGER,
                dependencies JSON,
                attempts INTEGER,
                error VARCHAR,
                start_time DOUBLE,
                end_time DOUBLE,
                metadata JSON,
                PRIMARY KEY (run_id, task_id),
                FOREIGN KEY (run_id) REFERENCES execution_runs(run_id)
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_plan (
                run_id VARCHAR,
                plan_json JSON,
                PRIMARY KEY (run_id),
                FOREIGN KEY (run_id) REFERENCES execution_runs(run_id)
            )
            """
        )

    def create_run(
        self, run_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a new execution run.

        Args:
            run_id: Unique identifier for the run
            metadata: Additional metadata for the run
        """
        metadata_json = json.dumps(metadata or {})
        self.conn.execute(
            """
            INSERT INTO execution_runs (run_id, start_time, status, metadata)
            VALUES (?, CURRENT_TIMESTAMP, 'RUNNING', ?)
            """,
            [run_id, metadata_json],
        )

    def save_plan(self, run_id: str, plan: List[Dict[str, Any]]) -> None:
        """Save an execution plan.

        Args:
            run_id: Unique identifier for the run
            plan: Execution plan to save
        """
        plan_json = json.dumps(plan)
        self.conn.execute(
            """
            INSERT INTO execution_plan (run_id, plan_json)
            VALUES (?, ?)
            ON CONFLICT (run_id) DO UPDATE SET plan_json = excluded.plan_json
            """,
            [run_id, plan_json],
        )

    def load_plan(self, run_id: str) -> Optional[List[Dict[str, Any]]]:
        """Load an execution plan.

        Args:
            run_id: Unique identifier for the run

        Returns:
            Execution plan or None if not found
        """
        result = self.conn.execute(
            """
            SELECT plan_json FROM execution_plan WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()

        if result is None:
            return None

        return json.loads(result[0])

    def save_task_status(self, run_id: str, task_status: TaskStatus) -> None:
        """Save a task status.

        Args:
            run_id: Unique identifier for the run
            task_status: Task status to save
        """
        task_dict = asdict(task_status)
        dependencies_json = json.dumps(task_dict["dependencies"])
        metadata_json = json.dumps(task_dict.get("metadata", {}))

        self.conn.execute(
            """
            INSERT INTO execution_state (
                run_id, task_id, state, unmet_dependencies, dependencies,
                attempts, error, start_time, end_time, metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, task_id) DO UPDATE SET
                state = excluded.state,
                unmet_dependencies = excluded.unmet_dependencies,
                dependencies = excluded.dependencies,
                attempts = excluded.attempts,
                error = excluded.error,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                metadata = excluded.metadata
            """,
            [
                run_id,
                task_dict["id"],
                task_dict["state"],
                task_dict["unmet_dependencies"],
                dependencies_json,
                task_dict["attempts"],
                task_dict["error"],
                task_dict["start_time"],
                task_dict["end_time"],
                metadata_json,
            ],
        )

    def load_task_statuses(self, run_id: str) -> Dict[str, TaskStatus]:
        """Load task statuses for a run.

        Args:
            run_id: Unique identifier for the run

        Returns:
            Dict mapping task IDs to TaskStatus objects
        """
        result = self.conn.execute(
            """
            SELECT
                task_id, state, unmet_dependencies, dependencies,
                attempts, error, start_time, end_time, metadata
            FROM execution_state
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchall()

        task_statuses = {}
        for row in result:
            (
                task_id,
                state,
                unmet_dependencies,
                dependencies_json,
                attempts,
                error,
                start_time,
                end_time,
                metadata_json,
            ) = row

            dependencies = json.loads(dependencies_json)
            json.loads(metadata_json)

            task_status = TaskStatus(
                id=task_id,
                state=TaskState(state),
                unmet_dependencies=unmet_dependencies,
                dependencies=dependencies,
                attempts=attempts,
                error=error,
                start_time=start_time,
                end_time=end_time,
            )
            task_statuses[task_id] = task_status

        return task_statuses

    def update_run_status(self, run_id: str, status: str) -> None:
        """Update the status of a run.

        Args:
            run_id: Unique identifier for the run
            status: New status of the run
        """
        self.conn.execute(
            """
            UPDATE execution_runs
            SET status = ?, end_time = CASE WHEN ? IN ('SUCCESS', 'FAILED') THEN CURRENT_TIMESTAMP ELSE NULL END
            WHERE run_id = ?
            """,
            [status, status, run_id],
        )

    def get_run_status(self, run_id: str) -> Optional[str]:
        """Get the status of a run.

        Args:
            run_id: Unique identifier for the run

        Returns:
            Status of the run or None if not found
        """
        result = self.conn.execute(
            """
            SELECT status FROM execution_runs WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()

        if result is None:
            return None

        return result[0]

    def list_runs(self) -> List[Dict[str, Any]]:
        """List all execution runs.

        Returns:
            List of execution runs
        """
        result = self.conn.execute(
            """
            SELECT run_id, start_time, end_time, status, metadata
            FROM execution_runs
            ORDER BY start_time DESC
            """
        ).fetchall()

        runs = []
        for row in result:
            run_id, start_time, end_time, status, metadata_json = row
            metadata = json.loads(metadata_json)
            runs.append(
                {
                    "run_id": run_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "status": status,
                    "metadata": metadata,
                }
            )

        return runs

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
