"""State Management for V2 Executor.

Provides execution state persistence and resume capabilities:
- Pipeline execution state tracking
- Task status persistence
- Resume point management
- Failure recovery state

Following the Zen of Python: "In the face of ambiguity, refuse the temptation to guess."
"""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlflow.core.executors.v2.parallel_strategy import TaskState, TaskStatus
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ExecutionState:
    """
    Immutable execution state snapshot.

    Following Kent Beck's simple design:
    State should be serializable and obvious.
    """

    run_id: str
    status: str  # "RUNNING", "SUCCESS", "FAILED", "PAUSED"
    start_time: datetime
    end_time: Optional[datetime] = None
    current_step: Optional[str] = None
    total_steps: int = 0
    completed_steps: int = 0
    failed_step: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data["start_time"] = self.start_time.isoformat()
        if self.end_time:
            data["end_time"] = self.end_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionState":
        """Create from dictionary (deserialization)."""
        # Convert ISO strings back to datetime objects
        data["start_time"] = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = datetime.fromisoformat(data["end_time"])
        return cls(**data)


@dataclass
class PipelineStateSnapshot:
    """Complete pipeline execution snapshot for resume capability."""

    execution_state: ExecutionState
    task_statuses: Dict[str, TaskStatus]
    completed_results: Dict[str, StepExecutionResult]
    plan: List[Dict[str, Any]]
    variables: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "execution_state": self.execution_state.to_dict(),
            "task_statuses": {
                step_id: {
                    "step_id": status.step_id,
                    "state": status.state.value,
                    "start_time": (
                        status.start_time.isoformat() if status.start_time else None
                    ),
                    "end_time": (
                        status.end_time.isoformat() if status.end_time else None
                    ),
                    "attempts": status.attempts,
                    "dependencies": list(status.dependencies),
                    "error_message": status.error_message,
                }
                for step_id, status in self.task_statuses.items()
            },
            "completed_results": {
                step_id: asdict(result)
                for step_id, result in self.completed_results.items()
            },
            "plan": self.plan,
            "variables": self.variables,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineStateSnapshot":
        """Create from dictionary (deserialization)."""
        # Reconstruct execution state
        execution_state = ExecutionState.from_dict(data["execution_state"])

        # Reconstruct task statuses
        task_statuses = {}
        for step_id, status_data in data["task_statuses"].items():
            task_statuses[step_id] = TaskStatus(
                step_id=status_data["step_id"],
                state=TaskState(status_data["state"]),
                start_time=(
                    datetime.fromisoformat(status_data["start_time"])
                    if status_data["start_time"]
                    else None
                ),
                end_time=(
                    datetime.fromisoformat(status_data["end_time"])
                    if status_data["end_time"]
                    else None
                ),
                attempts=status_data["attempts"],
                dependencies=set(status_data["dependencies"]),
                error_message=status_data["error_message"],
            )

        # Reconstruct completed results
        completed_results = {}
        for step_id, result_data in data["completed_results"].items():
            # Convert datetime strings back to datetime objects
            if result_data.get("start_time"):
                result_data["start_time"] = datetime.fromisoformat(
                    result_data["start_time"]
                )
            if result_data.get("end_time"):
                result_data["end_time"] = datetime.fromisoformat(
                    result_data["end_time"]
                )

            completed_results[step_id] = StepExecutionResult(**result_data)

        return cls(
            execution_state=execution_state,
            task_statuses=task_statuses,
            completed_results=completed_results,
            plan=data["plan"],
            variables=data.get("variables"),
        )


class StateManager:
    """
    Manages execution state persistence and resume functionality.

    Following Martin Fowler's Repository pattern:
    Encapsulates state storage and retrieval logic.
    """

    def __init__(self, state_dir: Optional[Path] = None):
        """
        Initialize state manager.

        Args:
            state_dir: Directory for state files (defaults to .sqlflow/state)
        """
        self.state_dir = state_dir or Path.cwd() / ".sqlflow" / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"State manager initialized: {self.state_dir}")

    def save_execution_state(self, snapshot: PipelineStateSnapshot) -> None:
        """
        Save execution state to persistent storage.

        Following the principle of "Errors should never pass silently":
        All I/O errors are logged and re-raised.
        """
        state_file = self.state_dir / f"{snapshot.execution_state.run_id}.json"

        try:
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(snapshot.to_dict(), f, indent=2, ensure_ascii=False)

            logger.debug(f"Saved execution state: {state_file}")

        except Exception as e:
            logger.error(f"Failed to save execution state: {e}")
            raise

    def load_execution_state(self, run_id: str) -> Optional[PipelineStateSnapshot]:
        """
        Load execution state from persistent storage.

        Returns None if state doesn't exist or can't be loaded.
        """
        state_file = self.state_dir / f"{run_id}.json"

        if not state_file.exists():
            logger.debug(f"No state file found: {state_file}")
            return None

        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            snapshot = PipelineStateSnapshot.from_dict(data)
            logger.info(f"Loaded execution state: {run_id}")
            return snapshot

        except Exception as e:
            logger.error(f"Failed to load execution state {run_id}: {e}")
            return None

    def list_resumes(self) -> List[ExecutionState]:
        """
        List all resumable executions.

        Returns execution states for failed or interrupted runs.
        """
        resumable_states = []

        for state_file in self.state_dir.glob("*.json"):
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                execution_state = ExecutionState.from_dict(data["execution_state"])

                # Include failed or incomplete runs
                if execution_state.status in ("FAILED", "RUNNING", "PAUSED"):
                    resumable_states.append(execution_state)

            except Exception as e:
                logger.warning(f"Skipping corrupted state file {state_file}: {e}")

        # Sort by start time (most recent first)
        resumable_states.sort(key=lambda s: s.start_time, reverse=True)

        logger.info(f"Found {len(resumable_states)} resumable executions")
        return resumable_states

    def delete_execution_state(self, run_id: str) -> bool:
        """
        Delete execution state.

        Returns True if deletion was successful.
        """
        state_file = self.state_dir / f"{run_id}.json"

        try:
            if state_file.exists():
                state_file.unlink()
                logger.info(f"Deleted execution state: {run_id}")
                return True
            else:
                logger.debug(f"State file not found: {run_id}")
                return False

        except Exception as e:
            logger.error(f"Failed to delete execution state {run_id}: {e}")
            return False

    def cleanup_old_states(self, keep_days: int = 7) -> int:
        """
        Clean up old state files.

        Args:
            keep_days: Number of days of state to keep

        Returns:
            Number of files cleaned up
        """
        from datetime import timedelta

        cutoff_time = datetime.now() - timedelta(days=keep_days)
        cleaned_count = 0

        for state_file in self.state_dir.glob("*.json"):
            try:
                # Check file modification time
                file_time = datetime.fromtimestamp(state_file.stat().st_mtime)

                if file_time < cutoff_time:
                    state_file.unlink()
                    cleaned_count += 1
                    logger.debug(f"Cleaned up old state: {state_file.name}")

            except Exception as e:
                logger.warning(f"Failed to clean up {state_file}: {e}")

        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} old state files")

        return cleaned_count


class ResumableOrchestrator:
    """
    Orchestrator with resume capability.

    Integrates state management with parallel execution strategy.
    """

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    def can_resume(self, run_id: str) -> bool:
        """Check if execution can be resumed."""
        snapshot = self.state_manager.load_execution_state(run_id)
        return snapshot is not None and snapshot.execution_state.status in (
            "FAILED",
            "RUNNING",
            "PAUSED",
        )

    def get_resume_point(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get resume point information.

        Returns summary of where execution left off.
        """
        snapshot = self.state_manager.load_execution_state(run_id)
        if not snapshot:
            return None

        state = snapshot.execution_state
        completed_count = sum(
            1
            for status in snapshot.task_statuses.values()
            if status.state == TaskState.SUCCESS
        )

        failed_steps = [
            step_id
            for step_id, status in snapshot.task_statuses.items()
            if status.state == TaskState.FAILED
        ]

        return {
            "run_id": run_id,
            "status": state.status,
            "total_steps": state.total_steps,
            "completed_steps": completed_count,
            "failed_steps": failed_steps,
            "last_activity": state.end_time or state.start_time,
            "error_message": state.error_message,
        }

    def create_resume_snapshot(
        self,
        run_id: str,
        plan: List[Dict[str, Any]],
        task_statuses: Dict[str, TaskStatus],
        completed_results: Dict[str, StepExecutionResult],
        variables: Optional[Dict[str, Any]] = None,
        failed_step: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> PipelineStateSnapshot:
        """Create snapshot for current execution state."""
        completed_count = sum(
            1 for status in task_statuses.values() if status.state == TaskState.SUCCESS
        )

        execution_state = ExecutionState(
            run_id=run_id,
            status="FAILED" if failed_step else "RUNNING",
            start_time=datetime.utcnow(),  # This should be tracked from actual start
            total_steps=len(plan),
            completed_steps=completed_count,
            failed_step=failed_step,
            error_message=error_message,
        )

        return PipelineStateSnapshot(
            execution_state=execution_state,
            task_statuses=task_statuses,
            completed_results=completed_results,
            plan=plan,
            variables=variables,
        )
