"""Task status tracking for SQLFlow executors."""

import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import List, Optional


class TaskState(str, Enum):
    """Task execution states."""

    PENDING = "PENDING"
    ELIGIBLE = "ELIGIBLE"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class TaskStatus:
    """Task status information."""

    id: str
    state: TaskState
    unmet_dependencies: int
    dependencies: List[str]
    attempts: int = 0
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def to_json(self) -> str:
        """Convert task status to JSON string.

        Returns
        -------
            JSON string representation of task status

        """
        return json.dumps(asdict(self))
