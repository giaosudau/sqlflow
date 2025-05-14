"""Base executor for SQLFlow pipelines."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from sqlflow.sqlflow.core.protocols import ExecutorProtocol


class BaseExecutor(ExecutorProtocol, ABC):
    """Base class for pipeline executors."""

    @abstractmethod
    def execute(self, plan: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a pipeline plan.
        
        Args:
            plan: List of operations to execute
            
        Returns:
            Dict containing execution results
        """
        pass

    @abstractmethod
    def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step in the pipeline.
        
        Args:
            step: Operation to execute
            
        Returns:
            Dict containing execution results
        """
        pass

    @abstractmethod
    def can_resume(self) -> bool:
        """Check if the executor supports resuming from failure.
        
        Returns:
            True if the executor supports resuming, False otherwise
        """
        pass

    @abstractmethod
    def resume(self) -> Dict[str, Any]:
        """Resume execution from the last failure.
        
        Returns:
            Dict containing execution results
        """
        pass
