"""Base executor for SQLFlow pipelines."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from sqlflow.core.protocols import ExecutorProtocol
from sqlflow.udfs.manager import PythonUDFManager


class BaseExecutor(ExecutorProtocol, ABC):
    """Base class for pipeline executors."""

    def __init__(self):
        """Initialize a BaseExecutor."""
        self.udf_manager = PythonUDFManager()
        self.discovered_udfs: Dict[str, Callable] = {}

    def discover_udfs(self, project_dir: Optional[str] = None) -> Dict[str, Callable]:
        """Discover UDFs in the project.

        Args:
            project_dir: Project directory (default: use UDFManager's default)

        Returns:
            Dictionary of UDF names to functions
        """
        if project_dir:
            self.udf_manager.project_dir = project_dir

        self.discovered_udfs = self.udf_manager.discover_udfs()
        return self.discovered_udfs

    def get_udfs_for_query(self, query: str) -> Dict[str, Callable]:
        """Get UDFs referenced in a query.

        Args:
            query: SQL query

        Returns:
            Dictionary of UDF names to functions
        """
        if not self.discovered_udfs:
            self.discover_udfs()

        udf_refs = self.udf_manager.extract_udf_references(query)
        return {
            name: self.discovered_udfs[name]
            for name in udf_refs
            if name in self.discovered_udfs
        }

    @abstractmethod
    def execute(self, plan: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a pipeline plan.

        Args:
            plan: List of operations to execute

        Returns:
            Dict containing execution results
        """

    @abstractmethod
    def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step in the pipeline.

        Args:
            step: Operation to execute

        Returns:
            Dict containing execution results
        """

    @abstractmethod
    def can_resume(self) -> bool:
        """Check if the executor supports resuming from failure.

        Returns:
            True if the executor supports resuming, False otherwise
        """

    @abstractmethod
    def resume(self) -> Dict[str, Any]:
        """Resume execution from the last failure.

        Returns:
            Dict containing execution results
        """
