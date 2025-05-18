"""Manager for discovering and handling Python User-Defined Functions (UDFs).

This module provides functionality to discover, register, and manage Python UDFs
that can be used in SQLFlow pipelines.
"""

import glob
import importlib.util
import inspect
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class PythonUDFManager:
    """Manages discovery and registration of Python UDFs for SQLFlow.

    This class handles discovering UDFs in the project structure, collecting their
    metadata, and extracting UDF references from SQL queries.
    """

    def __init__(self, project_dir: Optional[str] = None):
        """Initialize a PythonUDFManager.

        Args:
            project_dir: Path to project directory (default: current working directory)
        """
        self.project_dir = project_dir or os.getcwd()
        self.udfs: Dict[str, Callable] = {}
        self.udf_info: Dict[str, Dict[str, Any]] = {}

    def discover_udfs(
        self, python_udfs_dir: str = "python_udfs"
    ) -> Dict[str, Callable]:
        """Discover UDFs in the project structure.

        Scans Python files in the UDFs directory to find functions decorated with
        @python_scalar_udf or @python_table_udf.

        Args:
            python_udfs_dir: Path to UDFs directory relative to project_dir

        Returns:
            Dictionary of UDF name to function
        """
        udfs = {}
        udf_dir = os.path.join(self.project_dir, python_udfs_dir)

        if not os.path.exists(udf_dir):
            logger.warning(f"UDF directory not found: {udf_dir}")
            return udfs

        for py_file in glob.glob(f"{udf_dir}/**/*.py", recursive=True):
            module_path = os.path.relpath(py_file, self.project_dir)
            module_name = os.path.splitext(module_path)[0].replace(os.path.sep, ".")

            try:
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec is None or spec.loader is None:
                    logger.warning(f"Failed to load spec for {py_file}")
                    continue

                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Collect functions decorated with @python_scalar_udf or @python_table_udf
                for name, func in inspect.getmembers(module, inspect.isfunction):
                    if hasattr(func, "_is_sqlflow_udf"):
                        # Get custom UDF name if specified, otherwise use module.function
                        custom_name = getattr(func, "_udf_name", name)
                        udf_name = f"{module_name}.{custom_name}"
                        udfs[udf_name] = func

                        # Store additional UDF metadata
                        self.udf_info[udf_name] = {
                            "module": module_name,
                            "name": custom_name,
                            "original_name": name,
                            "type": getattr(func, "_udf_type", "unknown"),
                            "docstring": inspect.getdoc(func) or "",
                            "file_path": py_file,
                            "signature": str(inspect.signature(func)),
                        }

                        logger.info(
                            f"Discovered UDF: {udf_name} ({self.udf_info[udf_name]['type']})"
                        )

            except Exception as e:
                logger.error(f"Error loading UDFs from {py_file}: {str(e)}")

        self.udfs = udfs
        return udfs

    def get_udf(self, udf_name: str) -> Optional[Callable]:
        """Get a UDF by name.

        Args:
            udf_name: Name of the UDF (module.function)

        Returns:
            UDF function or None if not found
        """
        return self.udfs.get(udf_name)

    def get_udf_info(self, udf_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a UDF.

        Args:
            udf_name: Name of the UDF (module.function)

        Returns:
            Dictionary of UDF information or None if not found
        """
        return self.udf_info.get(udf_name)

    def list_udfs(self) -> List[Dict[str, Any]]:
        """List all discovered UDFs with their information.

        Returns:
            List of UDF information dictionaries
        """
        return [{"name": udf_name, **self.udf_info[udf_name]} for udf_name in self.udfs]

    def extract_udf_references(self, sql: str) -> Set[str]:
        """Extract UDF references from SQL query.

        Identifies calls to PYTHON_FUNC in the SQL query and extracts the
        UDF name references.

        Args:
            sql: SQL query text

        Returns:
            Set of UDF names referenced in the query
        """
        # Match PYTHON_FUNC("module.function", ...)
        udf_pattern = r'PYTHON_FUNC\(\s*[\'"]([a-zA-Z0-9_\.]+)[\'"]'
        matches = re.findall(udf_pattern, sql)

        # Filter to only include discovered UDFs
        return {match for match in matches if match in self.udfs}

    def register_udfs_with_engine(
        self, engine: Any, udf_names: Optional[List[str]] = None
    ):
        """Register UDFs with the SQL engine.

        Args:
            engine: SQLFlow engine instance
            udf_names: Optional list of UDF names to register. If None, registers all.
        """
        if not hasattr(engine, "register_python_udf"):
            logger.warning(
                f"Engine {engine.__class__.__name__} does not support UDF registration"
            )
            return

        names_to_register = udf_names or list(self.udfs.keys())

        for name in names_to_register:
            if name in self.udfs:
                try:
                    engine.register_python_udf(name, self.udfs[name])
                    logger.debug(f"Registered UDF {name} with engine")
                except Exception as e:
                    logger.error(f"Failed to register UDF {name}: {str(e)}")
            else:
                logger.warning(f"UDF {name} not found, skipping registration")

    def get_udfs_for_query(self, sql: str) -> Dict[str, Callable]:
        """Get UDFs referenced in a query.

        Args:
            sql: SQL query text

        Returns:
            Dictionary of UDF names to functions for UDFs referenced in the query
        """
        udf_refs = self.extract_udf_references(sql)
        return {name: self.udfs[name] for name in udf_refs if name in self.udfs}
