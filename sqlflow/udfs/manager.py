"""Manager for discovering and handling Python User-Defined Functions (UDFs).

This module provides functionality to discover, register, and manage Python UDFs
that can be used in SQLFlow pipelines.
"""

import glob
import importlib.util
import inspect
import os
import re
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

import pandas as pd

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class UDFDiscoveryError(Exception):
    """Error raised during UDF discovery process."""


class UDFExecutionError(Exception):
    """Error raised during UDF execution.

    This exception captures the context of the UDF execution (UDF name, query context)
    and wraps the original exception to provide better diagnostics.
    """

    def __init__(
        self, udf_name: str, original_error: Exception, sql_context: str = None
    ):
        """Initialize UDFExecutionError.

        Args:
            udf_name: Name of the UDF that raised the error
            original_error: Original exception from the UDF execution
            sql_context: Optional SQL context where the UDF was called
        """
        self.udf_name = udf_name
        self.original_error = original_error
        self.sql_context = sql_context

        # Create detailed error message
        message = (
            f"Error executing UDF '{udf_name}': {str(original_error)}\n"
            f"Original traceback: {traceback.format_exc()}"
        )

        if sql_context:
            message += f"\nSQL context: {sql_context}"

        super().__init__(message)


# Type alias for UDF functions
UDFFunc = TypeVar("UDFFunc", bound=Callable[..., Any])


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
        self.discovery_errors: Dict[str, str] = {}
        logger.debug(
            f"UDFManager initialized with project directory: {self.project_dir}"
        )

    def _extract_module_name(self, py_file: str, python_udfs_dir: str) -> str:
        """Extract a proper module name from a Python file path.

        Handles files in subdirectories to create appropriate namespacing.

        Args:
            py_file: Path to Python file
            python_udfs_dir: Base UDFs directory

        Returns:
            Properly formatted module name
        """
        # Get path relative to python_udfs_dir
        full_udfs_dir = os.path.join(self.project_dir, python_udfs_dir)
        relative_path = os.path.relpath(py_file, full_udfs_dir)

        # Convert path to module notation
        module_path = os.path.splitext(relative_path)[0]
        module_name = module_path.replace(os.path.sep, ".")

        # Prefix with python_udfs_dir name
        return f"{os.path.basename(python_udfs_dir)}.{module_name}"

    def _extract_parameter_details(self, func: Callable) -> Dict[str, Dict[str, Any]]:
        """Extract detailed parameter information from a function.

        Args:
            func: Function to analyze

        Returns:
            Dictionary of parameter details
        """
        # Check if parameter details were already captured by the decorator
        if hasattr(func, "_param_info"):
            return getattr(func, "_param_info")

        # Otherwise extract them manually
        sig = inspect.signature(func)
        param_details = {}

        for name, param in sig.parameters.items():
            param_info = {
                "kind": str(param.kind),
                "default": (
                    None if param.default is inspect.Parameter.empty else param.default
                ),
                "has_default": param.default is not inspect.Parameter.empty,
                "annotation": (
                    "Any"
                    if param.annotation is inspect.Parameter.empty
                    else str(param.annotation)
                ),
            }

            # Try to extract type hint in a user-friendly format
            if param.annotation is not inspect.Parameter.empty:
                # Check for DataFrame type with different ways to detect it
                if (
                    param.annotation is pd.DataFrame
                    or hasattr(param.annotation, "__module__")
                    and param.annotation.__module__ == "pandas.core.frame"
                    or str(param.annotation).endswith("DataFrame")
                ):
                    param_info["type"] = "DataFrame"
                elif (
                    hasattr(param.annotation, "__origin__")
                    and param.annotation.__origin__ is list
                ):
                    try:
                        param_info["type"] = (
                            f"List[{param.annotation.__args__[0].__name__}]"
                        )
                    except (AttributeError, IndexError):
                        param_info["type"] = "List"
                else:
                    param_info["type"] = (
                        param.annotation.__name__
                        if hasattr(param.annotation, "__name__")
                        else str(param.annotation)
                    )
            else:
                param_info["type"] = "Any"

            param_details[name] = param_info

        return param_details

    def _format_param(self, name: str, param: inspect.Parameter) -> str:
        """Format a single parameter for the signature string.

        Args:
            name: Parameter name
            param: Parameter object

        Returns:
            Formatted parameter string
        """
        # Handle parameter annotations
        annotation = ""
        if param.annotation is not inspect.Parameter.empty:
            # Special handling for DataFrame
            if param.annotation is pd.DataFrame or str(param.annotation).endswith(
                "DataFrame"
            ):
                annotation = ": DataFrame"
            elif hasattr(param.annotation, "__name__"):
                annotation = f": {param.annotation.__name__}"
            else:
                annotation = f": {str(param.annotation)}"

        # Handle default values
        default = ""
        if param.default is not inspect.Parameter.empty:
            if isinstance(param.default, str):
                default = f" = '{param.default}'"
            else:
                default = f" = {param.default}"

        return f"{name}{annotation}{default}"

    def _format_signature(self, func: Callable) -> str:  # noqa: C901
        """Format a function signature in a user-friendly way.

        Args:
            func: Function to format signature for

        Returns:
            Formatted signature string
        """
        try:
            sig = inspect.signature(func)
            params = []

            # Format each parameter using helper method
            for name, param in sig.parameters.items():
                params.append(self._format_param(name, param))

            # Handle return annotation
            return_annotation = ""
            if sig.return_annotation is not inspect.Parameter.empty:
                # Special handling for DataFrame
                if sig.return_annotation is pd.DataFrame or str(
                    sig.return_annotation
                ).endswith("DataFrame"):
                    return_annotation = " -> DataFrame"
                elif hasattr(sig.return_annotation, "__name__"):
                    return_annotation = f" -> {sig.return_annotation.__name__}"
                else:
                    return_annotation = f" -> {str(sig.return_annotation)}"

            return f"({', '.join(params)}){return_annotation}"
        except Exception as e:
            logger.warning(f"Error formatting signature for {func.__name__}: {str(e)}")
            return str(inspect.signature(func))

    def _extract_udf_metadata(
        self, func: Callable, module_name: str, original_name: str, file_path: str
    ) -> Dict[str, Any]:
        """Extract comprehensive metadata for a UDF.

        Args:
            func: UDF function
            module_name: Name of the module
            original_name: Original function name
            file_path: Path to the file

        Returns:
            Dictionary of UDF metadata
        """
        try:
            # Get custom UDF name if specified, otherwise use function name
            custom_name = getattr(func, "_udf_name", original_name)
            udf_name = f"{module_name}.{custom_name}"

            # Get docstring and parse it
            docstring = inspect.getdoc(func) or ""
            docstring_summary = docstring.split("\n\n")[0] if docstring else ""

            # Get required columns for table UDFs
            required_columns = getattr(func, "_required_columns", None)

            # Extract detailed parameter information
            param_details = self._extract_parameter_details(func)

            # Format signature in a user-friendly way
            formatted_signature = self._format_signature(func)
            raw_signature = str(inspect.signature(func))

            return {
                "module": module_name,
                "name": custom_name,
                "original_name": original_name,
                "full_name": udf_name,
                "type": getattr(func, "_udf_type", "unknown"),
                "docstring": docstring,
                "docstring_summary": docstring_summary,
                "file_path": file_path,
                "signature": raw_signature,
                "formatted_signature": formatted_signature,
                "param_details": param_details,
                "required_columns": required_columns,
                "discovery_time": None,  # Will be set by discover_udfs
            }
        except Exception as e:
            logger.warning(f"Error extracting metadata for {original_name}: {str(e)}")
            # Return minimal metadata to avoid breaking the discovery process
            return {
                "module": module_name,
                "name": original_name,
                "original_name": original_name,
                "full_name": f"{module_name}.{original_name}",
                "type": getattr(func, "_udf_type", "unknown"),
                "docstring": "",
                "file_path": file_path,
                "signature": str(inspect.signature(func)),
                "discovery_time": None,
            }

    def _process_udf_module(
        self,
        module_name: str,
        py_file: str,
        udfs: Dict[str, Callable],
        import_time: str,
    ) -> None:
        """Process a Python module to discover UDFs.

        Args:
            module_name: Name of the module
            py_file: Path to Python file
            udfs: Dictionary to store discovered UDFs
            import_time: Timestamp for discovery time
        """
        try:
            # Load the module
            module = self._load_module(module_name, py_file)
            if module is None:
                return  # Module loading failed, error already logged

            # Track if we found any UDFs in this module
            udfs_found = False

            # Collect functions decorated with @python_scalar_udf or @python_table_udf
            for name, func in inspect.getmembers(module, inspect.isfunction):
                if hasattr(func, "_is_sqlflow_udf"):
                    udfs_found = True
                    self._process_udf(
                        func, module_name, name, py_file, udfs, import_time
                    )

            # If no UDFs were found, log a helpful message
            if not udfs_found:
                logger.info(
                    f"No SQLFlow UDFs found in {py_file}. "
                    f"Make sure functions are decorated with @python_scalar_udf or @python_table_udf."
                )

        except Exception as e:
            error_msg = (
                f"Unexpected error loading UDFs from {py_file}: {str(e)}\n"
                f"This is likely an internal SQLFlow error and should be reported.\n"
                f"Original traceback: {traceback.format_exc()}"
            )
            logger.error(error_msg)
            self.discovery_errors[py_file] = error_msg

    def _load_module(self, module_name: str, py_file: str) -> Optional[Any]:
        """Load a Python module from a file.

        Args:
            module_name: Name of the module
            py_file: Path to Python file

        Returns:
            Loaded module or None if loading failed
        """
        try:
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                error_msg = f"Failed to load module spec for {py_file}. Please check if the file exists and is a valid Python module."
                logger.warning(error_msg)
                self.discovery_errors[py_file] = error_msg
                return None

            module = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(module)
                return module
            except Exception as import_error:
                error_msg = (
                    f"Failed to import module {module_name} from {py_file}: {str(import_error)}\n"
                    f"This might be due to syntax errors or missing dependencies in your UDF file.\n"
                    f"Original error: {traceback.format_exc()}"
                )
                logger.error(error_msg)
                self.discovery_errors[py_file] = error_msg
                return None
        except Exception as e:
            error_msg = f"Error loading module {module_name} from {py_file}: {str(e)}"
            logger.error(error_msg)
            self.discovery_errors[py_file] = error_msg
            return None

    def _process_udf(
        self,
        func: Callable,
        module_name: str,
        name: str,
        py_file: str,
        udfs: Dict[str, Callable],
        import_time: str,
    ) -> None:
        """Process a single UDF function.

        Args:
            func: UDF function
            module_name: Name of the module
            name: Function name
            py_file: Path to Python file
            udfs: Dictionary to store discovered UDFs
            import_time: Timestamp for discovery time
        """
        try:
            # Extract metadata
            metadata = self._extract_udf_metadata(func, module_name, name, py_file)

            # Set discovery time
            metadata["discovery_time"] = import_time

            # Get full UDF name
            udf_name = metadata["full_name"]

            # Store UDF and metadata
            udfs[udf_name] = func
            self.udf_info[udf_name] = metadata

            # Validate UDF metadata and log warnings if needed
            validation_warnings = self.validate_udf_metadata(udf_name)
            if validation_warnings:
                warning_msg = f"Discovered UDF {udf_name} has validation warnings: {', '.join(validation_warnings)}"
                logger.warning(warning_msg)
                # Store warnings but don't treat as fatal errors
                self.discovery_errors[f"{py_file}:{name}:warnings"] = warning_msg

            logger.info(f"Discovered UDF: {udf_name} ({metadata['type']})")
        except Exception as e:
            detailed_error = (
                f"Error processing UDF {name} in {py_file}: {str(e)}\n"
                f"This may be due to invalid decorator usage or signature issues.\n"
                f"Original traceback: {traceback.format_exc()}"
            )
            logger.error(detailed_error)
            self.discovery_errors[f"{py_file}:{name}"] = detailed_error

    def discover_udfs(  # noqa: C901
        self, python_udfs_dir: str = "python_udfs", strict: bool = False
    ) -> Dict[str, Callable]:
        """Discover UDFs in the project structure.

        Scans Python files in the UDFs directory to find functions decorated with
        @python_scalar_udf or @python_table_udf.

        Args:
            python_udfs_dir: Path to UDFs directory relative to project_dir
            strict: If True, raises an exception when the UDF directory doesn't exist.
                   If False, logs a warning and returns an empty dict (default: False)

        Returns:
            Dictionary of UDF name to function

        Raises:
            UDFDiscoveryError: If the UDF directory doesn't exist and strict=True
        """
        udfs = {}
        self.discovery_errors = {}
        udf_dir = os.path.join(self.project_dir, python_udfs_dir)

        if not os.path.exists(udf_dir):
            error_msg = f"UDF directory not found: {udf_dir}"
            logger.warning(error_msg)
            self.discovery_errors["directory_not_found"] = error_msg
            if strict:
                raise UDFDiscoveryError(error_msg)
            return udfs

        import_time = __import__("datetime").datetime.now().isoformat()

        for py_file in glob.glob(f"{udf_dir}/**/*.py", recursive=True):
            # Generate a module name that correctly reflects subdirectory structure
            module_name = self._extract_module_name(py_file, python_udfs_dir)

            # Process the module to find UDFs
            self._process_udf_module(module_name, py_file, udfs, import_time)

        self.udfs = udfs
        return udfs

    def validate_udf_metadata(self, udf_name: str) -> List[str]:
        """Validate the completeness and integrity of UDF metadata.

        Args:
            udf_name: Name of the UDF to validate

        Returns:
            List of validation warnings (empty if valid)
        """
        warnings = []
        if udf_name not in self.udf_info:
            return ["UDF not found in registry"]

        metadata = self.udf_info[udf_name]

        # Check for missing essential fields
        essential_fields = ["module", "name", "type", "signature"]
        for field in essential_fields:
            if field not in metadata or not metadata[field]:
                warnings.append(f"Missing essential metadata: {field}")

        # Validate UDF type
        if metadata.get("type") not in ["scalar", "table"]:
            warnings.append(f"Invalid UDF type: {metadata.get('type')}")

        # Validate required_columns for table UDFs
        if metadata.get("type") == "table" and "param_details" in metadata:
            param_details = metadata["param_details"]
            first_param_name = next(iter(param_details), None)

            if first_param_name:
                first_param = param_details[first_param_name]
                if "DataFrame" not in first_param.get(
                    "annotation", ""
                ) and "DataFrame" not in first_param.get("type", ""):
                    warnings.append(
                        "First parameter of table UDF should be a DataFrame"
                    )

        return warnings

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

    def get_discovery_errors(self) -> Dict[str, str]:
        """Get errors encountered during UDF discovery.

        Returns:
            Dictionary of file paths to error messages
        """
        return self.discovery_errors

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
        udf_pattern = r"PYTHON_FUNC\s*\(\s*[\'\"]([a-zA-Z0-9_\.]+)[\'\"]"
        matches = re.findall(udf_pattern, sql)

        # Filter to only include discovered UDFs
        return {match for match in matches if match in self.udfs}

    def register_udfs_with_engine(
        self, engine: Any, udf_names: Optional[List[str]] = None
    ):
        """Register UDFs with a SQLEngine.

        Args:
            engine: SQLEngine instance to register UDFs with
            udf_names: Optional list of UDF names to register (defaults to all discovered UDFs)

        Raises:
            KeyError: If a specified UDF name is not found
            AttributeError: If the engine doesn't support register_python_udf
        """
        if not hasattr(engine, "register_python_udf"):
            logger.warning(
                "Engine does not support registering Python UDFs. "
                "Make sure the engine implements register_python_udf."
            )
            return

        # If no UDF names specified, register all
        if udf_names is None:
            udf_names = list(self.udfs.keys())

        print(f"DEBUG: All UDFs to register: {udf_names}")

        registration_errors = []

        for udf_name in udf_names:
            try:
                if udf_name not in self.udfs:
                    raise KeyError(f"UDF '{udf_name}' not found in discovered UDFs")

                udf = self.udfs[udf_name]
                # Debug output to understand UDF attributes before registration
                print(f"DEBUG: Registering UDF {udf_name}")
                print(f"DEBUG: UDF type: {getattr(udf, '_udf_type', 'unknown')}")
                print(
                    f"DEBUG: UDF has output_schema attr: {hasattr(udf, '_output_schema')}"
                )
                if hasattr(udf, "_output_schema"):
                    print(f"DEBUG: UDF output_schema: {udf._output_schema}")
                print(
                    f"DEBUG: UDF _infer_schema: {getattr(udf, '_infer_schema', False)}"
                )
                print(
                    f"DEBUG: UDF attributes: {[attr for attr in dir(udf) if attr.startswith('_') and not attr.startswith('__')]}"
                )

                # Register the UDF with the engine
                engine.register_python_udf(udf_name, udf)
                logger.info(f"Registered UDF: {udf_name}")
            except Exception as e:
                error_msg = f"Error registering UDF {udf_name}: {str(e)}"
                logger.error(error_msg)
                registration_errors.append((udf_name, str(e)))

        # Log summary of registration
        if registration_errors:
            logger.warning(
                f"Failed to register {len(registration_errors)} UDFs with engine"
            )

        return registration_errors

    def get_udfs_for_query(self, sql: str) -> Dict[str, Callable]:
        """Get UDFs referenced in a query.

        Args:
            sql: SQL query text

        Returns:
            Dictionary of UDF names to functions for UDFs referenced in the query
        """
        udf_refs = self.extract_udf_references(sql)
        print(f"DEBUG: UDF references extracted from query: {udf_refs}")

        result = {}
        for name in udf_refs:
            if name in self.udfs:
                udf = self.udfs[name]
                print(f"DEBUG: Adding UDF {name} to query UDFs")
                print(
                    f"DEBUG: UDF {name} has _output_schema: {hasattr(udf, '_output_schema')}"
                )
                if hasattr(udf, "_output_schema"):
                    print(
                        f"DEBUG: UDF {name} output_schema value: {udf._output_schema}"
                    )
                result[name] = udf

        print(
            f"DEBUG: Returning {len(result)} UDFs for query, with names: {list(result.keys())}"
        )
        return result
