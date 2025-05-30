"""SQLFlow - SQL-based data pipeline tool."""

__version__ = "0.1.6"
__package_name__ = "sqlflow-core"

# Initialize logging with default configuration
from sqlflow.logging import configure_logging

# Set up default logging configuration
configure_logging()

# Apply UDF patches to handle default parameters
try:
    from sqlflow.udfs.udf_patch import patch_udf_manager

    patch_udf_manager()
except ImportError:
    # This can happen during installation when dependencies are not yet satisfied
    pass

# Import core modules for easier access
from sqlflow.core.variables import VariableContext, VariableSubstitutor
