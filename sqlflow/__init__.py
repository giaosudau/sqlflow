"""SQLFlow - SQL-based data pipeline tool."""

__version__ = "0.1.7"
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

# V2 Variables module uses pure functions - no need for global imports

from .exceptions import (
    ConnectorError,
    HealthCheckError,
    IncrementalError,
    ParameterError,
)

__all__ = [
    "ConnectorError",
    "ParameterError",
    "IncrementalError",
    "HealthCheckError",
]
