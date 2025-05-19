"""SQLFlow - SQL-based data pipeline tool."""

__version__ = "0.1.0"

# Apply UDF patches to handle default parameters
try:
    from sqlflow.udfs.udf_patch import patch_udf_manager

    patch_udf_manager()
except ImportError:
    # This can happen during installation when dependencies are not yet satisfied
    pass
