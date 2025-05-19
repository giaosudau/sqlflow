"""
Integration point for the enhanced UDF manager.

This module provides a function to patch the PythonUDFManager at runtime
to handle default parameters in UDFs when working with DuckDB.
"""

import logging

from sqlflow.udfs.enhanced_manager import enhance_udf_manager

logger = logging.getLogger(__name__)


def patch_udf_manager():
    """
    Patch the PythonUDFManager to handle default parameters.

    This function should be called during SQLFlow initialization
    to enhance the UDF manager with default parameter handling.
    """
    try:
        # Import the manager class
        from sqlflow.udfs.manager import PythonUDFManager

        # Apply the enhancement to all future instances
        original_init = PythonUDFManager.__init__

        def enhanced_init(self, *args, **kwargs):
            # Call the original init
            original_init(self, *args, **kwargs)
            # Apply our enhancement
            enhance_udf_manager(self)

        # Replace the init method
        PythonUDFManager.__init__ = enhanced_init

        logger.info(
            "Successfully patched PythonUDFManager to handle default parameters"
        )
    except Exception as e:
        logger.error(f"Failed to patch PythonUDFManager: {e}")
