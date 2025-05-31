"""SQLFlow state management module.

This module provides reliable, atomic state management for incremental loading
and connector operations. It follows the engineering principle of simplicity
while providing robust state persistence using DuckDB as the backend.
"""

from sqlflow.core.state.backends import DuckDBStateBackend, StateBackend
from sqlflow.core.state.watermark_manager import WatermarkManager

__all__ = [
    "StateBackend",
    "DuckDBStateBackend",
    "WatermarkManager",
]
