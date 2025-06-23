"""Execution package for context and resource management."""

from .context import (
    ExecutionContext,
    create_execution_context,
)

try:
    from .engines import DatabaseEngineAdapter, create_engine_adapter
except ImportError:
    # Engine adapters might not exist yet in clean architecture
    pass

__all__ = [
    "ExecutionContext",
    "create_execution_context",
]
