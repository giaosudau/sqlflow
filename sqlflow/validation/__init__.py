"""SQLFlow DSL Validation Module.

This module provides enhanced validation capabilities for SQLFlow's DSL,
including precise error reporting and connector parameter validation.
"""

from .errors import AggregatedValidationError, ValidationError
from .schemas import CONNECTOR_SCHEMAS, ConnectorSchema
from .validators import validate_connectors, validate_pipeline, validate_references

__all__ = [
    "ValidationError",
    "AggregatedValidationError",
    "ConnectorSchema",
    "CONNECTOR_SCHEMAS",
    "validate_pipeline",
    "validate_connectors",
    "validate_references",
]
