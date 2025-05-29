"""UDF (User Defined Function) module for DuckDB engine."""

from .handlers import ScalarUDFHandler, TableUDFHandler, UDFHandlerFactory
from .query_processor import UDFQueryProcessor
from .registration import UDFRegistrationStrategyFactory
from .validators import TableUDFSignatureValidator

__all__ = [
    "UDFHandlerFactory",
    "ScalarUDFHandler",
    "TableUDFHandler",
    "UDFQueryProcessor",
    "UDFRegistrationStrategyFactory",
    "TableUDFSignatureValidator",
]
