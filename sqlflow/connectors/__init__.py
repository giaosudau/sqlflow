"""Data source connectors for SQLFlow.

This package provides connectors for various data sources and destinations:
- CSV files (csv_connector)
- Parquet files (parquet_connector)
- PostgreSQL databases (postgres_connector)
- REST APIs (rest_connector)
- S3 object storage (s3_connector)
- Google Sheets (google_sheets_connector)
- Shopify e-commerce (shopify_connector)

Some connectors have external dependencies that must be installed separately:
- PostgreSQL connector requires psycopg2-binary
- S3 connector requires boto3
- Google Sheets connector requires google-api-python-client, google-auth
- Shopify connector requires requests

Connectors are automatically registered when this package is imported.
"""

from sqlflow.connectors.registry.destination_registry import (
    destination_registry as destination_connector_registry,
)
from sqlflow.connectors.registry.source_registry import (
    source_registry as source_connector_registry,
)

# Register PostgreSQL connector if dependencies are available
# If psycopg2 is not available, use a placeholder connector that will warn users about
# the missing dependency when they try to use it
try:
    from sqlflow.connectors.postgres import *
except ImportError:
    # Use a placeholder connector that satisfies the interface requirements
    from . import postgres_placeholder


# flake8: noqa
from .csv import *
from .google_sheets import *
from .in_memory import *
from .parquet import *
from .rest import *
from .s3 import *
from .shopify import *

__all__ = [
    "source_connector_registry",
    "destination_connector_registry",
]
