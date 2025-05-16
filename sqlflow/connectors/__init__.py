"""Data source connectors for SQLFlow.

This package provides connectors for various data sources and destinations:
- CSV files (csv_connector)
- Parquet files (parquet_connector)
- PostgreSQL databases (postgres_connector)
- REST APIs (rest_connector)
- S3 object storage (s3_connector)
- Google Sheets (google_sheets_connector)

Some connectors have external dependencies that must be installed separately:
- PostgreSQL connector requires psycopg2-binary
- S3 connector requires boto3
- Google Sheets connector requires google-api-python-client, google-auth

Connectors are automatically registered when this package is imported.
"""

from sqlflow.connectors.registry import (
    BIDIRECTIONAL_CONNECTOR_REGISTRY,
    CONNECTOR_REGISTRY,
    EXPORT_CONNECTOR_REGISTRY,
    get_bidirectional_connector_class,
    get_connector_class,
    get_export_connector_class,
    register_bidirectional_connector,
    register_connector,
    register_export_connector,
)

__all__ = [
    "CONNECTOR_REGISTRY",
    "EXPORT_CONNECTOR_REGISTRY",
    "BIDIRECTIONAL_CONNECTOR_REGISTRY",
    "get_connector_class",
    "get_export_connector_class",
    "get_bidirectional_connector_class",
    "register_connector",
    "register_export_connector",
    "register_bidirectional_connector",
]

# Import all built-in connectors to ensure they are registered at runtime

# Register PostgreSQL connector if dependencies are available
# If psycopg2 is not available, use a placeholder connector that will warn users about
# the missing dependency when they try to use it
try:
    from sqlflow.connectors import postgres_connector
except ImportError:
    # Use a placeholder connector that satisfies the interface requirements
    from sqlflow.connectors import postgres_placeholder
# Use try-except to handle missing dependencies gracefully
try:
    from sqlflow.connectors import csv_connector, csv_export_connector
except ImportError:
    pass

try:
    from sqlflow.connectors import parquet_connector
except ImportError:
    pass

try:
    from sqlflow.connectors import s3_connector
except ImportError:
    pass

try:
    from sqlflow.connectors import rest_connector
except ImportError:
    pass

try:
    from sqlflow.connectors import google_sheets_connector
except ImportError:
    pass
