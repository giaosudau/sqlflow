"""Connector parameter validation schemas for SQLFlow DSL."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class FieldSchema:
    """Schema definition for a single parameter field."""

    name: str
    required: bool = True
    field_type: str = "string"  # string, integer, boolean, array
    description: str = ""
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None  # regex pattern for string validation

    def validate(self, value: Any) -> List[str]:
        """Validate a field value against this schema.

        Args:
        ----
            value: The value to validate

        Returns:
        -------
            List of validation error messages, empty if valid

        """
        errors = []

        if value is None:
            if self.required:
                errors.append(f"Required field '{self.name}' is missing")
            return errors

        # Type validation
        errors.extend(self._validate_type(value))

        # Value validation
        errors.extend(self._validate_value(value))

        return errors

    def _validate_type(self, value: Any) -> List[str]:
        """Validate the type of a field value."""
        errors = []

        if self.field_type == "string" and not isinstance(value, str):
            errors.append(
                f"Field '{self.name}' must be a string, got {type(value).__name__}"
            )
        elif self.field_type == "integer" and not isinstance(value, int):
            errors.append(
                f"Field '{self.name}' must be an integer, got {type(value).__name__}"
            )
        elif self.field_type == "boolean" and not isinstance(value, bool):
            errors.append(
                f"Field '{self.name}' must be a boolean, got {type(value).__name__}"
            )
        elif self.field_type == "array" and not isinstance(value, list):
            errors.append(
                f"Field '{self.name}' must be an array, got {type(value).__name__}"
            )
        elif self.field_type == "number" and not isinstance(value, (int, float, str)):
            errors.append(
                f"Field '{self.name}' must be a number (int, float, or string), got {type(value).__name__}"
            )

        return errors

    def _validate_value(self, value: Any) -> List[str]:
        """Validate the value constraints of a field."""
        errors = []

        # Value validation
        if self.allowed_values and value not in self.allowed_values:
            errors.append(
                f"Field '{self.name}' must be one of {self.allowed_values}, got '{value}'"
            )

        # Pattern validation for strings
        if self.pattern and isinstance(value, str):
            import re

            if not re.match(self.pattern, value):
                errors.append(
                    f"Field '{self.name}' does not match required pattern: {self.pattern}"
                )

        return errors


@dataclass
class ConnectorSchema:
    """Schema definition for a connector type."""

    name: str
    description: str
    fields: List[FieldSchema]

    def validate(self, params: Dict[str, Any]) -> List[str]:
        """Validate connector parameters against this schema.

        Args:
        ----
            params: Dictionary of connector parameters

        Returns:
        -------
            List of validation error messages, empty if valid

        """
        errors = []
        provided_fields = set(params.keys())
        schema_fields = {field.name for field in self.fields}

        # Check for unknown fields
        unknown_fields = provided_fields - schema_fields
        if unknown_fields:
            errors.append(
                f"Unknown parameters for {self.name} connector: {', '.join(unknown_fields)}"
            )

        # Validate each field
        for field in self.fields:
            field_errors = field.validate(params.get(field.name))
            errors.extend(field_errors)

        return errors


# Define connector schemas for MVP
CSV_SCHEMA = ConnectorSchema(
    name="CSV",
    description="CSV file connector for reading/writing CSV files",
    fields=[
        FieldSchema(
            name="path",
            required=True,
            field_type="string",
            description="Path to the CSV file",
            pattern=r".*\.csv$",
        ),
        FieldSchema(
            name="delimiter",
            required=False,
            field_type="string",
            description="Field delimiter character",
            allowed_values=[",", ";", "\t", "|"],
        ),
        FieldSchema(
            name="has_header",
            required=False,
            field_type="boolean",
            description="Whether the first row contains column headers",
        ),
        FieldSchema(
            name="encoding",
            required=False,
            field_type="string",
            description="File encoding",
            allowed_values=["utf-8", "latin-1", "ascii"],
        ),
        FieldSchema(
            name="quote_char",
            required=False,
            field_type="string",
            description="Quote character for CSV parsing",
        ),
        # Incremental loading parameters (industry-standard)
        FieldSchema(
            name="sync_mode",
            required=False,
            field_type="string",
            description="Synchronization mode for incremental loading",
            allowed_values=["full_refresh", "incremental"],
        ),
        FieldSchema(
            name="primary_key",
            required=False,
            field_type="string",
            description="Primary key field for incremental loading",
        ),
        FieldSchema(
            name="cursor_field",
            required=False,
            field_type="string",
            description="Cursor field for incremental loading (e.g., timestamp, id)",
        ),
        # Performance parameters (industry-standard)
        FieldSchema(
            name="batch_size",
            required=False,
            field_type="integer",
            description="Number of records to process in each batch",
        ),
        FieldSchema(
            name="timeout_seconds",
            required=False,
            field_type="integer",
            description="Timeout for operations in seconds",
        ),
        FieldSchema(
            name="max_retries",
            required=False,
            field_type="integer",
            description="Maximum number of retry attempts",
        ),
    ],
)

POSTGRES_SCHEMA = ConnectorSchema(
    name="POSTGRES",
    description="PostgreSQL database connector with industry-standard parameters",
    fields=[
        # Core connection parameters (host is required unless connection string is provided)
        FieldSchema(
            name="host",
            required=False,  # Made optional - validation will check this conditionally
            field_type="string",
            description="PostgreSQL server hostname or IP address",
        ),
        FieldSchema(
            name="port",
            required=False,
            field_type="integer",
            description="PostgreSQL server port (default: 5432)",
        ),
        # Database name - support both legacy and industry-standard naming
        FieldSchema(
            name="database",
            required=False,
            field_type="string",
            description="Database name (industry-standard parameter)",
        ),
        FieldSchema(
            name="dbname",
            required=False,
            field_type="string",
            description="Database name (legacy parameter for backward compatibility)",
        ),
        # Username - support both legacy and industry-standard naming
        FieldSchema(
            name="username",
            required=False,
            field_type="string",
            description="Database username (industry-standard parameter)",
        ),
        FieldSchema(
            name="user",
            required=False,
            field_type="string",
            description="Database username (legacy parameter for backward compatibility)",
        ),
        FieldSchema(
            name="password",
            required=False,
            field_type="string",
            description="Database password",
        ),
        FieldSchema(
            name="schema",
            required=False,
            field_type="string",
            description="Database schema name (default: public)",
        ),
        # Data source parameters
        FieldSchema(
            name="table",
            required=False,
            field_type="string",
            description="Table name to read from",
        ),
        FieldSchema(
            name="query",
            required=False,
            field_type="string",
            description="Custom SQL query (alternative to table)",
        ),
        # Incremental loading parameters (industry-standard)
        FieldSchema(
            name="sync_mode",
            required=False,
            field_type="string",
            description="Synchronization mode for incremental loading",
            allowed_values=["full_refresh", "incremental"],
        ),
        FieldSchema(
            name="cursor_field",
            required=False,
            field_type="string",
            description="Cursor field for incremental loading (e.g., timestamp, id)",
        ),
        FieldSchema(
            name="primary_key",
            required=False,
            field_type="string",
            description="Primary key field(s) for incremental loading",
        ),
        # Connection management parameters
        FieldSchema(
            name="connect_timeout",
            required=False,
            field_type="integer",
            description="Connection timeout in seconds (default: 10)",
        ),
        FieldSchema(
            name="application_name",
            required=False,
            field_type="string",
            description="Application name for connection identification (default: sqlflow)",
        ),
        FieldSchema(
            name="min_connections",
            required=False,
            field_type="integer",
            description="Minimum connections in pool (default: 1)",
        ),
        FieldSchema(
            name="max_connections",
            required=False,
            field_type="integer",
            description="Maximum connections in pool (default: 5)",
        ),
        FieldSchema(
            name="sslmode",
            required=False,
            field_type="string",
            description="SSL mode for connection (default: prefer)",
            allowed_values=[
                "disable",
                "allow",
                "prefer",
                "require",
                "verify-ca",
                "verify-full",
            ],
        ),
        # Performance parameters (industry-standard)
        FieldSchema(
            name="batch_size",
            required=False,
            field_type="integer",
            description="Number of records to process in each batch (default: 10000)",
        ),
        FieldSchema(
            name="timeout_seconds",
            required=False,
            field_type="integer",
            description="Timeout for operations in seconds (default: 300)",
        ),
        FieldSchema(
            name="max_retries",
            required=False,
            field_type="integer",
            description="Maximum number of retry attempts (default: 3)",
        ),
        # Legacy connection string support (for backward compatibility)
        FieldSchema(
            name="connection",
            required=False,
            field_type="string",
            description="PostgreSQL connection string (legacy parameter)",
            pattern=r"^postgresql://.*",
        ),
    ],
)


# Custom validator function for PostgreSQL schema that checks conditional requirements
def validate_postgres_params(params: Dict[str, Any]) -> List[str]:
    """Custom validation for PostgreSQL parameters.

    Ensures either connection string OR individual connection parameters are provided.
    """
    # First run standard validation
    errors = POSTGRES_SCHEMA.validate(params)

    # Filter out the "Required field 'host' is missing" error - we'll handle this conditionally
    errors = [err for err in errors if "Required field 'host' is missing" not in err]

    # Check conditional requirements
    has_connection_string = params.get("connection")
    has_individual_params = params.get("host")

    if not has_connection_string and not has_individual_params:
        errors.append(
            "Either 'connection' (connection string) or 'host' (individual parameters) must be provided"
        )

    return errors


S3_SCHEMA = ConnectorSchema(
    name="S3",
    description="Enhanced Amazon S3 connector with cost management, partition awareness, and multi-format support",
    fields=[
        # Core S3 parameters
        FieldSchema(
            name="bucket",
            required=True,
            field_type="string",
            description="S3 bucket name",
        ),
        FieldSchema(
            name="key",
            required=False,  # Not always required - can use path_prefix for discovery
            field_type="string",
            description="S3 object key (file path)",
        ),
        FieldSchema(
            name="region",
            required=False,
            field_type="string",
            description="AWS region (default: us-east-1)",
        ),
        FieldSchema(
            name="path_prefix",
            required=False,
            field_type="string",
            description="S3 path prefix for object discovery",
        ),
        # Authentication parameters
        FieldSchema(
            name="access_key_id",
            required=False,
            field_type="string",
            description="AWS access key ID",
        ),
        FieldSchema(
            name="secret_access_key",
            required=False,
            field_type="string",
            description="AWS secret access key",
        ),
        FieldSchema(
            name="session_token",
            required=False,
            field_type="string",
            description="AWS session token",
        ),
        FieldSchema(
            name="endpoint_url",
            required=False,
            field_type="string",
            description="Custom S3 endpoint URL (for MinIO, localstack, etc.)",
        ),
        # File format and processing parameters
        FieldSchema(
            name="file_format",
            required=False,
            field_type="string",
            description="File format",
            allowed_values=["csv", "parquet", "json", "jsonl", "tsv", "avro"],
        ),
        FieldSchema(
            name="compression",
            required=False,
            field_type="string",
            description="Compression format",
            allowed_values=["gzip", "bzip2", "lz4", "snappy"],
        ),
        # Cost management parameters
        FieldSchema(
            name="cost_limit_usd",
            required=False,
            field_type="number",  # Allow both string and numeric for JSON compatibility
            description="Maximum cost limit in USD",
        ),
        FieldSchema(
            name="max_files_per_run",
            required=False,
            field_type="integer",
            description="Maximum number of files to process per run",
        ),
        FieldSchema(
            name="max_data_size_gb",
            required=False,
            field_type="number",  # Allow both string and numeric for JSON compatibility
            description="Maximum data size in GB",
        ),
        # Development features
        FieldSchema(
            name="dev_sampling",
            required=False,
            field_type="number",  # Allow both string and numeric for JSON compatibility
            description="Development sampling rate (0.0-1.0)",
        ),
        FieldSchema(
            name="dev_max_files",
            required=False,
            field_type="integer",
            description="Maximum files for development mode",
        ),
        # Partition awareness
        FieldSchema(
            name="partition_keys",
            required=False,
            field_type="string",  # Allow comma-separated string
            description="Partition keys (comma-separated)",
        ),
        FieldSchema(
            name="partition_filter",
            required=False,
            field_type="string",  # JSON string for complex filters
            description="Partition filter criteria",
        ),
        # Incremental loading parameters
        FieldSchema(
            name="sync_mode",
            required=False,
            field_type="string",
            description="Synchronization mode",
            allowed_values=["full_refresh", "incremental"],
        ),
        FieldSchema(
            name="cursor_field",
            required=False,
            field_type="string",
            description="Cursor field for incremental loading",
        ),
        # Performance parameters
        FieldSchema(
            name="batch_size",
            required=False,
            field_type="integer",
            description="Number of rows to process per batch",
        ),
        FieldSchema(
            name="parallel_workers",
            required=False,
            field_type="integer",
            description="Number of parallel processing workers",
        ),
        FieldSchema(
            name="max_retries",
            required=False,
            field_type="integer",
            description="Maximum number of retry attempts",
        ),
        FieldSchema(
            name="timeout_seconds",
            required=False,
            field_type="integer",
            description="Request timeout in seconds",
        ),
        # Security parameters
        FieldSchema(
            name="use_ssl",
            required=False,
            field_type="boolean",
            description="Use SSL for connections",
        ),
        FieldSchema(
            name="server_side_encryption",
            required=False,
            field_type="string",
            description="Server-side encryption configuration",
        ),
        # Format-specific parameters
        FieldSchema(
            name="csv_delimiter",
            required=False,
            field_type="string",
            description="CSV field delimiter",
        ),
        FieldSchema(
            name="csv_header",
            required=False,
            field_type="boolean",
            description="CSV has header row",
        ),
        FieldSchema(
            name="csv_encoding",
            required=False,
            field_type="string",
            description="CSV file encoding",
        ),
        FieldSchema(
            name="parquet_columns",
            required=False,
            field_type="string",  # Allow string for JSON compatibility
            description="Parquet columns to read",
        ),
        FieldSchema(
            name="parquet_filters",
            required=False,
            field_type="string",  # Allow string for JSON compatibility
            description="Parquet row group filters",
        ),
        FieldSchema(
            name="json_flatten",
            required=False,
            field_type="boolean",
            description="Flatten nested JSON objects",
        ),
        FieldSchema(
            name="json_max_depth",
            required=False,
            field_type="integer",
            description="Maximum JSON nesting depth",
        ),
        # Legacy parameter support for backward compatibility
        FieldSchema(
            name="format",
            required=False,
            field_type="string",
            description="File format (legacy parameter)",
            allowed_values=["csv", "parquet", "json", "jsonl", "tsv", "avro"],
        ),
        FieldSchema(
            name="prefix",
            required=False,
            field_type="string",
            description="S3 path prefix (legacy parameter)",
        ),
        FieldSchema(
            name="access_key",
            required=False,
            field_type="string",
            description="AWS access key (legacy parameter)",
        ),
        FieldSchema(
            name="secret_key",
            required=False,
            field_type="string",
            description="AWS secret key (legacy parameter)",
        ),
        # Testing parameters
        FieldSchema(
            name="mock_mode",
            required=False,
            field_type="boolean",
            description="Enable mock mode for testing (bypasses real S3 calls)",
        ),
    ],
)

# Registry of all connector schemas
CONNECTOR_SCHEMAS: Dict[str, ConnectorSchema] = {
    "CSV": CSV_SCHEMA,
    "POSTGRES": POSTGRES_SCHEMA,
    "S3": S3_SCHEMA,
}

# Shopify connector schema (based on implementation plan)
SHOPIFY_SCHEMA = ConnectorSchema(
    name="SHOPIFY",
    description="Shopify e-commerce connector for orders, customers, and products",
    fields=[
        # Required parameters
        FieldSchema(
            name="shop_domain",
            required=True,
            field_type="string",
            description="Shopify shop domain (e.g., 'mystore.myshopify.com')",
            pattern=r"^[a-zA-Z0-9-]+\.myshopify\.com$",
        ),
        FieldSchema(
            name="access_token",
            required=True,
            field_type="string",
            description="Shopify private app access token",
        ),
        # Standard sync parameters (Airbyte/Fivetran compatible)
        FieldSchema(
            name="sync_mode",
            required=False,
            field_type="string",
            description="Synchronization mode",
            allowed_values=["full_refresh", "incremental"],
        ),
        FieldSchema(
            name="cursor_field",
            required=False,
            field_type="string",
            description="Field to use for incremental loading",
        ),
        FieldSchema(
            name="lookback_window",
            required=False,
            field_type="string",
            description="ISO 8601 duration for lookback buffer (e.g., 'P7D')",
        ),
        # SME-specific parameters
        FieldSchema(
            name="flatten_line_items",
            required=False,
            field_type="boolean",
            description="Flatten order line items into separate rows",
        ),
        FieldSchema(
            name="financial_status_filter",
            required=False,
            field_type="array",
            description="Filter orders by financial status",
        ),
        FieldSchema(
            name="include_fulfillments",
            required=False,
            field_type="boolean",
            description="Include fulfillment data",
        ),
        FieldSchema(
            name="include_refunds",
            required=False,
            field_type="boolean",
            description="Include refund data",
        ),
        # Performance parameters
        FieldSchema(
            name="batch_size",
            required=False,
            field_type="integer",
            description="Number of records to process per batch",
        ),
        FieldSchema(
            name="timeout_seconds",
            required=False,
            field_type="integer",
            description="Request timeout in seconds",
        ),
        FieldSchema(
            name="max_retries",
            required=False,
            field_type="integer",
            description="Maximum number of retry attempts",
        ),
    ],
)

# Registry of all connector schemas
CONNECTOR_SCHEMAS: Dict[str, ConnectorSchema] = {
    "CSV": CSV_SCHEMA,
    "POSTGRES": POSTGRES_SCHEMA,
    "S3": S3_SCHEMA,
    "SHOPIFY": SHOPIFY_SCHEMA,
}
