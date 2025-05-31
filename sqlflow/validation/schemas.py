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
    ],
)

POSTGRES_SCHEMA = ConnectorSchema(
    name="POSTGRES",
    description="PostgreSQL database connector",
    fields=[
        FieldSchema(
            name="connection",
            required=True,
            field_type="string",
            description="PostgreSQL connection string",
            pattern=r"^postgresql://.*",
        ),
        FieldSchema(
            name="table",
            required=True,
            field_type="string",
            description="Table name to read from or write to",
        ),
        FieldSchema(
            name="schema",
            required=False,
            field_type="string",
            description="Database schema name",
        ),
        FieldSchema(
            name="query",
            required=False,
            field_type="string",
            description="Custom SQL query (alternative to table)",
        ),
    ],
)

S3_SCHEMA = ConnectorSchema(
    name="S3",
    description="Amazon S3 connector for reading/writing files",
    fields=[
        FieldSchema(
            name="bucket",
            required=True,
            field_type="string",
            description="S3 bucket name",
        ),
        FieldSchema(
            name="key",
            required=True,
            field_type="string",
            description="S3 object key (file path)",
        ),
        FieldSchema(
            name="region", required=False, field_type="string", description="AWS region"
        ),
        FieldSchema(
            name="format",
            required=False,
            field_type="string",
            description="File format",
            allowed_values=["csv", "parquet", "json"],
        ),
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
    ],
)

# Registry of all connector schemas
CONNECTOR_SCHEMAS: Dict[str, ConnectorSchema] = {
    "CSV": CSV_SCHEMA,
    "POSTGRES": POSTGRES_SCHEMA,
    "S3": S3_SCHEMA,
}
