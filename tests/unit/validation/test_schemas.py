"""Tests for connector parameter validation schemas."""

from sqlflow.validation.schemas import (
    CONNECTOR_SCHEMAS,
    CSV_SCHEMA,
    POSTGRES_SCHEMA,
    S3_SCHEMA,
    SHOPIFY_SCHEMA,
    ConnectorSchema,
    FieldSchema,
)


class TestFieldSchema:
    """Test FieldSchema validation functionality."""

    def test_required_field_missing(self):
        """Test validation of missing required field."""
        field = FieldSchema(name="test_field", required=True)
        errors = field.validate(None)

        assert len(errors) == 1
        assert "Required field 'test_field' is missing" in errors[0]

    def test_optional_field_missing(self):
        """Test validation of missing optional field."""
        field = FieldSchema(name="test_field", required=False)
        errors = field.validate(None)

        assert len(errors) == 0

    def test_string_type_validation(self):
        """Test string type validation."""
        field = FieldSchema(name="test_field", field_type="string")

        # Valid string
        errors = field.validate("valid_string")
        assert len(errors) == 0

        # Invalid type
        errors = field.validate(123)
        assert len(errors) == 1
        assert "must be a string" in errors[0]

    def test_integer_type_validation(self):
        """Test integer type validation."""
        field = FieldSchema(name="test_field", field_type="integer")

        # Valid integer
        errors = field.validate(42)
        assert len(errors) == 0

        # Invalid type
        errors = field.validate("not_an_int")
        assert len(errors) == 1
        assert "must be an integer" in errors[0]

    def test_boolean_type_validation(self):
        """Test boolean type validation."""
        field = FieldSchema(name="test_field", field_type="boolean")

        # Valid boolean
        errors = field.validate(True)
        assert len(errors) == 0

        errors = field.validate(False)
        assert len(errors) == 0

        # Invalid type
        errors = field.validate("true")
        assert len(errors) == 1
        assert "must be a boolean" in errors[0]

    def test_array_type_validation(self):
        """Test array type validation."""
        field = FieldSchema(name="test_field", field_type="array")

        # Valid array
        errors = field.validate([1, 2, 3])
        assert len(errors) == 0

        # Invalid type
        errors = field.validate("not_an_array")
        assert len(errors) == 1
        assert "must be an array" in errors[0]

    def test_allowed_values_validation(self):
        """Test validation against allowed values."""
        field = FieldSchema(
            name="test_field", allowed_values=["option1", "option2", "option3"]
        )

        # Valid value
        errors = field.validate("option1")
        assert len(errors) == 0

        # Invalid value
        errors = field.validate("invalid_option")
        assert len(errors) == 1
        assert "must be one of" in errors[0]
        assert "invalid_option" in errors[0]

    def test_pattern_validation(self):
        """Test regex pattern validation."""
        field = FieldSchema(
            name="test_field", field_type="string", pattern=r"^[a-z]+\.csv$"
        )

        # Valid pattern
        errors = field.validate("data.csv")
        assert len(errors) == 0

        # Invalid pattern
        errors = field.validate("data.txt")
        assert len(errors) == 1
        assert "does not match required pattern" in errors[0]


class TestConnectorSchema:
    """Test ConnectorSchema validation functionality."""

    def test_unknown_parameters(self):
        """Test detection of unknown parameters."""
        schema = ConnectorSchema(
            name="TEST",
            description="Test connector",
            fields=[FieldSchema(name="valid_param", required=True)],
        )

        params = {"valid_param": "value", "unknown_param": "value"}

        errors = schema.validate(params)
        assert len(errors) == 1
        assert "Unknown parameters" in errors[0]
        assert "unknown_param" in errors[0]

    def test_valid_parameters(self):
        """Test validation of valid parameters."""
        schema = ConnectorSchema(
            name="TEST",
            description="Test connector",
            fields=[
                FieldSchema(name="required_param", required=True),
                FieldSchema(name="optional_param", required=False),
            ],
        )

        params = {"required_param": "value"}

        errors = schema.validate(params)
        assert len(errors) == 0

    def test_missing_required_parameters(self):
        """Test detection of missing required parameters."""
        schema = ConnectorSchema(
            name="TEST",
            description="Test connector",
            fields=[FieldSchema(name="required_param", required=True)],
        )

        params = {}

        errors = schema.validate(params)
        assert len(errors) == 1
        assert "Required field 'required_param' is missing" in errors[0]


class TestBuiltInSchemas:
    """Test the built-in connector schemas."""

    def test_csv_schema_valid_params(self):
        """Test CSV schema with valid parameters."""
        params = {
            "path": "data.csv",
            "delimiter": ",",
            "has_header": True,
            "encoding": "utf-8",
        }

        errors = CSV_SCHEMA.validate(params)
        assert len(errors) == 0

    def test_csv_schema_minimal_params(self):
        """Test CSV schema with minimal required parameters."""
        params = {"path": "data.csv"}

        errors = CSV_SCHEMA.validate(params)
        assert len(errors) == 0

    def test_csv_schema_invalid_file_extension(self):
        """Test CSV schema with invalid file extension."""
        params = {"path": "data.txt"}

        errors = CSV_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "does not match required pattern" in errors[0]

    def test_csv_schema_invalid_delimiter(self):
        """Test CSV schema with invalid delimiter."""
        params = {"path": "data.csv", "delimiter": "invalid"}

        errors = CSV_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "must be one of" in errors[0]

    def test_postgres_schema_valid_params(self):
        """Test PostgreSQL schema with valid parameters."""
        params = {
            "connection": "postgresql://user:pass@localhost:5432/db",
            "table": "users",
            "schema": "public",
        }

        errors = POSTGRES_SCHEMA.validate(params)
        assert len(errors) == 0

    def test_postgres_schema_invalid_connection(self):
        """Test PostgreSQL schema with invalid connection string."""
        params = {"connection": "mysql://user:pass@localhost:3306/db", "table": "users"}

        errors = POSTGRES_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "does not match required pattern" in errors[0]

    def test_s3_schema_valid_params(self):
        """Test S3 schema with valid parameters."""
        params = {
            "bucket": "my-bucket",
            "key": "data/file.csv",
            "region": "us-east-1",
            "format": "csv",
        }

        errors = S3_SCHEMA.validate(params)
        assert len(errors) == 0

    def test_s3_schema_invalid_format(self):
        """Test S3 schema with invalid format."""
        params = {"bucket": "my-bucket", "key": "data/file.csv", "format": "xml"}

        errors = S3_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "must be one of" in errors[0]

    def test_shopify_schema_valid_params(self):
        """Test Shopify schema with valid parameters."""
        params = {
            "shop_domain": "mystore.myshopify.com",
            "access_token": "shpat_abcdef1234567890123456789012345678901234567890",
            "sync_mode": "incremental",
            "cursor_field": "updated_at",
            "flatten_line_items": True,
        }

        errors = SHOPIFY_SCHEMA.validate(params)
        assert len(errors) == 0

    def test_shopify_schema_missing_required(self):
        """Test Shopify schema with missing required parameters."""
        params = {"shop_domain": "mystore.myshopify.com"}  # Missing access_token

        errors = SHOPIFY_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "Required field 'access_token' is missing" in errors[0]

    def test_shopify_schema_invalid_domain(self):
        """Test Shopify schema with invalid shop domain."""
        params = {
            "shop_domain": "invalid-domain.com",
            "access_token": "shpat_abcdef1234567890123456789012345678901234567890",
        }

        errors = SHOPIFY_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "does not match required pattern" in errors[0]

    def test_shopify_schema_invalid_sync_mode(self):
        """Test Shopify schema with invalid sync mode."""
        params = {
            "shop_domain": "mystore.myshopify.com",
            "access_token": "shpat_abcdef1234567890123456789012345678901234567890",
            "sync_mode": "invalid_mode",
        }

        errors = SHOPIFY_SCHEMA.validate(params)
        assert len(errors) == 1
        assert "must be one of" in errors[0]

    def test_connector_schemas_registry(self):
        """Test that all expected connectors are in the registry."""
        expected_connectors = {"CSV", "POSTGRES", "S3", "SHOPIFY", "REST"}
        actual_connectors = set(CONNECTOR_SCHEMAS.keys())

        assert actual_connectors == expected_connectors

        # Verify each schema is properly configured
        for name, schema in CONNECTOR_SCHEMAS.items():
            assert schema.name == name
            assert schema.description
            assert len(schema.fields) > 0
