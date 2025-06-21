"""Tests for the comprehensive V2 exception hierarchy.

Following Week 7-8 requirements:
- Clear error messages with actionable information
- No silent failures
- Proper error context propagation
- Performance metrics collection
"""

from datetime import datetime

import pytest

from sqlflow.core.executors.v2.exceptions import (  # Base exceptions; Execution exceptions; Data exceptions; Infrastructure exceptions; Runtime exceptions
    ConfigurationError,
    ConnectionError,
    DatabaseError,
    DataConnectorError,
    DataTransformationError,
    DataValidationError,
    DependencyResolutionError,
    PermissionError,
    PipelineExecutionError,
    ResourceExhaustionError,
    SchemaValidationError,
    SecurityError,
    SQLFlowError,
    SQLFlowWarning,
    StepExecutionError,
    StepTimeoutError,
    UDFExecutionError,
    VariableSubstitutionError,
)


class TestBaseExceptions:
    """Test base exception classes."""

    def test_sqlflow_error_basic(self):
        """Test basic SQLFlowError functionality."""
        error = SQLFlowError("Test error message")

        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.error_code == "SQLFLOWERROR"
        assert error.context == {}
        assert error.suggested_actions == []
        assert not error.recoverable
        assert isinstance(error.timestamp, datetime)

    def test_sqlflow_error_with_full_context(self):
        """Test SQLFlowError with complete context."""
        context = {"table": "customers", "row_count": 1000}
        actions = ["Check data source", "Verify permissions"]

        error = SQLFlowError(
            message="Data processing failed",
            error_code="DATA_001",
            context=context,
            suggested_actions=actions,
            recoverable=True,
        )

        assert error.message == "Data processing failed"
        assert error.error_code == "DATA_001"
        assert error.context == context
        assert error.suggested_actions == actions
        assert error.recoverable

    def test_sqlflow_error_to_dict(self):
        """Test SQLFlowError serialization."""
        error = SQLFlowError(
            message="Test error",
            error_code="TEST_001",
            context={"key": "value"},
            suggested_actions=["Action 1"],
            recoverable=True,
        )

        error_dict = error.to_dict()

        assert error_dict["error_type"] == "SQLFlowError"
        assert error_dict["message"] == "Test error"
        assert error_dict["error_code"] == "TEST_001"
        assert error_dict["context"] == {"key": "value"}
        assert error_dict["suggested_actions"] == ["Action 1"]
        assert error_dict["recoverable"] is True
        assert "timestamp" in error_dict

    def test_sqlflow_error_string_representation(self):
        """Test SQLFlowError string representation with context."""
        error = SQLFlowError(
            message="Processing failed",
            context={"table": "users", "error_count": 5},
            suggested_actions=["Check data", "Retry operation"],
        )

        error_str = str(error)
        assert "Processing failed" in error_str
        assert "table=users" in error_str
        assert "error_count=5" in error_str
        assert "Check data" in error_str
        assert "Retry operation" in error_str

    def test_sqlflow_warning(self):
        """Test SQLFlowWarning functionality."""
        warning = SQLFlowWarning(
            message="Data quality issue detected",
            context={"affected_rows": 10},
            suggested_actions=["Review data source"],
        )

        assert warning.message == "Data quality issue detected"
        assert warning.context == {"affected_rows": 10}
        assert warning.suggested_actions == ["Review data source"]
        assert isinstance(warning.timestamp, datetime)


class TestExecutionExceptions:
    """Test execution-related exceptions."""

    def test_step_execution_error(self):
        """Test StepExecutionError with step context."""
        error = StepExecutionError(
            step_id="load_customers",
            step_type="load",
            message="Connection timeout",
            original_error=RuntimeError("Database unreachable"),
        )

        assert error.step_id == "load_customers"
        assert error.step_type == "load"
        assert "load_customers" in error.message
        assert "load" in error.message
        assert "Connection timeout" in error.message
        assert error.context["step_id"] == "load_customers"
        assert error.context["step_type"] == "load"
        assert "original_error" in error.context
        assert error.recoverable

    def test_pipeline_execution_error(self):
        """Test PipelineExecutionError with failed steps."""
        failed_steps = ["step1", "step2", "step3"]
        error = PipelineExecutionError(
            pipeline_id="customer_etl",
            message="Multiple steps failed",
            failed_steps=failed_steps,
        )

        assert error.pipeline_id == "customer_etl"
        assert error.failed_steps == failed_steps
        assert "customer_etl" in error.message
        assert error.context["pipeline_id"] == "customer_etl"
        assert error.context["failed_steps"] == failed_steps
        assert not error.recoverable

    def test_dependency_resolution_error_missing(self):
        """Test DependencyResolutionError for missing dependencies."""
        missing_deps = ["extract_data", "validate_schema"]
        error = DependencyResolutionError(
            step_id="transform_data", missing_dependencies=missing_deps
        )

        assert error.step_id == "transform_data"
        assert error.missing_dependencies == missing_deps
        assert "Missing dependencies" in error.message
        assert "extract_data" in error.message
        assert not error.recoverable

    def test_dependency_resolution_error_circular(self):
        """Test DependencyResolutionError for circular dependencies."""
        circular_deps = ["step_a", "step_b", "step_c", "step_a"]
        error = DependencyResolutionError(
            step_id="step_a", circular_dependencies=circular_deps
        )

        assert error.step_id == "step_a"
        assert error.circular_dependencies == circular_deps
        assert "Circular dependency" in error.message
        assert not error.recoverable

    def test_step_timeout_error(self):
        """Test StepTimeoutError with timing information."""
        error = StepTimeoutError(
            step_id="long_running_query",
            step_type="transform",
            timeout_seconds=300.0,
            actual_duration_seconds=450.0,
        )

        assert error.step_id == "long_running_query"
        assert error.step_type == "transform"
        assert error.timeout_seconds == 300.0
        assert error.actual_duration_seconds == 450.0
        assert "timed out after 300.0s" in error.message
        assert "ran for 450.0s" in error.message
        assert error.recoverable


class TestDataExceptions:
    """Test data-related exceptions."""

    def test_data_validation_error(self):
        """Test DataValidationError with validation context."""
        error = DataValidationError(
            message="Email format validation failed",
            table_name="customers",
            column_name="email",
            validation_rule="email_format_check",
            failed_records=25,
        )

        assert error.context["table_name"] == "customers"
        assert error.context["column_name"] == "email"
        assert error.context["validation_rule"] == "email_format_check"
        assert error.context["failed_records"] == 25
        assert error.recoverable

    def test_data_transformation_error(self):
        """Test DataTransformationError with SQL context."""
        sql_query = "SELECT * FROM customers WHERE invalid_column = 'value'"
        error = DataTransformationError(
            message="Column not found",
            transformation_type="aggregation",
            source_table="customers",
            target_table="customer_summary",
            sql_query=sql_query,
        )

        assert error.context["transformation_type"] == "aggregation"
        assert error.context["source_table"] == "customers"
        assert error.context["target_table"] == "customer_summary"
        assert error.context["sql_query"] == sql_query
        assert error.recoverable

    def test_data_connector_error(self):
        """Test DataConnectorError with connector context."""
        source_config = {
            "type": "postgres",
            "host": "localhost",
            "password": "secret123",  # Should be filtered out
            "database": "testdb",
        }

        error = DataConnectorError(
            connector_type="postgres",
            operation="read",
            message="Connection refused",
            source_config=source_config,
        )

        assert error.connector_type == "postgres"
        assert error.operation == "read"
        assert "postgres connector read failed" in error.message
        assert error.context["connector_type"] == "postgres"
        assert error.context["operation"] == "read"
        # Verify sensitive data is filtered
        assert "password" not in error.context["source_config"]
        assert error.context["source_config"]["host"] == "localhost"
        assert error.recoverable

    def test_schema_validation_error(self):
        """Test SchemaValidationError with schema differences."""
        expected_schema = {"id": "int", "name": "string", "email": "string"}
        actual_schema = {"id": "int", "name": "string", "phone": "string"}

        error = SchemaValidationError(
            message="Schema mismatch detected",
            expected_schema=expected_schema,
            actual_schema=actual_schema,
            missing_columns=["email"],
            extra_columns=["phone"],
            type_mismatches={"id": "expected int, got string"},
        )

        assert error.missing_columns == ["email"]
        assert error.extra_columns == ["phone"]
        assert error.type_mismatches == {"id": "expected int, got string"}
        assert error.context["expected_schema"] == expected_schema
        assert error.context["actual_schema"] == actual_schema
        assert error.recoverable


class TestInfrastructureExceptions:
    """Test infrastructure-related exceptions."""

    def test_database_error(self):
        """Test DatabaseError with database context."""
        query = "SELECT * FROM non_existent_table"
        error = DatabaseError(
            message="Table does not exist",
            database_type="postgresql",
            operation="select",
            query=query,
            error_code="42P01",
        )

        assert error.database_type == "postgresql"
        assert error.operation == "select"
        assert error.context["database_type"] == "postgresql"
        assert error.context["operation"] == "select"
        assert error.context["query"] == query
        assert error.context["database_error_code"] == "42P01"
        assert error.recoverable

    def test_connection_error(self):
        """Test ConnectionError with connection details."""
        error = ConnectionError(
            service="database",
            message="Connection timed out",
            host="db.example.com",
            port=5432,
            timeout_seconds=30.0,
            retry_count=3,
        )

        assert error.service == "database"
        assert error.host == "db.example.com"
        assert error.port == 5432
        assert "Connection to database failed" in error.message
        assert error.context["host"] == "db.example.com"
        assert error.context["port"] == 5432
        assert error.context["timeout_seconds"] == 30.0
        assert error.context["retry_count"] == 3
        assert error.recoverable

    def test_resource_exhaustion_error(self):
        """Test ResourceExhaustionError with resource metrics."""
        error = ResourceExhaustionError(
            resource_type="memory",
            message="Out of memory",
            current_usage=1024.0,
            limit=1000.0,
            unit="MB",
        )

        assert error.resource_type == "memory"
        assert error.current_usage == 1024.0
        assert error.limit == 1000.0
        assert "memory exhausted" in error.message
        assert error.context["resource_type"] == "memory"
        assert error.context["current_usage"] == 1024.0
        assert error.context["limit"] == 1000.0
        assert error.context["unit"] == "MB"
        assert error.recoverable

    def test_configuration_error(self):
        """Test ConfigurationError with config context."""
        error = ConfigurationError(
            message="Invalid database port",
            config_key="database.port",
            config_value="invalid_port",
            expected_type="integer",
            valid_values=[5432, 3306, 1433],
        )

        assert error.config_key == "database.port"
        assert error.config_value == "invalid_port"
        assert error.context["config_key"] == "database.port"
        assert error.context["config_value"] == "invalid_port"
        assert error.context["expected_type"] == "integer"
        assert error.context["valid_values"] == [5432, 3306, 1433]
        assert not error.recoverable


class TestRuntimeExceptions:
    """Test runtime-related exceptions."""

    def test_variable_substitution_error(self):
        """Test VariableSubstitutionError with variable context."""
        error = VariableSubstitutionError(
            message="Variable not found",
            variable_name="customer_id",
            template_text="SELECT * FROM customers WHERE id = ${customer_id}",
            missing_variables=["customer_id", "date_filter"],
        )

        assert error.variable_name == "customer_id"
        assert error.missing_variables == ["customer_id", "date_filter"]
        assert error.context["variable_name"] == "customer_id"
        assert "SELECT * FROM customers" in error.context["template_text"]
        assert error.context["missing_variables"] == ["customer_id", "date_filter"]
        assert error.recoverable

    def test_udf_execution_error(self):
        """Test UDFExecutionError with UDF context."""
        udf_code = "def calculate_total(price, tax): return price * (1 + tax)"
        original_error = TypeError("unsupported operand type(s)")

        error = UDFExecutionError(
            udf_name="calculate_total",
            message="Type error in calculation",
            udf_code=udf_code,
            input_args=[100, "invalid_tax"],
            original_error=original_error,
        )

        assert error.udf_name == "calculate_total"
        assert error.original_error == original_error
        assert "UDF 'calculate_total' execution failed" in error.message
        assert error.context["udf_name"] == "calculate_total"
        assert error.context["udf_code"] == udf_code
        assert error.context["input_args"] == [100, "invalid_tax"]
        assert error.context["original_error_type"] == "TypeError"
        assert error.recoverable

    def test_permission_error(self):
        """Test PermissionError with permission context."""
        error = PermissionError(
            resource="customers_table",
            operation="delete",
            message="User lacks delete permissions",
            user="analyst_user",
            required_permissions=["DELETE", "TRUNCATE"],
        )

        assert error.resource == "customers_table"
        assert error.operation == "delete"
        assert error.user == "analyst_user"
        assert "Permission denied for delete on customers_table" in error.message
        assert error.context["resource"] == "customers_table"
        assert error.context["operation"] == "delete"
        assert error.context["user"] == "analyst_user"
        assert error.context["required_permissions"] == ["DELETE", "TRUNCATE"]
        assert not error.recoverable

    def test_security_error(self):
        """Test SecurityError with security context."""
        error = SecurityError(
            message="Unauthorized access attempt detected",
            security_check="data_access_policy",
            violation_type="unauthorized_table_access",
        )

        assert error.security_check == "data_access_policy"
        assert error.violation_type == "unauthorized_table_access"
        assert "Security violation" in error.message
        assert error.context["security_check"] == "data_access_policy"
        assert error.context["violation_type"] == "unauthorized_table_access"
        assert not error.recoverable


class TestWeek7And8ExceptionRequirements:
    """Test specific Week 7-8 exception requirements."""

    def test_clear_error_messages_with_actionable_information(self):
        """Test that all exceptions provide clear, actionable error messages."""
        error = StepExecutionError(
            step_id="extract_data",
            step_type="load",
            message="Database connection failed",
            context={"database": "postgres", "host": "localhost"},
        )

        # Clear error message
        assert "extract_data" in error.message
        assert "load" in error.message
        assert "Database connection failed" in error.message

        # Actionable information
        assert len(error.suggested_actions) > 0
        assert any("check" in action.lower() for action in error.suggested_actions)
        assert any("verify" in action.lower() for action in error.suggested_actions)

    def test_proper_error_context_propagation(self):
        """Test that error context is properly propagated."""
        original_error = ValueError("Invalid input format")

        step_error = StepExecutionError(
            step_id="process_data",
            step_type="transform",
            message="Data processing failed",
            original_error=original_error,
            context={"table": "customers", "batch_size": 1000},
        )

        # Context includes step information
        assert step_error.context["step_id"] == "process_data"
        assert step_error.context["step_type"] == "transform"
        assert step_error.context["table"] == "customers"
        assert step_error.context["batch_size"] == 1000

        # Original error is preserved
        assert step_error.context["original_error"] == str(original_error)
        assert step_error.context["original_error_type"] == "ValueError"
        assert step_error.original_error == original_error

    def test_no_silent_failures_principle(self):
        """Test that exceptions follow the no silent failures principle."""
        # All exceptions should be raiseable and catchable
        exceptions_to_test = [
            SQLFlowError("Test error"),
            StepExecutionError("step1", "load", "Test failure"),
            DataValidationError("Validation failed"),
            DatabaseError("DB error"),
            VariableSubstitutionError("Variable error"),
        ]

        for exc in exceptions_to_test:
            # Should be raiseable
            with pytest.raises(SQLFlowError):
                raise exc

            # Should have meaningful message
            assert len(str(exc)) > 0
            assert exc.message

            # Should have timestamp
            assert isinstance(exc.timestamp, datetime)

    def test_exception_serialization_for_observability(self):
        """Test that exceptions can be serialized for observability."""
        error = DataConnectorError(
            connector_type="csv",
            operation="read",
            message="File not found",
            source_config={"path": "/data/missing.csv"},
            context={"expected_rows": 1000},
        )

        # Should be serializable
        error_dict = error.to_dict()

        # Contains all required fields
        assert "error_type" in error_dict
        assert "message" in error_dict
        assert "context" in error_dict
        assert "suggested_actions" in error_dict
        assert "timestamp" in error_dict

        # Context is preserved
        assert error_dict["context"]["connector_type"] == "csv"
        assert error_dict["context"]["operation"] == "read"
        assert error_dict["context"]["expected_rows"] == 1000

    def test_exception_hierarchy_inheritance(self):
        """Test that exception hierarchy is properly structured."""
        # All exceptions should inherit from SQLFlowError
        exceptions = [
            StepExecutionError("step1", "load", "Error"),
            PipelineExecutionError("pipeline1", "Error"),
            DataValidationError("Error"),
            DatabaseError("Error"),
            VariableSubstitutionError("Error"),
        ]

        for exc in exceptions:
            assert isinstance(exc, SQLFlowError)
            assert isinstance(exc, Exception)

            # Should have all base attributes
            assert hasattr(exc, "message")
            assert hasattr(exc, "error_code")
            assert hasattr(exc, "context")
            assert hasattr(exc, "suggested_actions")
            assert hasattr(exc, "recoverable")
            assert hasattr(exc, "timestamp")

    def test_recovery_information_provided(self):
        """Test that exceptions provide recovery information."""
        recoverable_error = DataValidationError(
            message="Data quality check failed",
            table_name="customers",
            validation_rule="email_format",
        )

        non_recoverable_error = ConfigurationError(
            message="Invalid configuration",
            config_key="database.type",
            config_value="invalid_db",
        )

        # Recoverable error should indicate it can be recovered
        assert recoverable_error.recoverable
        assert len(recoverable_error.suggested_actions) > 0

        # Non-recoverable error should indicate manual intervention needed
        assert not non_recoverable_error.recoverable
        assert len(non_recoverable_error.suggested_actions) > 0
