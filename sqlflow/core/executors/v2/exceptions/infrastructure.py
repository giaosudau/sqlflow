"""Infrastructure-related exceptions for SQLFlow V2 Executor.

These exceptions handle errors in database connections, configuration, and resources.
"""

from typing import Any, Dict, List, Optional

from .base import SQLFlowError


class DatabaseError(SQLFlowError):
    """Error in database operations."""

    def __init__(
        self,
        message: str,
        database_type: Optional[str] = None,
        operation: Optional[str] = None,
        query: Optional[str] = None,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Check database connectivity and credentials",
            "Verify database permissions",
            "Review database logs for additional details",
        ]

        if operation:
            actions.append(f"Verify {operation} operation is supported")

        if query:
            actions.extend(
                [
                    "Check SQL query syntax and semantics",
                    "Verify table and column names exist",
                ]
            )

        db_context = {}
        if database_type:
            db_context["database_type"] = database_type
        if operation:
            db_context["operation"] = operation
        if query:
            # Truncate long queries for readability
            display_query = query[:300] + "..." if len(query) > 300 else query
            db_context["query"] = display_query
        if error_code:
            db_context["database_error_code"] = error_code

        if context:
            db_context.update(context)

        super().__init__(
            message=message,
            error_code=error_code,
            context=db_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.database_type = database_type
        self.operation = operation


class ConnectionError(SQLFlowError):
    """Error establishing or maintaining connections."""

    def __init__(
        self,
        service: str,
        message: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
        retry_count: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            f"Check {service} service availability",
            "Verify network connectivity",
            "Check firewall and security group settings",
        ]

        if host and port:
            actions.append(f"Test direct connection to {host}:{port}")

        if timeout_seconds:
            actions.append(f"Consider increasing timeout from {timeout_seconds}s")

        if retry_count and retry_count > 0:
            actions.append(f"Connection failed after {retry_count} retries")

        conn_context = {"service": service}
        if host:
            conn_context["host"] = host
        if port:
            conn_context["port"] = port
        if timeout_seconds:
            conn_context["timeout_seconds"] = timeout_seconds
        if retry_count is not None:
            conn_context["retry_count"] = retry_count

        if context:
            conn_context.update(context)

        super().__init__(
            message=f"Connection to {service} failed: {message}",
            context=conn_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.service = service
        self.host = host
        self.port = port


class ResourceExhaustionError(SQLFlowError):
    """Error due to resource exhaustion."""

    def __init__(
        self,
        resource_type: str,
        message: str,
        current_usage: Optional[float] = None,
        limit: Optional[float] = None,
        unit: str = "units",
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            f"Increase {resource_type} allocation",
            "Optimize operations to use fewer resources",
            "Consider processing data in smaller chunks",
            "Review resource usage patterns",
        ]

        if current_usage and limit:
            usage_percent = (current_usage / limit) * 100
            actions.append(
                f"Current usage: {current_usage:.1f}{unit} ({usage_percent:.1f}% of limit)"
            )

        resource_context = {
            "resource_type": resource_type,
            "unit": unit,
        }
        if current_usage is not None:
            resource_context["current_usage"] = current_usage
        if limit is not None:
            resource_context["limit"] = limit

        if context:
            resource_context.update(context)

        super().__init__(
            message=f"{resource_type} exhausted: {message}",
            context=resource_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.resource_type = resource_type
        self.current_usage = current_usage
        self.limit = limit


class ConfigurationError(SQLFlowError):
    """Error in configuration or setup."""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        expected_type: Optional[str] = None,
        valid_values: Optional[List[Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Review configuration documentation",
            "Check configuration file syntax",
            "Verify all required configuration is provided",
        ]

        if config_key:
            actions.append(f"Check configuration key: {config_key}")

        if expected_type:
            actions.append(f"Ensure value is of type: {expected_type}")

        if valid_values:
            actions.append(f"Use one of valid values: {valid_values}")

        config_context = {}
        if config_key:
            config_context["config_key"] = config_key
        if config_value is not None:
            config_context["config_value"] = config_value
        if expected_type:
            config_context["expected_type"] = expected_type
        if valid_values:
            config_context["valid_values"] = valid_values

        if context:
            config_context.update(context)

        super().__init__(
            message=message,
            context=config_context,
            suggested_actions=actions,
            recoverable=False,  # Configuration errors usually require manual intervention
        )

        self.config_key = config_key
        self.config_value = config_value
