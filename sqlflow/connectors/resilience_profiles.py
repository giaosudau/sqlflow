"""Resilience profiles for different connector types.

This module provides default resilience configurations for each connector type
based on their characteristics and usage patterns.
"""

from typing import Dict, Optional

from sqlflow.connectors.resilience import (
    CircuitBreakerConfig,
    RateLimitConfig,
    RecoveryConfig,
    ResilienceConfig,
    RetryConfig,
)

# Import database-specific exceptions with fallback
try:
    import psycopg2

    PSYCOPG2_EXCEPTIONS = [
        psycopg2.OperationalError,
        psycopg2.InterfaceError,
        psycopg2.DatabaseError,
    ]
except ImportError:
    PSYCOPG2_EXCEPTIONS = []

try:
    from botocore.exceptions import ClientError

    AWS_EXCEPTIONS = [ClientError]
except ImportError:
    AWS_EXCEPTIONS = []

# REST API Connector (Critical Resilience)
DEFAULT_REST_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        max_delay=30.0,
        backoff_multiplier=2.0,
        jitter=True,
        retry_on_exceptions=[
            ConnectionError,
            TimeoutError,
        ],
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=5, recovery_timeout=60.0, success_threshold=2, timeout=30.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=60,
        burst_size=10,
        per_host=True,
        backpressure_strategy="wait",
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,
        recovery_check_interval=30.0,
    ),
)

# S3 Connector (High Resilience)
DEFAULT_S3_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        max_delay=60.0,
        backoff_multiplier=2.0,
        jitter=True,
        retry_on_exceptions=[
            ConnectionError,
            TimeoutError,
        ]
        + AWS_EXCEPTIONS,
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3, recovery_timeout=30.0, timeout=60.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=100, burst_size=5, backpressure_strategy="wait"
    ),
)

# PostgreSQL Connector (High Resilience)
DEFAULT_POSTGRES_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=0.5,
        max_delay=10.0,
        backoff_multiplier=2.0,
        jitter=True,
        retry_on_exceptions=[
            ConnectionError,
            TimeoutError,
        ]
        + PSYCOPG2_EXCEPTIONS,
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=30.0,
        timeout=30.0,
        excluded_exceptions=[
            ValueError,  # SQL syntax errors
            TypeError,  # Data type errors
        ],
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=False,  # DB credentials rarely change
        recovery_check_interval=15.0,
    ),
)

# File-based Connectors (Low Resilience)
DEFAULT_FILE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=2,
        initial_delay=0.1,
        max_delay=1.0,
        backoff_multiplier=1.5,
        jitter=False,
        retry_on_exceptions=[FileNotFoundError, PermissionError, OSError],
    ),
    # No circuit breaker or rate limiting for local files
    circuit_breaker=None,
    rate_limit=None,
    recovery=None,
)

# Google Sheets Connector (High Resilience)
DEFAULT_GOOGLE_SHEETS_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        max_delay=30.0,
        backoff_multiplier=2.0,
        jitter=True,
        retry_on_exceptions=[
            ConnectionError,
            TimeoutError,
        ],
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3, recovery_timeout=60.0, timeout=30.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=100,  # Google API limits
        burst_size=10,
        backpressure_strategy="wait",
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,
        recovery_check_interval=30.0,
    ),
)

# Shopify Connector (Critical Resilience)
DEFAULT_SHOPIFY_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        max_delay=30.0,
        backoff_multiplier=2.0,
        jitter=True,
        retry_on_exceptions=[
            ConnectionError,
            TimeoutError,
        ],
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3, recovery_timeout=60.0, timeout=30.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=2400,  # 40 requests/second
        burst_size=20,
        backpressure_strategy="wait",
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,
        recovery_check_interval=30.0,
    ),
)

# Connector type to configuration mapping
CONNECTOR_RESILIENCE_PROFILES: Dict[str, ResilienceConfig] = {
    # Network-dependent connectors (High/Critical resilience)
    "RestSource": DEFAULT_REST_CONFIG,
    "RestConnector": DEFAULT_REST_CONFIG,
    "S3Source": DEFAULT_S3_CONFIG,
    "S3Connector": DEFAULT_S3_CONFIG,
    "PostgresSource": DEFAULT_POSTGRES_CONFIG,
    "PostgresDestination": DEFAULT_POSTGRES_CONFIG,
    "PostgresConnector": DEFAULT_POSTGRES_CONFIG,
    "GoogleSheetsSource": DEFAULT_GOOGLE_SHEETS_CONFIG,
    "GoogleSheetsConnector": DEFAULT_GOOGLE_SHEETS_CONFIG,
    "ShopifySource": DEFAULT_SHOPIFY_CONFIG,
    "ShopifyConnector": DEFAULT_SHOPIFY_CONFIG,
    # File-based connectors (Low resilience)
    "CsvSource": DEFAULT_FILE_CONFIG,
    "CsvConnector": DEFAULT_FILE_CONFIG,
    "ParquetSource": DEFAULT_FILE_CONFIG,
    "ParquetConnector": DEFAULT_FILE_CONFIG,
    # In-memory connectors (No resilience)
    "InMemorySource": None,
    "InMemoryConnector": None,
    "MockConnector": None,
}


def get_resilience_config_for_connector(
    connector_class_name: str,
) -> Optional[ResilienceConfig]:
    """Get the default resilience configuration for a connector type.

    Args:
        connector_class_name: Name of the connector class

    Returns:
        ResilienceConfig for the connector type, or None if no resilience needed
    """
    return CONNECTOR_RESILIENCE_PROFILES.get(connector_class_name)


def validate_resilience_config(config: ResilienceConfig) -> None:
    """Validate a resilience configuration.

    Args:
        config: ResilienceConfig to validate

    Raises:
        ValueError: If configuration is invalid
    """
    if config is None:
        return

    if config.retry and config.retry.max_attempts < 1:
        raise ValueError("Retry max_attempts must be at least 1")

    if config.circuit_breaker and config.circuit_breaker.failure_threshold < 1:
        raise ValueError("Circuit breaker failure_threshold must be at least 1")

    if config.rate_limit and config.rate_limit.max_requests_per_minute < 1:
        raise ValueError("Rate limit max_requests_per_minute must be at least 1")


def list_available_profiles() -> Dict[str, str]:
    """List all available resilience profiles.

    Returns:
        Dictionary mapping connector types to profile descriptions
    """
    profiles = {}
    for connector_type, config in CONNECTOR_RESILIENCE_PROFILES.items():
        if config is None:
            profiles[connector_type] = "No resilience (in-memory/mock connector)"
        elif connector_type.startswith(("Rest", "Shopify")):
            profiles[connector_type] = "Critical resilience (API connector)"
        elif connector_type.startswith(("S3", "Postgres", "GoogleSheets")):
            profiles[connector_type] = "High resilience (network-dependent)"
        elif connector_type.startswith(("Csv", "Parquet")):
            profiles[connector_type] = "Low resilience (file-based)"
        else:
            profiles[connector_type] = "Custom resilience profile"

    return profiles
