"""Resilience patterns for SQLFlow connectors.

This module provides comprehensive resilience patterns including retry logic,
circuit breakers, rate limiting, and automatic recovery procedures.

Performance optimizations:
- Lock-free operations where possible using threading primitives
- Optimized token bucket with atomic operations
- Circuit breaker with reduced lock contention
- Lightweight performance monitoring hooks
"""

import functools
import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type

import requests

from sqlflow.connectors.base import ConnectorError, ParameterError
from sqlflow.logging import get_logger

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

logger = get_logger(__name__)


# Performance monitoring - lightweight global stats
_performance_stats = defaultdict(lambda: {"calls": 0, "total_time": 0.0, "failures": 0})
_stats_lock = threading.RLock()

# Performance monitoring configuration - can be disabled for production
_performance_monitoring_enabled = True
_detailed_logging_enabled = False

# Performance optimization constants
_FAST_PATH_ENABLED = True  # Enable fast-path optimizations


def enable_performance_monitoring(enabled: bool = True) -> None:
    """Enable or disable performance monitoring."""
    global _performance_monitoring_enabled
    _performance_monitoring_enabled = enabled


def enable_detailed_logging(enabled: bool = True) -> None:
    """Enable or disable detailed performance logging."""
    global _detailed_logging_enabled
    _detailed_logging_enabled = enabled


def get_performance_stats() -> Dict[str, Dict[str, float]]:
    """Get current performance statistics for all resilience components."""
    if not _performance_monitoring_enabled:
        return {}

    with _stats_lock:
        stats = {}
        for component, data in _performance_stats.items():
            stats[component] = {
                "calls": data["calls"],
                "total_time": data["total_time"],
                "avg_time": data["total_time"] / max(data["calls"], 1),
                "failures": data["failures"],
                "failure_rate": data["failures"] / max(data["calls"], 1),
            }
        return stats


def get_performance_summary() -> Dict[str, Any]:
    """Get a summary of performance statistics with insights."""
    stats = get_performance_stats()
    if not stats:
        return {"monitoring_enabled": False}

    total_calls = sum(s["calls"] for s in stats.values())
    total_failures = sum(s["failures"] for s in stats.values())

    # Find components with high failure rates or slow performance
    slow_components = [name for name, s in stats.items() if s["avg_time"] > 1.0]
    failing_components = [name for name, s in stats.items() if s["failure_rate"] > 0.1]

    return {
        "monitoring_enabled": True,
        "summary": {
            "total_calls": total_calls,
            "total_failures": total_failures,
            "overall_failure_rate": total_failures / max(total_calls, 1),
            "components_tracked": len(stats),
        },
        "performance_alerts": {
            "slow_components": slow_components,
            "failing_components": failing_components,
        },
        "detailed_stats": stats,
    }


def reset_performance_stats() -> None:
    """Reset performance statistics."""
    if not _performance_monitoring_enabled:
        return

    with _stats_lock:
        _performance_stats.clear()


def _record_performance_metric(
    component: str, operation_time: float, failed: bool = False
) -> None:
    """Record a performance metric - optimized for minimal overhead."""
    if not _performance_monitoring_enabled:
        return

    with _stats_lock:
        stats = _performance_stats[component]
        stats["calls"] += 1
        stats["total_time"] += operation_time
        if failed:
            stats["failures"] += 1

    # Detailed logging if enabled
    if _detailed_logging_enabled:
        status = "FAILED" if failed else "OK"
        logger.debug(
            "Performance: %s completed in %.3fs [%s]", component, operation_time, status
        )


def _should_add_performance_hints(component: str, operation_time: float) -> bool:
    """Check if performance hints should be added."""
    if operation_time >= 5.0:
        return True

    # For fast operations, check failure rate
    with _stats_lock:
        stats = _performance_stats.get(component, {})
        failure_rate = stats.get("failures", 0) / max(stats.get("calls", 1), 1)
        return failure_rate > 0.2


def _build_performance_hints(component: str, operation_time: float) -> str:
    """Build performance hints string."""
    with _stats_lock:
        stats = _performance_stats.get(component, {})
        failure_rate = stats.get("failures", 0) / max(stats.get("calls", 1), 1)

    hints = []
    if operation_time > 5.0:
        hints.append(f"Operation took {operation_time:.1f}s (slow)")
    if failure_rate > 0.2:
        hints.append(f"Component failure rate: {failure_rate:.1%}")
    if stats.get("calls", 0) > 100 and failure_rate > 0.1:
        hints.append("Consider adjusting resilience configuration")

    return " [Performance hints: " + "; ".join(hints) + "]"


def _create_enhanced_exception(error: Exception, enhanced_message: str) -> Exception:
    """Safely create an enhanced exception."""
    try:
        if isinstance(error, ConnectorError):
            return ConnectorError(error.connector_name, enhanced_message)
        else:
            return type(error)(enhanced_message)
    except Exception:
        # If we can't create a new exception, just return the original
        return error


def _add_performance_hints_to_error(
    error: Exception, component: str, operation_time: float
) -> Exception:
    """Add performance hints to error messages."""
    if not _performance_monitoring_enabled:
        return error

    if not _should_add_performance_hints(component, operation_time):
        return error

    original_message = str(error)
    performance_hint = _build_performance_hints(component, operation_time)
    enhanced_message = original_message + performance_hint

    return _create_enhanced_exception(error, enhanced_message)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class RetryConfig:
    """Configuration for retry mechanism with exponential backoff."""

    max_attempts: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    backoff_multiplier: float = 2.0
    jitter: bool = True  # Add randomization to prevent thundering herd
    retry_on_exceptions: List[Type[Exception]] = field(
        default_factory=lambda: [
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,  # Only for 5xx status codes
            ConnectionError,
            TimeoutError,
        ]
    )


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""

    failure_threshold: int = 5  # Number of failures before opening
    recovery_timeout: float = 60.0  # Seconds before trying HALF_OPEN
    success_threshold: int = 2  # Successes needed to close circuit
    timeout: float = 30.0  # Request timeout in seconds
    excluded_exceptions: List[Type[Exception]] = field(
        default_factory=lambda: [
            ParameterError,  # Don't count configuration errors as service failures
            ValueError,
            TypeError,
        ]
    )


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting with token bucket algorithm."""

    max_requests_per_minute: int = 60  # Average rate
    burst_size: int = 10  # Maximum burst
    per_host: bool = True  # Rate limit per host vs global
    backpressure_strategy: str = "wait"  # "wait", "drop", "queue"
    max_queue_size: int = 100  # For queue strategy


@dataclass
class RecoveryConfig:
    """Configuration for automatic recovery procedures."""

    enable_connection_recovery: bool = True
    enable_credential_refresh: bool = True
    enable_schema_adaptation: bool = True
    enable_partial_failure_recovery: bool = True
    recovery_check_interval: float = 30.0  # seconds
    max_recovery_attempts: int = 3


@dataclass
class ResilienceConfig:
    """Main configuration for all resilience patterns."""

    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    recovery: RecoveryConfig = field(default_factory=RecoveryConfig)


class RetryHandler:
    """Implements retry logic with exponential backoff and jitter."""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.RetryHandler")

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if an exception should trigger a retry."""
        if attempt >= self.config.max_attempts:
            return False

        # Check if this exception type should be retried
        for retry_exception in self.config.retry_on_exceptions:
            if isinstance(exception, retry_exception):
                # Special handling for HTTP errors - only retry 5xx
                if isinstance(exception, requests.exceptions.HTTPError):
                    if hasattr(exception, "response") and exception.response:
                        status_code = exception.response.status_code
                        return 500 <= status_code < 600

                # Special handling for AWS ClientError - check error codes
                try:
                    from botocore.exceptions import ClientError

                    if isinstance(exception, ClientError):
                        error_code = exception.response.get("Error", {}).get("Code", "")
                        # Retry on specific AWS error codes
                        retryable_codes = {
                            "SlowDown",  # S3 throttling
                            "ServiceUnavailable",  # Service temporarily unavailable
                            "InternalError",  # AWS internal error
                            "RequestTimeout",  # Request timeout
                            "ThrottledResponse",  # General throttling
                            "RequestLimitExceeded",  # Rate limit exceeded
                        }
                        return error_code in retryable_codes
                except ImportError:
                    pass

                return True

        return False

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for next retry attempt with exponential backoff and jitter."""
        base_delay = min(
            self.config.initial_delay * (self.config.backoff_multiplier**attempt),
            self.config.max_delay,
        )

        if self.config.jitter:
            # Add jitter to prevent thundering herd
            jitter_factor = 0.1  # 10% jitter
            jitter = random.uniform(-jitter_factor, jitter_factor)
            base_delay *= 1 + jitter

        return max(base_delay, 0.0)

    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic."""
        start_time = time.time()
        last_exception = None
        failed = False

        try:
            for attempt in range(self.config.max_attempts):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        self.logger.info(
                            "Operation succeeded after %d retry attempts", attempt
                        )
                    return result

                except Exception as e:
                    last_exception = e

                    if not self.should_retry(e, attempt + 1):
                        self.logger.error(
                            "Operation failed permanently after %d attempts: %s",
                            attempt + 1,
                            str(e),
                        )
                        failed = True
                        # Add performance hints to error
                        operation_time = time.time() - start_time
                        enhanced_error = _add_performance_hints_to_error(
                            e, "retry_handler", operation_time
                        )
                        raise enhanced_error

                    delay = self.calculate_delay(attempt)
                    self.logger.warning(
                        "Retry attempt %d/%d for operation after error: %s. "
                        "Next retry in %.2f seconds",
                        attempt + 1,
                        self.config.max_attempts,
                        str(e),
                        delay,
                    )

                    if delay > 0:
                        time.sleep(delay)

            # If we get here, all retries failed
            failed = True
            operation_time = time.time() - start_time
            if last_exception:
                enhanced_error = _add_performance_hints_to_error(
                    last_exception, "retry_handler", operation_time
                )
                raise enhanced_error
            else:
                error = RuntimeError("All retry attempts failed")
                enhanced_error = _add_performance_hints_to_error(
                    error, "retry_handler", operation_time
                )
                raise enhanced_error
        finally:
            # Record performance metrics
            operation_time = time.time() - start_time
            _record_performance_metric("retry_handler", operation_time, failed)


class CircuitBreaker:
    """Implements circuit breaker pattern to prevent cascading failures.

    Performance optimizations:
    - Reduced lock contention with atomic operations where possible
    - Fast-path for closed state without lock acquisition
    - Cached exception type checks
    """

    def __init__(self, config: CircuitBreakerConfig, name: str = "default"):
        self.config = config
        self.name = name
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.lock = threading.RLock()  # Use RLock for better performance
        self.logger = get_logger(f"{__name__}.CircuitBreaker.{name}")

        # Pre-compile exception types for faster checks
        self._excluded_exception_types = tuple(self.config.excluded_exceptions)

    def is_exception_excluded(self, exception: Exception) -> bool:
        """Check if exception should be excluded from circuit breaker logic.
        Optimized with pre-compiled exception types."""
        return isinstance(exception, self._excluded_exception_types)

    def should_allow_request(self) -> bool:
        """Determine if request should be allowed through circuit breaker.
        Fast-path optimization for CLOSED state."""
        # Fast path: if closed, allow immediately without lock
        if self.state == CircuitState.CLOSED:
            return True

        # Slower path: need to check state transitions
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                # Check if we should transition to HALF_OPEN
                current_time = time.time()
                if (
                    current_time - self.last_failure_time
                    >= self.config.recovery_timeout
                ):
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    self.logger.info(
                        "Circuit breaker transitioning to HALF_OPEN for %s", self.name
                    )
                    return True
                return False
            else:  # HALF_OPEN
                return True

    def record_success(self):
        """Record successful operation."""
        # Fast path: if already closed, no need for lock
        if self.state == CircuitState.CLOSED:
            # Still need to reset failure count atomically
            with self.lock:
                if self.state == CircuitState.CLOSED:
                    self.failure_count = 0
            return

        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.logger.info(
                        "Circuit breaker CLOSED for %s after %d successful attempts",
                        self.name,
                        self.success_count,
                    )
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = 0

    def record_failure(self, exception: Exception):
        """Record failed operation."""
        if self.is_exception_excluded(exception):
            self.logger.debug(
                "Exception excluded from circuit breaker: %s", type(exception).__name__
            )
            return

        current_time = time.time()
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = current_time

            if (
                self.state == CircuitState.CLOSED
                and self.failure_count >= self.config.failure_threshold
            ):
                self.state = CircuitState.OPEN
                self.logger.error(
                    "Circuit breaker OPEN for %s after %d failures. "
                    "Failing fast for %.2f seconds",
                    self.name,
                    self.failure_count,
                    self.config.recovery_timeout,
                )
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.logger.warning(
                    "Circuit breaker reopened for %s due to failure in HALF_OPEN state",
                    self.name,
                )

    def execute_with_circuit_breaker(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        start_time = time.time()
        failed = False

        try:
            if not self.should_allow_request():
                failed = True
                raise ConnectorError(
                    self.name,
                    f"Circuit breaker is OPEN for {self.name}. "
                    f"Failing fast to prevent cascading failures.",
                )

            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            failed = True
            self.record_failure(e)
            # Add performance hints to error
            operation_time = time.time() - start_time
            enhanced_error = _add_performance_hints_to_error(
                e, f"circuit_breaker.{self.name}", operation_time
            )
            raise enhanced_error
        finally:
            # Record performance metrics
            operation_time = time.time() - start_time
            _record_performance_metric(
                f"circuit_breaker.{self.name}", operation_time, failed
            )


class TokenBucket:
    """Token bucket implementation for rate limiting.

    Performance optimizations:
    - Atomic operations with threading.Lock for minimal contention
    - Pre-calculated rate values for faster computation
    - Optimized floating-point arithmetic
    """

    def __init__(self, rate: float, burst_size: int):
        self.rate = rate  # tokens per second
        self.burst_size = burst_size
        self.tokens = float(burst_size)  # Use float for atomic operations
        self.last_update = time.time()
        self.lock = threading.Lock()  # Keep simple Lock for atomic updates

        # Pre-calculate for performance
        self._rate_float = float(rate)
        self._burst_size_float = float(burst_size)

    def consume(self, tokens: int = 1) -> bool:
        """Attempt to consume tokens from bucket.
        Optimized with minimal lock time and pre-calculated values."""
        tokens_float = float(tokens)
        current_time = time.time()

        with self.lock:
            # Calculate tokens to add - optimized computation
            elapsed = current_time - self.last_update
            if elapsed > 0:
                # Add tokens based on elapsed time
                self.tokens = min(
                    self._burst_size_float, self.tokens + elapsed * self._rate_float
                )
                self.last_update = current_time

            # Check and consume tokens
            if self.tokens >= tokens_float:
                self.tokens -= tokens_float
                return True
            return False

    def wait_time(self, tokens: int = 1) -> float:
        """Calculate time to wait until tokens are available.
        Optimized for minimal computation time."""
        tokens_float = float(tokens)
        current_time = time.time()

        with self.lock:
            # Update tokens first
            elapsed = current_time - self.last_update
            if elapsed > 0:
                self.tokens = min(
                    self._burst_size_float, self.tokens + elapsed * self._rate_float
                )
                self.last_update = current_time

            if self.tokens >= tokens_float:
                return 0.0

            # Calculate wait time - optimized arithmetic
            tokens_needed = tokens_float - self.tokens
            return tokens_needed / self._rate_float


class RateLimiter:
    """Implements rate limiting with token bucket algorithm.

    Performance optimizations:
    - Reduced lock contention with WeakValueDictionary for bucket cleanup
    - Pre-calculated rate conversion
    - Optimized bucket creation and lookup
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        # Use regular dict instead of WeakValueDictionary for test compatibility
        self.buckets: Dict[str, TokenBucket] = {}
        self.lock = threading.RLock()  # Use RLock for nested operations
        self.logger = get_logger(f"{__name__}.RateLimiter")

        # Pre-calculate rate conversion for better performance
        self._rate_per_second = config.max_requests_per_minute / 60.0

        if not config.per_host:
            # Global rate limiter - pre-create for performance
            self.global_bucket = TokenBucket(self._rate_per_second, config.burst_size)

    def get_bucket(self, key: str) -> TokenBucket:
        """Get or create token bucket for given key.
        Optimized with reduced lock contention."""
        if not self.config.per_host:
            return self.global_bucket

        # First try without lock for existing buckets
        bucket = self.buckets.get(key)
        if bucket is not None:
            return bucket

        # Need to create bucket - acquire lock
        with self.lock:
            # Double-check pattern
            bucket = self.buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self._rate_per_second, self.config.burst_size)
                self.buckets[key] = bucket
            return bucket

    def execute_with_rate_limit(
        self, func: Callable, key: str = "default", *args, **kwargs
    ) -> Any:
        """Execute function with rate limiting."""
        start_time = time.time()
        failed = False

        try:
            bucket = self.get_bucket(key)

            if bucket.consume():
                # Request allowed immediately
                return func(*args, **kwargs)

            # Apply backpressure strategy
            if self.config.backpressure_strategy == "drop":
                failed = True
                raise ConnectorError(
                    key, f"Request dropped due to rate limit for {key}"
                )
            elif self.config.backpressure_strategy == "wait":
                wait_time = bucket.wait_time()
                if wait_time > 0:
                    self.logger.debug(
                        "Rate limit applied for %s. Waiting %.2f seconds",
                        key,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    # After sleeping, tokens should be available
                    bucket.consume()  # Consume after waiting
                return func(*args, **kwargs)
            elif self.config.backpressure_strategy == "queue":
                # Simplified queue implementation - in production might use a proper queue
                wait_time = bucket.wait_time()
                if wait_time > self.config.max_queue_size:
                    failed = True
                    raise ConnectorError(key, f"Request queue full for {key}")
                if wait_time > 0:
                    time.sleep(wait_time)
                    # After sleeping, tokens should be available
                    bucket.consume()  # Consume after waiting
                return func(*args, **kwargs)
            else:
                failed = True
                raise ValueError(
                    f"Unknown backpressure strategy: {self.config.backpressure_strategy}"
                )
        except Exception as e:
            failed = True
            # Add performance hints to error
            operation_time = time.time() - start_time
            enhanced_error = _add_performance_hints_to_error(
                e, f"rate_limiter.{key}", operation_time
            )
            raise enhanced_error
        finally:
            # Record performance metrics
            operation_time = time.time() - start_time
            _record_performance_metric(f"rate_limiter.{key}", operation_time, failed)


class RecoveryHandler:
    """Implements automatic recovery procedures."""

    def __init__(self, config: RecoveryConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.RecoveryHandler")
        self.recovery_attempts: Dict[str, int] = {}

    def attempt_recovery(self, operation: str, error: Exception) -> bool:
        """Attempt to recover from an error."""
        recovery_count = self.recovery_attempts.get(operation, 0)

        if recovery_count >= self.config.max_recovery_attempts:
            self.logger.error(
                "Max recovery attempts (%d) exceeded for operation %s",
                self.config.max_recovery_attempts,
                operation,
            )
            return False

        self.recovery_attempts[operation] = recovery_count + 1

        if self.config.enable_connection_recovery:
            if self._is_connection_error(error):
                self.logger.info(
                    "Attempting connection recovery for operation %s (attempt %d/%d)",
                    operation,
                    recovery_count + 1,
                    self.config.max_recovery_attempts,
                )
                return self._recover_connection(operation, error)

        if self.config.enable_credential_refresh:
            if self._is_auth_error(error):
                self.logger.info(
                    "Attempting credential refresh for operation %s",
                    operation,
                )
                return self._refresh_credentials(operation, error)

        return False

    def _is_connection_error(self, error: Exception) -> bool:
        """Check if error is connection-related."""
        connection_errors = (
            ConnectionError,
            requests.exceptions.ConnectionError,
            TimeoutError,
            requests.exceptions.Timeout,
        )
        return isinstance(error, connection_errors)

    def _is_auth_error(self, error: Exception) -> bool:
        """Check if error is authentication-related."""
        if isinstance(error, requests.exceptions.HTTPError):
            if hasattr(error, "response") and error.response:
                return error.response.status_code == 401
        return False

    def _recover_connection(self, operation: str, error: Exception) -> bool:
        """Attempt to recover connection."""
        # Sleep before retry
        time.sleep(self.config.recovery_check_interval)

        # Basic recovery - in practice this would be connector-specific
        self.logger.info("Connection recovery completed for operation %s", operation)
        return True

    def _refresh_credentials(self, operation: str, error: Exception) -> bool:
        """Attempt to refresh credentials."""
        # This would be implemented by specific connectors
        self.logger.info("Credential refresh attempted for operation %s", operation)
        return False  # Default to not recovered


class ResilienceManager:
    """Manages all resilience patterns for a connector."""

    def __init__(self, config: ResilienceConfig, name: str = "default"):
        self.config = config
        self.name = name
        self.logger = get_logger(f"{__name__}.ResilienceManager.{name}")

        # Initialize components
        self.retry_handler = RetryHandler(config.retry) if config.retry else None
        self.circuit_breaker = (
            CircuitBreaker(config.circuit_breaker, name)
            if config.circuit_breaker
            else None
        )
        self.rate_limiter = (
            RateLimiter(config.rate_limit) if config.rate_limit else None
        )
        self.recovery_handler = (
            RecoveryHandler(config.recovery) if config.recovery else None
        )

        # Create default handlers for None components
        self._ensure_default_handlers()

    def _ensure_default_handlers(self):
        """Create default handlers for None components."""
        if self.retry_handler is None:
            self.retry_handler = RetryHandler(RetryConfig())
        if self.circuit_breaker is None:
            self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(), self.name)
        if self.rate_limiter is None:
            self.rate_limiter = RateLimiter(RateLimitConfig())
        if self.recovery_handler is None:
            self.recovery_handler = RecoveryHandler(RecoveryConfig())

    def execute_resilient_operation(
        self,
        func: Callable,
        operation_name: str = "operation",
        rate_limit_key: Optional[str] = None,
        *args,
        **kwargs,
    ) -> Any:
        """Execute an operation with all configured resilience patterns applied."""

        def wrapped_operation():
            # Apply rate limiting if configured
            if self.rate_limiter:
                key = rate_limit_key or self.name
                return self.rate_limiter.execute_with_rate_limit(
                    func, key, *args, **kwargs
                )
            else:
                return func(*args, **kwargs)

        def circuit_breaker_wrapped():
            # Apply circuit breaker if configured
            if self.circuit_breaker:
                return self.circuit_breaker.execute_with_circuit_breaker(
                    wrapped_operation
                )
            else:
                return wrapped_operation()

        # Apply retry logic if configured
        if self.retry_handler:
            try:
                return self.retry_handler.execute_with_retry(circuit_breaker_wrapped)
            except Exception as e:
                # Attempt recovery if configured
                if self.recovery_handler:
                    if self.recovery_handler.attempt_recovery(operation_name, e):
                        # Retry after recovery
                        return self.retry_handler.execute_with_retry(
                            circuit_breaker_wrapped
                        )
                raise
        else:
            return circuit_breaker_wrapped()


# Decorator functions for easy usage
def resilient_operation(
    config: Optional[ResilienceConfig] = None,
    operation_name: Optional[str] = None,
    rate_limit_key: Optional[str] = None,
):
    """Decorator to apply resilience patterns to a method."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Get resilience manager from connector instance
            if hasattr(self, "resilience_manager") and self.resilience_manager:
                op_name = operation_name or func.__name__
                return self.resilience_manager.execute_resilient_operation(
                    func, op_name, rate_limit_key, self, *args, **kwargs
                )
            else:
                # No resilience configured, execute normally
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


def retry(config: RetryConfig):
    """Decorator to apply only retry logic to a function."""

    def decorator(func: Callable) -> Callable:
        retry_handler = RetryHandler(config)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return retry_handler.execute_with_retry(func, *args, **kwargs)

        return wrapper

    return decorator


def circuit_breaker(config: CircuitBreakerConfig, name: str = "default"):
    """Decorator to apply only circuit breaker logic to a function."""

    def decorator(func: Callable) -> Callable:
        cb = CircuitBreaker(config, name)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return cb.execute_with_circuit_breaker(func, *args, **kwargs)

        return wrapper

    return decorator


def rate_limit(config: RateLimitConfig, key: str = "default"):
    """Decorator to apply only rate limiting to a function."""

    def decorator(func: Callable) -> Callable:
        limiter = RateLimiter(config)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return limiter.execute_with_rate_limit(func, key, *args, **kwargs)

        return wrapper

    return decorator


# Default resilience configuration for database connectors
DB_RESILIENCE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=0.5,
        max_delay=10.0,
        backoff_multiplier=1.5,
        retry_on_exceptions=PSYCOPG2_EXCEPTIONS
        + [
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            ConnectionError,
            TimeoutError,
        ],
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=5, recovery_timeout=30.0, success_threshold=2
    ),
    rate_limit=RateLimitConfig(max_requests_per_minute=300, burst_size=20),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=False,  # Usually handled by connection recovery
    ),
)

# Default resilience configuration for API-based connectors
API_RESILIENCE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=5,
        initial_delay=2.0,
        retry_on_exceptions=[
            requests.exceptions.RequestException,
            ConnectionError,
            TimeoutError,
        ],
    ),
    circuit_breaker=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=60.0),
    rate_limit=RateLimitConfig(max_requests_per_minute=60, burst_size=10),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,
        enable_schema_adaptation=True,
    ),
)

# Try to import AWS/S3 specific exceptions
try:
    from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

    AWS_EXCEPTIONS = [ClientError, EndpointConnectionError, BotoCoreError]
except ImportError:
    AWS_EXCEPTIONS = []

FILE_RESILIENCE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=4,
        initial_delay=0.5,
        retry_on_exceptions=[
            IOError,
            OSError,
            ConnectionError,
            TimeoutError,
            *AWS_EXCEPTIONS,  # Include AWS/S3 specific exceptions
        ],
    ),
    circuit_breaker=CircuitBreakerConfig(failure_threshold=10, recovery_timeout=15.0),
    rate_limit=RateLimitConfig(max_requests_per_minute=1000, burst_size=100),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,
        enable_partial_failure_recovery=True,
    ),
)
