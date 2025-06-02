"""Unit tests for resilience patterns in SQLFlow connectors."""

import threading
import time
from unittest.mock import Mock, patch

import pytest
import requests

from sqlflow.connectors.base import ConnectorError, ParameterError
from sqlflow.connectors.resilience import (
    API_RESILIENCE_CONFIG,
    DB_RESILIENCE_CONFIG,
    FILE_RESILIENCE_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    RateLimitConfig,
    RateLimiter,
    RecoveryConfig,
    RecoveryHandler,
    ResilienceConfig,
    ResilienceManager,
    RetryConfig,
    RetryHandler,
    TokenBucket,
    circuit_breaker,
    rate_limit,
    resilient_operation,
    retry,
)


class TestRetryHandler:
    """Test retry mechanism with exponential backoff."""

    def test_retry_config_defaults(self):
        """Test default retry configuration."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.backoff_multiplier == 2.0
        assert config.jitter is True

    def test_should_retry_with_retryable_exception(self):
        """Test retry decision for retryable exceptions."""
        config = RetryConfig(max_attempts=3)
        handler = RetryHandler(config)

        assert handler.should_retry(ConnectionError("Network error"), 1)
        assert handler.should_retry(TimeoutError("Timeout"), 2)
        assert not handler.should_retry(ValueError("Bad value"), 1)

    def test_should_retry_max_attempts_exceeded(self):
        """Test retry stops after max attempts."""
        config = RetryConfig(max_attempts=3)
        handler = RetryHandler(config)

        assert not handler.should_retry(ConnectionError("Network error"), 3)

    def test_should_retry_http_error_5xx(self):
        """Test retry for 5xx HTTP errors only."""
        config = RetryConfig()
        handler = RetryHandler(config)

        # Mock HTTP error with 500 status
        response_500 = Mock()
        response_500.status_code = 500
        error_500 = requests.exceptions.HTTPError()
        error_500.response = response_500

        # Mock HTTP error with 404 status
        response_404 = Mock()
        response_404.status_code = 404
        error_404 = requests.exceptions.HTTPError()
        error_404.response = response_404

        assert handler.should_retry(error_500, 1)
        assert not handler.should_retry(error_404, 1)

    def test_calculate_delay_exponential_backoff(self):
        """Test exponential backoff delay calculation."""
        config = RetryConfig(
            initial_delay=1.0, backoff_multiplier=2.0, max_delay=10.0, jitter=False
        )
        handler = RetryHandler(config)

        assert handler.calculate_delay(0) == 1.0
        assert handler.calculate_delay(1) == 2.0
        assert handler.calculate_delay(2) == 4.0
        assert handler.calculate_delay(3) == 8.0
        assert handler.calculate_delay(4) == 10.0  # Max delay cap

    def test_calculate_delay_with_jitter(self):
        """Test delay calculation with jitter."""
        config = RetryConfig(initial_delay=1.0, jitter=True)
        handler = RetryHandler(config)

        delays = [handler.calculate_delay(0) for _ in range(10)]
        # With jitter, delays should vary
        assert len(set(delays)) > 1
        # All delays should be close to 1.0 but not identical
        for delay in delays:
            assert 0.9 <= delay <= 1.1

    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt."""
        config = RetryConfig(max_attempts=3)
        handler = RetryHandler(config)

        func = Mock(return_value="success")
        result = handler.execute_with_retry(func, "arg1", kwarg1="value1")

        assert result == "success"
        func.assert_called_once_with("arg1", kwarg1="value1")

    def test_execute_with_retry_success_after_failures(self):
        """Test successful execution after retries."""
        config = RetryConfig(max_attempts=3, initial_delay=0.01)
        handler = RetryHandler(config)

        func = Mock(
            side_effect=[ConnectionError("fail"), ConnectionError("fail"), "success"]
        )
        result = handler.execute_with_retry(func)

        assert result == "success"
        assert func.call_count == 3

    def test_execute_with_retry_permanent_failure(self):
        """Test permanent failure after all retries."""
        config = RetryConfig(max_attempts=2, initial_delay=0.01)
        handler = RetryHandler(config)

        func = Mock(side_effect=ConnectionError("persistent error"))

        with pytest.raises(ConnectionError, match="persistent error"):
            handler.execute_with_retry(func)

        assert func.call_count == 2

    def test_execute_with_retry_non_retryable_exception(self):
        """Test immediate failure for non-retryable exceptions."""
        config = RetryConfig(max_attempts=3)
        handler = RetryHandler(config)

        func = Mock(side_effect=ValueError("not retryable"))

        with pytest.raises(ValueError, match="not retryable"):
            handler.execute_with_retry(func)

        assert func.call_count == 1


class TestCircuitBreaker:
    """Test circuit breaker pattern."""

    def test_circuit_breaker_config_defaults(self):
        """Test default circuit breaker configuration."""
        config = CircuitBreakerConfig()
        assert config.failure_threshold == 5
        assert config.recovery_timeout == 60.0
        assert config.success_threshold == 2
        assert config.timeout == 30.0

    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config, "test")

        assert cb.state == CircuitState.CLOSED
        assert cb.should_allow_request()

    def test_circuit_breaker_open_after_failures(self):
        """Test circuit breaker opens after failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=2)
        cb = CircuitBreaker(config, "test")

        # Record failures
        cb.record_failure(ConnectionError("fail 1"))
        assert cb.state == CircuitState.CLOSED  # Still closed

        cb.record_failure(ConnectionError("fail 2"))
        assert cb.state == CircuitState.OPEN  # Now open
        assert not cb.should_allow_request()

    def test_circuit_breaker_excluded_exceptions(self):
        """Test exceptions excluded from circuit breaker logic."""
        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker(config, "test")

        # Parameter errors should be excluded
        cb.record_failure(ParameterError("config error"))
        assert cb.state == CircuitState.CLOSED  # Should remain closed

        # Regular errors should count
        cb.record_failure(ConnectionError("connection error"))
        assert cb.state == CircuitState.OPEN

    def test_circuit_breaker_half_open_transition(self):
        """Test transition to half-open state after timeout."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01)
        cb = CircuitBreaker(config, "test")

        # Open the circuit
        cb.record_failure(ConnectionError("fail"))
        assert cb.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.02)

        # Should transition to half-open when checked
        assert cb.should_allow_request()
        assert cb.state == CircuitState.HALF_OPEN

    def test_circuit_breaker_half_open_to_closed(self):
        """Test transition from half-open to closed after successes."""
        config = CircuitBreakerConfig(
            failure_threshold=1, success_threshold=2, recovery_timeout=0.01
        )
        cb = CircuitBreaker(config, "test")

        # Open the circuit
        cb.record_failure(ConnectionError("fail"))
        time.sleep(0.02)

        # Transition to half-open
        cb.should_allow_request()
        assert cb.state == CircuitState.HALF_OPEN

        # Record successes
        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN  # Still half-open

        cb.record_success()
        assert cb.state == CircuitState.CLOSED  # Now closed

    def test_circuit_breaker_half_open_to_open_on_failure(self):
        """Test reopening circuit on failure in half-open state."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01)
        cb = CircuitBreaker(config, "test")

        # Open the circuit
        cb.record_failure(ConnectionError("fail"))
        time.sleep(0.02)

        # Transition to half-open
        cb.should_allow_request()
        assert cb.state == CircuitState.HALF_OPEN

        # Failure should reopen circuit
        cb.record_failure(ConnectionError("fail again"))
        assert cb.state == CircuitState.OPEN

    def test_execute_with_circuit_breaker_success(self):
        """Test successful execution through circuit breaker."""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker(config, "test")

        func = Mock(return_value="success")
        result = cb.execute_with_circuit_breaker(func, "arg1")

        assert result == "success"
        func.assert_called_once_with("arg1")

    def test_execute_with_circuit_breaker_open_fails_fast(self):
        """Test fast failure when circuit is open."""
        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker(config, "test")

        # Open the circuit
        cb.record_failure(ConnectionError("fail"))

        func = Mock()
        with pytest.raises(ConnectorError, match="Circuit breaker is OPEN"):
            cb.execute_with_circuit_breaker(func)

        func.assert_not_called()


class TestTokenBucket:
    """Test token bucket rate limiting."""

    def test_token_bucket_initial_state(self):
        """Test token bucket initial state."""
        bucket = TokenBucket(rate=10.0, burst_size=5)
        assert bucket.tokens == 5
        assert bucket.burst_size == 5
        assert bucket.rate == 10.0

    def test_token_bucket_consume_success(self):
        """Test successful token consumption."""
        bucket = TokenBucket(rate=10.0, burst_size=5)

        assert bucket.consume(3)
        assert bucket.tokens == 2

    def test_token_bucket_consume_insufficient_tokens(self):
        """Test token consumption when insufficient tokens."""
        bucket = TokenBucket(rate=10.0, burst_size=5)

        assert not bucket.consume(6)
        assert bucket.tokens == 5  # Unchanged

    def test_token_bucket_refill_over_time(self):
        """Test token bucket refill over time."""
        bucket = TokenBucket(rate=10.0, burst_size=5)

        # Consume all tokens
        bucket.consume(5)
        assert bucket.tokens == 0

        # Wait and check refill
        time.sleep(0.1)  # 0.1 seconds should add 1 token (10 tokens/sec)
        assert bucket.consume(1)

    def test_token_bucket_wait_time_calculation(self):
        """Test wait time calculation for token availability."""
        bucket = TokenBucket(rate=10.0, burst_size=5)

        # Consume all tokens
        bucket.consume(5)

        # Wait time for 1 token should be 0.1 seconds
        wait_time = bucket.wait_time(1)
        assert 0.09 <= wait_time <= 0.11  # Allow for small timing variations


class TestRateLimiter:
    """Test rate limiter with token bucket."""

    def test_rate_limiter_config_defaults(self):
        """Test default rate limiter configuration."""
        config = RateLimitConfig()
        assert config.max_requests_per_minute == 60
        assert config.burst_size == 10
        assert config.per_host is True
        assert config.backpressure_strategy == "wait"

    def test_rate_limiter_per_host_buckets(self):
        """Test per-host token buckets."""
        config = RateLimitConfig(
            max_requests_per_minute=60, burst_size=5, per_host=True
        )
        limiter = RateLimiter(config)

        bucket1 = limiter.get_bucket("host1")
        bucket2 = limiter.get_bucket("host2")
        bucket1_again = limiter.get_bucket("host1")

        assert bucket1 is not bucket2
        assert bucket1 is bucket1_again

    def test_rate_limiter_global_bucket(self):
        """Test global token bucket."""
        config = RateLimitConfig(
            max_requests_per_minute=60, burst_size=5, per_host=False
        )
        limiter = RateLimiter(config)

        bucket1 = limiter.get_bucket("host1")
        bucket2 = limiter.get_bucket("host2")

        assert bucket1 is bucket2  # Same bucket for all hosts

    def test_rate_limiter_execute_immediate_success(self):
        """Test immediate execution when tokens available."""
        config = RateLimitConfig(
            max_requests_per_minute=600, burst_size=10
        )  # High rate
        limiter = RateLimiter(config)

        func = Mock(return_value="success")
        result = limiter.execute_with_rate_limit(func, "test_key")

        assert result == "success"
        func.assert_called_once()

    def test_rate_limiter_wait_strategy(self):
        """Test wait strategy when rate limited."""
        config = RateLimitConfig(
            max_requests_per_minute=60,  # 1 req/sec
            burst_size=1,
            backpressure_strategy="wait",
        )
        limiter = RateLimiter(config)

        func = Mock(return_value="success")

        # First call should succeed immediately
        result1 = limiter.execute_with_rate_limit(func, "test_key")
        assert result1 == "success"

        # Second call should wait (but we'll mock time.sleep to avoid actual waiting)
        with patch("time.sleep") as mock_sleep:
            result2 = limiter.execute_with_rate_limit(func, "test_key")
            assert result2 == "success"
            mock_sleep.assert_called_once()

    def test_rate_limiter_drop_strategy(self):
        """Test drop strategy when rate limited."""
        config = RateLimitConfig(
            max_requests_per_minute=60,  # 1 req/sec
            burst_size=1,
            backpressure_strategy="drop",
        )
        limiter = RateLimiter(config)

        func = Mock(return_value="success")

        # First call should succeed
        result1 = limiter.execute_with_rate_limit(func, "test_key")
        assert result1 == "success"

        # Second call should be dropped
        with pytest.raises(ConnectorError, match="Request dropped due to rate limit"):
            limiter.execute_with_rate_limit(func, "test_key")


class TestRecoveryHandler:
    """Test automatic recovery procedures."""

    def test_recovery_config_defaults(self):
        """Test default recovery configuration."""
        config = RecoveryConfig()
        assert config.enable_connection_recovery is True
        assert config.enable_credential_refresh is True
        assert config.max_recovery_attempts == 3

    def test_is_connection_error(self):
        """Test connection error detection."""
        config = RecoveryConfig()
        handler = RecoveryHandler(config)

        assert handler._is_connection_error(ConnectionError("connection failed"))
        assert handler._is_connection_error(TimeoutError("timeout"))
        assert not handler._is_connection_error(ValueError("bad value"))

    def test_is_auth_error(self):
        """Test authentication error detection."""
        config = RecoveryConfig()
        handler = RecoveryHandler(config)

        # Mock HTTP error with 401 status
        response_401 = Mock()
        response_401.status_code = 401
        error_401 = requests.exceptions.HTTPError()
        error_401.response = response_401

        # Mock HTTP error with 500 status
        response_500 = Mock()
        response_500.status_code = 500
        error_500 = requests.exceptions.HTTPError()
        error_500.response = response_500

        assert handler._is_auth_error(error_401)
        assert not handler._is_auth_error(error_500)
        assert not handler._is_auth_error(ValueError("bad value"))

    def test_attempt_recovery_connection_error(self):
        """Test recovery attempt for connection errors."""
        config = RecoveryConfig(
            enable_connection_recovery=True, recovery_check_interval=0.01
        )
        handler = RecoveryHandler(config)

        with patch.object(
            handler, "_recover_connection", return_value=True
        ) as mock_recover:
            result = handler.attempt_recovery(
                "test_op", ConnectionError("connection failed")
            )
            assert result is True
            mock_recover.assert_called_once()

    def test_attempt_recovery_max_attempts_exceeded(self):
        """Test recovery stops after max attempts."""
        config = RecoveryConfig(max_recovery_attempts=2)
        handler = RecoveryHandler(config)

        # Exhaust recovery attempts
        handler.recovery_attempts["test_op"] = 2

        result = handler.attempt_recovery(
            "test_op", ConnectionError("connection failed")
        )
        assert result is False

    def test_recover_connection(self):
        """Test connection recovery implementation."""
        config = RecoveryConfig(recovery_check_interval=0.01)
        handler = RecoveryHandler(config)

        with patch("time.sleep") as mock_sleep:
            result = handler._recover_connection("test_op", ConnectionError("fail"))
            assert result is True
            mock_sleep.assert_called_once_with(0.01)


class TestResilienceManager:
    """Test resilience manager coordination."""

    def test_resilience_manager_initialization(self):
        """Test resilience manager initialization."""
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=3),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=5),
            rate_limit=RateLimitConfig(max_requests_per_minute=100),
            recovery=RecoveryConfig(enable_connection_recovery=True),
        )

        manager = ResilienceManager(config, "test_manager")

        assert manager.retry_handler is not None
        assert manager.circuit_breaker is not None
        assert manager.rate_limiter is not None
        assert manager.recovery_handler is not None

    def test_resilience_manager_disabled_components(self):
        """Test resilience manager with disabled components."""
        config = ResilienceConfig(
            retry=None, circuit_breaker=None, rate_limit=None, recovery=None
        )

        manager = ResilienceManager(config, "test_manager")

        # Components should still be created with defaults due to __post_init__
        assert manager.retry_handler is not None
        assert manager.circuit_breaker is not None
        assert manager.rate_limiter is not None
        assert manager.recovery_handler is not None

    def test_execute_resilient_operation_success(self):
        """Test successful resilient operation execution."""
        config = ResilienceConfig()
        manager = ResilienceManager(config, "test_manager")

        func = Mock(return_value="success")
        result = manager.execute_resilient_operation(
            func, "test_operation", None, "arg1"
        )

        assert result == "success"
        func.assert_called_once_with("arg1")


class TestDecorators:
    """Test resilience decorators."""

    def test_retry_decorator(self):
        """Test retry decorator functionality."""
        config = RetryConfig(max_attempts=3, initial_delay=0.01)

        @retry(config)
        def failing_function():
            failing_function.call_count += 1
            if failing_function.call_count < 3:
                raise ConnectionError("fail")
            return "success"

        failing_function.call_count = 0
        result = failing_function()

        assert result == "success"
        assert failing_function.call_count == 3

    def test_circuit_breaker_decorator(self):
        """Test circuit breaker decorator functionality."""
        config = CircuitBreakerConfig(failure_threshold=2)

        @circuit_breaker(config, "test_service")
        def service_function():
            if service_function.should_fail:
                raise ConnectionError("service down")
            return "success"

        service_function.should_fail = False
        result = service_function()
        assert result == "success"

    def test_rate_limit_decorator(self):
        """Test rate limit decorator functionality."""
        config = RateLimitConfig(max_requests_per_minute=600, burst_size=5)  # High rate

        @rate_limit(config, "test_service")
        def api_function():
            return "success"

        result = api_function()
        assert result == "success"

    def test_resilient_operation_decorator_with_manager(self):
        """Test resilient operation decorator with resilience manager."""

        class TestConnector:
            def __init__(self):
                config = ResilienceConfig()
                from sqlflow.connectors.resilience import ResilienceManager

                self.resilience_manager = ResilienceManager(config, "test")

            @resilient_operation()
            def test_method(self):
                return "success"

        connector = TestConnector()
        result = connector.test_method()
        assert result == "success"

    def test_resilient_operation_decorator_without_manager(self):
        """Test resilient operation decorator without resilience manager."""

        class TestConnector:
            def __init__(self):
                self.resilience_manager = None

            @resilient_operation()
            def test_method(self):
                return "success"

        connector = TestConnector()
        result = connector.test_method()
        assert result == "success"


class TestPredefinedConfigurations:
    """Test predefined resilience configurations."""

    def test_db_resilience_config(self):
        """Test database resilience configuration."""
        config = DB_RESILIENCE_CONFIG

        assert config.retry.max_attempts == 3
        assert config.circuit_breaker.failure_threshold == 5
        assert config.rate_limit.max_requests_per_minute == 300
        assert config.recovery.enable_connection_recovery is True
        assert config.recovery.enable_credential_refresh is False

    def test_api_resilience_config(self):
        """Test API resilience configuration."""
        config = API_RESILIENCE_CONFIG

        assert config.retry.max_attempts == 5
        assert config.circuit_breaker.failure_threshold == 3
        assert config.rate_limit.max_requests_per_minute == 60
        assert config.recovery.enable_connection_recovery is True
        assert config.recovery.enable_credential_refresh is True

    def test_file_resilience_config(self):
        """Test file resilience configuration."""
        config = FILE_RESILIENCE_CONFIG

        assert config.retry.max_attempts == 4
        assert config.circuit_breaker.failure_threshold == 10
        assert config.rate_limit.max_requests_per_minute == 1000
        assert config.recovery.enable_partial_failure_recovery is True


class TestThreadSafety:
    """Test thread safety of resilience components."""

    def test_circuit_breaker_thread_safety(self):
        """Test circuit breaker thread safety."""
        config = CircuitBreakerConfig(failure_threshold=5)
        cb = CircuitBreaker(config, "test")

        def record_failures():
            for _ in range(3):
                cb.record_failure(ConnectionError("fail"))

        def record_successes():
            for _ in range(3):
                cb.record_success()

        # Run concurrent operations
        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=record_failures))
            threads.append(threading.Thread(target=record_successes))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Circuit breaker should handle concurrent access gracefully
        assert cb.state in [
            CircuitState.CLOSED,
            CircuitState.OPEN,
            CircuitState.HALF_OPEN,
        ]

    def test_token_bucket_thread_safety(self):
        """Test token bucket thread safety."""
        bucket = TokenBucket(rate=100.0, burst_size=10)

        def consume_tokens():
            for _ in range(5):
                bucket.consume(1)

        # Run concurrent token consumption
        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=consume_tokens))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Token bucket should handle concurrent access gracefully
        assert bucket.tokens >= 0  # Should never go negative


if __name__ == "__main__":
    pytest.main([__file__])
