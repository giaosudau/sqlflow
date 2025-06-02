# SQLFlow Resilience Patterns Technical Specification

**Document Version:** 1.0  
**Date:** January 2025  
**Task:** 2.4 - Resilience Patterns Implementation  
**Status:** Documentation Phase  

## Overview

This specification defines comprehensive resilience patterns for SQLFlow connectors to ensure production reliability. The implementation provides reusable components for retry logic, circuit breakers, rate limiting, and automatic recovery procedures.

## Design Principles

1. **Fail-Fast with Recovery**: Detect failures quickly but provide automatic recovery mechanisms
2. **Configurable Policies**: All resilience behaviors should be configurable per connector
3. **Observability**: All resilience actions should be logged and monitored
4. **Backward Compatibility**: Existing connectors should work without changes
5. **Industry Standards**: Follow established patterns from libraries like Netflix Hystrix and resilience4j

## Core Components

### 1. Retry Strategy with Exponential Backoff

**Purpose**: Handle transient failures with intelligent retry logic

**Configuration**:
```python
@dataclass
class RetryConfig:
    max_attempts: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0     # seconds
    backoff_multiplier: float = 2.0
    jitter: bool = True         # Add randomization to prevent thundering herd
    retry_on_exceptions: List[Type[Exception]] = field(default_factory=lambda: [
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError  # Only for 5xx status codes
    ])
```

**Usage Example**:
```python
@retry(RetryConfig(max_attempts=5, initial_delay=2.0))
def connect_to_api(self):
    # Connection logic here
    pass
```

### 2. Circuit Breaker Pattern

**Purpose**: Prevent cascading failures by detecting unhealthy services and failing fast

**States**:
- **CLOSED**: Normal operation, requests pass through
- **OPEN**: Service is unhealthy, fail fast without calling service
- **HALF_OPEN**: Test if service has recovered

**Configuration**:
```python
@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5          # Number of failures before opening
    recovery_timeout: float = 60.0      # Seconds before trying HALF_OPEN
    success_threshold: int = 2          # Successes needed to close circuit
    timeout: float = 30.0               # Request timeout in seconds
    excluded_exceptions: List[Type[Exception]] = field(default_factory=lambda: [
        ParameterError,  # Don't count configuration errors as service failures
        ValueError
    ])
```

**Usage Example**:
```python
@circuit_breaker(CircuitBreakerConfig(failure_threshold=3))
def call_external_service(self):
    # External service call here
    pass
```

### 3. Rate Limiting with Token Bucket

**Purpose**: Respect API rate limits and prevent service overload

**Algorithm**: Token bucket - allows bursts while maintaining average rate

**Configuration**:
```python
@dataclass
class RateLimitConfig:
    max_requests_per_minute: int = 60   # Average rate
    burst_size: int = 10                # Maximum burst
    per_host: bool = True               # Rate limit per host vs global
    backpressure_strategy: str = "wait" # "wait", "drop", "queue"
    max_queue_size: int = 100           # For queue strategy
```

**Usage Example**:
```python
@rate_limit(RateLimitConfig(max_requests_per_minute=100, burst_size=20))
def make_api_request(self):
    # API request here
    pass
```

### 4. Automatic Recovery Procedures

**Purpose**: Self-healing capabilities for common failure scenarios

**Recovery Strategies**:
- **Connection Recovery**: Automatic reconnection for database/API connections
- **Credential Refresh**: Automatic token refresh for OAuth/JWT
- **Schema Adaptation**: Handle minor schema changes gracefully
- **Partial Failure Recovery**: Continue processing when possible

**Configuration**:
```python
@dataclass
class RecoveryConfig:
    enable_connection_recovery: bool = True
    enable_credential_refresh: bool = True
    enable_schema_adaptation: bool = True
    enable_partial_failure_recovery: bool = True
    recovery_check_interval: float = 30.0  # seconds
    max_recovery_attempts: int = 3
```

## Implementation Architecture

### Core Classes

```python
# Core resilience components
class ResilienceManager:
    """Manages all resilience patterns for a connector."""
    
class RetryHandler:
    """Implements retry logic with exponential backoff."""
    
class CircuitBreaker:
    """Implements circuit breaker pattern."""
    
class RateLimiter:
    """Implements rate limiting with token bucket."""
    
class RecoveryHandler:
    """Implements automatic recovery procedures."""
```

### Integration with Base Connector

```python
class Connector(ABC):
    """Enhanced base connector with resilience patterns."""
    
    def __init__(self):
        self.resilience_manager = None
        
    def configure_resilience(self, config: ResilienceConfig) -> None:
        """Configure resilience patterns for this connector."""
        self.resilience_manager = ResilienceManager(config)
        
    @resilient_operation  # Decorator that applies all configured resilience patterns
    def test_connection(self) -> ConnectionTestResult:
        """Test connection with resilience patterns applied."""
        # Implementation here
```

### Connector-Specific Configurations

Different connector types have different resilience needs:

**Database Connectors** (PostgreSQL, MySQL, etc.):
```python
DB_RESILIENCE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        retry_on_exceptions=[psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.DatabaseError]
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=30.0  # 30 seconds for faster recovery in production
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=300,  # High throughput for DB connections
        burst_size=50
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=False  # DB credentials rarely change
    )
)
```

**API Connectors** (REST, Shopify, Stripe, etc.):
```python
API_RESILIENCE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=5,
        initial_delay=2.0,
        retry_on_exceptions=[requests.exceptions.RequestException]
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3,  # APIs can be more flaky
        recovery_timeout=60.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=60,   # Conservative for API limits
        burst_size=10
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,  # OAuth tokens expire
        enable_schema_adaptation=True    # APIs change schemas frequently
    )
)
```

**File Connectors** (S3, local files, etc.):
```python
FILE_RESILIENCE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=4,
        initial_delay=0.5,
        retry_on_exceptions=[IOError, OSError, ClientError]
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=10,  # File systems are usually reliable
        recovery_timeout=15.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=1000,  # High throughput for file ops
        burst_size=100
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,  # Cloud credentials can expire
        enable_partial_failure_recovery=True  # Continue with available files
    )
)
```

## Error Classification

### Transient vs Permanent Errors

**Transient Errors** (retry appropriate):
- Network timeouts
- Temporary service unavailability (5xx HTTP errors)
- Connection reset
- Rate limit exceeded (with backoff)
- Temporary file locks

**Permanent Errors** (fail fast):
- Invalid credentials (401 Unauthorized)
- Invalid parameters (400 Bad Request)
- Resource not found (404 Not Found)
- Permission denied (403 Forbidden)
- Invalid configuration

### HTTP Status Code Handling

```python
TRANSIENT_HTTP_ERRORS = {
    408,  # Request Timeout
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
}

PERMANENT_HTTP_ERRORS = {
    400,  # Bad Request
    401,  # Unauthorized
    403,  # Forbidden
    404,  # Not Found
    405,  # Method Not Allowed
    422,  # Unprocessable Entity
}
```

## Monitoring and Observability

### Metrics to Track

1. **Retry Metrics**:
   - Number of retry attempts per operation
   - Success rate after retries
   - Time spent in retry delays

2. **Circuit Breaker Metrics**:
   - Circuit state (open/closed/half-open)
   - Number of rejected requests
   - Recovery time when circuit closes

3. **Rate Limiting Metrics**:
   - Requests per minute
   - Queue depth
   - Dropped/delayed requests

4. **Recovery Metrics**:
   - Number of automatic recoveries
   - Recovery success rate
   - Time to recovery

### Logging Standards

```python
# Retry logging
logger.warning(
    "Retry attempt %d/%d for operation %s after error: %s. "
    "Next retry in %.2f seconds",
    attempt, max_attempts, operation_name, error, delay
)

# Circuit breaker logging
logger.error(
    "Circuit breaker OPEN for %s after %d failures. "
    "Failing fast for %.2f seconds",
    service_name, failure_count, recovery_timeout
)

# Rate limiting logging
logger.debug(
    "Rate limit applied for %s. Current rate: %d req/min. "
    "Request %s",
    service_name, current_rate, action  # action: "delayed", "queued", "dropped"
)
```

## Testing Strategy

### Unit Tests

1. **Retry Logic Tests**:
   - Test exponential backoff timing
   - Test max attempts enforcement
   - Test exception filtering
   - Test jitter randomization

2. **Circuit Breaker Tests**:
   - Test state transitions
   - Test failure threshold behavior
   - Test recovery timeout
   - Test half-open state behavior

3. **Rate Limiting Tests**:
   - Test token bucket algorithm
   - Test burst behavior
   - Test different backpressure strategies

### Integration Tests

1. **Simulated Failure Tests**:
   - Network failures
   - Service timeouts
   - Rate limit scenarios
   - Authentication failures

2. **Recovery Tests**:
   - Automatic reconnection
   - Credential refresh
   - Schema change adaptation

### Performance Tests

1. **Throughput Impact**:
   - Measure overhead of resilience patterns
   - Test under normal conditions
   - Test under failure conditions

2. **Memory Usage**:
   - Circuit breaker state storage
   - Rate limiter token tracking
   - Retry attempt tracking

## Implementation Plan

### Phase 1: Core Infrastructure (Days 1-2)
1. Implement basic retry mechanism with exponential backoff
2. Create circuit breaker pattern implementation
3. Implement token bucket rate limiter
4. Create resilience manager coordinator

### Phase 2: Integration (Days 3-4)
1. Integrate with base connector class
2. Create connector-specific configurations
3. Add decorator-based API for easy usage
4. Implement error classification system

### Phase 3: Enhanced Features (Day 5)
1. Add automatic recovery procedures
2. Implement monitoring and metrics collection
3. Add configuration validation
4. Create comprehensive logging

### Phase 4: Testing (Days 6-7)
1. Comprehensive unit test suite
2. Integration tests with simulated failures
3. Performance benchmarking
4. Real-world scenario testing

## Configuration Examples

### Production Configuration
```python
PRODUCTION_RESILIENCE_CONFIG = {
    "postgres": ResilienceConfig(
        retry=RetryConfig(max_attempts=3, initial_delay=1.0),
        circuit_breaker=CircuitBreakerConfig(failure_threshold=5),
        rate_limit=RateLimitConfig(max_requests_per_minute=300),
        recovery=RecoveryConfig(enable_connection_recovery=True)
    ),
    "shopify": ResilienceConfig(
        retry=RetryConfig(max_attempts=5, initial_delay=2.0),
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3),
        rate_limit=RateLimitConfig(max_requests_per_minute=40),  # Shopify limit
        recovery=RecoveryConfig(
            enable_credential_refresh=True,
            enable_schema_adaptation=True
        )
    )
}
```

### Development Configuration
```python
DEVELOPMENT_RESILIENCE_CONFIG = {
    # More aggressive retries for flaky dev environments
    "default": ResilienceConfig(
        retry=RetryConfig(max_attempts=5, initial_delay=0.5),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=10,  # More tolerant in dev
            recovery_timeout=30.0
        ),
        rate_limit=RateLimitConfig(
            max_requests_per_minute=30,  # Lower to avoid hitting dev limits
            burst_size=5
        )
    )
}
```

## Success Criteria

### Functional Requirements
- [ ] 99.5% success rate even with 20% underlying service failure rate
- [ ] Zero rate limit violations during normal operation
- [ ] Automatic recovery from 95% of transient failures
- [ ] All resilience actions properly logged and observable

### Performance Requirements
- [ ] <5% performance overhead under normal conditions
- [ ] <100ms additional latency for resilience processing
- [ ] Memory usage increase <10% for resilience state

### Integration Requirements
- [ ] Zero breaking changes to existing connector interfaces
- [ ] Configurable per-connector and per-operation
- [ ] Easy to add to new connectors with minimal code changes

## Future Enhancements

1. **Adaptive Rate Limiting**: Automatically adjust limits based on service responses
2. **Machine Learning Recovery**: Learn from failure patterns to improve recovery
3. **Cross-Connector Coordination**: Share circuit breaker state across connector instances
4. **Advanced Monitoring**: Integration with external monitoring systems (Prometheus, etc.)

---

This specification provides the foundation for implementing production-ready resilience patterns in SQLFlow connectors, ensuring reliable operation in challenging production environments while maintaining the simplicity and ease-of-use that SQLFlow is known for. 