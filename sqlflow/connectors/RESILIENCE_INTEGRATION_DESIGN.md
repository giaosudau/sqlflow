# SQLFlow Connector Resilience Integration Design

**Document Version**: 1.1  
**Author**: Principal Software Architect (PSA)  
**Date**: 2024-01-16  
**Status**: Design Approved

---
*Version 1.1 Update: Incorporated feedback from PSE, PPM, DE, and DA review. See Section 13 for details.*
---

## 1. Executive Summary

This document outlines the technical design for reintegrating resilience patterns into SQLFlow connectors after the architectural refactor. The design implements a **tiered resilience architecture** that balances simplicity with advanced capabilities, following the 80/20 principle and industry best practices from Airbyte, Fivetran, and Airflow.

### Design Principles
- **Simplicity First**: Smart defaults that work out-of-the-box
- **Progressive Enhancement**: Three-tier configuration model
- **Connector-Specific**: Tailored resilience based on connector characteristics
- **Performance-Aware**: Minimal overhead for stable connections
- **Python-Native**: Leverages Python's built-in capabilities and established patterns

## 2. Current State Analysis

### 2.1 Post-Refactor Architecture
- ✅ **Clean Interfaces**: `Connector`, `DestinationConnector` base classes
- ✅ **Modular Design**: Separated source/destination registries
- ✅ **Consistent Patterns**: Standardized `configure()`, `read()`, `write()` methods
- ❌ **No Resilience**: All resilience patterns removed during refactor
- ✅ **Available Module**: `resilience.py` exists but not integrated

### 2.2 Connector Inventory
| Connector | Type | Network Dependent | Resilience Priority |
|-----------|------|------------------|-------------------|
| CSV | Source/Dest | ❌ | Low |
| Parquet | Source/Dest | ❌ | Low |
| In-Memory | Source/Dest | ❌ | None |
| PostgreSQL | Source/Dest | ✅ | High |
| S3 | Source/Dest | ✅ | High |
| REST API | Source | ✅ | Critical |
| Google Sheets | Source | ✅ | High |
| Shopify | Source | ✅ | Critical |

## 3. Design Architecture

### 3.1 Three-Tier Resilience Model

#### **Tier 1: Smart Defaults (80% of users)**
```yaml
# No configuration needed - automatic resilience
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    # Automatic: 3 retries, exponential backoff, circuit breaker
```

#### **Tier 2: Simple Overrides (15% of users)**
```yaml
# Basic tuning with simple parameters
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    resilience:
      retry_attempts: 5
      timeout: 60
      rate_limit: 100  # requests per minute
```

#### **Tier 3: Full Control (5% of users)**
```yaml
# Complete resilience configuration
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    resilience:
      retry:
        max_attempts: 5
        initial_delay: 2.0
        backoff_multiplier: 2.5
        jitter: true
      circuit_breaker:
        failure_threshold: 3
        recovery_timeout: 120.0
      rate_limit:
        max_requests_per_minute: 120
        burst_size: 20
```

### 3.2 Integration Points

#### 3.2.1 Base Connector Enhancement
```python
# sqlflow/connectors/base/connector.py
class Connector(ABC):
    def __init__(self):
        self.state = ConnectorState.CREATED
        self.resilience_manager: Optional[ResilienceManager] = None
        # ... existing attributes

    def configure(self, params: Dict[str, Any]) -> None:
        # Configure resilience if applicable
        if self._needs_resilience():
            self._configure_resilience(params)
        
    def _configure_resilience(self, params: Dict[str, Any]) -> None:
        """Configure resilience patterns based on connector type"""
        resilience_config = self._get_resilience_config(params)
        self.resilience_manager = ResilienceManager(
            config=resilience_config,
            name=f"{self.__class__.__name__}_{id(self)}"
        )
```

#### 3.2.2 Resilience-Aware Operations
```python
# Decorator-based approach for critical operations
@resilient_operation(operation_name="test_connection")
def test_connection(self) -> ConnectionTestResult:
    # Implementation with automatic resilience
    
@resilient_operation(operation_name="read_data", rate_limit_key="read")
def read(self, object_name: str, **kwargs) -> Iterator[DataChunk]:
    # Implementation with automatic resilience
```

## 4. Connector-Specific Default Configurations

### 4.1 Network-Dependent Connectors

#### **REST API Connector (Critical Resilience)**
```python
DEFAULT_REST_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        max_delay=30.0,
        backoff_multiplier=2.0,
        jitter=True,
        retry_on_exceptions=[
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError  # 5xx only
        ]
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2,
        timeout=30.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=60,
        burst_size=10,
        per_host=True,
        backpressure_strategy="wait"
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=True,
        recovery_check_interval=30.0
    )
)
```

#### **S3 Connector (High Resilience)**
```python
DEFAULT_S3_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=1.0,
        max_delay=60.0,
        backoff_multiplier=2.0,
        retry_on_exceptions=[
            ClientError,  # S3 throttling
            ConnectionError,
            TimeoutError
        ]
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=30.0,
        timeout=60.0
    ),
    rate_limit=RateLimitConfig(
        max_requests_per_minute=100,  # Conservative for S3
        burst_size=5,
        backpressure_strategy="wait"
    )
)
```

#### **PostgreSQL Connector (High Resilience)**
```python
DEFAULT_POSTGRES_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=3,
        initial_delay=0.5,
        max_delay=10.0,
        retry_on_exceptions=[
            OperationalError,  # DB connection issues
            InterfaceError,
            ConnectionError
        ]
    ),
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=30.0,
        timeout=30.0,
        excluded_exceptions=[
            ProgrammingError,  # SQL syntax errors
            DataError  # Data type errors
        ]
    ),
    recovery=RecoveryConfig(
        enable_connection_recovery=True,
        enable_credential_refresh=False,  # DB credentials rarely change
        recovery_check_interval=15.0
    )
)
```

### 4.2 File-Based Connectors

#### **CSV/Parquet Connectors (Low Resilience)**
```python
DEFAULT_FILE_CONFIG = ResilienceConfig(
    retry=RetryConfig(
        max_attempts=2,
        initial_delay=0.1,
        max_delay=1.0,
        retry_on_exceptions=[
            FileNotFoundError,
            PermissionError,
            OSError
        ]
    ),
    # No circuit breaker or rate limiting for local files
    circuit_breaker=None,
    rate_limit=None
)
```

#### **In-Memory Connector (No Resilience)**
```python
# No resilience configuration - operations are atomic
DEFAULT_MEMORY_CONFIG = None
```

## 5. Implementation Tasks & Dependencies

### 5.1 Phase 1: Foundation (Weeks 1-2)

#### **Task 1.1: Enhance Base Connector Classes**
- **Owner**: PSE (Principal Software Engineer)
- **Dependencies**: None
- **Effort**: 3 days

**Requirements:**
- Add resilience manager integration to `Connector` base class
- Add `_needs_resilience()` method to determine if connector requires resilience
- Add `_configure_resilience()` method with connector-specific defaults
- Update `DestinationConnector` with similar patterns

**Definition of Done:**
- [ ] `Connector` class has resilience manager attribute
- [ ] `_configure_resilience()` method implemented
- [ ] `_needs_resilience()` returns correct boolean for connector types
- [ ] No breaking changes to existing connector interfaces
- [ ] Unit tests pass for base classes
- [ ] Documentation updated

**Testing Requirements:**
- Unit tests for resilience configuration logic
- Mock resilience manager integration tests
- Backward compatibility tests with existing connectors

#### **Task 1.2: Create Connector Resilience Profiles**
- **Owner**: PSA (Principal Software Architect)
- **Dependencies**: Task 1.1
- **Effort**: 2 days

**Requirements:**
- Create `resilience_profiles.py` with default configurations
- Implement connector-type mapping to resilience profiles
- Add profile validation and error handling

**Definition of Done:**
- [ ] Default resilience profiles defined for each connector type
- [ ] Profile selection logic implemented
- [ ] Profile validation with clear error messages
- [ ] Documentation for each profile's design rationale
- [ ] Unit tests for profile selection and validation

#### **Task 1.3: Implement Simple Configuration Parser**
- **Owner**: DE (Data Engineer)
- **Dependencies**: Task 1.2
- **Effort**: 2 days

**Requirements:**
- Parse Tier 2 simple resilience configuration
- Convert simple parameters to full `ResilienceConfig` objects
- Handle configuration validation and defaults

**Definition of Done:**
- [ ] Simple parameter parsing (retry_attempts, timeout, rate_limit)
- [ ] Conversion to full ResilienceConfig objects
- [ ] Parameter validation with helpful error messages
- [ ] Tier 2 configuration documented with examples
- [ ] Unit tests for all parameter combinations

### 5.2 Phase 2: Network Connector Integration (Weeks 3-4)

#### **Task 2.1: Integrate REST API Connector**
- **Owner**: DE (Data Engineer)
- **Dependencies**: Task 1.3
- **Effort**: 3 days

**Requirements:**
- Update `RestSource` to use resilience patterns
- Replace existing retry logic with resilience manager
- Add rate limiting integration for API requests
- Update connection testing with resilience

**Code Verification:**
Based on `sqlflow/connectors/rest/source.py` lines 26-80:
- Current implementation has basic retry logic in `_make_request()`
- Has `max_retries=3` and `retry_delay=1.0` parameters
- Uses session-based requests
- Integration point: `_make_request()` method

**Definition of Done:**
- [ ] `RestSource._make_request()` uses resilience manager
- [ ] Rate limiting applied to API requests
- [ ] Circuit breaker protects against API failures
- [ ] Existing retry parameters converted to resilience config
- [ ] Connection test updated with resilience
- [ ] All existing functionality preserved
- [ ] Integration tests with real API endpoints

#### **Task 2.2: Integrate S3 Connector**
- **Owner**: PSE (Principal Software Engineer)
- **Dependencies**: Task 2.1
- **Effort**: 4 days

**Requirements:**
- Update `S3Source` to use resilience patterns for boto3 operations
- Add circuit breaker for S3 API calls
- Implement retry logic for S3 throttling and temporary failures
- Update both `test_connection()` and `discover()` methods

**Technical Note (per PSE review):**
Instead of wrapping every `boto3` call individually, a resilient client wrapper class should be considered to encapsulate resilience logic and reduce code duplication.

**Code Verification:**
Based on `sqlflow/connectors/s3/source.py`:
- Uses `boto3.client("s3")` for operations
- Has `test_connection()` method that calls S3 APIs
- Discovery operations in `_discover_with_prefix()`
- Integration points: All `self.s3_client.*()` calls

**Definition of Done:**
- [ ] All boto3 S3 operations wrapped with resilience
- [ ] S3 throttling handled gracefully
- [ ] Circuit breaker prevents cascading failures
- [ ] Discovery operations respect rate limits
- [ ] Connection testing uses resilience patterns
- [ ] Performance impact measured and acceptable
- [ ] Integration tests with S3/MinIO

#### **Task 2.3: Integrate PostgreSQL Connector**
- **Owner**: PSE (Principal Software Engineer)
- **Dependencies**: Task 2.2
- **Effort**: 2 days

**Requirements:**
- Update `PostgresSource` and `PostgresDestination` with resilience
- Add connection recovery for database disconnections
- Implement retry logic for transient database errors
- Exclude non-retryable errors (syntax, permission errors)

**Technical Note (per PSE review):**
Resilience decorators should be applied to methods that execute database operations (e.g., the `read` and `write` methods), not the `engine` property that creates the SQLAlchemy engine. This ensures resilience is applied to the actual query execution.

**Code Verification:**
Based on `sqlflow/connectors/postgres/source.py` and `destination.py`:
- Uses SQLAlchemy engine for connections
- Has connection management in `engine` property
- Read operations use `pd.read_sql_query()`
- Write operations use `df.to_sql()`

**Definition of Done:**
- [ ] Database connection operations wrapped with resilience
- [ ] Connection recovery implemented
- [ ] Transient vs permanent error classification
- [ ] Both source and destination updated
- [ ] SQLAlchemy integration tested
- [ ] Connection pool behavior preserved
- [ ] Integration tests with PostgreSQL

### 5.3 Phase 3: Specialized Connectors (Week 5)

#### **Task 3.1: Integrate Google Sheets Connector**
- **Owner**: DE (Data Engineer)
- **Dependencies**: Task 2.3
- **Effort**: 2 days

**Requirements:**
- Update `GoogleSheetsSource` with Google API resilience
- Handle Google API quotas and rate limiting
- Implement retry for Google API errors
- Add circuit breaker for quota exceeded scenarios

**Definition of Done:**
- [ ] Google Sheets API calls wrapped with resilience
- [ ] Quota handling with appropriate backoff
- [ ] Circuit breaker for API quota exhaustion
- [ ] Credential refresh integration
- [ ] Google API error classification
- [ ] Integration tests with Google Sheets API

#### **Task 3.2: Integrate Shopify Connector**
- **Owner**: DE (Data Engineer)
- **Dependencies**: Task 3.1
- **Effort**: 2 days

**Requirements:**
- Update `ShopifySource` with Shopify API resilience
- Implement Shopify-specific rate limiting (40 requests/second)
- Add retry logic for Shopify API errors
- Handle Shopify webhook delivery failures

**Definition of Done:**
- [ ] Shopify API calls wrapped with resilience
- [ ] Shopify rate limiting respected
- [ ] API bucket leak algorithm implemented
- [ ] Webhook retry mechanism added
- [ ] Shopify error code classification
- [ ] Integration tests with Shopify test store

### 5.4 Phase 4: File Connectors & Documentation (Week 6)

#### **Task 4.1: Integrate File-Based Connectors**
- **Owner**: JDA (Junior Data Analyst) with PSE mentoring
- **Dependencies**: Task 3.2
- **Effort**: 2 days

**Requirements:**
- Update CSV, Parquet connectors with minimal resilience
- Add retry logic for file system operations
- Handle permission and not-found errors gracefully

**Definition of Done:**
- [ ] File operations wrapped with basic resilience
- [ ] File system error retry logic
- [ ] Graceful error handling for missing files
- [ ] Performance impact minimized
- [ ] File connector tests updated

#### **Task 4.2: User-Facing Documentation**
- **Owner**: PPM (Principal Product Manager)
- **Dependencies**: Task 4.1
- **Effort**: 2 days

**Requirements**:
- Create a new user guide for Connector Resilience.
- Explain the three-tier model with clear examples.
- Document the simple (Tier 2) and advanced (Tier 3) configuration options.
- Add the "Glossary of Terms" (see Appendix).
- Update individual connector documentation to reference the new resilience features.

**Definition of Done**:
- [ ] New "Connector Resilience" guide is published.
- [ ] All configuration options are documented with examples.
- [ ] Connector READMEs are updated.

#### **Task 4.3: Comprehensive Integration Testing**
- **Owner**: PSE (Principal Software Engineer)
- **Dependencies**: Task 4.2
- **Effort**: 3 days

**Requirements:**
- End-to-end pipeline tests with resilience
- Performance benchmarking with/without resilience
- Failure injection testing
- Documentation and examples update

**Definition of Done:**
- [ ] All connectors tested in full pipeline context
- [ ] Performance benchmarks showing <5% overhead
- [ ] Failure injection tests pass
- [ ] Code examples in documentation are verified
- [ ] Migration guide for existing configurations is complete

## 6. Testing Strategy

### 6.1 Unit Testing Requirements

**Resilience Manager Tests:**
- Configuration parsing and validation
- Retry logic with different exception types
- Circuit breaker state transitions
- Rate limiting token bucket algorithm
- Recovery mechanism triggers

**Connector Integration Tests:**
- Resilience configuration inheritance
- Method decoration and wrapping
- Error propagation and handling
- Configuration override behavior

### 6.2 Integration Testing Requirements

**Real Service Tests:**
- Network failure simulation
- API rate limiting scenarios
- Database connection failures
- File system permission errors
- Recovery from various failure modes

**Performance Tests:**
- Latency impact measurement
- Memory usage analysis
- Throughput comparison
- Resource utilization monitoring

### 6.3 Failure Injection Testing

**Chaos Engineering Scenarios:**
- Random network failures during operations
- Intermittent API timeouts
- Database connection drops
- File system I/O errors
- Service overload conditions

## 7. Configuration Examples & Migration

### 7.1 Before (Current State)
```yaml
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    timeout: 30
    max_retries: 3
```

### 7.2 After - Tier 1 (Automatic)
```yaml
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    # Automatic resilience with smart defaults
```

### 7.3 After - Tier 2 (Simple)
```yaml
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    resilience:
      retry_attempts: 5
      timeout: 60
      rate_limit: 100
```

### 7.4 After - Tier 3 (Advanced)
```yaml
sources:
  api_data:
    type: "rest"
    url: "https://api.example.com/data"
    resilience:
      retry:
        max_attempts: 5
        initial_delay: 2.0
        backoff_multiplier: 2.5
        jitter: true
      circuit_breaker:
        failure_threshold: 3
        recovery_timeout: 120.0
      rate_limit:
        max_requests_per_minute: 120
        burst_size: 20
```

## 8. Performance Considerations

### 8.1 Overhead Analysis
- **Target**: <5% performance overhead for stable connections
- **Memory**: <10MB additional memory per connector instance
- **Latency**: <1ms additional latency per operation
- **Monitoring**: Resilience metrics collection without performance impact

### 8.2 Optimization Strategies
- Lazy initialization of resilience components
- Connection pooling with resilience
- Efficient state management
- Minimal object allocation during normal operation
- **Benchmarking**: Explicitly benchmark the lazy initialization strategy to confirm low overhead.

## 9. Monitoring & Observability

### 9.1 Metrics Collection
```python
# Automatically collected metrics
connector_retries_total{connector_type="rest", operation="read"}
connector_circuit_breaker_state{connector_type="s3"}
connector_rate_limit_wait_time{connector_type="shopify"}
connector_recovery_attempts{connector_type="postgres"}
```

### 9.2 Health Checks
- Connector health status with resilience state
- Circuit breaker status monitoring
- Rate limiting queue status
- Recovery mechanism health

### 9.3 User-Visible Logging
- **(per PPM review)** Implement structured, human-readable logging for resilience events.
- **Example Log Message**: `INFO [ResilienceManager] Retrying operation 'read' (attempt 2/3) after error: Connection timed out. Next retry in 2.0s.`
- **Example Log Message**: `WARN [ResilienceManager] Circuit breaker for 'PostgresSource' is now OPEN. Failing fast.`

## 10. Migration Strategy

### 10.1 Backward Compatibility
- Existing configurations work unchanged
- Graceful degradation if resilience unavailable
- Clear migration path from old to new patterns
- Deprecation warnings for old patterns

### 10.2 Rollout Plan
1. **Week 1-2**: Foundation and testing
2. **Week 3-4**: Network connectors (REST, S3, PostgreSQL)
3. **Week 5**: Specialized connectors (Google, Shopify)
4. **Week 6**: File connectors, documentation, and final testing

## 11. Risk Mitigation

### 11.1 Technical Risks
- **Performance Impact**: Continuous benchmarking and optimization
- **Complexity**: Tiered approach with smart defaults
- **Integration Issues**: Comprehensive testing strategy
- **Backward Compatibility**: Careful interface design

### 11.2 Business Risks
- **User Adoption**: Progressive enhancement model
- **Support Burden**: Clear documentation and examples
- **Operational Impact**: Gradual rollout with monitoring

## 12. Success Criteria

### 12.1 Technical Success
- [ ] All network-dependent connectors have resilience patterns
- [ ] <5% performance overhead in normal operation
- [ ] 90% reduction in transient failure reports
- [ ] Zero breaking changes to existing configurations

### 12.2 User Success
- [ ] Out-of-the-box reliability for new users
- [ ] Simple configuration for common customizations
- [ ] Advanced control for power users
- [ ] Clear migration path for existing users

## 13. Stakeholder Review Feedback

This version of the document incorporates feedback from a review session with the PSE, PPM, DE, and DA.

- **[PSE] Technical Refinement**: Added notes to the S3 and PostgreSQL integration tasks to suggest a resilient client wrapper for `boto3` and to clarify where resilience decorators should be applied for SQLAlchemy operations.
- **[PPM] User Experience**: Added a dedicated task for creating user-facing documentation and a section on user-visible logging to make the feature's behavior transparent to users.
- **[DE] Feasibility & Estimation**: Increased the effort estimate for the S3 Connector integration from 3 to 4 days to account for the complexity of `boto3` error handling. Added a note to benchmark the lazy initialization strategy.
- **[DA] Simplicity & Clarity**: Added an appendix with a glossary of terms to make the concepts more accessible to all users.

## 14. Appendix: Glossary of Terms

- **Retry**: Automatically attempting an operation again if it fails due to a temporary issue (like a network glitch). Uses "exponential backoff" to wait longer between each subsequent retry, preventing a system from being overwhelmed.
- **Circuit Breaker**: A safety mechanism that stops an operation from being attempted for a set period after it has failed a certain number of times. This prevents a failing service from being flooded with requests and gives it time to recover. After a timeout, it will "half-open" to let a test request through. If it succeeds, the circuit "closes" and returns to normal.
- **Rate Limiting**: A mechanism to control the number of requests a connector can make to an external service within a specific time period (e.g., 100 requests per minute). This is essential for respecting API usage policies and avoiding being blocked.

---

**Document Control:**
- **Review Cycle**: Weekly with PSE, PPM, DE, DA
- **Approval Authority**: PSA + PSE
- **Implementation Start**: Upon PSA + PSE approval
- **Target Completion**: 6 weeks from start date 