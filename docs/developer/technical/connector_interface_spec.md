# SQLFlow Connector Interface Specification

**Document Version:** 1.0  
**Date:** January 2025  
**Task:** 2.1 - Connector Interface Standardization  
**Status:** In Progress

---

## Executive Summary

This specification defines the standardized interface for all SQLFlow connectors, ensuring consistency, reliability, and industry-standard parameter compatibility across all connector implementations. The interface provides a foundation for incremental loading, health monitoring, and resilience patterns.

## Goals

1. **Standardized Interface**: All connectors implement the same base interface
2. **Industry Compatibility**: Support Airbyte/Fivetran-compatible parameters
3. **Incremental Loading**: Built-in support for watermark-based incremental reading
4. **Health Monitoring**: Comprehensive health checks and performance metrics
5. **Error Handling**: Standardized error types and clear error messages
6. **Parameter Validation**: Robust validation framework for all parameters

## Core Interface Components

### 1. Base Connector Class

```python
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Union
from enum import Enum

class SyncMode(Enum):
    """Synchronization modes for connectors."""
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"
    CDC = "cdc"

class ConnectorState(Enum):
    """State of a connector."""
    CREATED = auto()
    CONFIGURED = auto()
    READY = auto()
    ERROR = auto()

class ConnectorType(Enum):
    """Type of connector based on data flow direction."""
    SOURCE = "source"
    EXPORT = "export"
    BIDIRECTIONAL = "bidirectional"

class Connector(ABC):
    """Standardized base class for all source connectors."""
    
    def __init__(self):
        self.state = ConnectorState.CREATED
        self.name: Optional[str] = None
        self.connector_type = ConnectorType.SOURCE
        self.connection_params: Dict[str, Any] = {}
        self.is_connected: bool = False
        self.health_status: str = "unknown"
        self._parameter_validator: Optional[ParameterValidator] = None
```

### 2. Industry-Standard Parameters

```python
STANDARD_PARAMS = {
    # Connection parameters
    "host": str,
    "port": int,
    "database": str,
    "username": str,
    "password": str,
    
    # Incremental loading parameters
    "sync_mode": str,  # "full_refresh", "incremental", "cdc"
    "cursor_field": str,  # Field for incremental loading
    "primary_key": Union[str, List[str]],  # Primary key(s)
    
    # Performance parameters
    "batch_size": int,  # Default: 10000
    "timeout_seconds": int,  # Default: 300
    "max_retries": int,  # Default: 3
    
    # Data handling parameters
    "schema": str,  # Schema/namespace
    "table": str,  # Table/object name
    "query": str,  # Custom query override
    "path": str,  # File path for file-based connectors
    "has_header": bool,  # CSV header flag
}
```

### 3. Parameter Validation Framework

```python
class ParameterValidator:
    """Standardized parameter validation for all connectors."""
    
    def __init__(self, connector_type: str):
        self.connector_type = connector_type
        self.required_params = self._get_required_params()
        self.optional_params = self._get_optional_params()
        
    def validate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters and return normalized version."""
        # Check required parameters
        missing = set(self.required_params) - set(params.keys())
        if missing:
            raise ParameterError(f"Missing required parameters: {missing}")
            
        # Validate parameter types
        validated = {}
        for key, value in params.items():
            validated[key] = self._validate_param_type(key, value)
            
        # Set defaults for optional parameters
        for key, default in self.optional_params.items():
            if key not in validated:
                validated[key] = default
                
        return validated
```

### 4. Standardized Exception Hierarchy

```python
class ParameterError(ConnectorError):
    """Parameter validation errors."""
    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Parameter error: {message}")

class IncrementalError(ConnectorError):
    """Incremental loading errors."""
    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Incremental error: {message}")

class HealthCheckError(ConnectorError):
    """Health check errors."""
    def __init__(self, message: str, connector_name: str = "unknown", **kwargs):
        super().__init__(connector_name, f"Health check error: {message}")
```

## Required Methods

### 1. Configuration and Validation

```python
@abstractmethod
def configure(self, params: Dict[str, Any]) -> None:
    """Configure the connector with parameters."""
    
def validate_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize parameters using standardized framework."""
    
def validate_incremental_params(self, params: Dict[str, Any]) -> None:
    """Validate incremental loading parameters."""
```

### 2. Connection Management

```python
@abstractmethod
def test_connection(self) -> ConnectionTestResult:
    """Test the connection to the data source."""
    
def check_health(self) -> Dict[str, Any]:
    """Comprehensive health check with performance metrics."""
    
def get_performance_metrics(self) -> Dict[str, Any]:
    """Get performance metrics for monitoring."""
```

### 3. Data Discovery

```python
@abstractmethod
def discover(self) -> List[str]:
    """Discover available objects in the data source."""
    
@abstractmethod
def get_schema(self, object_name: str) -> Schema:
    """Get schema for an object."""
```

### 4. Data Reading

```python
@abstractmethod
def read(
    self,
    object_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    batch_size: int = 10000,
) -> Iterator[DataChunk]:
    """Read data from the source in chunks."""
    
def read_incremental(
    self,
    object_name: str,
    cursor_field: str,
    cursor_value: Optional[Any] = None,
    columns: Optional[List[str]] = None,
    batch_size: int = 10000,
) -> Iterator[DataChunk]:
    """Read data incrementally from the source using cursor-based approach."""
    
def supports_incremental(self) -> bool:
    """Check if the connector supports incremental reading."""
    
def get_cursor_value(
    self, data_chunk: DataChunk, cursor_field: str
) -> Optional[Any]:
    """Extract the maximum cursor value from a data chunk."""
```

## Health Monitoring Interface

### Health Check Response Format

```python
{
    "status": "healthy" | "unhealthy",
    "connected": bool,
    "response_time_ms": float,
    "last_check": str,  # ISO timestamp
    "capabilities": {
        "incremental": bool,
        "batch_reading": bool,
        "health_monitoring": bool
    },
    "error": str  # Only present if unhealthy
}
```

### Performance Metrics Format

```python
{
    "connection_time_ms": float,
    "query_time_ms": float,
    "rows_per_second": float,
    "bytes_transferred": int
}
```

## Connector-Specific Requirements

### CSV Connector

**Required Parameters:**
- `path`: File path to CSV file

**Optional Parameters:**
- `delimiter`: Field delimiter (default: ",")
- `has_header`: Whether file has header row (default: true)
- `encoding`: File encoding (default: "utf-8")

### PostgreSQL Connector

**Required Parameters:**
- `host`: Database host
- `database`: Database name
- `username`: Username
- `password`: Password

**Optional Parameters:**
- `port`: Database port (default: 5432)
- `schema`: Schema name (default: "public")
- `ssl_mode`: SSL mode (default: "prefer")

### S3 Connector

**Required Parameters:**
- `bucket`: S3 bucket name
- `prefix`: Object prefix/path

**Optional Parameters:**
- `aws_access_key_id`: AWS access key
- `aws_secret_access_key`: AWS secret key
- `region`: AWS region (default: "us-east-1")
- `file_format`: File format (default: "csv")

## Implementation Guidelines

### 1. Parameter Validation

```python
def configure(self, params: Dict[str, Any]) -> None:
    """Configure the connector with parameters."""
    # Always validate parameters first
    validated_params = self.validate_params(params)
    
    # Store validated parameters
    self.connection_params = validated_params
    
    # Validate incremental parameters if applicable
    if validated_params.get("sync_mode") == "incremental":
        self.validate_incremental_params(validated_params)
    
    # Update state
    self.state = ConnectorState.CONFIGURED
```

### 2. Health Monitoring

```python
def check_health(self) -> Dict[str, Any]:
    """Comprehensive health check with performance metrics."""
    start_time = time.time()
    
    try:
        # Test basic connectivity
        self._test_connection()
        
        # Test read capability
        self._test_read_capability()
        
        # Calculate response time
        response_time = (time.time() - start_time) * 1000
        
        self.health_status = "healthy"
        return {
            "status": "healthy",
            "connected": True,
            "response_time_ms": response_time,
            "last_check": datetime.utcnow().isoformat(),
            "capabilities": {
                "incremental": self.supports_incremental(),
                "batch_reading": True,
                "health_monitoring": True
            }
        }
    except Exception as e:
        self.health_status = "unhealthy"
        return {
            "status": "unhealthy",
            "connected": False,
            "error": str(e),
            "last_check": datetime.utcnow().isoformat()
        }
```

### 3. Incremental Loading

```python
def read_incremental(
    self,
    object_name: str,
    cursor_field: str,
    cursor_value: Optional[Any] = None,
    columns: Optional[List[str]] = None,
    batch_size: int = 10000,
) -> Iterator[DataChunk]:
    """Read data incrementally from the source using cursor-based approach."""
    if not self.supports_incremental():
        raise IncrementalError("Connector does not support incremental loading")
        
    # Default implementation filters by cursor field
    if cursor_value is not None:
        filters = {cursor_field: {">=": cursor_value}}
    else:
        filters = None

    return self.read(
        object_name=object_name,
        columns=columns,
        filters=filters,
        batch_size=batch_size,
    )
```

## Testing Requirements

### Unit Tests

Each connector must implement:

1. **Parameter Validation Tests**
   - Valid parameter combinations
   - Invalid parameter handling
   - Required parameter validation
   - Type validation

2. **Health Check Tests**
   - Successful health checks
   - Failed connection scenarios
   - Performance metric collection

3. **Incremental Loading Tests**
   - Cursor-based filtering
   - Watermark extraction
   - Empty result handling

### Integration Tests

1. **Real Connection Tests**
   - Actual database/service connections
   - Authentication validation
   - Network failure handling

2. **Data Reading Tests**
   - Large dataset handling
   - Schema validation
   - Performance benchmarks

## Migration Guide

### Existing Connector Updates

1. **Update Base Class**
   ```python
   # Before
   class MyConnector:
       def __init__(self):
           pass
   
   # After
   class MyConnector(Connector):
       def __init__(self):
           super().__init__()
           self.connector_type = ConnectorType.SOURCE
   ```

2. **Add Parameter Validation**
   ```python
   def configure(self, params: Dict[str, Any]) -> None:
       # Add validation
       validated_params = self.validate_params(params)
       # Existing configuration logic
       self.connection_params = validated_params
   ```

3. **Implement Health Checks**
   ```python
   def check_health(self) -> Dict[str, Any]:
       # Implement health check logic
       return super().check_health()
   ```

## Success Criteria

1. **Interface Consistency**: All connectors implement the same interface
2. **Parameter Compatibility**: 100% compatibility with industry-standard parameters
3. **Health Monitoring**: All connectors provide health status and metrics
4. **Error Handling**: Standardized error types and clear messages
5. **Test Coverage**: >90% test coverage for all interface methods
6. **Documentation**: Complete API documentation for all methods

## Implementation Timeline

- **Day 1**: Complete specification and base class updates
- **Day 2**: Update existing connectors (CSV, PostgreSQL)
- **Day 3**: Implement health monitoring and parameter validation
- **Day 4**: Testing and validation

This specification ensures that all SQLFlow connectors provide a consistent, reliable, and industry-compatible interface for data integration. 