# Enhanced Connector Registry

## Overview

The Enhanced Connector Registry provides advanced connector management capabilities that support profile-based configuration, default parameter management, and type validation. It extends the basic registry functionality to enable seamless integration with SQLFlow's profile system.

## Key Features

- **Profile-Based Configuration**: Resolves configuration from profiles with proper precedence
- **Default Configuration Management**: Manages connector defaults and parameter inheritance
- **Type Validation**: Validates parameter types and required fields
- **Backward Compatibility**: Works with all existing connectors without modification
- **Configuration Resolution**: Merges defaults, profile params, and override options

## Architecture

```python
# Configuration Resolution Flow
1. Connector Defaults (lowest precedence)
2. Profile Parameters (medium precedence) 
3. Override Options (highest precedence)
```

## Usage

### Basic Registration

```python
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.csv.source import CSVSource

# Register with defaults and metadata
enhanced_registry.register_source(
    "csv",
    CSVSource,
    default_config={
        "has_header": True,
        "delimiter": ",",
        "encoding": "utf-8"
    },
    required_params=["path"],
    description="CSV file source connector"
)
```

### Configuration Resolution

```python
# Resolve configuration from multiple sources
result = enhanced_registry.resolve_configuration(
    connector_type="csv",
    is_source=True,
    profile_params={"delimiter": "|", "encoding": "utf-8"},
    override_options={"path": "/data/file.csv", "delimiter": ","}
)

# Result shows final resolved configuration
print(result.resolved_config)
# {
#     "has_header": True,      # Default
#     "delimiter": ",",        # Override (wins over profile)
#     "encoding": "utf-8",     # Profile
#     "path": "/data/file.csv" # Override
# }

print(result.overridden_params)  # ["delimiter"]
print(result.validation_warnings)  # []
```

### Connector Creation

```python
# Create connector with resolved configuration
connector = enhanced_registry.create_source_connector(
    "csv",
    result.resolved_config
)

# Connector is fully configured and ready to use
data = connector.read()
```

## Integration with ProfileManager

The enhanced registry works seamlessly with the ProfileManager:

```python
from sqlflow.core.profiles import ProfileManager
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry

# Load profile
profile_manager = ProfileManager("profiles/", "dev")
connector_profile = profile_manager.get_connector_profile("csv_default")

# Resolve configuration
result = enhanced_registry.resolve_configuration(
    connector_profile.connector_type,
    is_source=True,
    profile_params=connector_profile.params,
    override_options={"path": "/custom/path.csv"}
)

# Create connector
connector = enhanced_registry.create_source_connector(
    connector_profile.connector_type,
    result.resolved_config
)
```

## Configuration Precedence

The registry follows a clear configuration precedence model:

1. **Connector Defaults** (Lowest Priority)
   - Built-in sensible defaults
   - Defined at registration time
   - Example: `{"has_header": True, "delimiter": ","}`

2. **Profile Parameters** (Medium Priority)
   - Environment-specific settings
   - Loaded from profile YAML files
   - Example: `{"delimiter": "|", "encoding": "latin1"}`

3. **Override Options** (Highest Priority)
   - Pipeline-specific overrides
   - Specified in OPTIONS clause
   - Example: `{"path": "/specific/file.csv"}`

### Example Resolution

```yaml
# Connector defaults
{
  "has_header": true,
  "delimiter": ",",
  "encoding": "utf-8",
  "timeout": 30
}

# Profile parameters
{
  "delimiter": "|",
  "encoding": "latin1"
}

# Override options
{
  "path": "/data/file.csv",
  "timeout": 60
}

# Final resolved configuration
{
  "has_header": true,      # Default
  "delimiter": "|",        # Profile (overrides default)
  "encoding": "latin1",    # Profile (overrides default)
  "timeout": 60,           # Override (overrides default)
  "path": "/data/file.csv" # Override (new parameter)
}
```

## Validation Features

### Required Parameters

```python
enhanced_registry.register_source(
    "postgres",
    PostgresSource,
    required_params=["host", "database", "username"]
)

# Missing required parameters generate warnings
result = enhanced_registry.resolve_configuration(
    "postgres",
    profile_params={"host": "localhost"}  # Missing database, username
)

print(result.validation_warnings)
# ["Required parameter 'database' is missing",
#  "Required parameter 'username' is missing"]
```

### Type Validation

The registry automatically extracts type information from connector classes and validates configuration:

```python
class TypedConnector(SourceConnector):
    def configure(self, params: Dict[str, Any]) -> None:
        self.timeout: int = params.get("timeout", 30)
        self.ssl: bool = params.get("ssl", False)

# Type validation happens automatically
result = enhanced_registry.resolve_configuration(
    "typed",
    profile_params={"timeout": "invalid", "ssl": "yes"}
)

print(result.validation_warnings)
# ["Parameter 'timeout' expected type int, got str",
#  "Parameter 'ssl' expected type bool, got str"]
```

## Connector Information

Get detailed information about registered connectors:

```python
# Get connector metadata
info = enhanced_registry.get_source_connector_info("csv")
print(info.connector_type)     # "csv"
print(info.default_config)    # {"has_header": True, ...}
print(info.required_params)   # ["path"]
print(info.description)       # "CSV file source connector"

# List all connectors
sources = enhanced_registry.list_source_connectors()
destinations = enhanced_registry.list_destination_connectors()

# Get defaults for a connector
defaults = enhanced_registry.get_connector_defaults("csv")
```

## Backward Compatibility

The enhanced registry maintains full backward compatibility:

```python
# Existing connectors work without modification
enhanced_registry.register_source("csv", CSVSource)

# Legacy connectors without config parameter
class LegacyConnector:
    def __init__(self):
        pass
    
    def configure(self, params):
        self.params = params

enhanced_registry.register_source("legacy", LegacyConnector)

# Both modern and legacy connectors work
connector1 = enhanced_registry.create_source_connector("csv", config)
connector2 = enhanced_registry.create_source_connector("legacy", config)
```

## Best Practices

### 1. Define Sensible Defaults

```python
enhanced_registry.register_source(
    "csv",
    CSVSource,
    default_config={
        "has_header": True,        # Most CSV files have headers
        "delimiter": ",",          # Standard CSV delimiter
        "encoding": "utf-8",       # Modern standard encoding
        "quote_char": '"',         # Standard quote character
        "skip_rows": 0            # Usually don't skip rows
    }
)
```

### 2. Specify Required Parameters

```python
enhanced_registry.register_source(
    "postgres",
    PostgresSource,
    required_params=[
        "host",           # Connection target
        "database",       # Database name
        "username"        # Authentication
    ]
    # Note: password often comes from environment/secrets
)
```

### 3. Add Descriptions

```python
enhanced_registry.register_source(
    "s3",
    S3Source,
    description="Amazon S3 object storage source connector with AWS SDK integration"
)
```

### 4. Handle Configuration Resolution

```python
def create_connector_from_profile(profile_name, override_options=None):
    # Load profile
    connector_profile = profile_manager.get_connector_profile(profile_name)
    
    # Resolve configuration
    result = enhanced_registry.resolve_configuration(
        connector_profile.connector_type,
        is_source=True,
        profile_params=connector_profile.params,
        override_options=override_options or {}
    )
    
    # Log warnings
    for warning in result.validation_warnings:
        logger.warning(f"Configuration warning: {warning}")
    
    # Create connector
    return enhanced_registry.create_source_connector(
        connector_profile.connector_type,
        result.resolved_config
    )
```

## Migration from Basic Registry

Migrating from the basic registry is straightforward:

```python
# Before: Basic registry
from sqlflow.connectors.registry import source_registry
source_registry.register("csv", CSVSource)
connector_class = source_registry.get("csv")
connector = connector_class(config)

# After: Enhanced registry  
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
enhanced_registry.register_source("csv", CSVSource, default_config={...})
connector = enhanced_registry.create_source_connector("csv", config)
```

## Error Handling

The enhanced registry provides clear error messages:

```python
# Unknown connector type
try:
    enhanced_registry.get_source_connector_class("unknown")
except ValueError as e:
    print(e)  # "Unknown source connector type: unknown. Available types: [...]"

# Configuration resolution warnings
result = enhanced_registry.resolve_configuration("postgres", profile_params={})
if result.validation_warnings:
    for warning in result.validation_warnings:
        logger.warning(f"Configuration issue: {warning}")
```

## Performance Considerations

The enhanced registry is designed for performance:

- **Lazy Registration**: Connectors are only instantiated when needed
- **Configuration Caching**: Resolved configurations can be cached
- **Type Extraction**: Type information is extracted once at registration
- **Memory Efficient**: Stores only metadata, not instances

## Testing

The enhanced registry includes comprehensive test coverage:

```python
# Unit tests
pytest tests/unit/connectors/registry/test_enhanced_registry.py

# Integration tests with ProfileManager
pytest tests/integration/connectors/test_enhanced_registry_integration.py
```

## Future Enhancements

Planned enhancements for the enhanced registry:

- **Schema Validation**: JSON Schema validation for connector configurations
- **Plugin Discovery**: Automatic discovery of connector plugins
- **Performance Metrics**: Built-in performance monitoring for connectors
- **Configuration Templates**: Reusable configuration templates
- **Hot Reloading**: Dynamic connector registration without restart 