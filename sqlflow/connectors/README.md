# SQLFlow Connectors

This package provides connectors for various data sources and destinations. Connectors are the primary way for SQLFlow to interact with external systems.

## Connector Types

SQLFlow supports three types of connectors:

1. **Source Connectors**: Used for reading data from external systems
2. **Export Connectors**: Used for writing data to external systems
3. **Bidirectional Connectors**: Support both reading and writing operations

## Available Connectors

| Connector Type | Source | Export | Implementation |
|---------------|--------|--------|----------------|
| CSV           | ✅     | ✅     | Separate classes |
| PostgreSQL    | ✅     | ✅     | Bidirectional |
| REST          | ✅     | ✅     | Bidirectional |
| S3            | ✅     | ✅     | Bidirectional |
| Parquet       | ✅     | ✅     | Separate classes |
| Google Sheets | ✅     | ✅     | Bidirectional |

## Implementation

### Source Connectors

Source connectors inherit from the `Connector` base class and are registered using the `@register_connector` decorator. They must implement:

- `configure(params: Dict[str, Any]) -> None`
- `test_connection() -> ConnectionTestResult`
- `discover() -> List[str]`
- `get_schema(object_name: str) -> Schema`
- `read(object_name: str, columns: Optional[List[str]] = None, filters: Optional[Dict[str, Any]] = None, batch_size: int = 10000) -> Iterator[DataChunk]`

### Export Connectors

Export connectors inherit from the `ExportConnector` base class and are registered using the `@register_export_connector` decorator. They must implement:

- `configure(params: Dict[str, Any]) -> None`
- `test_connection() -> ConnectionTestResult`
- `write(object_name: str, data_chunk: DataChunk, mode: str = "append") -> None`

### Bidirectional Connectors

Bidirectional connectors inherit from the `BidirectionalConnector` base class, which combines the interfaces of both `Connector` and `ExportConnector`. They are registered using the `@register_bidirectional_connector` decorator, which automatically registers them as both source and export connectors.

Example:

```python
from sqlflow.connectors.google_sheets.source import GoogleSheetsSource
from sqlflow.connectors.registry.source_registry import source_registry

# Register the Google Sheets source connector
source_registry.register("google_sheets", GoogleSheetsSource)
```

## Adding a New Connector

To add a new connector:

1. Determine if it needs to support reading, writing, or both
2. Inherit from the appropriate base class(es)
3. Implement the required methods
4. Register it using the appropriate decorator(s)

### Example: Bidirectional Connector

```python
from sqlflow.connectors.base import BidirectionalConnector, ConnectionTestResult, ConnectorState, Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_bidirectional_connector

@register_bidirectional_connector("MY_SERVICE")
class MyServiceConnector(BidirectionalConnector):
    def __init__(self):
        super().__init__()
        # Initialize properties
        
    def configure(self, params):
        # Configure from params
        
    def test_connection(self):
        # Test the connection
        
    def discover(self):
        # Return list of available objects
        
    def get_schema(self, object_name):
        # Return schema for the object
        
    def read(self, object_name, columns=None, filters=None, batch_size=10000):
        # Read data from the source
        
    def write(self, object_name, data_chunk, mode="append"):
        # Write data to the destination
```

## Testing Connectors

Each connector should have a comprehensive test suite covering:

1. Configuration validation
2. Connection testing
3. Schema retrieval
4. Reading data
5. Writing data (for export and bidirectional connectors)
6. Error handling

See the test files in `tests/unit/connectors/` for examples.

## Documentation

When creating a new connector, remember to:

1. Add docstrings to all classes and methods
2. Update the connector's parameter documentation
3. Add examples to the connector usage guide
4. Update the connectors list in the documentation 