# SQLFlow Connectors

SQLFlow provides a comprehensive set of connectors for reading from and writing to various data sources. This directory contains all available connectors with their documentation and implementation.

## 🗂️ Connector Catalog

| Connector | Source | Destination | Status | Documentation |
|-----------|--------|-------------|--------|---------------|
| **CSV** | ✅ | ✅ | ✅ Production | [📖 Overview](csv/README.md) • [📥 Source](csv/SOURCE.md) • [📤 Destination](csv/DESTINATION.md) |
| **In-Memory** | ✅ | ✅ | ✅ Production | [📖 Overview](in_memory/README.md) • [📥 Source](in_memory/SOURCE.md) • [📤 Destination](in_memory/DESTINATION.md) |
| **S3** | ✅ | ✅ | ✅ Production | [📖 Overview](s3/README.md) • [📥 Source](s3/SOURCE.md) • [📤 Destination](s3/DESTINATION.md) |
| **Google Sheets** | ✅ | ❌ | ✅ Production | [📖 Overview](google_sheets/README.md) • [📥 Source](google_sheets/SOURCE.md) |
| **Parquet** | ✅ | ✅ | ✅ Production | [📖 Overview](parquet/README.md) • [📥 Source](parquet/SOURCE.md) • [📤 Destination](parquet/DESTINATION.md) |
| **REST API** | ✅ | ❌ | ✅ Production | [📖 Overview](rest/README.md) • [📥 Source](rest/SOURCE.md) |
| **PostgreSQL** | ✅ | ✅ | 🔄 Migrating | [📖 Overview](postgres/README.md) • [📥 Source](postgres/SOURCE.md) • [📤 Destination](postgres/DESTINATION.md) |
| **Shopify** | ✅ | ❌ | 🔄 Migrating | [📖 Overview](shopify/README.md) • [📥 Source](shopify/SOURCE.md) |

### Legend
- ✅ **Production**: Fully implemented and tested
- 🔄 **Migrating**: Currently being migrated to new architecture
- ❌ **Not Available**: Feature not implemented

## 🚀 Quick Start

### 1. Choose Your Connector
Browse the catalog above and click on the connector documentation you need.

### 2. Configure Your Profile
Add the connector configuration to your `profiles/dev.yml`:

```yaml
sources:
  my_source:
    type: "csv"
    path: "data/input.csv"

destinations:
  my_dest:
    type: "csv"
    path: "data/output.csv"
```

### 3. Use in Pipeline
Reference the connector in your pipeline:

```sql
-- pipelines/my_pipeline.sql
FROM source('my_source')
TO destination('my_dest');
```

## 📋 Connector Types

### Source Connectors
Extract data from external systems into SQLFlow pipelines.

**Features:**
- Schema discovery and validation
- Incremental loading support
- Connection testing
- Batch processing
- Error handling and retries

### Destination Connectors  
Write processed data to external systems.

**Features:**
- Multiple write modes (append, replace, merge)
- Schema validation
- Transactional writes
- Error handling

## 🏗️ Architecture

### New Connector Interface (v2)
SQLFlow uses a modern, unified connector architecture:

```python
from sqlflow.connectors.base.connector import Connector
from sqlflow.connectors.base.destination_connector import DestinationConnector

# Source connectors inherit from Connector
class MySource(Connector):
    def configure(self, params: Dict[str, Any]) -> None: ...
    def test_connection(self) -> ConnectionTestResult: ...
    def discover(self) -> List[str]: ...
    def get_schema(self, object_name: str) -> Schema: ...
    def read(self, object_name: str, **kwargs) -> Iterator[DataChunk]: ...

# Destination connectors inherit from DestinationConnector  
class MyDestination(DestinationConnector):
    def __init__(self, config: Dict[str, Any]): ...
    def write(self, df: pd.DataFrame, options: Dict[str, Any] = None) -> None: ...
```

### Registry System
Connectors are automatically registered for use:

```python
from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.registry.destination_registry import destination_registry

# Connectors register themselves
source_registry.register("csv", CSVSource)
destination_registry.register("csv", CSVDestination)
```

## 🔧 Development

### Creating a New Connector

1. **Create connector directory**: `sqlflow/connectors/my_connector/`
2. **Implement source/destination classes**
3. **Register in `__init__.py`**
4. **Add comprehensive tests**
5. **Create documentation files**:
   - `README.md` - Overview and quick start
   - `SOURCE.md` - Complete source documentation
   - `DESTINATION.md` - Complete destination documentation (if applicable)

### Testing
All connectors must pass:
- Unit tests for all functionality
- Integration tests with real data sources  
- Example pipelines in `examples/`
- Connection and schema discovery tests

## 📚 Additional Resources

- **[Connector Development Guide](../docs/developer-guides/connector-development.md)** - How to build custom connectors
- **[Configuration Reference](../docs/reference/configuration.md)** - Complete configuration options
- **[Examples](../examples/)** - Real-world usage examples
- **[API Documentation](../docs/api/)** - Programmatic connector usage

## 🤝 Contributing

We welcome connector contributions! See our [Contribution Guidelines](../CONTRIBUTING.md) for:
- Code standards and testing requirements
- Documentation templates
- Review process
- Community guidelines

## 📞 Support

- **Documentation Issues**: [GitHub Issues](https://github.com/giaosudau/sqlflow/issues)
- **Community**: [Discord/Slack Community]
- **Enterprise Support**: [Contact Information]

---

*Last Updated: [Current Date] • SQLFlow Connector Architecture v2.0* 