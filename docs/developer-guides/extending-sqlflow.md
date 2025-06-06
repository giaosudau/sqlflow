# Extending SQLFlow: Developer Guide

## Overview

SQLFlow's extensibility is designed around three core principles: **simplicity**, **standards compliance**, and **developer productivity**. This guide provides a high-level overview of SQLFlow's extension points and architectural patterns for developers who want to build custom functionality.

**Core Extension Philosophy**
- **SQL-native approach**: Extensions enhance SQL rather than replace it
- **Industry standards**: Compatible with existing tools and patterns
- **Minimal complexity**: Focus on business logic, not infrastructure

## 🎯 Extension Architecture

SQLFlow provides three primary extension points for developers:

### 1. **Connectors** - Data Integration
Connect SQLFlow to external data sources and destinations with a standardized interface that handles resilience, performance, and state management automatically.

**Key Capabilities:**
- Industry-standard parameter compatibility (Airbyte/Fivetran)
- Automatic incremental loading with watermark management  
- Built-in resilience patterns (retry, circuit breaker, rate limiting)
- Performance optimization (batching, connection pooling)

### 2. **Transform Layer** - Data Processing
Extend SQL with custom transformation logic while maintaining SQL-native patterns and automatic optimization.

**Key Capabilities:**
- Multiple transformation modes (REPLACE, APPEND, UPSERT, INCREMENTAL)
- Automatic state management and watermark tracking
- Schema evolution and data quality validation
- Performance monitoring and optimization

### 3. **Python UDFs** - Custom Logic
Integrate Python functions seamlessly into SQL queries for business logic that's difficult to express in pure SQL.

**Key Capabilities:**
- Scalar UDFs for row-level transformations
- Table UDFs for dataset-level processing
- Automatic discovery and registration
- Type safety and validation

## 📚 Developer Documentation

### Building Connectors
**📖 [Connector Development Guide](building-connectors.md)**
- Connector architecture and patterns
- Step-by-step implementation guide
- Industry-standard parameter handling
- Testing and validation strategies

**📖 [State Management](state-management.md)**
- Incremental loading patterns
- Watermark management
- Error handling and recovery
- Performance optimization

### Building Transforms
**📖 [Transform Development Guide](building-transforms.md)**
- Transform layer architecture
- Custom transformation strategies
- State management integration
- Performance and monitoring

### Python Integration
**📖 [UDF Development Guide](building-udfs.md)**
- UDF types and patterns
- Function discovery and registration
- Type safety and validation
- Integration with SQL execution

## 🚀 Quick Start Patterns

### Simple Connector
```python
from sqlflow.connectors.base import Connector, register_connector

@register_connector("MYAPI")
class MyAPIConnector(Connector):
    def configure(self, params):
        # Industry-standard parameter handling
        self.api_key = params["api_key"]
        self.base_url = params["base_url"]
    
    def read(self, object_name, **kwargs):
        # Implement data reading logic
        # Return DataChunk objects
        pass
```

### Simple Transform
```sql
-- Automatic state management and optimization
CREATE TABLE daily_metrics MODE INCREMENTAL BY updated_at AS
SELECT 
    DATE(created_at) as metric_date,
    COUNT(*) as total_records,
    AVG(value) as avg_value
FROM raw_data
WHERE updated_at > @last_watermark
GROUP BY DATE(created_at);
```

### Simple UDF
```python
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    """Calculate tax on a price with automatic type handling."""
    return price * (1 + tax_rate)
```

## 🏗️ Architecture Principles

### 1. **Convention Over Configuration**
SQLFlow provides intelligent defaults while allowing customization when needed. Extensions should follow this pattern by providing sensible defaults for common use cases.

### 2. **Industry Standards First**  
All extensions should prioritize compatibility with existing tools and standards (Airbyte, Fivetran, dbt patterns) to minimize migration effort.

### 3. **Automatic State Management**
Extensions benefit from SQLFlow's built-in state management, watermarking, and recovery systems without additional implementation effort.

### 4. **Performance by Design**
The platform automatically provides performance optimizations (caching, batching, parallel execution) that extensions inherit.

### 5. **Testing-First Development**
Comprehensive testing infrastructure ensures extensions work reliably across different environments and scenarios.

## 🔧 Development Workflow

### 1. **Setup Development Environment**
```bash
git clone https://github.com/yourorg/sqlflow.git
cd sqlflow
pip install -e ".[dev]"
pytest  # Verify setup
```

### 2. **Choose Extension Type**
- **Data Integration**: Build a connector
- **Data Processing**: Build transform functions  
- **Business Logic**: Build Python UDFs

### 3. **Follow Documentation**
Each extension type has detailed guides with examples, testing strategies, and best practices.

### 4. **Test and Validate**
```bash
# Run connector tests
pytest tests/unit/connectors/

# Run UDF tests  
pytest tests/unit/udfs/

# Run integration tests
pytest tests/integration/
```

### 5. **Contribute Back**
Extensions that solve common problems are welcomed as contributions to the core platform.

## 📊 Current Implementation Status

### Production Ready ✅
- **Connector Framework**: Industry-standard interface with resilience patterns
- **State Management**: Atomic watermark management with DuckDB backend
- **Python UDFs**: Scalar and table UDFs with automatic discovery
- **Transform Layer**: Multiple strategies with automatic optimization

### In Development 🚧
- **Advanced SaaS Connectors**: Shopify, Stripe, HubSpot integrations
- **Transform Optimization**: ML-based query optimization
- **Enhanced Monitoring**: Real-time performance metrics and alerting

### Planned 📋
- **UDF Marketplace**: Shareable UDF ecosystem
- **Advanced Transforms**: Window functions and complex aggregations
- **Distributed Execution**: Multi-node processing capabilities

## 🎯 Success Patterns

### For Data Engineers
**Focus**: Building robust, scalable data pipelines
- Use connector framework for reliable data integration
- Leverage automatic state management for incremental processing
- Implement comprehensive error handling and monitoring

### For Data Analysts  
**Focus**: Extending SQL capabilities for business logic
- Build Python UDFs for complex calculations
- Use transform layer for data modeling and aggregation
- Focus on business logic rather than infrastructure

### For Platform Teams
**Focus**: Standardized, maintainable extensions
- Follow industry-standard parameter conventions
- Implement comprehensive testing and documentation
- Design for reusability across teams and projects

## 📖 Related Documentation

- **[Architecture Overview](architecture-overview.md)** - System design and technical decisions
- **[State Management](state-management.md)** - Incremental loading and watermark system  
- **[UDF System](udf-system.md)** - Complete UDF architecture and implementation
- **[Connector Examples](../examples/connector_interface_demo/)** - Practical implementation examples

---

**Ready to extend SQLFlow?** Choose your extension type above and follow the detailed guides. The SQLFlow community is here to help with questions, code reviews, and contributions. 