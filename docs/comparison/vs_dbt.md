# SQLFlow vs dbt

> **MVP Status**: This comparison is part of SQLFlow's MVP documentation and will be expanded as SQLFlow matures.

This document compares SQLFlow with [dbt (data build tool)](https://www.getdbt.com/), a popular transformation tool in the modern data stack.

## Overview

**dbt** is a transformation tool that helps analytics engineers transform data in their warehouses by writing modular SQL. It's designed to work with existing data warehouses and focuses specifically on the transformation layer of the data pipeline.

**SQLFlow** is an end-to-end data pipeline platform that covers ingestion, transformation, and export in a unified SQL-based workflow. It includes its own embedded engine (DuckDB) and doesn't require a separate data warehouse.

## Key Differences

| Feature | SQLFlow | dbt |
|---------|---------|-----|
| **Scope** | Complete data pipeline (source to destination) | Transformation only (within warehouse) |
| **Execution Engine** | Built-in DuckDB engine | Relies on external data warehouse |
| **Data Ingestion** | Native support via SOURCE directives | Requires external tools (Fivetran, Airbyte, etc.) |
| **Data Export** | Native support via EXPORT directives | Requires external tools |
| **Primary Language** | Extended SQL | SQL with Jinja templating |
| **Project Structure** | `.sf` files with SQL directives | `.sql` files with YAML configurations |
| **Dependencies** | Automatic dependency detection | Explicit `ref()` function calls |
| **Testing** | Basic testing capabilities (MVP phase) | Comprehensive testing framework |
| **Documentation** | Basic documentation (MVP phase) | Comprehensive documentation generation |
| **Maturity** | Early-stage project | Mature ecosystem with many adopters |

## When to Choose SQLFlow

Consider SQLFlow when:

- You want a **unified tool** for your entire data workflow
- You prefer working with **pure SQL** without learning Jinja templating
- You need a **lightweight solution** that doesn't require a data warehouse
- You want **automatic dependency tracking** without explicit references
- You're working with **smaller to medium datasets** that fit well with DuckDB
- You need a **single, simple tool** rather than integrating multiple components

## When to Choose dbt

Consider dbt when:

- You're working with an **existing data warehouse**
- You need only the **transformation layer** of your data pipeline
- You require **mature testing and documentation** capabilities
- You're working with **very large datasets** that require warehouse scaling
- You need an **established ecosystem** with many plugins and integrations
- You want to leverage **Jinja templating** for advanced SQL generation

## Feature Comparison

### Project Structure

**SQLFlow**:
```sql
-- pipeline.sf
SOURCE users TYPE CSV PARAMS {
  "path": "data/users.csv",
  "has_header": true
};

LOAD users_table FROM users;

CREATE TABLE user_metrics AS
SELECT
  country,
  COUNT(*) AS user_count
FROM users_table
GROUP BY country;

EXPORT
  SELECT * FROM user_metrics
TO "output/metrics.csv"
TYPE CSV
OPTIONS { "header": true };
```

**dbt**:
```sql
-- models/user_metrics.sql
SELECT
  country,
  COUNT(*) AS user_count
FROM {{ ref('users') }}
GROUP BY country
```

```yaml
# models/schema.yml
version: 2
models:
  - name: user_metrics
    description: "Aggregated user metrics by country"
    columns:
      - name: country
        description: "User country"
      - name: user_count
        description: "Count of users"
```

### Dependency Management

**SQLFlow** automatically detects dependencies based on table references in SQL queries, creating a directed acyclic graph (DAG) without requiring explicit references.

**dbt** requires explicit references using the `ref()` function to create dependencies between models.

### Testing

**SQLFlow** is still developing its testing capabilities (MVP phase).

**dbt** has a mature testing framework, supporting:
- Schema tests (uniqueness, not null, accepted values)
- Custom SQL tests
- Data quality assertions

### Documentation

**SQLFlow** provides basic documentation capabilities (MVP phase).

**dbt** offers comprehensive documentation generation:
- Auto-generated documentation site
- Column and model descriptions
- Lineage graphs
- Test results integration

## Integration Possibilities

It's worth noting that SQLFlow and dbt can be complementary in some scenarios:

- Use **SQLFlow** for data ingestion and initial processing
- Use **dbt** for complex transformations in a data warehouse
- Use **SQLFlow** for final exports and deliverables

## Conclusion

SQLFlow and dbt serve different but overlapping needs in the data ecosystem. SQLFlow aims to provide a simpler, more unified approach to data pipelines with its all-in-one design, while dbt excels as a specialized transformation tool within the modern data stack.

The choice between them depends on your specific needs, existing infrastructure, and preferences regarding the scope and complexity of your data tooling. 