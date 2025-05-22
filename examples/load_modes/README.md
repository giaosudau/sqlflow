# Load Modes Examples

This directory contains example SQLFlow pipelines demonstrating the use of different load modes and schema compatibility validation.

## Examples

### Schema Compatibility (`schema_compatibility.sf`)

This example demonstrates how SQLFlow validates schema compatibility between source and target tables when using different load modes (REPLACE, APPEND, MERGE). It includes:

- Creating tables with compatible and incompatible schemas
- Appending data with compatible schemas
- Merging data with merge keys
- Handling schema compatibility errors
- Working with subset selection to ensure compatibility

## Running the Examples

To run these examples, use the SQLFlow CLI:

```bash
# Run the schema compatibility example
sqlflow run examples/load_modes/schema_compatibility.sf
```

## Load Modes

SQLFlow supports the following load modes:

### REPLACE Mode

REPLACE mode creates a new table or replaces an existing one with the source data.

```sql
LOAD target_table
FROM source_table
MODE REPLACE
```

### APPEND Mode

APPEND mode adds rows from the source table to the target table. Requires schema compatibility.

```sql
LOAD target_table
FROM source_table
MODE APPEND
```

### MERGE Mode

MERGE mode updates existing rows and inserts new ones based on merge keys. Requires schema compatibility.

```sql
LOAD target_table
FROM source_table
MODE MERGE
MERGE_KEYS (id)
```

## Schema Compatibility

When using APPEND or MERGE modes, SQLFlow validates that:

1. All columns in the source table exist in the target table
2. The data types of corresponding columns are compatible

For more information, see the [Schema Compatibility documentation](../../docs/user/reference/schema_compatibility.md). 