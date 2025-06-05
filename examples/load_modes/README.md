# Load Modes Examples

This directory contains example SQLFlow pipelines demonstrating the use of different load modes and schema compatibility validation.

## Examples

### Basic Load Modes (`basic_load_modes.sf`)

This example demonstrates the basic usage of all three load modes (REPLACE, APPEND, UPSERT) with CSV data sources. It includes:

- Loading initial data with REPLACE mode (default)
- Adding new records with APPEND mode
- Updating existing records with UPSERT mode
- Using composite keys for upserting

### Schema Compatibility (`schema_compatibility.sf`)

This example demonstrates how SQLFlow validates schema compatibility between source and target tables when using different load modes. It includes:

- Creating tables with compatible and incompatible schemas
- Appending data with compatible schemas
- Upserting data with upsert keys
- Handling schema compatibility issues
- Working with subset selection to ensure compatibility
- Demonstrating type compatibility rules

## Running the Examples

To run these examples, use the SQLFlow CLI:

```bash
# Run the basic load modes example
sqlflow run examples/load_modes/basic_load_modes.sf

# Run the schema compatibility example
sqlflow run examples/load_modes/schema_compatibility.sf
```

## Understanding the Examples

### Data Files

The examples use the following CSV files in the `data/` directory:

- `users.csv`: Base user data with 5 users
- `new_users.csv`: Additional users to append (users 6-8)
- `users_updates.csv`: Updates to existing users and one new user
- `inventory_updates.csv`: Product inventory data with composite keys (product_id, warehouse_id)

### Load Modes

SQLFlow supports the following load modes:

#### REPLACE Mode

REPLACE mode creates a new table or replaces an existing one with the source data. This is the default mode.

```sql
-- Default mode (REPLACE is implicit)
LOAD target_table FROM source_table;

-- Explicitly specifying REPLACE mode
LOAD target_table FROM source_table MODE REPLACE;
```

#### APPEND Mode

APPEND mode adds rows from the source table to the target table without affecting existing data. Requires schema compatibility.

```sql
LOAD target_table FROM source_table MODE APPEND;
```

#### UPSERT Mode

UPSERT mode updates existing rows and inserts new ones based on upsert keys. Requires schema compatibility.

```sql
LOAD target_table FROM source_table MODE UPSERT KEY id;

-- With multiple (composite) upsert keys
LOAD target_table FROM source_table MODE UPSERT KEY id, category;
```

## Schema Compatibility

When using APPEND or UPSERT modes, SQLFlow validates that:

1. All columns in the source table exist in the target table
2. The data types of corresponding columns are compatible

The `schema_compatibility.sf` example demonstrates various scenarios including:
- Compatible schema appends
- Handling extra columns in the source table
- Type compatibility issues
- Subset selection to ensure compatibility

## Advanced Techniques

The examples also demonstrate advanced techniques:

1. **Subset Selection**: When source and target schemas don't align perfectly, you can use a subquery to select only compatible columns:
   ```sql
   LOAD target_table FROM (
       SELECT id, name, email FROM source_table
   ) MODE APPEND;
   ```

2. **Composite Keys**: For complex data models, you can use multiple columns as upsert keys:
   ```sql
   LOAD inventory_table FROM inventory_updates 
   MODE UPSERT KEY product_id, warehouse_id;
   ```

## Learn More

For more detailed information, see the [Load Modes documentation](../../docs/user/reference/load_modes.md) and [Schema Compatibility documentation](../../docs/user/reference/schema_compatibility.md). 