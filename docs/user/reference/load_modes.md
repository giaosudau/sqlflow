# Load Modes in SQLFlow

SQLFlow provides three different load modes when loading data from a source into a table:

- **REPLACE**: Create a new table or replace an existing one (default mode)
- **APPEND**: Add data to an existing table without affecting existing data
- **MERGE**: Update existing records that match on specified keys and insert new records

## Syntax

The general syntax for using load modes is:

```sql
LOAD target_table FROM source_name [MODE mode_type] [MERGE_KEYS key1, key2, ...];
```

Where:
- `target_table` is the name of the table to load data into
- `source_name` is the name of the source defined earlier in the pipeline
- `mode_type` is one of: `REPLACE`, `APPEND`, or `MERGE`
- `key1, key2, ...` are the column names to use as merge keys (required for MERGE mode)

## REPLACE Mode (Default)

The REPLACE mode creates a new table or replaces an existing one with the data from the source.

```sql
-- Default mode (REPLACE)
LOAD users_table FROM users_source;

-- Explicitly specifying REPLACE mode
LOAD users_table FROM users_source MODE REPLACE;
```

Key features:
- Drops the existing table if it exists and creates a new one
- The fastest mode as it doesn't require compatibility checks
- No constraints on schema differences between executions
- Useful for full refreshes of tables

## APPEND Mode

The APPEND mode adds data to an existing table without affecting the existing data.

```sql
LOAD users_table FROM users_source MODE APPEND;
```

Key features:
- If the target table doesn't exist, it will be created
- If the target table exists, new data will be appended
- Schema compatibility is validated before loading
- All columns in the source must exist in the target with compatible types
- Extra columns in the target table are ignored

### Schema Requirements for APPEND

For APPEND mode, SQLFlow enforces the following schema requirements:

1. Every column in the source must exist in the target table
2. Data types of source columns must be compatible with target columns
3. Source data must meet target table constraints (NOT NULL, etc.)

Error handling:
```sql
-- Handling schema compatibility errors
TRY
    LOAD users_table FROM users_source MODE APPEND;
CATCH
    -- Alternative logic if APPEND fails
    LOG "APPEND failed, falling back to subset selection";
    
    -- Select only compatible columns
    LOAD users_table FROM (
        SELECT id, name, email FROM users_source
    ) MODE APPEND;
END TRY;
```

## MERGE Mode

The MERGE mode updates existing records that match on specified keys and inserts new records.

```sql
LOAD users_table FROM users_source MODE MERGE MERGE_KEYS user_id;
```

Key features:
- Requires one or more merge keys to identify matching records
- Updates existing records when merge keys match
- Inserts new records when no match is found
- Schema compatibility is validated before loading
- Most computationally intensive mode, but avoids duplicates

### Merge Keys

Merge keys are columns used to match records between the source and target tables. 
They act similar to a primary key or unique identifier.

#### Requirements for Merge Keys:

1. **Existence**: Merge keys must exist in both source and target tables
2. **Type Compatibility**: Merge keys must have compatible data types
3. **Uniqueness**: While not strictly enforced, merge keys should uniquely identify records in the target table

#### Multiple Merge Keys

You can specify multiple columns as merge keys for composite key matching:

```sql
LOAD users_table FROM users_source MODE MERGE MERGE_KEYS user_id, email;
```

This will match records where both `user_id` AND `email` match between source and target.

#### Validation

SQLFlow performs the following validations for merge keys:

- Checks that all merge keys exist in both source and target tables
- Validates that merge key columns have compatible types
- Warns if uniqueness of merge keys cannot be verified

### Advanced MERGE Handling

For advanced MERGE needs, you can use conditional logic:

```sql
-- Custom MERGE with different handling based on conditions
LOAD target_table FROM (
    SELECT
        s.*,
        -- Custom update date for existing records
        CASE WHEN t.id IS NOT NULL THEN current_timestamp ELSE s.created_at END AS last_updated
    FROM source s
    LEFT JOIN target t ON s.id = t.id
) MODE MERGE MERGE_KEYS id;
```

## Schema Compatibility

All load modes except REPLACE perform schema compatibility validation:

- For APPEND: Ensures source columns exist in target table with compatible types
- For MERGE: Validates both schema compatibility and merge key validity

For more details on schema compatibility, see [Schema Compatibility](schema_compatibility.md).

## Performance Considerations

### REPLACE Mode
- Fastest for full refreshes
- Inefficient for large tables with small updates
- Requires no additional processing for validation

### APPEND Mode
- Efficient for adding new records
- May create duplicates if not carefully managed
- Requires schema validation overhead

### MERGE Mode
- Most resource-intensive (requires lookups on merge keys)
- Consider indexing merge keys in the underlying database
- For large operations, consider batching or using temporary tables

## Common Issues and Troubleshooting

### Schema Incompatibility

**Issue**: When using APPEND or MERGE mode, you get schema compatibility errors.

**Solution**:
1. Use a subquery to select only the compatible columns:
   ```sql
   LOAD users_table FROM (
       SELECT id, name, email FROM source_with_extra_columns
   ) MODE APPEND;
   ```
2. Transform data types in the source to match target:
   ```sql
   LOAD users_table FROM (
       SELECT 
           id,
           CAST(numeric_value AS INTEGER) AS numeric_value
       FROM source
   ) MODE APPEND;
   ```

### Duplicate Records

**Issue**: APPEND mode creates duplicate records in the target table.

**Solutions**:
1. Use MERGE mode instead with appropriate merge keys
2. Add a filtering step to remove potential duplicates:
   ```sql
   LOAD users_table FROM (
       SELECT s.* FROM source s
       LEFT JOIN target t ON s.id = t.id
       WHERE t.id IS NULL
   ) MODE APPEND;
   ```

### Merge Key Selection

**Issue**: Not sure which columns to use as merge keys.

**Guidelines**:
1. Use business keys or natural identifiers when available
2. Consider timestamps or version indicators for time-based merges
3. Use composite keys when a single column isn't sufficient
4. Avoid using volatile columns (those likely to change)

## Examples

### REPLACE Mode Example

```sql
SOURCE users_csv TYPE CSV PARAMS {
  "file_path": "data/users.csv", 
  "has_header": true
};

-- Replace the entire users_table with data from users_csv
LOAD users_table FROM users_csv;
```

### APPEND Mode Example

```sql
SOURCE new_users TYPE CSV PARAMS {
  "file_path": "data/new_users.csv",
  "has_header": true
};

-- Add new_users data to users_table
LOAD users_table FROM new_users MODE APPEND;
```

### MERGE Mode Example

```sql
SOURCE user_updates TYPE CSV PARAMS {
  "file_path": "data/user_updates.csv",
  "has_header": true
};

-- Update existing users and insert new users based on user_id
LOAD users_table FROM user_updates MODE MERGE MERGE_KEYS user_id;
```

### Multiple Merge Keys Example

```sql
SOURCE product_inventory TYPE CSV PARAMS {
  "file_path": "data/inventory_updates.csv",
  "has_header": true
};

-- Update inventory using product_id and warehouse_id as composite key
LOAD inventory FROM product_inventory MODE MERGE MERGE_KEYS product_id, warehouse_id;
```

### Handling Schema Evolution

```sql
-- Approach 1: Using TRY/CATCH
TRY
    -- Attempt direct APPEND
    LOAD target FROM source MODE APPEND;
CATCH
    -- Fall back to column subset on failure
    LOG "Schema incompatible, selecting compatible columns only";
    LOAD target FROM (
        SELECT id, name, email FROM source
    ) MODE APPEND;
END TRY;

-- Approach 2: Using conditional logic
IF ${enable_schema_evolution} == "true" THEN
    -- Create a new table with the evolved schema
    LOAD target FROM source MODE REPLACE;
ELSE
    -- Use the safe APPEND approach
    LOAD target FROM source MODE APPEND;
END IF;
```

## Best Practices

1. **For REPLACE mode**: 
   - Use for initial table creation or full refreshes
   - Consider the impact on downstream dependencies
   - Be cautious with production tables that other processes depend on

2. **For MERGE mode**: 
   - Choose merge keys that uniquely identify records in the target table
   - Ensure merge keys have appropriate indexes in the underlying database
   - Use multiple merge keys when a single column isn't sufficient for unique identification
   - Add a timestamp or version column to track record updates

3. **For APPEND mode**:
   - Consider adding uniqueness constraints if duplicates are a concern
   - Be aware that schema compatibility only checks column existence and type, not values
   - Use filtering to prevent duplicates when needed

4. **For all modes**:
   - Validate your data quality before loading
   - Consider adding data quality checks in your pipeline
   - Use transactions when possible to ensure atomicity
   - Document your load strategy and the rationale for your choice of mode 