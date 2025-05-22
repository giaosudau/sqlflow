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

## APPEND Mode

The APPEND mode adds data to an existing table without affecting the existing data.

```sql
LOAD users_table FROM users_source MODE APPEND;
```

Key features:
- If the target table doesn't exist, it will be created
- If the target table exists, new data will be appended
- Schema compatibility is validated before loading

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

## Schema Compatibility

All load modes except REPLACE perform schema compatibility validation:

- For APPEND: Ensures source columns exist in target table with compatible types
- For MERGE: Validates both schema compatibility and merge key validity

For more details on schema compatibility, see [Schema Compatibility](schema_compatibility.md).

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

## Best Practices

1. **For MERGE mode**: 
   - Choose merge keys that uniquely identify records in the target table
   - Ensure merge keys have appropriate indexes in the underlying database
   - Use multiple merge keys when a single column isn't sufficient for unique identification

2. **For APPEND mode**:
   - Consider adding uniqueness constraints if duplicates are a concern
   - Be aware that schema compatibility only checks column existence and type, not values

3. **For all modes**:
   - Validate your data quality before loading
   - Consider adding data quality checks in your pipeline 