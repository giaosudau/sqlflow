# In-Memory Destination

## Overview
The `InMemoryDestination` connector writes a pandas DataFrame to a global, in-memory data store. It is the counterpart to the `InMemorySource` and is used for internal testing and development to capture the output of a pipeline without writing to a real database or file.

## Configuration
To use the `InMemoryDestination`, you need to specify the `type` as `in_memory` in the destination section of your profile configuration.

### Required Parameters
- `table_name` (string): The key in the global `IN_MEMORY_DATA_STORE` dictionary where the output pandas DataFrame will be stored. If the key already exists, it will be overwritten.

### Optional Parameters
This connector has no optional parameters.

## Usage Example
Here is an example of how to configure an `in_memory` destination in your `profiles.yml`.

```yaml
# profiles/my_profile.yml
my_profile:
  destinations:
    my_test_output:
      type: in_memory
      table_name: 'output_table_456'
  ...
```

In your pipeline, you can then reference this destination:

```sql
-- pipelines/my_pipeline.sql
WRITE 'my_test_output';
```

After the pipeline runs, you can access the resulting DataFrame from the `IN_MEMORY_DATA_STORE` in your Python test script to make assertions.

```python
from sqlflow.connectors.in_memory import IN_MEMORY_DATA_STORE
import pandas as pd

# After running the SQLFlow pipeline...

# Access the output data
output_df = IN_MEMORY_DATA_STORE.get('output_table_456')

assert output_df is not None
assert len(output_df) > 0
print("Data written to in-memory store successfully.")
```

## Features
- ✅ In-memory data writing
- ✅ Overwrites existing data for the same `table_name`
- ❌ No support for `append`, `merge`, or other advanced write modes. The behavior is always `replace`.

## Limitations
- **Not for Production**: This connector is not thread-safe and is not intended for use in production environments.
- **Global State**: Data is stored in a global variable, which can be affected by other tests or parts of the application if not managed carefully. 