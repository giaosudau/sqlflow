# In-Memory Source

## Overview
The `InMemorySource` connector reads a pandas DataFrame from a global, in-memory data store. It is designed for internal testing and development, allowing you to easily simulate a data source without any external dependencies.

## Configuration
To use the `InMemorySource`, you need to specify the `type` as `in_memory` in the source section of your profile configuration.

### Required Parameters
- `table_name` (string): The key in the global `IN_MEMORY_DATA_STORE` dictionary that holds the pandas DataFrame you want to read.

### Optional Parameters
This connector has no optional parameters.

## Usage Example
Here is an example of how to configure an `in_memory` source in your `profiles.yml`.

```yaml
# profiles/my_profile.yml
my_profile:
  sources:
    my_test_data:
      type: in_memory
      table_name: 'test_data_123'
  ...
```

In your pipeline, you can then reference this source:

```sql
-- pipelines/my_pipeline.sql
READ 'my_test_data';
```

Before running the pipeline, you would need to ensure the `IN_MEMORY_DATA_STORE` is populated, typically within a Python test script:

```python
from sqlflow.connectors.in_memory import IN_MEMORY_DATA_STORE
import pandas as pd

# Populate the data store
data = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=data)
IN_MEMORY_DATA_STORE['test_data_123'] = df

# Now, running your SQLFlow pipeline will read this DataFrame
```

## Features
- ✅ In-memory data reading
- ❌ No support for incremental loading, filtering, or other advanced features.

## Limitations
- **Not for Production**: This connector is not thread-safe and is not intended for use in production environments.
- **Global State**: Data is stored in a global variable, which can be affected by other tests or parts of the application if not managed carefully. 