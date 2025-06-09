# In-Memory Source Connector

The In-Memory Source connector is a utility for testing and debugging. It allows you to introduce a pandas DataFrame or a list of dictionaries directly into a pipeline programmatically, without reading from an external file or database.

## ✅ Features

- **Testing Utility**: Ideal for unit and integration tests of pipeline logic.
- **Direct Data Injection**: Pass data directly into the `read` method's `options`.
- **DataFrame and Dictionary Support**: Accepts data as a pandas DataFrame or a list of dictionaries.

## 📋 Configuration

This connector is not typically configured via a `SOURCE` block in a pipeline file. Instead, it is instantiated and used directly in Python test code. It does not have any configurable parameters in its `__init__` method.

## 💡 How to Use

You use this connector by passing the data you want to "read" directly into the `options` argument of its `read` method.

| `read()` Options | Type | Description | Required |
|---|---|---|:---:|
| `data` | `pd.DataFrame` or `list[dict]` | The data to be returned by the connector. | ✅ |
| `schema` | `dict` | An optional schema definition. | |

### Example (in a Python test)
```python
import pandas as pd
from sqlflow.connectors.in_memory import InMemorySource

# 1. Prepare your test data
test_data = pd.DataFrame([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
])

# 2. Instantiate the connector
in_memory_source = InMemorySource(config={})

# 3. "Read" the data by passing it in the options
read_options = {"data": test_data}
df = in_memory_source.read(options=read_options)

# df is now a pandas DataFrame identical to test_data
print(df)
```
This connector is used internally by the SQLFlow test suite to simulate data sources without requiring external files or services.

---
**Version**: 1.0 • **Status**: ✅ For Testing Only • **Incremental**: ❌ Not Supported 