# SQLFlow Tutorials

## Variable Substitution

When using variables with default values, **default values containing spaces must be quoted**:

- ✅ `${region|"us east"}` (valid)
- ❌ `${region|us east}` (**invalid**, will cause a validation error)

Unquoted default values with spaces are not allowed and will cause a pipeline validation error.

# ... existing content ... 