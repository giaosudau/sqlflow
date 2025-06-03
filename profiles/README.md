# SQLFlow Profile Best Practices

This README provides guidelines for creating and maintaining SQLFlow profiles across the project.

## Quick Reference

### ✅ Correct Profile Format
```yaml
engines:
  duckdb:
    mode: memory|persistent
    path: "target/db.duckdb"  # Required for persistent mode
    memory_limit: 2GB

log_level: info

variables:
  environment: development
  data_dir: "data"

connectors:
  postgres:
    type: POSTGRES
    params:
      host: localhost
      port: 5432
```

### ❌ Common Mistakes

**Nested Profile Format (Deprecated)**
```yaml
# DON'T DO THIS
profile_name:
  engines:
    duckdb: ...
```

**Missing Path for Persistent Mode**
```yaml
# DON'T DO THIS
engines:
  duckdb:
    mode: persistent
    # Missing path field!
```

**Deprecated Field Names**
```yaml
# DON'T DO THIS
engines:
  duckdb:
    mode: persistent
    database_path: "..."  # Should be 'path'
```

## Profile Validation

SQLFlow automatically validates profiles and provides:
- ✅ **Auto-fix** for deprecated formats with warnings
- ❌ **Clear errors** for invalid configurations
- 📝 **Format guidance** with examples

## Standard Profiles

### Development Profile (`dev.yml`)
```yaml
engines:
  duckdb:
    mode: memory  # Fast, non-persistent
    memory_limit: 2GB
log_level: debug
variables:
  environment: development
```

### Production Profile (`production.yml`)
```yaml
engines:
  duckdb:
    mode: persistent  # Data persistence
    path: "/data/production.db"
    memory_limit: 8GB
log_level: warning
log_file: "/logs/sqlflow.log"
variables:
  environment: production
```

### Testing Profile (`test.yml`)
```yaml
engines:
  duckdb:
    mode: persistent  # Keep test data for debugging
    path: "target/test.db"
    memory_limit: 1GB
log_level: debug
variables:
  environment: test
```

## Environment Variables

Use environment variables for sensitive data:

```yaml
engines:
  duckdb:
    path: "${DB_PATH:-target/default.db}"

variables:
  api_key: "${API_KEY}"

connectors:
  postgres:
    type: POSTGRES
    params:
      password: "${DB_PASSWORD}"
```

## Migration Guide

If you have profiles in the old format, SQLFlow will auto-fix them with warnings. To manually update:

### Old Format → New Format
```yaml
# Old (will be auto-fixed)
profile_name:
  engines:
    duckdb:
      database_path: "target/db.duckdb"

# New (recommended)
engines:
  duckdb:
    mode: persistent
    path: "target/db.duckdb"
```

## Documentation

For complete profile documentation, see:
- [Profile Guide](../docs/user/guides/profiles.md)
- [Configuration Reference](../docs/user/reference/configuration.md)

## Validation Errors

Common validation errors and solutions:

| Error | Solution |
|-------|----------|
| `Missing required 'engines' section` | Add `engines:` section |
| `DuckDB persistent mode requires 'path' field` | Add `path: "target/db.duckdb"` |
| `Invalid DuckDB mode 'xyz'` | Use `mode: memory` or `mode: persistent` |
| `'variables' section must be a dictionary` | Format variables as `key: value` pairs |

## Contributing

When creating example profiles:
1. Use the standard format (no nested wrappers)
2. Use `path` instead of `database_path`
3. Include comments explaining the configuration
4. Test with `sqlflow pipeline list --profile <name>`

---

*This README is automatically updated. For issues, see [SQLFlow Issues](https://github.com/your-org/sqlflow/issues)* 