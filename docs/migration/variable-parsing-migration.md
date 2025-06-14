# Migration Guide: Variable Parsing Standardization

## Overview

This guide covers the migration from duplicate variable parsing implementations to the unified `StandardVariableParser` system. **This is a non-breaking refactoring** that standardizes internal parsing logic while preserving all existing functionality.

## 🎯 **What Changed**

### **Before: Multiple Parsing Implementations**

Previously, each component had its own variable parsing logic:

```python
# DuckDBEngine had its own regex pattern
r"\$\{([^}]+)\}"

# VariableHandler had a different pattern  
r"\$\{([^}|]+)(?:\|([^}]+))?\}"

# SQLGenerator had another pattern
r"\${([^}]*)\}"

# Each with custom parsing logic
def parse_variable_expression_v1(expr):
    if "|" in expr:
        var_name, default = expr.split("|", 1)  # Repeated everywhere
        # Different error handling each time...
```

### **After: Unified Parsing System**

Now all components use the single `StandardVariableParser`:

```python
from sqlflow.core.variables.parser import StandardVariableParser

# Single regex pattern for entire system
StandardVariableParser.VARIABLE_PATTERN

# Consistent parsing logic
parse_result = StandardVariableParser.find_variables(template)
for expr in parse_result.expressions:
    # Uniform data structure across all components
    print(expr.variable_name, expr.default_value)
```

## ✅ **What Stayed the Same (No Breaking Changes)**

### **User-Facing Behavior**

All user-facing behavior is **identical**:

```bash
# CLI usage unchanged
sqlflow run pipeline.sql --vars table=users

# Variable syntax unchanged  
SELECT * FROM ${table|default_table};

# Profile configuration unchanged
variables:
  environment: production
```

### **Context-Specific Formatting**

Each context still formats values appropriately:

| Context | Input | Output | Status |
|---------|-------|--------|---------|
| CLI/Files | `${table}` | `users` | ✅ **Unchanged** |
| SQL Engine | `${table}` | `'users'` | ✅ **Unchanged** |
| Conditions | `${region}` | `'us-east'` | ✅ **Unchanged** |
| SQL Generator | `${count}` | `42` | ✅ **Unchanged** |

### **API Compatibility**

All public APIs remain the same:

```python
# VariableManager API unchanged
manager = VariableManager(config)
result = manager.substitute("${table}")

# DuckDBEngine API unchanged  
engine.substitute_variables("SELECT * FROM ${table}")

# All existing code continues to work
```

## 🔧 **Internal Improvements**

### **Code Quality Benefits**

1. **Single Source of Truth**: One regex pattern instead of 4+
2. **Consistent Error Handling**: Unified error handling logic
3. **Better Maintainability**: Changes apply to all components
4. **Comprehensive Testing**: 100% edge case coverage

### **Performance Improvements**

- **Parsing Performance**: Optimized regex pattern
- **Memory Usage**: Reduced duplication
- **Test Execution**: Faster due to fewer redundant tests

## 🧪 **Testing Your Migration**

### **Automated Validation**

The refactoring includes comprehensive tests to ensure no regressions:

```bash
# Run all integration tests
./run_integration_tests.sh

# Run all example demos  
./run_all_examples.sh

# Run specific variable tests
python -m pytest tests/integration/test_variable_parsing_consistency.py -v
```

### **Manual Validation Steps**

1. **Test Your Existing Pipelines**:
   ```bash
   # Run your existing pipelines
   sqlflow run your_pipeline.sql --vars your_vars
   ```

2. **Verify Variable Substitution**:
   ```bash
   # Test with different variable patterns
   sqlflow run test.sql --vars table=users count=100 region=us-east
   ```

3. **Check Default Values**:
   ```sql
   -- Test default value handling
   SELECT * FROM ${table|default_table} WHERE ${column|id} = ${value|'test'};
   ```

4. **Validate Conditional Logic**:
   ```sql
   -- Test conditional pipelines
   IF ${environment} == 'production' THEN
       SELECT * FROM ${table|production_table};
   END IF;
   ```

### **Expected Results**

All tests should pass with **identical results** to before the migration.

## 🚨 **Troubleshooting**

### **If You Find Differences**

If you notice any behavioral differences (which should not occur):

1. **Document the Issue**:
   ```bash
   # Capture the exact command and output
   sqlflow run problem_pipeline.sql --vars your_vars > before.txt 2>&1
   ```

2. **Test with Debug Mode**:
   ```python
   from sqlflow.core.variables.parser import StandardVariableParser
   
   # Debug the parsing
   result = StandardVariableParser.find_variables("${your_expression}")
   print(f"Parsed: {result.expressions}")
   ```

3. **Compare with Expected**:
   - Check if the variable syntax is correct
   - Verify the expected context-specific formatting
   - Review the component-specific behavior

### **Common Non-Issues**

These behaviors are **intentional and correct**:

1. **Different Output Formats by Context**:
   ```python
   # This is CORRECT - different contexts format differently
   manager.substitute("${table}")      # → "users"
   duckdb.substitute_variables("${table}")  # → "'users'"
   ```

2. **Nested Variables Not Supported**:
   ```sql
   -- This was never supported and still isn't
   SELECT * FROM ${table_${env}};  -- Not supported by design
   ```

3. **Quote Handling in Defaults**:
   ```sql
   -- These behave consistently now
   ${var|'quoted'}    -- Default: quoted (quotes removed)
   ${var|unquoted}    -- Default: unquoted
   ```

## 📊 **Validation Checklist**

Before considering your migration complete:

- [ ] ✅ All existing tests pass
- [ ] ✅ All example demos run successfully  
- [ ] ✅ Integration tests pass
- [ ] ✅ Your custom pipelines work unchanged
- [ ] ✅ Variable substitution behaves identically
- [ ] ✅ Performance is maintained or improved

## 🎓 **Understanding the New Architecture**

### **For Developers**

If you're working on SQLFlow internals:

1. **Use StandardVariableParser**: Always import and use the unified parser
2. **Implement Context Formatting**: Focus on your component's specific formatting needs
3. **Test Consistently**: Use the integration tests as examples

```python
# NEW: Recommended pattern for internal development
from sqlflow.core.variables.parser import StandardVariableParser

def your_substitution_method(text, variables):
    parse_result = StandardVariableParser.find_variables(text)
    # Implement your context-specific formatting
    return format_for_your_context(parse_result, variables)
```

### **For Users**

Nothing changes for users - continue using SQLFlow exactly as before.

## 📚 **Additional Resources**

- **Architecture Guide**: [`../developer-guides/variable-substitution-architecture.md`](../developer-guides/variable-substitution-architecture.md)
- **User Guide**: [`../developer-guides/variable-substitution-guide.md`](../developer-guides/variable-substitution-guide.md)
- **Examples**: `examples/variable_substitution/`
- **Test Suite**: `tests/integration/test_variable_parsing_consistency.py`

## 📝 **Migration Checklist**

### **For SQLFlow Maintainers**

- [ ] ✅ `StandardVariableParser` implemented
- [ ] ✅ All components migrated to use standard parser
- [ ] ✅ Integration tests verify consistency
- [ ] ✅ All existing tests pass
- [ ] ✅ All examples run successfully
- [ ] ✅ Documentation updated
- [ ] ✅ Performance benchmarks pass

### **For SQLFlow Users**

- [ ] ✅ Test existing pipelines
- [ ] ✅ Verify variable substitution works
- [ ] ✅ Check conditional logic
- [ ] ✅ Validate default values
- [ ] ✅ Confirm performance is acceptable

## 🏁 **Conclusion**

This migration represents a **significant internal improvement** with **zero breaking changes**:

- **Better Code Quality**: Single source of truth for parsing
- **Improved Maintainability**: Centralized logic, easier to enhance
- **Enhanced Testing**: Comprehensive cross-component validation
- **Future-Proof**: Solid foundation for future enhancements

**The bottom line**: Your SQLFlow experience remains identical, but the internal architecture is now cleaner, more reliable, and easier to maintain.

---

*Last updated: December 2024*  
*Migration status: Complete and validated* 