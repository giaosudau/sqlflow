# Variable Substitution Architecture

## Overview

SQLFlow uses a centralized variable substitution system to ensure consistent behavior across validation, compilation, and execution phases.

## Architecture

### Current State
We have **two variable substitution systems** that need to be unified:

1. **Legacy System** (`sqlflow/core/variables.py`):
   - `VariableContext` + `VariableSubstitutor`
   - Priority-based variable resolution
   - No environment variable support
   - Used by: planner, some CLI components

2. **Modern System** (`sqlflow/core/variable_substitution.py`):
   - `VariableSubstitutionEngine`
   - Environment variable support
   - Simplified interface
   - Used by: validation, compilation, execution

### Target Architecture

**Single Source of Truth**: `VariableSubstitutionEngine` should be the only variable substitution system.

```
┌─────────────────────────────────────────────────────────────┐
│                Variable Substitution Engine                 │
├─────────────────────────────────────────────────────────────┤
│  Priority Order:                                           │
│  1. Explicit variables (CLI/profile)                       │
│  2. Environment variables                                   │
│  3. Default values (${var|default})                        │
├─────────────────────────────────────────────────────────────┤
│  Features:                                                  │
│  ✅ Environment variable support                           │
│  ✅ Default value handling                                 │
│  ✅ Context-aware formatting                               │
│  ✅ Missing variable validation                            │
└─────────────────────────────────────────────────────────────┘
```

### Integration Points

1. **Validation** (`sqlflow/cli/validation_helpers.py`) ✅ Fixed
2. **Compilation** (`sqlflow/cli/pipeline.py`) ✅ Using modern system
3. **Execution** (`sqlflow/core/executors/`) ✅ Using modern system
4. **Planning** (`sqlflow/core/planner.py`) ⚠️ **Needs migration**

## Migration Plan

### Phase 1: Extend VariableSubstitutionEngine ✅ COMPLETE
- Add priority-based variable resolution
- Maintain backward compatibility
- Add comprehensive validation

### Phase 2: Migrate Planner (TODO)
- Update `Planner` to use `VariableSubstitutionEngine`
- Maintain priority order: CLI > Profile > SET variables > Environment > Defaults
- Remove dependency on legacy system

### Phase 3: Deprecate Legacy System (TODO)
- Mark `VariableContext` and `VariableSubstitutor` as deprecated
- Add migration warnings
- Update all remaining references

## Implementation Guidelines

### DO ✅
```python
# Use the modern system
from sqlflow.core.variable_substitution import VariableSubstitutionEngine

# With priority support
all_variables = {}
all_variables.update(profile_variables)  # Lower priority
all_variables.update(cli_variables)      # Higher priority

engine = VariableSubstitutionEngine(all_variables)
result = engine.substitute(data)
```

### DON'T ❌
```python
# Don't use the legacy system
from sqlflow.core.variables import VariableContext, VariableSubstitutor

# This will be deprecated
context = VariableContext(...)
substitutor = VariableSubstitutor(context)
```

## Benefits of Unified System

1. **Consistency**: Same behavior across all components
2. **Environment Variables**: Automatic support everywhere
3. **Maintainability**: Single place to fix bugs and add features
4. **Testing**: Easier to test and validate behavior
5. **Documentation**: Clear, unified patterns for developers

## Validation Strategy

- All variable substitution must go through `VariableSubstitutionEngine`
- Add linting rules to detect legacy system usage
- Comprehensive test coverage for all scenarios
- Architecture tests to prevent regression

## Current Status

- ✅ **Validation**: Using modern system
- ✅ **Compilation**: Using modern system  
- ✅ **Execution**: Using modern system
- ⚠️ **Planning**: Still using legacy system (needs migration)
- ✅ **Testing**: Comprehensive coverage added

## Next Steps

1. Extend `VariableSubstitutionEngine` with priority support
2. Migrate `Planner` to use modern system
3. Add deprecation warnings to legacy system
4. Update documentation and examples
5. Add architecture tests 