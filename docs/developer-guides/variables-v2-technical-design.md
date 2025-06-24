# SQLFlow Variables Module V2 - Technical Design & Implementation Plan

## Executive Summary

This document outlines the technical design and implementation plan for SQLFlow Variables Module V2, a complete rewrite following Zen of Python principles. The current V1 implementation suffers from over-engineering, complex class hierarchies, and multiple parsing systems. V2 will provide a simple, functional, and performant solution while maintaining all business logic and backward compatibility.

**🎉 Phase 1 COMPLETED (December 2024)**:
- ✅ Core V2 functional module implemented in `sqlflow/core/variables/v2/`
- ✅ All V1 syntax patterns supported (`${var}`, `${var|default}`, `${var|"quoted"}`)
- ✅ Priority resolution system (CLI > Profile > SET > ENV)
- ✅ Context-specific formatting (SQL, Text, AST, JSON)
- ✅ Simple validation and error handling
- ✅ Comprehensive test suite with 22 passing tests
- ✅ All pre-commit checks passing
- ✅ Clean, well-organized V2 folder structure

## Current State Analysis

### V1 Implementation Issues (Expert Panel Findings)

1. **Over-Engineering**: 7 files with complex class hierarchies for simple string operations
2. **Multiple Parsers**: `parser.py` and `unified_parser.py` violate "one obvious way"
3. **Complex Error Handling**: Over-engineered error system with strategies and reports
4. **Performance Overhead**: Object-oriented approach adds unnecessary complexity
5. **Inconsistent APIs**: V1 and V2 have different interfaces
6. **Violation of SOLID Principles**: Single Responsibility, Dependency Inversion issues

### V2 Implementation Benefits

1. **Zen of Python Compliance**: Simple, explicit, one obvious way
2. **Functional Programming**: Pure functions, immutable transformations
3. **Performance**: Reduced overhead, optimized algorithms
4. **Maintainability**: Clear, focused functions
5. **Backward Compatibility**: Gradual migration path
6. **Type Safety**: Comprehensive type hints

## Phase 1: Core Functional Module (Week 1-2)

### Objective
Implement the core V2 functional module with all essential business logic from V1.

### Tasks

#### Task 1.1: Core Substitution Functions
**Description**: Implement pure functions for variable substitution with support for all V1 syntax patterns.

**Technical Details**:
- Support `${variable}`, `${variable|default}`, `${variable|"quoted default"}` syntax
- Handle `$variable` (simple dollar) syntax
- Implement context-aware formatting (SQL, Text, AST, JSON)
- Optimize for performance with minimal regex usage

**Implementation**:
```python
# sqlflow/core/variables/v2.py
from typing import Any, Dict, Optional, Union
import re
import os

def substitute_variables(text: str, variables: Dict[str, Any]) -> str:
    """Pure function for variable substitution with all V1 syntax support."""
    if not text or not variables:
        return text
    
    # Unified regex pattern for all syntax forms
    pattern = r'\$\{([^}|]+)(?:\|([^}]+))?\}'
    
    def replace(match):
        var_name = match.group(1).strip()
        default = match.group(2).strip() if match.group(2) else None
        
        if var_name in variables:
            return str(variables[var_name])
        elif default is not None:
            return _clean_default_value(default)
        else:
            return match.group(0)  # Keep original if no replacement
    
    return re.sub(pattern, replace, text)

def substitute_simple_dollar(text: str, variables: Dict[str, Any]) -> str:
    """Handle simple $variable syntax with word boundary checking."""
    # Implementation with boundary checking to avoid partial matches
    pass
```

**Definition of Done (DOD)**:
- [x] All V1 syntax patterns supported and tested
- [ ] Performance benchmarks show 50%+ improvement over V1
- [x] Unit tests cover all edge cases
- [x] Type hints are comprehensive and accurate
- [x] Code review completed and approved

#### Task 1.2: Priority Resolution System
**Description**: Implement the V1 priority system (CLI > Profile > SET > ENV) as pure functions.

**Technical Details**:
- Preserve exact V1 priority order
- Support for all variable sources
- Efficient merging with conflict resolution
- Clear logging and debugging

**Implementation**:
```python
def resolve_variables(
    cli_vars: Optional[Dict[str, Any]] = None,
    profile_vars: Optional[Dict[str, Any]] = None,
    set_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Resolve variables with V1 priority order: CLI > Profile > SET > ENV."""
    result = {}
    
    # Apply in priority order (lowest to highest)
    result.update(env_vars or {})
    result.update(set_vars or {})
    result.update(profile_vars or {})
    result.update(cli_vars or {})
    
    return result

def resolve_from_environment() -> Dict[str, Any]:
    """Get all environment variables as a dictionary."""
    return dict(os.environ)
```

**Definition of Done (DOD)**:
- [x] Priority order exactly matches V1 behavior
- [x] All variable sources supported
- [x] Conflict resolution tested and documented
- [x] Performance optimized for large variable sets
- [x] Integration tests with real CLI/profile scenarios

#### Task 1.3: Context-Specific Formatting
**Description**: Implement context-aware formatting for SQL, Text, AST, and JSON contexts.

**Technical Details**:
- SQL context: Proper quoting, NULL handling, SQL injection prevention
- Text context: Simple string conversion
- AST context: Python literal formatting
- JSON context: JSON serialization

**Implementation**:
```python
def format_for_context(value: Any, context: str = "text") -> str:
    """Format value for specific context with V1 compatibility."""
    if value is None:
        return "NULL" if context == "sql" else ""
    
    if context == "sql":
        if isinstance(value, str):
            return f"'{value.replace("'", "''")}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return f"'{str(value).replace("'", "''")}'"
    
    elif context == "json":
        import json
        return json.dumps(value)
    
    elif context == "ast":
        return repr(value)
    
    else:  # text context
        return str(value)
```

**Definition of Done (DOD)**:
- [x] All V1 context formatters replicated
- [x] SQL injection prevention tested
- [x] Performance benchmarks for each context
- [x] Edge cases handled (None, special characters, etc.)
- [x] Documentation with examples

#### Task 1.4: Validation and Error Handling
**Description**: Implement simple, effective validation and error handling.

**Technical Details**:
- Simple validation returning missing variables
- Clear error messages with context
- Performance-optimized validation
- No complex error strategies

**Implementation**:
```python
def validate_variables(text: str, variables: Dict[str, Any]) -> list[str]:
    """Validate variables and return missing ones."""
    pattern = r'\$\{([^}|]+)(?:\|([^}]+))?\}'
    missing = []
    
    for match in re.finditer(pattern, text):
        var_name = match.group(1).strip()
        default = match.group(2)
        
        if var_name not in variables and default is None:
            missing.append(var_name)
    
    return missing

class VariableError(Exception):
    """Simple variable error with context."""
    def __init__(self, message: str, variable_name: str = None, context: str = None):
        super().__init__(message)
        self.variable_name = variable_name
        self.context = context
```

**Definition of Done (DOD)**:
- [x] Validation covers all V1 scenarios
- [x] Error messages are clear and actionable
- [x] Performance optimized for large templates
- [x] Integration with logging system
- [x] Error handling tested with edge cases

### Phase 1 Milestones

**Week 1**:
- [x] Core substitution functions implemented
- [x] Priority resolution system working
- [x] Basic tests passing

**Week 2**:
- [x] Context formatting complete
- [x] Validation and error handling done
- [ ] Performance benchmarks completed
- [x] Phase 1 review and approval

## Phase 2: Backward Compatibility Layer (Week 3)

### Objective
Create a backward compatibility layer that allows existing V1 code to work with V2 functions.

### Tasks

#### Task 2.1: VariableManager Compatibility Wrapper
**Description**: Create a VariableManager class that wraps V2 functions for backward compatibility.

**Technical Details**:
- Implement VariableManager interface using V2 functions
- Preserve all V1 public methods and behavior
- Maintain performance characteristics
- Clear deprecation warnings

**Implementation**:
```python
# sqlflow/core/variables/__init__.py
from .v2 import substitute_variables, resolve_variables, validate_variables, format_for_context
import warnings

class VariableManager:
    """Backward compatibility wrapper for V2 functions."""
    
    def __init__(self, config=None):
        warnings.warn(
            "VariableManager is deprecated. Use V2 functions directly.",
            DeprecationWarning,
            stacklevel=2
        )
        
        if config:
            self.variables = resolve_variables(
                cli_vars=getattr(config, 'cli_variables', {}),
                profile_vars=getattr(config, 'profile_variables', {}),
                set_vars=getattr(config, 'set_variables', {}),
                env_vars=getattr(config, 'env_variables', {})
            )
        else:
            self.variables = resolve_from_environment()
    
    def substitute(self, data: Any) -> Any:
        """Substitute variables using V2 functions."""
        if isinstance(data, str):
            return substitute_variables(data, self.variables)
        elif isinstance(data, dict):
            return substitute_in_dict(data, self.variables)
        elif isinstance(data, list):
            return substitute_in_list(data, self.variables)
        else:
            return data
    
    def validate(self, content: str) -> ValidationResult:
        """Validate variables using V2 functions."""
        missing = validate_variables(content, self.variables)
        return ValidationResult(
            is_valid=len(missing) == 0,
            missing_variables=missing
        )
```

**Definition of Done (DOD)**:
- [ ] All V1 VariableManager methods implemented
- [ ] Behavior exactly matches V1 implementation
- [ ] Deprecation warnings added
- [ ] All existing tests pass unchanged
- [ ] Performance impact measured and documented

#### Task 2.2: Import Compatibility
**Description**: Ensure all existing imports continue to work.

**Technical Details**:
- Maintain existing import paths
- Redirect imports to V2 functions where appropriate
- Preserve module structure
- Clear migration documentation

**Implementation**:
```python
# sqlflow/core/variables/__init__.py
# Backward compatibility imports
from .v2 import (
    substitute_variables,
    resolve_variables,
    validate_variables,
    format_for_context,
    substitute_in_dict,
    substitute_in_list
)

# Legacy classes for backward compatibility
from .compatibility import VariableManager, VariableConfig, ValidationResult

__all__ = [
    # V2 functions
    "substitute_variables",
    "resolve_variables", 
    "validate_variables",
    "format_for_context",
    "substitute_in_dict",
    "substitute_in_list",
    # Legacy classes
    "VariableManager",
    "VariableConfig", 
    "ValidationResult"
]
```

**Definition of Done (DOD)**:
- [ ] All existing imports work without changes
- [ ] No breaking changes introduced
- [ ] Import performance optimized
- [ ] Migration guide created
- [ ] Integration tests pass

#### Task 2.3: Configuration Compatibility
**Description**: Ensure VariableConfig and related classes work with V2.

**Technical Details**:
- Preserve VariableConfig interface
- Maintain priority resolution logic
- Support all V1 configuration options
- Clear migration path

**Implementation**:
```python
@dataclass
class VariableConfig:
    """Backward compatibility configuration class."""
    cli_variables: Dict[str, Any] = field(default_factory=dict)
    profile_variables: Dict[str, Any] = field(default_factory=dict)
    set_variables: Dict[str, Any] = field(default_factory=dict)
    env_variables: Dict[str, Any] = field(default_factory=dict)
    
    def resolve_priority(self) -> Dict[str, Any]:
        """Resolve variables using V2 function."""
        return resolve_variables(
            cli_vars=self.cli_variables,
            profile_vars=self.profile_variables,
            set_vars=self.set_variables,
            env_vars=self.env_variables
        )
```

**Definition of Done (DOD)**:
- [ ] VariableConfig works with V2 functions
- [ ] Priority resolution matches V1 exactly
- [ ] All configuration options supported
- [ ] Performance benchmarks show no regression
- [ ] Configuration tests pass

### Phase 2 Milestones

**Week 3**:
- [ ] Backward compatibility layer complete
- [ ] All existing code works unchanged
- [ ] Integration tests passing
- [ ] Performance validation complete

## Phase 3: Module Migration (Week 4-5)

### Objective
Migrate all SQLFlow modules to use V2 functions directly.

### Tasks

#### Task 3.1: Core Module Migration
**Description**: Update core modules to use V2 functions directly.

**Technical Details**:
- Update `sqlflow/core/planner_main.py`
- Update `sqlflow/core/executors/`
- Update `sqlflow/cli/` modules
- Remove dependency on V1 classes

**Implementation Plan**:
```python
# Before (V1)
from sqlflow.core.variables.manager import VariableManager, VariableConfig
config = VariableConfig(cli_variables=variables)
manager = VariableManager(config)
result = manager.substitute(text)

# After (V2)
from sqlflow.core.variables.v2 import substitute_variables, resolve_variables
variables = resolve_variables(cli_vars=variables)
result = substitute_variables(text, variables)
```

**Definition of Done (DOD)**:
- [ ] All core modules migrated to V2
- [ ] No V1 class dependencies remaining
- [ ] Performance improvements measured
- [ ] All tests passing
- [ ] Code review completed

#### Task 3.2: Executor Migration
**Description**: Update V2 executors to use V2 variable functions.

**Technical Details**:
- Update `sqlflow/core/executors/v2/`
- Ensure consistency with V2 design principles
- Optimize for performance
- Maintain functionality

**Implementation**:
```python
# sqlflow/core/executors/v2/steps/transform.py
from sqlflow.core.variables.v2 import substitute_variables, format_for_context

def execute_transform_step(step_config: Dict[str, Any], variables: Dict[str, Any]) -> Dict[str, Any]:
    """Execute transform step with V2 variable substitution."""
    # Substitute variables in step configuration
    resolved_config = substitute_in_dict(step_config, variables)
    
    # Format values for SQL context
    sql_query = resolved_config.get('query', '')
    formatted_query = substitute_variables(sql_query, variables)
    
    return {
        'type': 'transform',
        'query': formatted_query,
        'variables_used': list(variables.keys())
    }
```

**Definition of Done (DOD)**:
- [ ] All V2 executors use V2 variable functions
- [ ] Performance benchmarks show improvement
- [ ] Functionality preserved
- [ ] Tests updated and passing
- [ ] Documentation updated

#### Task 3.3: CLI Migration
**Description**: Update CLI modules to use V2 functions.

**Technical Details**:
- Update `sqlflow/cli/` modules
- Maintain CLI interface compatibility
- Improve error messages
- Optimize performance

**Implementation**:
```python
# sqlflow/cli/pipeline.py
from sqlflow.core.variables.v2 import substitute_variables, resolve_variables, validate_variables

def _compile_pipeline_to_plan(pipeline_path: str, target_path: str, variables: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Compile pipeline with V2 variable handling."""
    # Resolve all variables with priority
    all_variables = resolve_variables(
        cli_vars=variables or {},
        profile_vars=profile_variables or {},
        env_vars=dict(os.environ)
    )
    
    # Validate variables before processing
    missing = validate_variables(pipeline_text, all_variables)
    if missing:
        logger.warning(f"Missing variables: {missing}")
    
    # Substitute variables in pipeline
    resolved_pipeline = substitute_variables(pipeline_text, all_variables)
    
    # Continue with pipeline compilation...
```

**Definition of Done (DOD)**:
- [ ] All CLI commands work with V2
- [ ] Error messages improved
- [ ] Performance optimized
- [ ] User experience maintained
- [ ] CLI tests updated

#### Task 3.4: Test Migration
**Description**: Update all tests to use V2 functions.

**Technical Details**:
- Update unit tests
- Update integration tests
- Update performance tests
- Maintain test coverage

**Implementation**:
```python
# tests/unit/test_variables_v2.py
import pytest
from sqlflow.core.variables.v2 import substitute_variables, resolve_variables, validate_variables

def test_basic_substitution():
    """Test basic variable substitution."""
    result = substitute_variables("Hello ${name}", {"name": "World"})
    assert result == "Hello World"

def test_priority_resolution():
    """Test variable priority resolution."""
    variables = resolve_variables(
        cli_vars={"env": "prod"},
        profile_vars={"env": "dev"},
        env_vars={"env": "test"}
    )
    assert variables["env"] == "prod"  # CLI wins

def test_validation():
    """Test variable validation."""
    missing = validate_variables("Hello ${name} ${age}", {"name": "Alice"})
    assert missing == ["age"]
```

**Definition of Done (DOD)**:
- [ ] All tests migrated to V2
- [ ] Test coverage maintained or improved
- [ ] Performance tests updated
- [ ] Integration tests passing
- [ ] Test documentation updated

### Phase 3 Milestones

**Week 4**:
- [ ] Core modules migrated
- [ ] Executors updated
- [ ] Tests migrated

**Week 5**:
- [ ] CLI migration complete
- [ ] All modules using V2
- [ ] Performance validation
- [ ] Phase 3 review

## Phase 4: Cleanup and Optimization (Week 6)

### Objective
Remove V1 implementation, optimize performance, and finalize the migration.

### Tasks

#### Task 4.1: V1 Implementation Removal
**Description**: Remove all V1 implementation files and classes.

**Technical Details**:
- Remove V1 files safely
- Update imports and dependencies
- Clean up unused code
- Verify no regressions

**Files to Remove**:
- `sqlflow/core/variables/manager.py` (after compatibility layer)
- `sqlflow/core/variables/parser.py`
- `sqlflow/core/variables/unified_parser.py`
- `sqlflow/core/variables/substitution_engine.py`
- `sqlflow/core/variables/formatters.py`
- `sqlflow/core/variables/error_handling.py`
- `sqlflow/core/variables/validator.py`

**Definition of Done (DOD)**:
- [ ] All V1 files removed
- [ ] No broken imports
- [ ] All tests passing
- [ ] Performance improved
- [ ] Codebase cleaned up

#### Task 4.2: Performance Optimization
**Description**: Optimize V2 functions for maximum performance.

**Technical Details**:
- Profile and optimize hot paths
- Implement caching where beneficial
- Optimize regex patterns
- Memory usage optimization

**Implementation**:
```python
# Performance optimizations
import re
from functools import lru_cache

# Compile regex patterns once
_VARIABLE_PATTERN = re.compile(r'\$\{([^}|]+)(?:\|([^}]+))?\}')

@lru_cache(maxsize=1000)
def _clean_default_value(default: str) -> str:
    """Clean default value with caching for performance."""
    # Remove quotes if present
    if (default.startswith('"') and default.endswith('"')) or \
       (default.startswith("'") and default.endswith("'")):
        return default[1:-1]
    return default
```

**Definition of Done (DOD)**:
- [ ] Performance benchmarks show 50%+ improvement
- [ ] Memory usage optimized
- [ ] Caching implemented where beneficial
- [ ] Performance tests added
- [ ] Optimization documented

#### Task 4.3: Documentation Update
**Description**: Update all documentation to reflect V2 implementation.

**Technical Details**:
- Update API documentation
- Update user guides
- Update developer guides
- Create migration guide

**Documentation to Update**:
- `docs/reference/variables.md`
- `docs/user-guides/variable-substitution.md`
- `docs/developer-guides/architecture-overview.md`
- API docstrings
- README files

**Definition of Done (DOD)**:
- [ ] All documentation updated
- [ ] Migration guide created
- [ ] API documentation complete
- [ ] Examples updated
- [ ] Documentation reviewed

#### Task 4.4: Final Validation
**Description**: Comprehensive validation of the complete migration.

**Technical Details**:
- End-to-end testing
- Performance validation
- Security review
- Compatibility verification

**Validation Checklist**:
- [ ] All existing functionality preserved
- [ ] Performance targets met
- [ ] Security requirements satisfied
- [ ] Backward compatibility maintained
- [ ] Code quality standards met

**Definition of Done (DOD)**:
- [ ] End-to-end tests passing
- [ ] Performance benchmarks completed
- [ ] Security review passed
- [ ] Final code review approved
- [ ] Release notes prepared

### Phase 4 Milestones

**Week 6**:
- [ ] V1 implementation removed
- [ ] Performance optimization complete
- [ ] Documentation updated
- [ ] Final validation passed
- [ ] Release ready

## Technical Architecture

### V2 Module Structure
```
sqlflow/core/variables/
├── __init__.py          # Main interface and backward compatibility
├── v2.py               # Core V2 functional implementation
├── compatibility.py    # Backward compatibility layer
└── types.py           # Type definitions and dataclasses
```

### Core Functions Interface
```python
# Main V2 interface
def substitute_variables(text: str, variables: Dict[str, Any]) -> str
def resolve_variables(cli_vars=None, profile_vars=None, set_vars=None, env_vars=None) -> Dict[str, Any]
def validate_variables(text: str, variables: Dict[str, Any]) -> list[str]
def format_for_context(value: Any, context: str = "text") -> str
def substitute_in_dict(data: Dict[str, Any], variables: Dict[str, Any]) -> Dict[str, Any]
def substitute_in_list(data: List[Any], variables: Dict[str, Any]) -> List[Any]
```

### Performance Targets
- **Substitution Speed**: 50%+ faster than V1
- **Memory Usage**: 30%+ reduction
- **Startup Time**: 20%+ faster
- **Large Template Handling**: 2x improvement

## Risk Mitigation

### Technical Risks
1. **Breaking Changes**: Mitigated by comprehensive backward compatibility layer
2. **Performance Regression**: Mitigated by continuous benchmarking
3. **Functionality Loss**: Mitigated by extensive testing and validation
4. **Migration Complexity**: Mitigated by gradual migration approach

### Mitigation Strategies
1. **Feature Flags**: Enable gradual rollout
2. **Comprehensive Testing**: Unit, integration, and performance tests
3. **Rollback Plan**: Ability to revert to V1 if needed
4. **Documentation**: Clear migration guides and examples

## Success Metrics

### Technical Metrics
- [ ] 50%+ performance improvement
- [ ] 30%+ reduction in code complexity
- [ ] 100% test coverage maintained
- [ ] Zero breaking changes
- [ ] All existing functionality preserved

### Quality Metrics
- [ ] Code review approval
- [ ] Performance benchmarks passed
- [ ] Security review completed
- [ ] Documentation quality score > 90%
- [ ] User satisfaction maintained

## Conclusion

The SQLFlow Variables Module V2 implementation will provide a simple, performant, and maintainable solution that follows Zen of Python principles while preserving all existing functionality. The phased approach ensures a smooth migration with minimal risk and maximum benefit.

The V2 implementation will serve as a foundation for future SQLFlow development, providing a clean and efficient variable substitution system that developers can easily understand, use, and extend. 