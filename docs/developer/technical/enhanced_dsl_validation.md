# Enhanced SQLFlow DSL Validation: Technical Design (Revised)

This document outlines the technical design for enhanced validation capabilities in SQLFlow's Domain-Specific Language (DSL), focusing on delivering immediate user value through precise error reporting and iterative feature development.

## 1. Overview

### 1.1 Motivation

SQLFlow's current validation capabilities lack precision and user-friendliness that modern developers expect. Users frequently encounter validation errors requiring manual hunting through code. This enhancement delivers database-quality error reporting through a **MVP-first approach** that prioritizes immediate user value over architectural complexity.

**Core Problems Addressed:**
- **Imprecise Error Location**: Users struggle to find validation errors in their code
- **Generic Error Messages**: Current messages lack context about what's wrong and how to fix it
- **Missing Connector Validation**: No validation of connector-specific parameters
- **Poor Developer Experience**: Error resolution takes significantly longer than necessary

### 1.2 Product Strategy & MVP Definition

Based on user feedback analysis and industry best practices, this design follows an **incremental delivery strategy**:

**MVP (Weeks 1-2): Core Value**
- ✅ Precise line/column error reporting for all DSL validation errors
- ✅ Required parameter validation for core connectors (CSV, PostgreSQL, S3)
- ✅ Clear, actionable error messages
- ✅ Cross-reference validation for SOURCE/LOAD relationships
- ✅ Simple CLI integration with smart caching

**Phase 2 (Weeks 3-4): Enhanced Experience**
- Enhanced error formatting with visual indicators
- Complete connector schema coverage
- Optional parameter validation
- Duplicate name detection

**Phase 3 (Week 5+): Intelligence Layer**
- Typo detection and correction
- Context-aware suggestions
- Performance optimizations
- Advanced CLI options (based on user feedback)

### 1.3 Scope

**In Scope (MVP):**
- SOURCE statement validation (required parameters only)
- LOAD statement validation (source reference checking)
- EXPORT statement validation (basic connector type validation)
- Precise error location tracking
- Basic cross-reference validation
- Simple CLI commands with smart caching

**In Scope (Later Phases):**
- Advanced parameter validation (types, ranges, formats)
- Intelligent suggestions and typo correction
- Enhanced error formatting with visual indicators
- Performance optimization for large files
- Advanced CLI options (only if users request them)

**Out of Scope:**
- SQL syntax validation within CREATE TABLE statements
- Semantic SQL validation (table/column existence in SQL)
- Real-time validation during editing

## 2. Architecture Overview

### 2.1 Simplified Validation Architecture

Based on technical review feedback, the architecture follows **Python best practices** with a focus on **simplicity and maintainability**:

```python
# Single-pass validation with functional composition
def validate_pipeline(pipeline_text: str) -> List[ValidationError]:
    """Main validation entry point - functional and stateless"""
    parser = PositionAwareParser(pipeline_text)
    errors = []
    
    try:
        statements = parser.parse_statements()
        # Functional validation - no mutable state
        errors.extend(validate_syntax(statements))
        errors.extend(validate_connectors(statements))
        errors.extend(validate_references(statements))
    except ParseError as e:
        errors.append(ValidationError.from_parse_error(e))
    
    return sorted(errors, key=lambda e: (e.line, e.column))
```

### 2.2 Core Components

#### 2.2.1 Position-Aware Parser Enhancement
Extends existing parser to track character-level positions without architectural changes:

```python
@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    column: int
    char_position: int  # New: absolute character position

class PositionAwareParser(Parser):
    """Enhanced parser with precise position tracking"""
    def _advance(self) -> Token:
        token = super()._advance()
        # Track position for error reporting
        self._position_tracker.update(token)
        return token
```

#### 2.2.2 Python-Native Connector Schemas
Replace JSON schemas with typed Python classes for better maintainability:

```python
@dataclass
class ConnectorSchema:
    """Type-safe connector parameter definition"""
    required_params: Set[str]
    optional_params: Dict[str, type] = field(default_factory=dict)
    param_descriptions: Dict[str, str] = field(default_factory=dict)
    
    def validate_params(self, params: Dict[str, Any]) -> List[ValidationError]:
        """Validate parameters against schema"""
        errors = []
        
        # Check required parameters
        missing = self.required_params - params.keys()
        for param in missing:
            errors.append(ValidationError(
                f"Missing required parameter '{param}'",
                suggestions=[f"Add '{param}': <value> to parameters"]
            ))
        
        return errors

# MVP connector definitions
CONNECTOR_SCHEMAS = {
    'csv': ConnectorSchema(
        required_params={'file'},
        optional_params={'delimiter': str, 'header': bool},
        param_descriptions={
            'file': 'Path to the CSV file',
            'delimiter': 'Field separator character (default: ",")',
            'header': 'Whether first row contains column names (default: true)'
        }
    ),
    'postgresql': ConnectorSchema(
        required_params={'host', 'database'},
        optional_params={'port': int, 'schema': str},
        param_descriptions={
            'host': 'PostgreSQL server hostname or IP',
            'database': 'Database name to connect to',
            'port': 'Port number (default: 5432)',
            'schema': 'Schema name (default: public)'
        }
    ),
    's3': ConnectorSchema(
        required_params={'bucket', 'key'},
        optional_params={'region': str, 'access_key_id': str},
        param_descriptions={
            'bucket': 'S3 bucket name',
            'key': 'Object key (path within bucket)',
            'region': 'AWS region (default: us-east-1)',
            'access_key_id': 'AWS access key for authentication'
        }
    )
}
```

#### 2.2.3 Functional Validation Functions
Each validation type implemented as pure functions:

```python
def validate_connectors(statements: List[Statement]) -> List[ValidationError]:
    """Validate connector parameters - pure function"""
    errors = []
    for stmt in statements:
        if isinstance(stmt, SourceStatement):
            schema = CONNECTOR_SCHEMAS.get(stmt.connector_type)
            if schema:
                errors.extend(schema.validate_params(stmt.params))
            else:
                errors.append(ValidationError(
                    f"Unknown connector type '{stmt.connector_type}'",
                    line=stmt.line,
                    column=stmt.column,
                    suggestions=[f"Supported connectors: {', '.join(CONNECTOR_SCHEMAS.keys())}"]
                ))
    return errors

def validate_references(statements: List[Statement]) -> List[ValidationError]:
    """Validate cross-references between statements"""
    errors = []
    sources = {stmt.name for stmt in statements if isinstance(stmt, SourceStatement)}
    
    for stmt in statements:
        if isinstance(stmt, LoadStatement):
            if stmt.source_name not in sources:
                errors.append(ValidationError(
                    f"Source '{stmt.source_name}' not defined",
                    line=stmt.line,
                    column=stmt.source_column,
                    suggestions=[
                        f"Define source first: SOURCE {stmt.source_name} TYPE ... PARAMS {{...}};",
                        f"Available sources: {', '.join(sources)}" if sources else "No sources defined yet"
                    ]
                ))
    return errors
```

### 2.3 Error Representation

Simple, extensible error model using Python dataclasses:

```python
@dataclass
class ValidationError:
    """Immutable validation error with precise location"""
    message: str
    line: int
    column: int = 0
    error_type: str = "ValidationError"
    suggestions: List[str] = field(default_factory=list)
    help_url: Optional[str] = None
    
    def __str__(self) -> str:
        """Simple, clear error formatting for MVP"""
        result = f"❌ {self.error_type} at line {self.line}"
        if self.column > 0:
            result += f", column {self.column}"
        result += f": {self.message}"
        
        if self.suggestions:
            result += "\n\n💡 Suggestions:"
            for suggestion in self.suggestions:
                result += f"\n  - {suggestion}"
        
        if self.help_url:
            result += f"\n\n📖 Help: {self.help_url}"
            
        return result
    
    @classmethod
    def from_parse_error(cls, parse_error: ParseError) -> 'ValidationError':
        """Convert parser errors to validation errors"""
        return cls(
            message=parse_error.message,
            line=parse_error.line,
            column=parse_error.column,
            error_type="Syntax Error"
        )
```

## 3. MVP Implementation Details

### 3.1 Phase 1: Core Validation (Weeks 1-2)

**Objective**: Deliver immediate value with precise error reporting and basic validation

**Implementation Strategy:**
1. **Enhance existing parser** with position tracking (minimal changes)
2. **Add connector schema validation** for top 3 connectors
3. **Implement cross-reference validation** for SOURCE/LOAD relationships
4. **Create simple error formatting** without complex visual indicators
5. **Simple CLI integration** with smart caching

**Code Changes Required:**

```python
# sqlflow/parser/lexer.py - Add position tracking
class Lexer:
    def __init__(self, text: str):
        # ... existing code ...
        self.char_position = 0  # Add absolute position tracking
    
    def _create_token(self, token_type: TokenType, value: str) -> Token:
        token = Token(
            type=token_type,
            value=value,
            line=self.line,
            column=self.column,
            char_position=self.char_position  # New field
        )
        return token

# sqlflow/validation/ - New module
from .schemas import CONNECTOR_SCHEMAS
from .validators import validate_connectors, validate_references
from .errors import ValidationError

def validate_pipeline(pipeline_text: str) -> List[ValidationError]:
    """Main validation entry point"""
    # Implementation as shown above
```

### 3.2 Success Criteria for MVP

**Technical Metrics:**
- ✅ 100% of validation errors include precise line/column information
- ✅ Validation completes in <100ms for typical pipeline files (<1MB)
- ✅ Zero false positives for connector parameter validation
- ✅ Cross-reference validation catches 100% of undefined source references
- ✅ CLI validation cached for unchanged files (sub-5ms response time)

**User Experience Metrics:**
- ✅ Error resolution time reduced by 50% (baseline measurement required)
- ✅ Users can locate errors without "hunting" through files
- ✅ Clear understanding of what's wrong and how to fix it
- ✅ Smooth CLI workflow without validation delays

### 3.3 Non-Goals for MVP

**Explicitly Not Included:**
- Visual error indicators with arrows and context lines
- Typo detection and correction algorithms
- Advanced parameter validation (types, ranges, formats)
- Performance optimization for large files
- Suggestion intelligence beyond basic recommendations
- Complex CLI flags or options

## 4. Phase 2 & 3 Roadmap

### 4.1 Phase 2: Enhanced Experience (Weeks 3-4)

**Features:**
- Visual error formatting with code context
- Complete connector schema coverage
- Optional parameter validation with type checking
- Improved suggestion quality

**Example Enhanced Error Format:**
```
❌ Invalid Parameter in SOURCE 'user_data' at line 7, column 15

   6 | SOURCE user_data TYPE csv PARAMS {
   7 |   "delimeter": ",",
     |   ^^^^^^^^^^^^
     |   Unknown parameter 'delimeter'
   8 |   "file": "users.csv"

💡 Suggestions:
  - Did you mean 'delimiter' instead of 'delimeter'?
  - Valid CSV parameters: file, delimiter, header, encoding

📖 Help: https://docs.sqlflow.dev/connectors/csv#parameters
```

### 4.2 Phase 3: Intelligence Layer (Week 5+)

**Features:**
- Fuzzy string matching for typo correction
- Context-aware parameter suggestions
- Performance optimization for large files
- Advanced CLI options (only if users request them)

## 5. Implementation Considerations

### 5.1 Performance Strategy

**MVP Performance Approach:**
- Single-pass validation (parse + validate simultaneously)
- Smart caching for unchanged files
- Early termination on first N errors (configurable, default: 10)

**Smart Caching Implementation:**
```python
class ValidationCache:
    """Simple file-based validation cache"""
    
    def __init__(self, cache_dir: str = ".sqlflow_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def get_cached_result(self, file_path: str) -> Optional[List[ValidationError]]:
        """Get cached validation result if file unchanged"""
        cache_file = self._get_cache_file(file_path)
        if not cache_file.exists():
            return None
            
        file_mtime = Path(file_path).stat().st_mtime
        cache_mtime = cache_file.stat().st_mtime
        
        if file_mtime > cache_mtime:
            return None  # File newer than cache
            
        # Load cached result
        with open(cache_file, 'r') as f:
            cached_data = json.load(f)
            return [ValidationError(**error) for error in cached_data]
    
    def store_result(self, file_path: str, errors: List[ValidationError]):
        """Store validation result in cache"""
        cache_file = self._get_cache_file(file_path)
        with open(cache_file, 'w') as f:
            json.dump([asdict(error) for error in errors], f)

# Usage in CLI
def validate_pipeline_with_caching(file_path: str) -> List[ValidationError]:
    cache = ValidationCache()
    
    # Check cache first
    cached_errors = cache.get_cached_result(file_path)
    if cached_errors is not None:
        return cached_errors
    
    # Validate and cache
    with open(file_path, 'r') as f:
        content = f.read()
    
    errors = validate_pipeline(content)
    cache.store_result(file_path, errors)
    return errors
```

**Future Optimizations:**
- Parallel validation for independent statement types
- Incremental validation (only validate changed sections)
- Memory-based caching for repeated validations in same session

### 5.2 Testing Strategy

```python
# Test structure following Python best practices
def test_csv_connector_validation():
    """Test CSV connector parameter validation"""
    pipeline = '''
    SOURCE users TYPE csv PARAMS {
        "file": "users.csv"
    };
    '''
    errors = validate_pipeline(pipeline)
    assert len(errors) == 0

def test_missing_required_parameter():
    """Test error when required parameter is missing"""
    pipeline = '''
    SOURCE users TYPE csv PARAMS {
        "delimiter": ","
    };
    '''
    errors = validate_pipeline(pipeline)
    assert len(errors) == 1
    assert "Missing required parameter 'file'" in errors[0].message
    assert errors[0].line == 2  # Precise location
```

### 5.3 Extensibility Design

**Adding New Connectors:**
```python
# Simple addition to connector registry
CONNECTOR_SCHEMAS['bigquery'] = ConnectorSchema(
    required_params={'project_id', 'dataset'},
    optional_params={'location': str, 'job_timeout': int},
    param_descriptions={
        'project_id': 'Google Cloud project ID',
        'dataset': 'BigQuery dataset name',
        'location': 'Data location (default: US)',
        'job_timeout': 'Query timeout in seconds (default: 300)'
    }
)
```

**Custom Validation Logic:**
```python
# For connectors requiring special validation
class BigQuerySchema(ConnectorSchema):
    def validate_params(self, params: Dict[str, Any]) -> List[ValidationError]:
        errors = super().validate_params(params)
        
        # Custom validation for BigQuery-specific rules
        if 'project_id' in params:
            project_id = params['project_id']
            if not re.match(r'^[a-z][a-z0-9-]*[a-z0-9]$', project_id):
                errors.append(ValidationError(
                    f"Invalid project_id format: '{project_id}'",
                    suggestions=["Project IDs must contain only lowercase letters, numbers, and hyphens"]
                ))
        
        return errors
```

## 6. CLI Integration Strategy

### 6.1 Simple Command Structure (MVP)

Following **YAGNI principles**, the MVP includes only essential commands:

```bash
sqlflow validate pipeline.sf    # Validation only
sqlflow run pipeline.sf         # Validate + execute (with smart caching)
```

**Design Philosophy:**
- **Simple is better than complex** - start with minimal interface
- **Make it work first** - prove validation value before adding complexity
- **Add options only when users request them** - driven by real usage patterns

### 6.2 CLI Implementation

```python
# sqlflow/cli/commands/validate.py
@click.command()
@click.argument('pipeline_file')
def validate(pipeline_file: str):
    """Validate a SQLFlow pipeline without executing it"""
    try:
        errors = validate_pipeline_with_caching(pipeline_file)
        
        if errors:
            for error in errors:
                click.echo(str(error), err=True)
            click.echo(f"\n❌ Found {len(errors)} validation error(s)", err=True)
            sys.exit(1)
        else:
            click.echo("✅ Pipeline validation passed!")
    except Exception as e:
        click.echo(f"❌ Validation failed: {str(e)}", err=True)
        sys.exit(1)

# sqlflow/cli/commands/run.py
@click.command()
@click.argument('pipeline_file')
def run(pipeline_file: str):
    """Run a SQLFlow pipeline (includes validation)"""
    # Always validate before running
    try:
        errors = validate_pipeline_with_caching(pipeline_file)
        
        if errors:
            for error in errors:
                click.echo(str(error), err=True)
            click.echo(f"\n❌ Pipeline validation failed. Fix errors before running.", err=True)
            sys.exit(1)
        
        # Validation passed, proceed with execution
        click.echo("✅ Pipeline validation passed!")
        execute_pipeline_file(pipeline_file)
        
    except Exception as e:
        click.echo(f"❌ Execution failed: {str(e)}", err=True)
        sys.exit(1)
```

### 6.3 User Experience Examples

**Development Workflow:**
```bash
# Quick validation while editing
$ sqlflow validate pipeline.sf
✅ Pipeline validation passed!

# Test run
$ sqlflow run pipeline.sf  
✅ Pipeline validation passed!
🚀 Executing pipeline...
✅ Pipeline completed successfully!

# Cached validation (second run)
$ sqlflow run pipeline.sf
✅ Pipeline validated (cached - 2ms)
🚀 Executing pipeline...
```

**Error Handling:**
```bash
$ sqlflow validate broken_pipeline.sf
❌ ValidationError at line 5, column 15: Missing required parameter 'file'

💡 Suggestions:
  - Add 'file': <value> to parameters

❌ Found 1 validation error(s)

$ sqlflow run broken_pipeline.sf
❌ ValidationError at line 5, column 15: Missing required parameter 'file'

💡 Suggestions:
  - Add 'file': <value> to parameters

❌ Pipeline validation failed. Fix errors before running.
```

### 6.4 Future CLI Enhancements (Phase 2+)

**Add features only based on user feedback:**
- `--skip-validation` flag (if users request emergency override)
- `--strict` mode (treat warnings as errors)
- `--verbose` / `--quiet` output modes
- Configuration file support
- IDE integration hooks

**Philosophy:** Start simple, add complexity only when proven necessary through user feedback and usage patterns.

## 7. Success Metrics & Monitoring

### 7.1 User Experience Metrics

**MVP Success Criteria:**
- **Error Resolution Time**: <2 minutes average (baseline: ~5 minutes)
- **First-Time Success Rate**: >80% of pipelines validate successfully on first attempt
- **User Satisfaction**: >4.0/5.0 rating for error message quality
- **CLI Performance**: Validation perceived as "instant" (<100ms including caching)

**Measurement Strategy:**
- Telemetry in CLI to track validation usage and success rates
- User surveys after validation errors
- Support ticket analysis for validation-related issues
- Performance metrics for caching effectiveness

### 7.2 Technical Metrics

**Performance Targets:**
- **Validation Speed**: <100ms for files under 1MB
- **Cache Hit Rate**: >90% for repeated validations
- **Memory Usage**: <50MB peak memory for typical pipelines
- **Error Accuracy**: <5% false positive rate for validation errors

**Quality Metrics:**
- **Test Coverage**: >95% line coverage for validation code
- **Documentation Coverage**: 100% of public APIs documented
- **Schema Coverage**: 100% of supported connectors have schemas

## 8. Risk Mitigation

### 8.1 Technical Risks

**Risk**: Parser performance degradation with position tracking
**Mitigation**: Benchmark existing parser performance, implement position tracking incrementally

**Risk**: Connector schema maintenance burden
**Mitigation**: Use Python classes for type safety, automated testing for all schemas

**Risk**: False positive validation errors frustrating users
**Mitigation**: Conservative validation rules for MVP, extensive testing with real pipelines

**Risk**: Caching bugs causing stale validation results
**Mitigation**: Simple file-timestamp based caching, clear cache invalidation strategy

### 8.2 Product Risks

**Risk**: MVP feels incomplete compared to design vision
**Mitigation**: Focus on core value (precise errors), communicate roadmap clearly

**Risk**: User expectations set too high by design document
**Mitigation**: Clear communication about MVP scope, regular feedback collection

**Risk**: Users request features we deliberately excluded from MVP
**Mitigation**: Have Phase 2 plan ready, implement based on prioritized user feedback

---

## Conclusion

This revised design prioritizes **immediate user value** through an MVP-first approach while maintaining **architectural simplicity** and **Python best practices**. By focusing on precise error location and basic connector validation with a simple CLI interface, we can deliver meaningful improvements to the developer experience within 2 weeks, then iterate based on user feedback.

The simplified architecture using **functional validation**, **Python-native schemas**, **single-pass processing**, and **simple CLI commands** ensures maintainability while providing a solid foundation for future enhancements. This approach balances user needs, technical feasibility, and product strategy to deliver a successful validation enhancement for SQLFlow. 