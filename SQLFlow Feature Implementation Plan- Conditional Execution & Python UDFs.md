# SQLFlow Implementation Plan: Conditional Execution & Python UDFs

## Epic 1: Conditional Execution (IF/ELSE)

**Goal:** Implement SQL-native conditional block execution where branches are evaluated and selected at planning time based on variable context.

### Task 1.1: Lexer & AST Updates for Conditional Syntax

**Description:** Add token types and AST node structures for IF/ELSE conditional blocks.

**Status:** ✅ COMPLETED

**Files Impacted:** 
- `sqlflow/parser/lexer.py`
- `sqlflow/parser/ast.py`

**Subtasks:**
1.1.1: Add new token types to `TokenType` enum:
```python
IF = auto()
THEN = auto()
ELSE_IF = auto()  # Matches "ELSEIF" or "ELSE IF"
ELSE = auto()
END_IF = auto()   # Matches "ENDIF" or "END IF"
```

1.1.2: Add regex patterns to `Lexer.patterns`:
```python
(TokenType.IF, re.compile(r"IF\b", re.IGNORECASE)),
(TokenType.THEN, re.compile(r"THEN\b", re.IGNORECASE)),
(TokenType.ELSE_IF, re.compile(r"ELSE\s*IF\b", re.IGNORECASE)),
(TokenType.ELSE, re.compile(r"ELSE\b", re.IGNORECASE)),
(TokenType.END_IF, re.compile(r"END\s*IF\b", re.IGNORECASE)),
```

1.1.3: Create AST node classes in `ast.py`:
```python
@dataclass
class ConditionalBranchStep(PipelineStep):
    """A single branch within a conditional block."""
    condition: str  # Raw condition expression
    steps: List[PipelineStep]  # Steps to execute if condition is true
    line_number: int

@dataclass
class ConditionalBlockStep(PipelineStep):
    """Block containing multiple conditional branches and optional else."""
    branches: List[ConditionalBranchStep]  # IF/ELSEIF branches
    else_branch: Optional[List[PipelineStep]]  # ELSE branch (may be None)
    line_number: int
```

**Testing Requirements:**
- Verify lexer correctly tokenizes all conditional keywords
- Verify AST node classes can be properly instantiated
- Ensure all required attributes are present in AST nodes

**Definition of Done:**
- Lexer can tokenize all conditional keywords (case-insensitive)
- AST node classes are defined with proper inheritance
- Unit tests pass for all token types and AST node classes

### Task 1.2: Parser Implementation for Conditional Blocks

**Description:** Update Parser to recognize and parse conditional block syntax into AST structures.

**Status:** ✅ COMPLETED

**Files Impacted:**
- `sqlflow/parser/parser.py`

**Subtasks:**
1.2.1: Add case for `TokenType.IF` in `Parser._parse_statement`:
```python
if self.peek_token().type == TokenType.IF:
    return self._parse_conditional_block()
```

1.2.2: Implement `_parse_conditional_block` method:
```python
def _parse_conditional_block(self) -> ConditionalBlockStep:
    """Parse an IF/ELSEIF/ELSE/ENDIF block."""
    start_line = self.current_token.line
    branches = []
    else_branch = None
    
    # Parse initial IF branch
    self._consume(TokenType.IF)
    condition = self._parse_condition_expression()
    self._consume(TokenType.THEN)
    if_branch_steps = self._parse_branch_statements([TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF])
    branches.append(ConditionalBranchStep(condition, if_branch_steps, start_line))
    
    # Parse ELSEIF branches
    while self.peek_token().type == TokenType.ELSE_IF:
        self._consume(TokenType.ELSE_IF)
        condition = self._parse_condition_expression()
        self._consume(TokenType.THEN)
        elseif_branch_steps = self._parse_branch_statements([TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF])
        branches.append(ConditionalBranchStep(condition, elseif_branch_steps, self.current_token.line))
    
    # Parse optional ELSE branch
    if self.peek_token().type == TokenType.ELSE:
        self._consume(TokenType.ELSE)
        else_branch = self._parse_branch_statements([TokenType.END_IF])
    
    # Consume END IF
    self._consume(TokenType.END_IF)
    self._consume(TokenType.SEMICOLON)
    
    return ConditionalBlockStep(branches, else_branch, start_line)
```

1.2.3: Implement condition expression parsing:
```python
def _parse_condition_expression(self) -> str:
    """Parse a condition expression until THEN."""
    condition_tokens = []
    while self.peek_token().type != TokenType.THEN:
        token = self._consume_any()
        condition_tokens.append(token.value)
        
    return " ".join(condition_tokens).strip()
```

1.2.4: Implement branch statement parsing:
```python
def _parse_branch_statements(self, terminator_tokens: List[TokenType]) -> List[PipelineStep]:
    """Parse statements until reaching one of the terminator tokens."""
    branch_steps = []
    while self.peek_token().type not in terminator_tokens:
        step = self._parse_statement()
        branch_steps.append(step)
    return branch_steps
```

**Testing Requirements:**
- Test parsing of single IF-THEN-ENDIF blocks
- Test parsing of IF-THEN-ELSE-ENDIF blocks
- Test parsing of IF-THEN-ELSEIF-THEN-ELSE-ENDIF blocks
- Test error handling for invalid syntax
- Test nested conditional blocks

**Definition of Done:**
- Parser correctly constructs `ConditionalBlockStep` AST nodes from valid syntax
- Parser raises appropriate errors for invalid syntax
- All unit tests for parsing pass
- Integration tests with Lexer-Parser pipeline pass

### Task 1.3: Condition Evaluation Logic

**Description:** Create an evaluator that resolves conditions against variable values.

**Status:** ✅ COMPLETED

**Files Impacted:**
- New: `sqlflow/core/evaluator.py`

**Subtasks:**
1.3.1: Create the `ConditionEvaluator` class:
```python
class ConditionEvaluator:
    """Evaluates conditional expressions with variable substitution."""
    
    def __init__(self, variables: Dict[str, Any]):
        """Initialize with a variables dictionary.
        
        Args:
            variables: Dictionary of variable names to values
        """
        self.variables = variables
    
    def evaluate(self, condition: str) -> bool:
        """Evaluate a condition expression to a boolean result.
        
        Args:
            condition: String containing the condition to evaluate
            
        Returns:
            Boolean result of the condition evaluation
            
        Raises:
            EvaluationError: If the condition cannot be evaluated
        """
        # First substitute variables
        substituted_condition = self._substitute_variables(condition)
        
        try:
            # Safe evaluation using Python's ast module
            return self._safe_eval(substituted_condition)
        except Exception as e:
            raise EvaluationError(f"Failed to evaluate condition: {condition}. Error: {str(e)}")
```

1.3.2: Implement variable substitution:
```python
def _substitute_variables(self, condition: str) -> str:
    """Replace ${var} with the variable value."""
    # Reuse SQLGenerator's variable substitution pattern
    for name, value in self.variables.items():
        pattern = r"\$\{" + name + r"(?:\|[^}]*)?\}"
        
        # Convert value to appropriate string representation
        if isinstance(value, str):
            # Keep strings as quoted strings
            str_value = f"'{value}'"
        else:
            str_value = str(value)
            
        condition = re.sub(pattern, str_value, condition)
    
    # Handle default values for any remaining variables
    def replace_with_default(match):
        parts = match.group(0)[2:-1].split("|")
        if len(parts) > 1:
            # Use the default value, properly quoted if it's a string
            default = parts[1].strip()
            if default.startswith('"') or default.startswith("'"):
                return default  # Already a string literal
            if default.lower() in ('true', 'false'):
                return default.lower()  # Boolean literal
            try:
                float(default)  # Test if number
                return default  # Numeric literal
            except ValueError:
                return f"'{default}'"  # Treat as string
        return "None"  # No default provided
    
    return re.sub(r"\$\{[^}]*\}", replace_with_default, condition)
```

1.3.3: Implement safe evaluation:
```python
def _safe_eval(self, expr: str) -> bool:
    """Safely evaluate an expression to a boolean result."""
    # Only allow specific operations for security
    allowed_names = {
        'True': True, 'False': False, 'None': None,
        'and': and_op, 'or': or_op, 'not': not_op,
        'eq': eq_op, 'ne': ne_op, 'lt': lt_op, 
        'le': le_op, 'gt': gt_op, 'ge': ge_op
    }
    
    # Replace common operators with function calls for evaluation
    expr = expr.replace("==", " eq ")
    expr = expr.replace("!=", " ne ")
    expr = expr.replace("<", " lt ")
    expr = expr.replace("<=", " le ")
    expr = expr.replace(">", " gt ")
    expr = expr.replace(">=", " ge ")
    
    # Parse and evaluate safely
    expr_ast = ast.parse(expr, mode='eval')
    return eval(compile(expr_ast, '<string>', 'eval'), {"__builtins__": {}}, allowed_names)
```

1.3.4: Define helper operations:
```python
# Define operation functions
def and_op(a, b):
    return a and b

def or_op(a, b):
    return a or b

def not_op(a):
    return not a

def eq_op(a, b):
    return a == b

def ne_op(a, b):
    return a != b

def lt_op(a, b):
    return a < b

def le_op(a, b):
    return a <= b

def gt_op(a, b):
    return a > b

def ge_op(a, b):
    return a >= b
```

**Testing Requirements:**
- Test basic comparisons (==, !=, <, >, <=, >=)
- Test logical operators (AND, OR, NOT)
- Test with variables of different types (string, number, boolean)
- Test variable substitution with default values
- Test error handling for invalid expressions

**Definition of Done:**
- Evaluator correctly processes variable substitution 
- Evaluator correctly evaluates all supported operations
- Evaluator securely rejects unauthorized expressions
- All unit tests pass

### Task 1.4: Planner Integration for Conditional Resolution

**Description:** Modify the ExecutionPlanBuilder to evaluate conditions and select active branches during planning.

**Status:** ✅ COMPLETED

**Files Impacted:**
- `sqlflow/core/planner.py`

**Subtasks:**
1.4.1: Add flattening method to ExecutionPlanBuilder:
```python
def _flatten_conditional_blocks(self, pipeline: Pipeline, variables: Dict[str, Any]) -> Pipeline:
    """Replace conditional blocks with steps from active branches."""
    flattened_pipeline = Pipeline()
    evaluator = ConditionEvaluator(variables)
    
    for step in pipeline.steps:
        if isinstance(step, ConditionalBlockStep):
            active_steps = self._resolve_conditional_block(step, evaluator)
            for active_step in active_steps:
                flattened_pipeline.add_step(active_step)
        else:
            flattened_pipeline.add_step(step)
            
    return flattened_pipeline
```

1.4.2: Add conditional resolution method:
```python
def _resolve_conditional_block(
    self, conditional_block: ConditionalBlockStep, evaluator: ConditionEvaluator
) -> List[PipelineStep]:
    """Determine active branch based on condition evaluation."""
    # Process each branch until a true condition is found
    for branch in conditional_block.branches:
        try:
            if evaluator.evaluate(branch.condition):
                # Process any nested conditionals in the active branch
                flat_branch_steps = []
                for step in branch.steps:
                    if isinstance(step, ConditionalBlockStep):
                        flat_branch_steps.extend(
                            self._resolve_conditional_block(step, evaluator)
                        )
                    else:
                        flat_branch_steps.append(step)
                return flat_branch_steps
        except Exception as e:
            # Log the error but continue to next branch
            logger.warning(f"Error evaluating condition: {branch.condition}. Error: {str(e)}")
    
    # If no branch condition is true, use the else branch if available
    if conditional_block.else_branch:
        flat_else_steps = []
        for step in conditional_block.else_branch:
            if isinstance(step, ConditionalBlockStep):
                flat_else_steps.extend(
                    self._resolve_conditional_block(step, evaluator)
                )
            else:
                flat_else_steps.append(step)
        return flat_else_steps
    
    # No condition was true and no else branch
    return []
```

1.4.3: Modify build_plan to use flattened pipeline:
```python
def build_plan(self, pipeline: Pipeline) -> List[Dict[str, Any]]:
    """Build an execution plan from a pipeline.
    
    Args:
        pipeline: The validated pipeline to build a plan for
        
    Returns:
        A list of execution steps in topological order
        
    Raises:
        PlanningError: If the plan cannot be built
    """
    # Initialize state
    self.dependency_resolver = DependencyResolver()
    self.step_id_map = {}
    self.step_dependencies = {}
    
    # Extract variables from context (this should come from outside in real implementation)
    # For MVP, this could be passed in or retrieved from a global context
    variables = {}  # Replace with actual variable source
    
    # Flatten conditional blocks to just the active branch steps
    flattened_pipeline = self._flatten_conditional_blocks(pipeline, variables)
    
    # Build dependency graph using flattened pipeline
    self._build_dependency_graph(flattened_pipeline)
    
    # Setup additional dependencies for correct execution order
    source_steps, load_steps = self._get_sources_and_loads(flattened_pipeline)
    self._add_load_dependencies(source_steps, load_steps)
    
    # Generate unique IDs for each step
    self._generate_step_ids(flattened_pipeline)
    
    # Resolve execution order based on dependencies
    try:
        execution_order = self._resolve_execution_order()
    except Exception as e:
        raise PlanningError(f"Failed to resolve execution order: {str(e)}") from e
    
    # Create execution steps from pipeline steps in the determined order
    return self._build_execution_steps(flattened_pipeline, execution_order)
```

**Testing Requirements:**
- Test basic conditional branch selection
- Test nested conditional resolution
- Test integration with variable context
- Test error handling for condition evaluation
- Test with complex dependency patterns

**Definition of Done:**
- Planner correctly selects active branch based on condition evaluation
- Planner correctly handles nested conditionals
- DAG dependencies are correctly maintained in pruned plan
- All unit and integration tests pass

### Task 1.5: DAG Builder Update for Conditionals

**Description:** Ensure the DAG visualization reflects only the active branch steps.

**Status:** ✅ COMPLETED

**Files Impacted:**
- `sqlflow/visualizer/dag_builder_ast.py`

**Subtasks:**
1.5.1: Verified no changes needed because we're using the flattened pipeline approach:
- The DAG builder successfully processes the flattened pipeline with only the active branch steps
- Created comprehensive tests in `tests/unit/visualizer/test_conditional_dag.py`
- Verified visualizer displays correct dependencies with conditional execution

**Testing Requirements:**
- Test DAG creation with flattened pipelines
- Test DAG with nested conditionals
- Test DAG with mixed conditional and regular steps
- Verify no cycles are created in the dependency graph

**Definition of Done:**
- DAG visualization correctly reflects only the active branches
- All dependencies are accurately represented in the graph
- All tests pass for DAG building with conditional execution

### Task 1.6: Documentation & Examples

**Description:** Create documentation and example pipelines for conditional execution.

**Files Impacted:**
- `docs/conditionals.md`
- `examples/conditional_pipelines/`

**Subtasks:**
1.6.1: Create conditional execution documentation:
```markdown
# Conditional Execution in SQLFlow

SQLFlow supports conditional blocks in pipelines, allowing different operations to execute
based on variable values. Conditions are evaluated at planning time, so only the active
branch becomes part of the execution plan.

## Syntax

```sql
IF condition THEN
  -- Statements to execute if condition is true
ELSE IF another_condition THEN
  -- Statements to execute if another_condition is true
ELSE
  -- Statements to execute if no conditions are true
END IF;
```

## Condition Expressions

Conditions support:
- Comparison operators: ==, !=, <, >, <=, >=
- Logical operators: AND, OR, NOT
- Variable references: ${variable_name}
- Default values: ${variable_name|default_value}

## Example

```sql
SET environment = "${ENV|development}";

IF environment == "production" THEN
  -- Production-specific operations
  SOURCE prod_db TYPE POSTGRES PARAMS {
    "connection_string": "${PROD_DB_CONN}"
  };
ELSE IF environment == "staging" THEN
  -- Staging-specific operations
  SOURCE staging_db TYPE POSTGRES PARAMS {
    "connection_string": "${STAGING_DB_CONN}"
  };
ELSE
  -- Development operations
  SOURCE local_csv TYPE CSV PARAMS {
    "path": "data/local_data.csv"
  };
END IF;
```
```

1.6.2: Create example conditional pipeline:
```sql
-- Example conditional pipeline based on environment variable
SET environment = "${ENV|development}";
SET date = "${run_date|2023-01-01}";

-- Conditional source selection
IF environment == "production" THEN
  SOURCE orders TYPE POSTGRES PARAMS {
    "connection_string": "${PROD_DB_CONN}",
    "query": "SELECT * FROM orders WHERE order_date = '${date}'"
  };
ELSE IF environment == "staging" THEN
  SOURCE orders TYPE POSTGRES PARAMS {
    "connection_string": "${STAGING_DB_CONN}",
    "query": "SELECT * FROM orders WHERE order_date = '${date}'"
  };
ELSE
  SOURCE orders TYPE CSV PARAMS {
    "path": "data/orders_${date}.csv",
    "has_header": true
  };
END IF;

-- Load data regardless of source
LOAD orders_data FROM orders;

-- Conditional transformation logic
IF environment == "production" THEN
  -- More complex aggregation for production
  CREATE TABLE order_summary AS
  SELECT
    DATE_TRUNC('day', order_date) AS day,
    product_category,
    COUNT(*) AS order_count,
    SUM(amount) AS total_sales,
    AVG(amount) AS avg_order_value
  FROM orders_data
  GROUP BY 1, 2;
ELSE
  -- Simplified aggregation for non-production
  CREATE TABLE order_summary AS
  SELECT
    DATE_TRUNC('day', order_date) AS day,
    COUNT(*) AS order_count,
    SUM(amount) AS total_sales
  FROM orders_data
  GROUP BY 1;
END IF;

-- Export results
EXPORT
  SELECT * FROM order_summary
TO "${OUTPUT_PATH}/order_summary_${date}.csv"
TYPE LOCAL_FILE
OPTIONS { "format": "csv", "header": true };
```

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test example pipelines to ensure they run correctly
- Verify all syntax and examples are consistent with implementation

**Definition of Done:**
- Documentation clearly explains conditional execution
- Documentation covers all supported syntax and features
- Example pipelines are valid and functional
- All syntax and feature descriptions match implementation

## Epic 2: Python User-Defined Functions (UDFs)

**Goal:** Enable SQLFlow to discover, register, and use Python functions as UDFs within SQL queries.

### Task 2.1: UDF Infrastructure

**Description:** Create the core infrastructure for defining, discovering, and managing Python UDFs.

**Files Impacted:**
- New: `sqlflow/udfs/__init__.py`
- New: `sqlflow/udfs/decorators.py`
- New: `sqlflow/udfs/manager.py`

**Subtasks:**
2.1.1: Create UDF decorator in `decorators.py`:
```python
import functools
from typing import Any, Callable, List, Optional, TypeVar, Union, cast

import pandas as pd

FuncType = TypeVar('FuncType', bound=Callable[..., Any])

def python_scalar_udf(func: FuncType) -> FuncType:
    """Decorator to mark a function as a SQLFlow scalar UDF.
    
    Args:
        func: Python function to register as a UDF
        
    Returns:
        The decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    wrapper._is_sqlflow_udf = True
    wrapper._udf_type = "scalar"
    return cast(FuncType, wrapper)

def python_table_udf(func: FuncType) -> FuncType:
    """Decorator to mark a function as a SQLFlow table UDF.
    
    Args:
        func: Python function that takes a DataFrame and returns a DataFrame
        
    Returns:
        The decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if not isinstance(result, pd.DataFrame):
            raise ValueError(
                f"Table UDF {func.__name__} must return a pandas DataFrame, got {type(result)}"
            )
        return result
    
    wrapper._is_sqlflow_udf = True
    wrapper._udf_type = "table"
    return cast(FuncType, wrapper)
```

2.1.2: Create UDF manager in `manager.py`:
```python
import glob
import importlib.util
import inspect
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

class PythonUDFManager:
    """Manages discovery and registration of Python UDFs."""
    
    def __init__(self, project_dir: Optional[str] = None):
        """Initialize a PythonUDFManager.
        
        Args:
            project_dir: Path to project directory (default: current working directory)
        """
        self.project_dir = project_dir or os.getcwd()
        self.udfs: Dict[str, Callable] = {}
        self.udf_info: Dict[str, Dict[str, Any]] = {}
    
    def discover_udfs(self, python_udfs_dir: str = "python_udfs") -> Dict[str, Callable]:
        """Discover UDFs in the project structure.
        
        Args:
            python_udfs_dir: Path to UDFs directory relative to project_dir
            
        Returns:
            Dictionary of UDF name to function
        """
        udfs = {}
        udf_dir = os.path.join(self.project_dir, python_udfs_dir)
        
        if not os.path.exists(udf_dir):
            logger.warning(f"UDF directory not found: {udf_dir}")
            return udfs
        
        for py_file in glob.glob(f"{udf_dir}/*.py"):
            module_name = os.path.basename(py_file)[:-3]
            
            try:
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec is None or spec.loader is None:
                    logger.warning(f"Failed to load spec for {py_file}")
                    continue
                    
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # Collect functions decorated with @python_scalar_udf or @python_table_udf
                for name, func in inspect.getmembers(module, inspect.isfunction):
                    if hasattr(func, '_is_sqlflow_udf'):
                        udf_name = f"{module_name}.{name}"
                        udfs[udf_name] = func
                        
                        # Store additional UDF metadata
                        self.udf_info[udf_name] = {
                            'module': module_name,
                            'name': name,
                            'type': getattr(func, '_udf_type', 'unknown'),
                            'docstring': inspect.getdoc(func) or "",
                            'file_path': py_file,
                            'signature': str(inspect.signature(func))
                        }
                        
                        logger.info(f"Discovered UDF: {udf_name} ({self.udf_info[udf_name]['type']})")
            
            except Exception as e:
                logger.error(f"Error loading UDFs from {py_file}: {str(e)}")
                
        self.udfs = udfs
        return udfs
    
    def get_udf(self, udf_name: str) -> Optional[Callable]:
        """Get a UDF by name.
        
        Args:
            udf_name: Name of the UDF (module.function)
            
        Returns:
            UDF function or None if not found
        """
        return self.udfs.get(udf_name)
    
    def get_udf_info(self, udf_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a UDF.
        
        Args:
            udf_name: Name of the UDF (module.function)
            
        Returns:
            Dictionary of UDF information
        """
        return self.udf_info.get(udf_name)
    
    def list_udfs(self) -> List[Dict[str, Any]]:
        """List all discovered UDFs with their information.
        
        Returns:
            List of UDF information dictionaries
        """
        return [
            {
                'name': udf_name,
                **self.udf_info[udf_name]
            }
            for udf_name in self.udfs
        ]
    
    def extract_udf_references(self, sql: str) -> List[str]:
        """Extract UDF references from SQL query.
        
        Args:
            sql: SQL query
            
        Returns:
            List of UDF names referenced in the query
        """
        # Simple regex-based extraction for MVP
        # This could be improved with proper SQL parsing
        import re
        
        # Match PYTHON_FUNC("module.function", ...)
        udf_pattern = r'PYTHON_FUNC\(\s*[\'"]([a-zA-Z0-9_\.]+)[\'"]'
        matches = re.findall(udf_pattern, sql)
        
        # Filter to only include discovered UDFs
        return [match for match in matches if match in self.udfs]
```

**Testing Requirements:**
- Test UDF decorator functionality
- Test UDF discovery with various module structures
- Test UDF information collection
- Test UDF reference extraction from SQL

**Definition of Done:**
- UDF decorators correctly mark functions
- PythonUDFManager correctly discovers UDFs
- Manager extracts metadata from UDFs
- Manager identifies UDF references in SQL
- All unit tests pass

### Task 2.2: Engine Integration for UDFs

**Description:** Extend the SQLEngine interface and implement UDF registration for DuckDB.

**Files Impacted:**
- `sqlflow/core/engines/base.py` (abstract base class)
- `sqlflow/core/engines/duckdb_engine.py`

**Subtasks:**
2.2.1: Update SQLEngine interface in `base.py`:
```python
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

class SQLEngine(ABC):
    """Base class for SQL engines."""
    
    @abstractmethod
    def register_python_udf(self, name: str, function: Callable) -> None:
        """Register a Python UDF with the engine.
        
        Args:
            name: Name to register the UDF as
            function: Python function to register
        """
        pass
    
    @abstractmethod
    def process_query_for_udfs(self, query: str, udfs: Dict[str, Callable]) -> str:
        """Process a query to replace UDF references with engine-specific syntax.
        
        Args:
            query: Original SQL query with UDF references
            udfs: Dictionary of UDF names to functions
            
        Returns:
            Processed query with engine-specific UDF references
        """
        pass
    
    # ... existing abstract methods ...
```

2.2.2: Implement UDF support in DuckDBEngine:
```python
import re
from typing import Any, Callable, Dict, List, Optional

import duckdb
import pandas as pd

from sqlflow.core.engines.base import SQLEngine

class DuckDBEngine(SQLEngine):
    """DuckDB engine for SQLFlow."""
    
    def __init__(self, db_path: str = ":memory:"):
        """Initialize a DuckDBEngine.
        
        Args:
            db_path: Path to DuckDB database file or ":memory:" for in-memory
        """
        self.db_path = db_path
        self.connection = duckdb.connect(db_path)
        self.registered_udfs: Dict[str, Callable] = {}
    
    def register_python_udf(self, name: str, function: Callable) -> None:
        """Register a Python UDF with DuckDB.
        
        Args:
            name: Name to register the UDF as
            function: Python function to register
        """
        # DuckDB requires the full function name including the required types
        udf_type = getattr(function, '_udf_type', 'scalar')
        
        if udf_type == 'scalar':
            # Register scalar UDF
            self.connection.create_function(name, function)
        elif udf_type == 'table':
            # Register table UDF (table-producing function)
            # DuckDB registration mechanism depends on version
            try:
                # Newer DuckDB versions
                self.connection.create_table_function(name, function)
            except AttributeError:
                # Older DuckDB versions - fallback
                self.connection.table_function(name, function)
        
        self.registered_udfs[name] = function
    
    def process_query_for_udfs(self, query: str, udfs: Dict[str, Callable]) -> str:
        """Process a query to replace PYTHON_FUNC calls with DuckDB function calls.
        
        Args:
            query: Original SQL query with PYTHON_FUNC calls
            udfs: Dictionary of UDF names to functions
            
        Returns:
            Processed query with DuckDB function calls
        """
        # Register UDFs with DuckDB
        for udf_name, udf_func in udfs.items():
            if udf_name not in self.registered_udfs:
                self.register_python_udf(udf_name, udf_func)
        
        # Replace PYTHON_FUNC("module.function", args) with module.function(args)
        def replace_python_func(match):
            full_match = match.group(0)
            udf_name = match.group(1)
            args = match.group(2)
            
            if udf_name in udfs:
                return f"{udf_name}({args})"
            else:
                return full_match  # Keep as is if not registered
        
        pattern = r'PYTHON_FUNC\(\s*[\'"]([a-zA-Z0-9_\.]+)[\'"]\s*,\s*(.*?)\)'
        processed_query = re.sub(pattern, replace_python_func, query, flags=re.DOTALL)
        
        return processed_query
    
    def execute_query(self, query: str, udfs: Optional[Dict[str, Callable]] = None) -> Any:
        """Execute a query with optional UDFs.
        
        Args:
            query: SQL query to execute
            udfs: Optional dictionary of UDFs to register
            
        Returns:
            Query result
        """
        if udfs:
            query = self.process_query_for_udfs(query, udfs)
        
        return self.connection.execute(query)
    
    # ... existing methods ...
```

**Testing Requirements:**
- Test UDF registration with DuckDB
- Test query processing for scalar UDFs
- Test query processing for table UDFs
- Test error handling for invalid UDFs
- Test with a variety of UDF argument types

**Definition of Done:**
- SQLEngine interface includes UDF methods
- DuckDBEngine correctly registers Python UDFs
- Query processing correctly transforms PYTHON_FUNC syntax
- Unit tests pass for both scalar and table UDFs
- Integration tests with actual UDF execution pass

### Task 2.3: Executor Integration for UDF Orchestration

**Description:** Update executor to discover and provide UDFs during query execution.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/executors/local_executor.py`
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
2.3.1: Add UDF management to BaseExecutor:
```python
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from sqlflow.udfs.manager import PythonUDFManager

class BaseExecutor(ABC):
    """Base class for SQLFlow executors."""
    
    def __init__(self):
        """Initialize a BaseExecutor."""
        self.udf_manager = PythonUDFManager()
        self.discovered_udfs = {}
    
    def discover_udfs(self) -> Dict[str, Any]:
        """Discover UDFs in the project.
        
        Returns:
            Dictionary of UDF names to functions
        """
        self.discovered_udfs = self.udf_manager.discover_udfs()
        return self.discovered_udfs
    
    def get_query_udfs(self, query: str) -> Dict[str, Any]:
        """Get UDFs referenced in a query.
        
        Args:
            query: SQL query
            
        Returns:
            Dictionary of UDF names to functions
        """
        if not self.discovered_udfs:
            self.discover_udfs()
        
        udf_refs = self.udf_manager.extract_udf_references(query)
        return {name: self.discovered_udfs[name] for name in udf_refs if name in self.discovered_udfs}
    
    # ... existing abstract methods ...
```

2.3.2: Update LocalExecutor to use UDFs:
```python
def _execute_transform(self, step: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a transform step.
    
    Args:
        step: Transform step to execute
        
    Returns:
        Execution result
    """
    sql = step["query"]
    table_name = step["name"]
    
    try:
        # Register all tables with DuckDB
        for tbl, chunk in self.table_data.items():
            df = chunk.pandas_df
            self.duckdb_engine.connection.register(tbl, df)
        
        # Get UDFs referenced in the query
        query_udfs = self.get_query_udfs(sql)
        
        # Execute the query with UDFs
        result = self.duckdb_engine.execute_query(sql, udfs=query_udfs).fetchdf()
        
        self.table_data[table_name] = DataChunk(result)
        self.step_table_map[step["id"]] = table_name
        if step["id"] not in self.step_output_mapping:
            self.step_output_mapping[step["id"]] = []
        self.step_output_mapping[step["id"]].append(table_name)
        
        return {"status": "success"}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
```

2.3.3: Update ThreadPoolExecutor to use UDFs:
```python
def execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single step in the pipeline.
    
    Args:
        step: Operation to execute
        
    Returns:
        Dict containing execution results
    """
    step_type = step.get("type", "unknown")
    
    if step_type == "transform":
        # Get UDFs for this step
        query = step.get("query", "")
        udfs = self.get_query_udfs(query)
        
        # Include UDFs in execution context
        execution_context = {
            "udfs": udfs
        }
        
        # Pass context to engine
        # ... rest of implementation ...
    
    # ... rest of method ...
```

**Testing Requirements:**
- Test UDF discovery in executor
- Test UDF extraction from queries
- Test UDF integration with query execution
- Test error handling for missing or invalid UDFs

**Definition of Done:**
- Executors correctly discover UDFs
- Executors extract UDF references from queries
- UDFs are properly registered with the engine
- Unit and integration tests pass

### Task 2.4: CLI Support for UDFs

**Description:** Add CLI commands to list and inspect UDFs.

**Files Impacted:**
- `sqlflow/cli/commands/udf.py` (new)
- `sqlflow/cli/main.py`

**Subtasks:**

2.4.1: Create UDF command module:
```python
"""CLI commands for Python UDFs."""

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from sqlflow.project import Project
from sqlflow.udfs.manager import PythonUDFManager

app = typer.Typer(help="Manage Python UDFs")
console = Console()

@app.command("list")
def list_udfs(
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", "-p", help="Project directory"
    ),
):
    """List available Python UDFs in the project."""
    project_dir = project_dir or "."
    udf_manager = PythonUDFManager(project_dir)
    udfs = udf_manager.discover_udfs()
    
    if not udfs:
        console.print("No Python UDFs found in the project.")
        return
    
    table = Table(title="Python UDFs")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Signature", style="yellow")
    table.add_column("Description", style="white")
    
    for udf_info in udf_manager.list_udfs():
        table.add_row(
            udf_info["name"],
            udf_info["type"],
            udf_info["signature"],
            udf_info["docstring"].split("\n")[0] if udf_info["docstring"] else ""
        )
    
    console.print(table)

@app.command("info")
def udf_info(
    udf_name: str = typer.Argument(..., help="UDF name (module.function)"),
    project_dir: Optional[str] = typer.Option(
        None, "--project-dir", "-p", help="Project directory"
    ),
):
    """Show detailed information about a specific UDF."""
    project_dir = project_dir or "."
    udf_manager = PythonUDFManager(project_dir)
    udf_manager.discover_udfs()
    
    udf_info = udf_manager.get_udf_info(udf_name)
    if not udf_info:
        console.print(f"UDF '{udf_name}' not found.")
        return
    
    console.print(f"[bold cyan]UDF: {udf_name}[/bold cyan]")
    console.print(f"Type: [green]{udf_info['type']}[/green]")
    console.print(f"File: {udf_info['file_path']}")
    console.print(f"Signature: [yellow]{udf_info['signature']}[/yellow]")
    
    if udf_info["docstring"]:
        console.print("\n[bold]Documentation:[/bold]")
        console.print(udf_info["docstring"])
```

2.4.2: Update main.py to include UDF commands:
```python
from sqlflow.cli import connect
from sqlflow.cli.pipeline import pipeline_app
from sqlflow.cli.udf import app as udf_app  # Import the UDF command module
from sqlflow.project import Project

app = typer.Typer(
    help="SQLFlow - SQL-based data pipeline tool.",
    no_args_is_help=True,
)

app.add_typer(pipeline_app, name="pipeline")
app.add_typer(connect.app, name="connect")
app.add_typer(udf_app, name="udf")  # Add the UDF command
```

2.4.3: Update CLI help output:
```python
def cli():
    """Entry point for the command line."""
    # Fix for the help command issue with Typer
    if len(sys.argv) == 1 or "--help" in sys.argv or "-h" in sys.argv:
        print("SQLFlow - SQL-based data pipeline tool.")
        print("\nCommands:")
        print("  pipeline    Work with SQLFlow pipelines.")
        print("  connect     Manage and test connection profiles.")
        print("  udf         Manage Python UDFs.")  # Add UDF to help text
        print("  init        Initialize a new SQLFlow project.")
        # ...
        
        # Check if help is requested for a specific command
        if len(sys.argv) > 2 and ("--help" in sys.argv or "-h" in sys.argv):
            command = sys.argv[1]
            # ...
            elif command == "udf":
                print("\nUDF Commands:")
                print("  list        List available Python UDFs.")
                print("  info        Show detailed information about a specific UDF.")
            # ...
```

**Testing Requirements:**
- Test `udf list` command with various project structures
- Test `udf info` command with valid and invalid UDF names
- Verify help text is displayed correctly

**Definition of Done:**
- CLI correctly discovers and lists UDFs
- UDF details are displayed in readable format
- All CLI commands have proper help text
- Commands handle errors gracefully

### Task 2.5: Documentation for Python UDFs

**Description:** Create comprehensive documentation for using Python UDFs in SQLFlow.

**Files Impacted:**
- `docs/features/python_udfs.md` (new)
- `examples/udf_demo.sf` (new)
- `examples/python_udfs/example_udf.py` (new)

**Subtasks:**
2.5.1: Create Python UDF documentation:
```markdown
# Python User-Defined Functions (UDFs) in SQLFlow

SQLFlow supports Python User-Defined Functions (UDFs), which allow you to extend SQL's capabilities 
with custom Python code. UDFs can process data row-by-row (scalar functions) or process
entire tables (table functions).

## Defining UDFs

UDFs are defined in Python files within a `python_udfs` directory at the root of your project.
Each UDF must be decorated with either `@python_scalar_udf` or `@python_table_udf`.

### Example: Scalar UDF

```python
# python_udfs/string_utils.py
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def capitalize_words(text: str) -> str:
    """Capitalize each word in a string.
    
    Args:
        text: Input string
        
    Returns:
        String with each word capitalized
    """
    if text is None:
        return None
    return ' '.join(word.capitalize() for word in text.split())
```

### Example: Table UDF

```python
# python_udfs/data_processors.py
import pandas as pd
from sqlflow.udfs.decorators import python_table_udf

@python_table_udf
def add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales metrics to a DataFrame.
    
    Args:
        df: Input DataFrame with 'amount' column
        
    Returns:
        DataFrame with added metrics
    """
    result = df.copy()
    result['tax'] = result['amount'] * 0.07
    result['total'] = result['amount'] + result['tax']
    return result
```

## Using UDFs in SQL

Once defined, UDFs can be used in SQL queries with the `PYTHON_FUNC()` syntax:

### Using Scalar UDFs

```sql
CREATE TABLE customer_names AS
SELECT
  customer_id,
  PYTHON_FUNC("string_utils.capitalize_words", name) AS formatted_name
FROM customers;
```

### Using Table UDFs

```sql
CREATE TABLE sales_with_metrics AS
SELECT * FROM PYTHON_FUNC("data_processors.add_sales_metrics", sales);
```

## Listing Available UDFs

You can list all available UDFs with the CLI:

```bash
sqlflow udf list
```

## UDF Limitations and Best Practices

- UDFs run in the same process as SQLFlow, so resource-intensive operations can impact performance.
- UDFs should be focused on data processing and avoid side effects (like writing files).
- For better performance:
  - Keep UDFs small and focused
  - Use vectorized operations in pandas when possible
  - Consider moving complex processing to dedicated transforms
```

2.5.2: Create example UDF file:
```python
# examples/python_udfs/example_udf.py
import pandas as pd
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf

@python_scalar_udf
def calculate_discount(price: float, rate: float = 0.1) -> float:
    """Calculate discount amount.
    
    Args:
        price: Original price
        rate: Discount rate (default: 0.1)
        
    Returns:
        Discount amount
    """
    if price is None:
        return None
    return price * rate

@python_table_udf
def add_discounted_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Add discounted price columns to a DataFrame.
    
    Args:
        df: DataFrame with 'price' column
        
    Returns:
        DataFrame with additional columns
    """
    result = df.copy()
    result['discount_10'] = result['price'] * 0.1
    result['discount_20'] = result['price'] * 0.2
    result['final_price_10'] = result['price'] - result['discount_10']
    result['final_price_20'] = result['price'] - result['discount_20']
    return result
```

2.5.3: Create example UDF pipeline:
```sql
-- examples/udf_demo.sf

-- Source data
SOURCE products TYPE CSV PARAMS {
  "path": "data/products.csv",
  "has_header": true
};

LOAD products INTO products_table;

-- Use scalar UDF to calculate discount
CREATE TABLE discounted_products AS
SELECT
  product_id,
  name,
  category,
  price,
  PYTHON_FUNC("example_udf.calculate_discount", price) AS discount_amount,
  price - PYTHON_FUNC("example_udf.calculate_discount", price) AS discounted_price
FROM products_table;

-- Use table UDF to add multiple discount calculations
CREATE TABLE product_pricing AS
SELECT * FROM PYTHON_FUNC("example_udf.add_discounted_prices", products_table);

-- Export results
EXPORT
  SELECT * FROM discounted_products
TO "output/discounted_products.csv"
TYPE CSV
OPTIONS { "header": true };

EXPORT
  SELECT * FROM product_pricing
TO "output/product_pricing.csv"
TYPE CSV
OPTIONS { "header": true };
```

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test example code to ensure it runs correctly
- Verify all syntax and examples are consistent with implementation

**Definition of Done:**
- Documentation clearly explains UDF functionality
- Documentation covers both scalar and table UDFs
- Example code is correct and functional
- All syntax and feature descriptions match implementation

## Epic Implementation Timeline

### Week 1: Conditional Execution Foundations
- Tasks 1.1 & 1.2: Lexer, AST, Parser updates
- Task 1.3: Condition evaluation logic
- Initial unit tests

### Week 2: Conditional Execution Integration
- Task 1.4: Planner integration
- Task 1.5: DAG builder updates
- Integration tests
- Task 1.6: Documentation

### Week 3: UDF Core Infrastructure
- Task 2.1: UDF infrastructure
- Task 2.2: Engine integration
- Initial unit tests

### Week 4: UDF Integration & Finalization
- Task 2.3: Executor integration
- Task 2.4: CLI support
- Task 2.5: Documentation
- Final integration tests

## Risk Mitigation

1. **DuckDB UDF Implementation Complexity**:
   - Backup Plan: If native DuckDB UDF integration is too complex, implement a simpler transformation approach that preprocesses Python function calls before submitting SQL.

2. **Nested Conditional Complexity**:
   - Mitigation: Start with flat conditional support, add nested support incrementally.
   - Limit nesting depth in MVP.

3. **Integration Test Coverage**:
   - Strategy: Create comprehensive test suites covering edge cases early.
   - Focus on validating execution paths match expected conditional branch evaluation.

