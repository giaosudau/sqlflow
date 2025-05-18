# SQLFlow Implementation Task Tracker

## Recent Progress (April-May 2024)
- ✅ **Completed core UDF infrastructure** with decorator implementation and engine integration
- ✅ **Standardized Table UDF signatures** with proper validation (commit d12a5eb3)
- ✅ **Enhanced test coverage** for UDF functionality, particularly for table UDFs
- 🔄 **Ongoing work** on complex UDF integration testing and error reporting improvements

## MVP Focus Areas
The primary focus for MVP release is completing the UDF feature set:
1. 🔥 Resolving integration tests for complex UDF pipelines
2. 🔥 Improving error reporting for UDF lifecycle
3. 🔥 Updating documentation with clear examples
4. 🔥 Creating an end-to-end demo showcasing UDF capabilities

## Overview
This document tracks the implementation status of Conditional Execution, Python UDFs, Data Loading, Schema Management, and Full Lifecycle features for SQLFlow. Each task includes description, implementation details, testing requirements, and Definition of Done.

## Epics

### Epic 1: Conditional Execution (IF/ELSE) ✅ COMPLETED

**Goal:** Implement SQL-native conditional block execution where branches are evaluated and selected at planning time based on variable context.

| Task | Description | Status | Assignee |
|------|-------------|--------|----------|
| [Task 1.1](#task-11-lexer--ast-updates-for-conditional-syntax) | Lexer & AST Updates for Conditional Syntax | ✅ COMPLETED | |
| [Task 1.2](#task-12-parser-implementation-for-conditional-blocks) | Parser Implementation for Conditional Blocks | ✅ COMPLETED | |
| [Task 1.3](#task-13-condition-evaluation-logic) | Condition Evaluation Logic | ✅ COMPLETED | |
| [Task 1.4](#task-14-planner-integration-for-conditional-resolution) | Planner Integration for Conditional Resolution | ✅ COMPLETED | |
| [Task 1.5](#task-15-dag-builder-update-for-conditionals) | DAG Builder Update for Conditionals | ✅ COMPLETED | |
| [Task 1.6](#task-16-documentation--examples) | Documentation & Examples | ✅ COMPLETED | |

### Epic 2: Python User-Defined Functions (UDFs) - MVP CRITICAL 🔥

**Goal:** Enable SQLFlow to discover, register, and use Python functions as UDFs within SQL queries.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 2.1](#task-21-udf-infrastructure) | UDF Infrastructure | ✅ COMPLETED | | |
| [Task 2.2](#task-22-engine-integration-for-udfs) | Engine Integration for UDFs | ✅ COMPLETED | | |
| [Task 2.3](#task-23-executor-integration-for-udf-orchestration) | Executor Integration for UDF Orchestration | ✅ COMPLETED | | |
| [Task 2.4](#task-24-cli-support-for-udfs) | CLI Support for UDFs | ✅ COMPLETED | | |
| [Task 2.5](#task-25-documentation-for-python-udfs) | Documentation for Python UDFs | ✅ COMPLETED | | |

**Note:** The following tasks (2.6 - 2.18) are for MVP refinement and stabilization of the UDF feature, with the first several being critical for basic functionality.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 2.6](#task-26-standardize-table-udf-signature--decorator-logic) | Standardize Table UDF Signature & Decorator Logic | ✅ COMPLETED | | |
| [Task 2.7](#task-27-refactor-duckdb-engine-for-table-udf-registration) | Refactor DuckDB Engine for Table UDF Registration | ✅ COMPLETED | | |
| [Task 2.8](#task-28-resolve-complex-udf-integration-test) | Resolve Complex UDF Integration Test | 🔄 IN PROGRESS | 🔥 MVP Critical | |
| [Task 2.9](#task-29-enhance-udf-unit-testing-table-udfs--engine) | Enhance UDF Unit Testing (Table UDFs & Engine) | ✅ COMPLETED | | |
| [Task 2.10](#task-210-improve-udf-discovery--metadata) | Improve UDF Discovery & Metadata | ⬜ NOT STARTED | 🔥 MVP Critical | |
| [Task 2.11](#task-211-enhance-udf-lifecycle-error-reporting) | Enhance UDF Lifecycle Error Reporting | 🔄 IN PROGRESS | 🔥 MVP Critical | |
| [Task 2.12](#task-212-verify-and-polish-cli-for-udfs) | Verify and Polish CLI for UDFs | ⬜ NOT STARTED | 🔥 MVP Critical | |
| [Task 2.13](#task-213-update-udf-documentation-for-mvp) | Update UDF Documentation for MVP | ⬜ NOT STARTED | 🔥 MVP Critical | |
| [Task 2.14](#task-214-review-and-refine-sqlengine-udf-interface) | Review and Refine SQLEngine UDF Interface | ⬜ NOT STARTED | | |
| [Task 2.15](#task-215-create-basic-end-to-end-udf-demo-for-mvp) | Create Basic End-to-End UDF Demo for MVP | ⬜ NOT STARTED | 🔥 MVP Critical | |
| [Task 2.16](#task-216-implement-duckdb-version-compatibility) | Implement DuckDB Version Compatibility | ⬜ NOT STARTED | | |
| [Task 2.17](#task-217-optimize-type-mapping-for-udfs) | Optimize Type Mapping for UDFs | ⬜ NOT STARTED | | |
| [Task 2.18](#task-218-performance-profiling-and-optimization) | Performance Profiling and Optimization | ⬜ NOT STARTED | | |

### Epic 3: Enhanced Load Controls (MODE Parameter)

**Goal:** Implement unified data loading controls with various operation modes (REPLACE, APPEND, MERGE) to support common data warehousing patterns.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 3.1](#task-31-parser-updates-for-load-modes) | Parser Updates for Load Modes | ⬜ NOT STARTED | | |
| [Task 3.2](#task-32-sql-generator-for-load-modes) | SQL Generator for Load Modes | ⬜ NOT STARTED | | |
| [Task 3.3](#task-33-schema-compatibility-validation) | Schema Compatibility Validation | ⬜ NOT STARTED | | |
| [Task 3.4](#task-34-merge-key-handling) | Merge Key Handling | ⬜ NOT STARTED | | |
| [Task 3.5](#task-35-load-mode-documentation) | Load Mode Documentation | ⬜ NOT STARTED | | |

### Epic 4: Core Connector Framework

**Goal:** Create a robust connector framework for accessing and moving data between various sources and destinations.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 4.1](#task-41-connector-interface-design) | Connector Interface Design | ⬜ NOT STARTED | | |
| [Task 4.2](#task-42-postgres-connector-implementation) | PostgreSQL Connector Implementation | ⬜ NOT STARTED | | |
| [Task 4.3](#task-43-s3-connector-implementation) | S3 Connector Implementation | ⬜ NOT STARTED | | |
| [Task 4.4](#task-44-connector-registry-and-discovery) | Connector Registry and Discovery | ⬜ NOT STARTED | | |
| [Task 4.5](#task-45-connector-documentation) | Connector Documentation | ⬜ NOT STARTED | | |

### Epic 5: Schema Management & Evolution

**Goal:** Implement comprehensive schema management capabilities to monitor source schemas, detect changes, and provide options for handling schema evolution.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 5.1](#task-51-schema-representation-and-storage) | Schema Representation and Storage | ⬜ NOT STARTED | | |
| [Task 5.2](#task-52-schema-comparison-algorithm) | Schema Comparison Algorithm | ⬜ NOT STARTED | | |
| [Task 5.3](#task-53-schema-drift-detection-integration) | Schema Drift Detection Integration | ⬜ NOT STARTED | | |
| [Task 5.4](#task-54-cli-commands-for-schema-management) | CLI Commands for Schema Management | ⬜ NOT STARTED | | |
| [Task 5.5](#task-55-schema-management-documentation) | Schema Management Documentation | ⬜ NOT STARTED | | |

### Epic 6: Execution Model & Scalability

**Goal:** Develop a robust execution model for SQLFlow that handles dependencies, manages resources, and provides status tracking.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 6.1](#task-61-dag-execution-enhancements) | DAG Execution Enhancements | ⬜ NOT STARTED | | |
| [Task 6.2](#task-62-parallel-execution-implementation) | Parallel Execution Implementation | ⬜ NOT STARTED | | |
| [Task 6.3](#task-63-failure-handling-and-recovery) | Failure Handling and Recovery | ⬜ NOT STARTED | | |
| [Task 6.4](#task-64-execution-status-tracking) | Execution Status Tracking | ⬜ NOT STARTED | | |
| [Task 6.5](#task-65-execution-metrics-collection) | Execution Metrics Collection | ⬜ NOT STARTED | | |

## Detailed Task Specifications

### Task 1.1: Lexer & AST Updates for Conditional Syntax

**Description:** Add token types and AST node structures to support IF/ELSE conditional blocks in SQLFlow's parser.

**Files Impacted:** 
- `sqlflow/parser/lexer.py`
- `sqlflow/parser/ast.py`

**Subtasks:**
1. Add new token types to `TokenType` enum:
   - `IF`, `THEN`, `ELSE_IF`, `ELSE`, `END_IF`
2. Add regex patterns to `Lexer.patterns` for each token type
3. Create AST node classes in `ast.py`:
   - `ConditionalBranchStep` for branches within a conditional block
   - `ConditionalBlockStep` for blocks containing multiple branches

**Testing Requirements:**
- Unit tests verifying lexer correctly tokenizes conditional keywords
- Unit tests verifying AST nodes can be properly instantiated
- Tests confirming case-insensitivity of keywords
- Coverage for both valid and invalid syntax patterns

**Definition of Done:**
- All token types and AST classes implemented
- All tests passing with >90% coverage
- Code formatted according to Black and isort standards
- Type hints used throughout implementation
- Documentation following Google style docstrings

### Task 1.2: Parser Implementation for Conditional Blocks

**Description:** Update the Parser to recognize and parse conditional block syntax into AST structures.

**Files Impacted:**
- `sqlflow/parser/parser.py`

**Subtasks:**
1. Add case for `TokenType.IF` in `Parser._parse_statement`
2. Implement `_parse_conditional_block` method
3. Implement condition expression parsing
4. Implement branch statement parsing

**Testing Requirements:**
- Test parsing of single `IF-THEN-ENDIF` blocks
- Test parsing of `IF-THEN-ELSE-ENDIF` blocks
- Test parsing of `IF-THEN-ELSEIF-THEN-ELSE-ENDIF` blocks
- Test error handling with invalid syntax
- Test nested conditional blocks
- Test integration with the full Lexer-Parser pipeline

**Definition of Done:**
- Parser correctly constructs AST nodes from valid syntax
- Parser raises appropriate errors for invalid syntax
- All tests passing with >90% coverage
- Code formatted per project standards
- Type hints and docstrings in place

### Task 1.3: Condition Evaluation Logic

**Description:** Create an evaluator that resolves conditions against variable values during execution planning.

**Files Impacted:**
- New: `sqlflow/core/evaluator.py`

**Subtasks:**
1. Create the `ConditionEvaluator` class
2. Implement variable substitution logic
3. Implement secure condition evaluation
4. Define helper operations for condition evaluation

**Testing Requirements:**
- Test basic comparisons (==, !=, <, >, <=, >=)
- Test logical operators (AND, OR, NOT)
- Test with variables of different types (string, number, boolean)
- Test variable substitution with default values
- Test error handling for invalid expressions
- Test security against injection attacks

**Definition of Done:**
- Evaluator correctly processes variable substitutions
- Evaluator correctly evaluates all supported operations
- Evaluator securely rejects unauthorized expressions
- All tests passing with >90% coverage
- Code follows PEP 8 and project style guidelines
- Type hints and documentation complete

### Task 1.4: Planner Integration for Conditional Resolution

**Description:** Modify the ExecutionPlanBuilder to evaluate conditions and select active branches during planning.

**Files Impacted:**
- `sqlflow/core/planner.py`

**Subtasks:**
1. Add flattening method to ExecutionPlanBuilder
2. Add conditional resolution method
3. Modify build_plan to use flattened pipeline

**Testing Requirements:**
- Test basic conditional branch selection
- Test nested conditional resolution
- Test integration with variable context
- Test error handling for condition evaluation
- Test with complex dependency patterns
- Verify DAG integrity after conditional resolution

**Definition of Done:**
- Planner correctly selects active branch based on condition evaluation
- Planner correctly handles nested conditionals
- DAG dependencies are correctly maintained in pruned plan
- All tests passing with >90% coverage
- Documentation updated to reflect changes

### Task 1.5: DAG Builder Update for Conditionals

**Description:** Ensure the DAG visualization reflects only the active branch steps after conditional resolution.

**Files Impacted:**
- `sqlflow/visualizer/dag_builder_ast.py`

**Subtasks:**
1. Verify compatibility with flattened pipeline approach
2. Update DAG building to handle conditional steps if needed
3. Implement comprehensive testing

**Testing Requirements:**
- Test DAG creation with flattened pipelines
- Test DAG with nested conditionals
- Test DAG with mixed conditional and regular steps
- Verify no cycles are created in the dependency graph

**Definition of Done:**
- DAG visualization correctly reflects only the active branches
- All dependencies are accurately represented in the graph
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 1.6: Documentation & Examples

**Description:** Create comprehensive documentation and example pipelines for conditional execution.

**Files Impacted:**
- `docs/conditionals.md`
- `examples/conditional_pipelines/`

**Subtasks:**
1. Create conditional execution documentation
2. Create example conditional pipelines
3. Document best practices and common patterns

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test example pipelines to ensure they run correctly
- Verify all syntax and examples are consistent with implementation

**Definition of Done:**
- Documentation clearly explains conditional execution ✅
- Documentation covers all supported syntax and features ✅
- Example pipelines are valid and functional ✅
- All syntax and feature descriptions match implementation ✅

### Task 2.1: UDF Infrastructure

**Description:** Create the core infrastructure for defining, discovering, and managing Python UDFs.

**Files Impacted:**
- New: `sqlflow/udfs/__init__.py`
- New: `sqlflow/udfs/decorators.py`
- New: `sqlflow/udfs/manager.py`

**Subtasks:**
1. Create UDF decorators (`python_scalar_udf` and `python_table_udf`)
2. Implement UDF manager for discovery and registration
3. Implement UDF reference extraction from SQL queries

**Testing Requirements:**
- Test UDF decorator functionality
- Test UDF discovery with various module structures
- Test UDF information collection
- Test UDF reference extraction from SQL
- Test error handling with invalid UDFs

**Definition of Done:**
- UDF decorators correctly mark functions
- UDFManager correctly discovers UDFs in project structure
- Manager extracts metadata from UDFs
- Manager identifies UDF references in SQL
- All tests passing with >90% coverage
- Documentation complete with examples

### Task 2.2: Engine Integration for UDFs

**Description:** Extend the SQLEngine interface and implement UDF registration for DuckDB.

**Files Impacted:**
- `sqlflow/core/engines/base.py` (abstract base class)
- `sqlflow/core/engines/duckdb_engine.py`

**Subtasks:**
1. Update SQLEngine interface with UDF methods
2. Implement UDF registration in DuckDBEngine
3. Implement query processing for UDF calls
4. Update query execution to support UDFs

**Testing Requirements:**
- Test UDF registration with DuckDB
- Test query processing for scalar UDFs
- Test query processing for table UDFs
- Test error handling for invalid UDFs
- Test with a variety of UDF argument types
- Test with different DuckDB versions

**Definition of Done:**
- SQLEngine interface includes UDF methods
- DuckDBEngine correctly registers Python UDFs
- Query processing correctly transforms UDF syntax
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 2.3: Executor Integration for UDF Orchestration

**Description:** Update executors to discover and provide UDFs during query execution.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/executors/local_executor.py`
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Add UDF management to BaseExecutor
2. Update LocalExecutor to use UDFs
3. Update ThreadPoolExecutor to use UDFs

**Testing Requirements:**
- Test UDF discovery in executor
- Test UDF extraction from queries
- Test UDF integration with query execution
- Test error handling for missing or invalid UDFs
- Test performance with complex UDFs

**Definition of Done:**
- Executors correctly discover UDFs in project
- Executors extract UDF references from queries
- UDFs are properly registered with the engine
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 2.4: CLI Support for UDFs

**Description:** Add CLI commands to list and inspect UDFs in a project.

**Files Impacted:**
- `sqlflow/cli/commands/udf.py` (new)
- `sqlflow/cli/main.py`

**Subtasks:**
1. Create UDF command module with list and info commands ✅
2. Update main.py to include UDF commands ✅
3. Update CLI help output ✅

**Testing Requirements:**
- Test `udf list` command with various project structures ✅
- Test `udf info` command with valid and invalid UDF names ✅
- Verify help text is displayed correctly ✅
- Test CLI handling of errors ✅

**Definition of Done:**
- CLI correctly discovers and lists UDFs ✅
- UDF details are displayed in readable format ✅
- All CLI commands have proper help text ✅
- Commands handle errors gracefully ✅
- All tests passing with >90% coverage ✅

### Task 2.5: Documentation for Python UDFs

**Description:** Create comprehensive documentation for using Python UDFs in SQLFlow.

**Files Impacted:**
- `docs/features/python_udfs.md` (new)
- `examples/udf_demo.sf` (new)
- `examples/python_udfs/example_udf.py` (new)

**Subtasks:**
1. Create Python UDF documentation
2. Create example UDF file
3. Create example UDF pipeline

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test example code to ensure it runs correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers best practices

**Definition of Done:**
- Documentation clearly explains UDF functionality ✅
- Documentation covers both scalar and table UDFs ✅
- Example code is correct and functional ✅
- All syntax and feature descriptions match implementation ✅
- Documentation follows project style guidelines ✅

### Task 2.6: Standardize Table UDF Signature & Decorator Logic

**Description:** Standardize Table UDF signatures to DataFrame + kwargs and update decorator logic accordingly to ensure consistent behavior and prepare for robust engine integration. This addresses parameter handling issues identified for MVP.

**Files Impacted:**
- `sqlflow/udfs/decorators.py`

**Subtasks:**
1. ✅ Modify `python_table_udf` to expect `pandas.DataFrame` as the first argument; subsequent arguments should primarily be keyword arguments.
2. ✅ Ensure the decorator captures necessary signature metadata (parameter names, types if available) for engine integration.
3. ✅ Rigorously validate that the UDF returns a `pandas.DataFrame`.
4. ✅ Add clear docstrings explaining the expected signature for users creating table UDFs.

**Testing Requirements:**
- ✅ Unit tests for `python_table_udf` covering:
    - Valid signatures (DataFrame only, DataFrame + kwargs).
    - Invalid signatures (e.g., missing DataFrame, incorrect return type).
    - Correct metadata capture.
- ✅ Ensure existing scalar UDF tests remain unaffected.

**Definition of Done:**
- ✅ `python_table_udf` enforces the standardized signature.
- ✅ Decorator correctly captures and exposes UDF signature metadata.
- ✅ All new and existing unit tests for decorators pass (>90% coverage for changes).
- ✅ Code is formatted (Black, isort), type-hinted, and well-documented.

**Status:** ✅ COMPLETED in commit d12a5eb3 (April 2024)

### Task 2.7: Refactor DuckDB Engine for Table UDF Registration

**Description:** Update the DuckDB engine to correctly register table UDFs based on the standardized signature. This includes improving error handling for registration failures.

**Files Impacted:**
- `sqlflow/core/engines/duckdb_engine.py`

**Subtasks:**
1. ✅ Modify `register_python_udf` method for `udf_type == 'table'`.
2. ✅ Implement logic to inspect the standardized UDF signature (from metadata captured by the decorator).
3. ✅ Correctly map the UDF signature to DuckDB's API requirements, with fallback mechanisms for different versions.
4. ✅ Implement robust error handling for registration failures with clear error messages.

**Testing Requirements:**
- ✅ Unit tests for `DuckDBEngine.register_python_udf` specifically for table UDFs:
    - Successful registration of valid table UDFs.
    - Handling of UDFs with various keyword arguments.
    - Informative error messages for registration failures.
- ✅ Integration tests verifying table UDFs are correctly callable and executable.

**Definition of Done:**
- ✅ DuckDB engine reliably registers table UDFs adhering to the new standard.
- ✅ Error messages during registration failures are clear and actionable for the user.
- ✅ All related unit and integration tests pass (>90% coverage for changes).

**Status:** ✅ COMPLETED in commit d12a5eb3 (April 2024)

### Task 2.8: Resolve Complex UDF Integration Test

**Description:** Address the issues in the `test_complex_udf_pipeline` integration test, refactor it according to the standardized UDF signatures and improved registration, and ensure it passes reliably.

**Files Impacted:**
- `tests/integration/test_python_udf_execution.py`

**Subtasks:**
1. Analyze the reasons for the original "Test is too complex and needs refactoring" skip.
2. Refactor the `customer_summary` table UDF in `create_udf_file` (within the test setup) to conform to the `DataFrame + kwargs` standard.
3. Update the SQL query in `create_pipeline_file` (within the test setup) that calls `PYTHON_FUNC("sales_analysis.customer_summary", ...)` to pass arguments correctly if needed.
4. Unskip the `test_complex_udf_pipeline` test by removing or commenting out the `@pytest.mark.skip` decorator.
5. Debug and fix any issues until the test passes consistently.
6. Ensure the test adequately covers interaction between scalar and table UDFs within a single pipeline.

**Testing Requirements:**
- The `test_complex_udf_pipeline` must pass without errors.
- The test should validate data transformations through multiple UDFs (scalar and table) and SQL steps.

**Definition of Done:**
- The `@pytest.mark.skip` is removed from `test_complex_udf_pipeline`.
- The test passes reliably in the CI/CD environment.
- The test serves as a robust example of end-to-end UDF functionality.

### Task 2.9: Enhance UDF Unit Testing (Table UDFs & Engine)

**Description:** Augment unit tests for table UDF decorators and DuckDB engine registration to cover more edge cases, signature variations, and error conditions, ensuring higher reliability.

**Files Impacted:**
- `tests/unit/udfs/test_decorators.py`
- Potentially a new `tests/unit/core/engines/test_duckdb_engine_udfs.py` or augment existing engine tests.

**Subtasks:**
1. In `tests/unit/udfs/test_decorators.py`:
    - Add more test cases for `python_table_udf` with complex but valid signatures (e.g., multiple kwargs, type-hinted kwargs).
    - Test error conditions for `python_table_udf` (e.g., UDF not returning a DataFrame, UDF raising exceptions).
2. Create or augment DuckDB engine unit tests:
    - Test registration of table UDFs with different valid parameter setups.
    - Test specific error scenarios during registration (e.g., DuckDB rejecting a type).

**Testing Requirements:**
- Increased line and branch coverage for `sqlflow/udfs/decorators.py` and UDF-related parts of `sqlflow/core/engines/duckdb_engine.py`.
- Tests should cover both successful operations and expected failure modes.

**Definition of Done:**
- Unit test suite for UDF decorators and DuckDB engine registration is more comprehensive.
- Code coverage for the targeted modules is demonstrably increased.
- All new and existing unit tests pass.

### Task 2.10: Improve UDF Discovery & Metadata

**Description:** Enhance UDF discovery to support UDFs in subdirectories within the `python_udfs` folder and ensure accurate, comprehensive metadata (especially signatures) is captured.

**Files Impacted:**
- `sqlflow/udfs/manager.py` (`PythonUDFManager`)

**Subtasks:**
1. Modify `PythonUDFManager.discover_udfs` to search for `*.py` files recursively within the specified `python_udfs_dir`.
2. Adjust UDF naming convention to reflect module paths if UDFs are in subdirectories (e.g., `subdir.module_name.function_name`).
3. Ensure `self.udf_info[udf_name]['signature']` in `discover_udfs` captures a user-friendly and accurate representation of the UDF's signature, including parameter names and type hints (if available via `inspect.signature`).

**Testing Requirements:**
- Unit tests for `PythonUDFManager.discover_udfs` with UDFs placed in direct `python_udfs` folder and in subdirectories.
- Tests verifying that UDFs from subdirectories are correctly named and their metadata (especially signature) is accurately captured.
- Test `sqlflow udf list` and `sqlflow udf info` with UDFs in subdirectories.

**Definition of Done:**
- `PythonUDFManager` correctly discovers and namespaces UDFs from subdirectories.
- UDF metadata stored in `udf_info` (and displayed by CLI) includes accurate and complete signature information.
- All related tests pass.

### Task 2.11: Enhance UDF Lifecycle Error Reporting

**Description:** Improve error reporting across the UDF lifecycle (discovery, registration, execution) to make it more user-friendly, informative, and actionable.

**Files Impacted:**
- `sqlflow/udfs/manager.py`
- `sqlflow/udfs/decorators.py`
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/core/executors/base_executor.py` (and its implementations)

**Subtasks:**
1. **Discovery:** In `PythonUDFManager.discover_udfs`, if a Python file in the `python_udfs` directory (or subdirectories) fails to import, log a clear warning detailing the problematic file path and the import error.
2. **Registration:** Ensure `DuckDBEngine.register_python_udf` (as refined in Task 2.7) throws specific, helpful exceptions if DuckDB rejects a UDF (e.g., `UDFRegistrationError("Failed to register UDF 'my_udf': DuckDB error: ...")`).
3. **Execution:** In `BaseExecutor` (and its implementations like `LocalExecutor`), when a UDF call within `execute_query` (via the engine) raises an exception, catch it and re-raise it or log it with additional context: the UDF name being executed and the original traceback from the UDF.

**Testing Requirements:**
- Test scenarios where a UDF file has syntax errors or missing imports (discovery phase).
- Test scenarios where a correctly discovered UDF fails DuckDB registration due to an incompatible signature or internal DuckDB issue.
- Test scenarios where a registered UDF raises an exception during its execution within a pipeline.
- Verify error messages are clear and point to the source of the problem.

**Definition of Done:**
- Error messages provided to the user at each stage of the UDF lifecycle (discovery, registration, execution) are informative, clear, and help in debugging.
- SQLFlow handles these error conditions gracefully without crashing unexpectedly.

### Task 2.12: Verify and Polish CLI for UDFs

**Description:** Thoroughly verify that UDF CLI commands (`list`, `info`) function correctly with all recent UDF enhancements (recursive discovery, improved metadata) and ensure the output is polished and user-friendly.

**Files Impacted:**
- `sqlflow/cli/commands/udf.py`

**Subtasks:**
1. Test `sqlflow udf list` with a project structure including UDFs in the root `python_udfs` folder and in subdirectories. Verify all are listed with correct namespacing.
2. Test `sqlflow udf info <udf_name>` for UDFs in various locations, ensuring the displayed information (module, name, type, docstring, file path, signature) is accurate and reflects the latest metadata improvements (Task 2.10).
3. Review the output format of both commands for clarity and readability. Ensure signatures are displayed in a helpful way.
4. Test error handling, e.g., `sqlflow udf info non_existent_udf`.

**Testing Requirements:**
- Manual and/or automated CLI tests covering various scenarios.
- Verification of output against known UDF definitions and structures.

**Definition of Done:**
- UDF CLI commands are robust and accurately reflect the state of discoverable UDFs.
- CLI output is well-formatted, user-friendly, and provides all pertinent information, especially accurate UDF signatures.
- CLI commands handle edge cases (e.g., no UDFs found, UDF not found) gracefully.

### Task 2.13: Update UDF Documentation for MVP

**Description:** Revise all Python UDF documentation to be accurate for the MVP state, reflecting standardized signatures, best practices for DuckDB, troubleshooting tips, and performance considerations.

**Files Impacted:**
- `docs/features/python_udfs.md`
- Example UDF files in `examples/python_udfs/`
- Example UDF pipeline files like `examples/udf_demo.sf`

**Subtasks:**
1. Update the "Defining UDFs" section in `docs/features/python_udfs.md` to clearly state the standardized signature for `python_table_udf` (i.e., first argument `pandas.DataFrame`, subsequent arguments ideally as `**kwargs`). Provide clear examples.
2. Revise all code examples for table UDFs in the documentation and in `examples/python_udfs/` to adhere to this new standard.
3. Ensure example pipelines in `examples/` that use table UDFs are updated accordingly.
4. Add a dedicated "Troubleshooting UDFs with DuckDB" section covering common errors (registration failures due to signature, type issues, runtime exceptions in UDFs) and how to debug them.
5. Add a "Performance Considerations" subsection within the UDF documentation, specifically mentioning the potential overhead of frequent data transfer between Python and DuckDB for UDF execution, especially with large datasets.
6. Review and update the "Using UDFs in SQL" section to ensure `PYTHON_FUNC` usage examples are clear and correct for both scalar and table UDFs.

**Testing Requirements:**
- Thorough review of all updated documentation sections for technical accuracy, clarity, and completeness.
- All example code snippets and full example pipelines in the documentation must be tested and confirmed to run correctly with the MVP UDF implementation.

**Definition of Done:**
- UDF documentation accurately reflects the MVP implementation, particularly table UDF signatures.
- Examples are correct, runnable, and demonstrate best practices for MVP.
- Troubleshooting and performance sections provide valuable guidance to users.
- Documentation is consistent, clear, and follows project style guidelines.

### Task 2.14: Review and Refine SQLEngine UDF Interface

**Description:** Conduct a final review of the UDF-related methods in the `SQLEngine` base class (`sqlflow/core/engines/base.py`) to ensure they are well-defined, serve the MVP needs for DuckDB effectively, and provide a solid foundation for future multi-engine support.

**Files Impacted:**
- `sqlflow/core/engines/base.py`

**Subtasks:**
1. Review the signatures and docstrings of `register_python_udf(self, name: str, function: Callable)` and `process_query_for_udfs(self, query: str, udfs: Dict[str, Callable])`.
2. Confirm these methods are sufficiently abstract for other engines while providing necessary functionality for the current DuckDB implementation.
3. Ensure type hints are accurate and comprehensive.
4. Discuss if any additional abstract methods or properties related to UDF capabilities (e.g., type mapping support) might be beneficial for future extensibility, even if not implemented for MVP.

**Testing Requirements:**
- This is primarily a design and code review task. No new tests are directly associated, but the outcome should inform the robustness of the current DuckDB integration and future engine integrations.

**Definition of Done:**
- The `SQLEngine` UDF interface methods are confirmed to be clear, well-documented, and appropriate for the MVP scope (DuckDB).
- Potential considerations for future multi-engine support regarding the UDF interface are noted.

### Task 2.15: Create Basic End-to-End UDF Demo for MVP

**Description:** Develop a simple, easily runnable end-to-end demo pipeline that clearly showcases the MVP capabilities of both scalar and table Python UDFs with the DuckDB engine.

**Files Impacted:**
- New demo pipeline file in `demos/` (e.g., `demos/simple_udf_showcase/pipeline.sf`)
- Associated Python UDF file in `demos/simple_udf_showcase/python_udfs/demo_transforms.py`
- Potentially sample data CSV in `demos/simple_udf_showcase/data/sample_data.csv`

**Subtasks:**
1. Design a small, coherent data processing scenario that benefits from both a scalar UDF and a table UDF.
2. Create the necessary Python UDFs in a `python_udfs` subdirectory for the demo.
    - Example Scalar UDF: e.g., `format_string(text: str) -> str`
    - Example Table UDF: e.g., `add_derived_column(df: pd.DataFrame, new_col_name: str = "derived") -> pd.DataFrame` (using a kwarg).
3. Create a `pipeline.sf` file that:
    - Loads sample data (e.g., from an inline CSV or a small CSV file).
    - Uses the scalar UDF in a `CREATE TABLE AS SELECT ...` statement.
    - Uses the table UDF in a `CREATE TABLE AS SELECT * FROM PYTHON_FUNC(...)` statement.
    - Optionally exports the results or simply allows users to inspect intermediate tables.
4. Ensure the demo is well-commented, explaining each step and UDF usage.
5. Add a brief README within the demo directory explaining how to run it and what it demonstrates.

**Testing Requirements:**
- The demo pipeline must run successfully from start to finish using the SQLFlow CLI.
- The output or intermediate tables produced by the demo must match expected results.

**Definition of Done:**
- A self-contained, runnable UDF demo pipeline is available in the `demos/` directory.
- The demo clearly illustrates the definition and usage of both scalar and table UDFs for MVP.
- The demo is easy to understand and serves as a good starting point for new users.

### Task 3.1: Parser Updates for Load Modes

**Description:** Extend the SQLFlow parser to support MODE parameter in LOAD and CREATE TABLE statements.

**Files Impacted:**
- `sqlflow/parser/lexer.py`
- `sqlflow/parser/ast.py`
- `sqlflow/parser/parser.py`

**Subtasks:**
1. Add new token types for MODE and related keywords (REPLACE, APPEND, MERGE)
2. Extend AST node structure for LOAD directive to include mode information
3. Update parser to recognize and parse mode syntax
4. Add support for merge key specification for MERGE mode

**Testing Requirements:**
- Test parsing of LOAD statements with different modes
- Test parsing of CREATE TABLE AS statements with modes
- Test merge key specification syntax
- Test error handling for invalid mode specifications
- Test case insensitivity for mode keywords

**Definition of Done:**
- Parser correctly identifies and processes all mode types
- AST representation includes mode and relevant parameters
- Validation logic ensures required parameters (e.g., merge keys for MERGE mode)
- All tests passing with >90% coverage
- Code follows project style guidelines

### Task 3.2: SQL Generator for Load Modes

**Description:** Implement SQL generation strategies for different load modes in SQLEngine implementations.

**Files Impacted:**
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`
- Other engine implementations

**Subtasks:**
1. Extend SQLEngine interface to support different load modes
2. Implement REPLACE mode SQL generation (CREATE OR REPLACE TABLE)
3. Implement APPEND mode SQL generation (INSERT INTO)
4. Implement MERGE mode SQL generation (INSERT INTO ON CONFLICT DO UPDATE)
5. Add transaction handling for modes that require it

**Testing Requirements:**
- Test SQL generation for each mode with various scenarios
- Test transaction handling and atomicity
- Test error handling for each mode
- Test engine-specific optimizations
- Test with edge cases (empty data, schema mismatches)

**Definition of Done:**
- Each mode generates correct SQL for its target engine
- Operations are transactional where required
- Generated SQL is optimized for the target engine
- Error cases are properly handled
- All tests passing with >90% coverage
- Documentation updated to reflect implementation details

### Task 3.3: Schema Compatibility Validation

**Description:** Implement validation logic to ensure schema compatibility for different load modes, especially for APPEND and MERGE.

**Files Impacted:**
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/core/executors/base_executor.py`

**Subtasks:**
1. Implement schema comparison logic for compatibility checks
2. Add pre-execution validation for APPEND mode
3. Add pre-execution validation for MERGE mode
4. Implement clear error reporting for schema incompatibilities
5. Add configuration for validation strictness

**Testing Requirements:**
- Test schema compatibility validation with matching schemas
- Test with schema mismatches (column types, nullability)
- Test with subset/superset schemas
- Test error messages clarity
- Test with different validation strictness settings

**Definition of Done:**
- Schema compatibility is correctly validated before execution
- Clear error messages explain incompatibilities
- Validation logic respects configuration settings
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 3.4: Merge Key Handling

**Description:** Implement special handling for merge keys in MERGE mode, including validation and SQL generation.

**Files Impacted:**
- `sqlflow/parser/parser.py`
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`

**Subtasks:**
1. Extend parse logic to capture merge key specifications
2. Implement validation to ensure merge keys exist in both source and target
3. Add type compatibility checking for merge keys
4. Generate optimized merge SQL based on key specifications
5. Implement clear error reporting for merge key issues

**Testing Requirements:**
- Test merge key parsing with various syntax forms
- Test validation with valid and invalid key specifications
- Test SQL generation with different key combinations
- Test error handling for missing or incompatible keys
- Test with composite (multi-column) keys

**Definition of Done:**
- Merge keys are correctly parsed and validated
- Generated SQL properly uses merge keys for matching
- Clear error messages for key-related issues
- Performance optimized for the merge operation
- All tests passing with >90% coverage
- Documentation updated with merge key examples

### Task 3.5: Load Mode Documentation

**Description:** Create comprehensive documentation for load modes and their usage.

**Files Impacted:**
- `docs/load_modes.md` (new)
- `examples/load_modes/` (new directory)

**Subtasks:**
1. Document REPLACE mode with examples
2. Document APPEND mode with examples
3. Document MERGE mode with merge key syntax
4. Create example pipelines for each mode
5. Document best practices and common patterns

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test all example pipelines to ensure they run correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers edge cases and error scenarios

**Definition of Done:**
- Documentation clearly explains each load mode
- Examples demonstrate practical use cases
- Best practices and limitations are documented
- Example pipelines are functional and well-commented
- Documentation follows project style guidelines

### Task 4.1: Connector Interface Design

**Description:** Design and implement the core Connector interface for SQLFlow.

**Files Impacted:**
- `sqlflow/connectors/base.py`
- `sqlflow/connectors/connector_engine.py`

**Subtasks:**
1. Define the Connector abstract base class with core methods
2. Implement error handling and retry mechanisms
3. Define data exchange formats and protocols
4. Design parameter validation and configuration mechanisms
5. Implement connection testing functionality

**Testing Requirements:**
- Test interface with mock implementations
- Test error handling with simulated failures
- Test retry logic with transient errors
- Test parameter validation
- Test resource management (connection opening/closing)

**Definition of Done:**
- Connector interface is clearly defined with appropriate methods
- Error handling is robust and informative
- Resource management is properly implemented
- Documentation is complete with usage examples
- Tests verify interface contract compliance
- All tests passing with >90% coverage

### Task 4.2: PostgreSQL Connector Implementation

**Description:** Implement a robust PostgreSQL connector for reading data and schema information.

**Files Impacted:**
- `sqlflow/connectors/postgres_connector.py`

**Subtasks:**
1. Implement connection management with pooling
2. Implement schema retrieval using INFORMATION_SCHEMA
3. Implement data reading with efficient batching
4. Add query result parsing and data type mapping
5. Implement error handling specific to PostgreSQL

**Testing Requirements:**
- Test connection to PostgreSQL with various configurations
- Test schema retrieval for tables
- Test data reading with different queries
- Test error handling with various PostgreSQL error scenarios
- Test performance with large datasets
- Test resource cleanup

**Definition of Done:**
- PostgreSQL connector reliably connects and retrieves data
- Schema information is correctly extracted
- Data is efficiently streamed in batches
- Error messages are clear and actionable
- Connection resources are properly managed
- All tests passing with >90% coverage
- Documentation updated with PostgreSQL connection examples

### Task 4.3: S3 Connector Implementation

**Description:** Implement S3 connector for reading and writing CSV and Parquet files.

**Files Impacted:**
- `sqlflow/connectors/s3_connector.py`
- `sqlflow/connectors/csv_connector.py`
- `sqlflow/connectors/parquet_connector.py`

**Subtasks:**
1. Implement S3 authentication and connection management
2. Implement CSV file reading with schema inference
3. Implement Parquet file reading with metadata extraction
4. Implement CSV and Parquet file writing to S3
5. Add error handling and retry logic specific to S3
6. Implement efficient streaming for large files

**Testing Requirements:**
- Test S3 authentication with various methods
- Test reading CSV files with different formats
- Test reading Parquet files with complex schemas
- Test writing data to S3 in different formats
- Test error handling with S3-specific errors
- Test performance with large files
- Test with various AWS regions

**Definition of Done:**
- S3 connector reliably reads and writes data
- Schema inference works correctly for both formats
- Data is efficiently streamed in both directions
- Error messages are clear and actionable
- Connection resources are properly managed
- All tests passing with >90% coverage
- Documentation updated with S3 connection examples

### Task 4.4: Connector Registry and Discovery

**Description:** Implement a registry system to discover and manage connectors.

**Files Impacted:**
- `sqlflow/connectors/registry.py`

**Subtasks:**
1. Design connector registration mechanism
2. Implement connector discovery logic
3. Create type-based connector lookup
4. Add parameter validation for connector types
5. Implement plugin architecture for custom connectors

**Testing Requirements:**
- Test registration of built-in connectors
- Test discovery of all available connectors
- Test type-based lookup with various patterns
- Test parameter validation for different connector types
- Test error handling for missing or invalid connectors

**Definition of Done:**
- Registry correctly manages all available connectors
- Type-based lookup works reliably
- Parameters are validated before connector creation
- Error messages are clear for missing or invalid connectors
- Plugin architecture is documented
- All tests passing with >90% coverage
- Documentation updated with connector registry usage

### Task 4.5: Connector Documentation

**Description:** Create comprehensive documentation for using connectors in SQLFlow.

**Files Impacted:**
- `docs/connectors/` (new directory)
- `examples/connectors/` (new directory)

**Subtasks:**
1. Document connector interface and common patterns
2. Create PostgreSQL connector documentation with examples
3. Create S3 connector documentation with examples
4. Document error handling and retry mechanisms
5. Create example pipelines using various connectors

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test all example pipelines to ensure they run correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers common error scenarios and their resolution

**Definition of Done:**
- Documentation clearly explains connector usage
- Examples demonstrate practical use cases
- Common error scenarios and their resolution are documented
- Example pipelines are functional and well-commented
- Documentation follows project style guidelines

### Task 5.1: Schema Representation and Storage

**Description:** Design and implement schema representation and storage mechanisms.

**Files Impacted:**
- `sqlflow/core/schema.py` (new)
- `sqlflow/core/storage/schema_storage.py` (new)

**Subtasks:**
1. Define schema representation classes (Schema, ColumnSchema)
2. Implement serialization/deserialization for schemas
3. Create schema storage mechanism (file-based for MVP)
4. Implement schema fingerprinting for quick comparison
5. Add metadata tracking (creation time, source, etc.)

**Testing Requirements:**
- Test schema representation with various column types
- Test serialization and deserialization
- Test storage and retrieval of schemas
- Test fingerprinting with schema changes
- Test with large schemas
- Test filesystem interactions

**Definition of Done:**
- Schema representation is comprehensive and extensible
- Serialization/deserialization works correctly
- Storage mechanism is reliable and efficient
- Fingerprinting correctly identifies schema changes
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 5.2: Schema Comparison Algorithm

**Description:** Implement algorithm to compare schemas and identify differences.

**Files Impacted:**
- `sqlflow/core/schema.py`

**Subtasks:**
1. Implement comprehensive schema comparison algorithm
2. Add detection for added and removed columns
3. Add detection for type changes and nullable changes
4. Implement detection for position changes
5. Create structured difference report format
6. Add compatibility assessment based on change types

**Testing Requirements:**
- Test comparison with identical schemas
- Test with added/removed columns
- Test with type changes (compatible and incompatible)
- Test with nullability changes
- Test with position changes
- Test with multiple simultaneous changes
- Test performance with large schemas

**Definition of Done:**
- Comparison algorithm correctly identifies all types of changes
- Difference reports are clear and structured
- Compatibility assessment is accurate
- Algorithm performs efficiently even with large schemas
- All tests passing with >90% coverage
- Documentation clearly explains comparison results

### Task 5.3: Schema Drift Detection Integration

**Description:** Integrate schema drift detection into SQLFlow's execution flow.

**Files Impacted:**
- `sqlflow/connectors/connector_engine.py`
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/planner.py`

**Subtasks:**
1. Add schema capture during initial execution
2. Implement schema comparison before subsequent executions
3. Add configuration for drift detection behavior (alert, permissive)
4. Implement clear error reporting for schema drift
5. Add hooks for schema evolution handling

**Testing Requirements:**
- Test initial schema capture and storage
- Test detection of various schema changes
- Test behavior with different configuration settings
- Test error reporting for schema drift
- Test with real-world schema evolution scenarios
- Test integration with pipeline execution flow

**Definition of Done:**
- Schema drift detection is properly integrated
- Configuration options control behavior as expected
- Error reporting is clear and actionable
- Detection doesn't significantly impact performance
- All tests passing with >90% coverage
- Documentation updated with drift detection information

### Task 5.4: CLI Commands for Schema Management

**Description:** Implement CLI commands for managing and inspecting schemas.

**Files Impacted:**
- `sqlflow/cli/commands/schema.py` (new)
- `sqlflow/cli/main.py`

**Subtasks:**
1. Implement `schema show` command to display current schema
2. Implement `schema history` command to show schema changes
3. Implement `schema accept` command to accept current schema
4. Implement `schema compare` command for manual comparison
5. Add detailed output formatting for schema information

**Testing Requirements:**
- Test each command with various schemas
- Test with valid and invalid pipeline/source combinations
- Test output formatting and readability
- Test command error handling
- Test shell completion for commands

**Definition of Done:**
- CLI commands correctly manage and display schema information
- Output is well-formatted and readable
- Error handling is robust
- Commands are documented in help system
- All tests passing with >90% coverage
- Documentation updated with CLI command examples

### Task 5.5: Schema Management Documentation

**Description:** Create comprehensive documentation for schema management features.

**Files Impacted:**
- `docs/schema_management.md` (new)
- `examples/schema_management/` (new directory)

**Subtasks:**
1. Document schema representation and storage
2. Document drift detection and configuration options
3. Document CLI commands for schema management
4. Create workflow examples for handling schema changes
5. Document best practices for schema evolution

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test all example workflows to ensure they work correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers common schema evolution scenarios

**Definition of Done:**
- Documentation clearly explains schema management features
- Examples demonstrate practical use cases
- Best practices provide clear guidance
- Example workflows are functional and well-commented
- Documentation follows project style guidelines

### Task 6.1: DAG Execution Enhancements

**Description:** Enhance the DAG execution model to improve reliability and control.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/dag.py` (new or enhanced)

**Subtasks:**
1. Refine DAG construction from pipeline steps
2. Implement dependency tracking and validation
3. Add cycle detection and prevention
4. Implement execution order determination
5. Add hooks for execution lifecycle events

**Testing Requirements:**
- Test DAG construction with various pipelines
- Test dependency tracking with complex relationships
- Test cycle detection with various graph patterns
- Test execution order determination
- Test with edge cases (empty pipelines, single node)

**Definition of Done:**
- DAG representation correctly captures dependencies
- Cycles are detected and prevented
- Execution order is correctly determined
- Hooks allow for extensibility
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 6.2: Parallel Execution Implementation

**Description:** Implement parallel execution of independent tasks for improved performance.

**Files Impacted:**
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Implement ThreadPoolExecutor with configurable thread count
2. Add task queue management and scheduling
3. Implement dependency-aware task submission
4. Add progress tracking for parallel tasks
5. Implement resource management and limits

**Testing Requirements:**
- Test parallel execution with independent tasks
- Test dependency handling during parallel execution
- Test with varying thread pool sizes
- Test progress tracking accuracy
- Test resource management with memory-intensive tasks
- Test error handling during parallel execution

**Definition of Done:**
- Independent tasks execute in parallel
- Dependencies are correctly respected
- Thread pool size is configurable
- Progress is accurately tracked
- Resources are properly managed
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 6.3: Failure Handling and Recovery

**Description:** Implement robust failure handling and recovery mechanisms.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/errors.py`

**Subtasks:**
1. Define failure policies (FAIL_FAST, CONTINUE)
2. Implement task state tracking
3. Add failure propagation to dependent tasks
4. Implement partial pipeline execution
5. Create detailed error context collection
6. Add recovery mechanisms for transient failures

**Testing Requirements:**
- Test behavior with different failure policies
- Test failure propagation to dependent tasks
- Test partial pipeline execution after failures
- Test error context collection
- Test recovery from transient failures
- Test with various error scenarios

**Definition of Done:**
- Failure policies behave as expected
- Task state is correctly tracked and updated
- Dependent tasks are handled according to policy
- Error contexts provide useful debugging information
- Recovery mechanisms work for supported error types
- All tests passing with >90% coverage
- Documentation clearly explains failure handling

### Task 6.4: Execution Status Tracking

**Description:** Implement comprehensive execution status tracking and reporting.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/status.py` (new)
- `sqlflow/cli/commands/status.py` (new)

**Subtasks:**
1. Define execution status model (PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED)
2. Implement status updates during execution
3. Create status persistence mechanism
4. Add real-time status reporting in console
5. Implement CLI commands for status queries
6. Create detailed execution logs

**Testing Requirements:**
- Test status updates through execution lifecycle
- Test status persistence and retrieval
- Test console reporting accuracy
- Test CLI status commands
- Test log generation and format
- Test with various execution scenarios

**Definition of Done:**
- Status is accurately tracked through execution
- Status updates are persisted and retrievable
- Console reporting provides clear visibility
- CLI commands provide useful status information
- Logs contain comprehensive execution details
- All tests passing with >90% coverage
- Documentation explains status tracking features

### Task 6.5: Execution Metrics Collection

**Description:** Implement collection of execution metrics for monitoring and optimization.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/metrics.py` (new)

**Subtasks:**
1. Define key metrics for collection (execution time, records processed, etc.)
2. Implement metrics collection during execution
3. Add metrics storage mechanism
4. Create metrics reporting capabilities
5. Implement historical metrics comparison
6. Add export capabilities for external monitoring

**Testing Requirements:**
- Test metrics collection accuracy
- Test metrics storage and retrieval
- Test reporting output format
- Test historical comparison functionality
- Test with various execution patterns
- Test export formats

**Definition of Done:**
- Relevant metrics are accurately collected
- Metrics are properly stored and retrievable
- Reporting provides useful insights
- Historical comparison helps identify trends
- Export formats are compatible with monitoring tools
- All tests passing with >90% coverage
- Documentation explains available metrics and their usage

### Task 2.16: Implement DuckDB Version Compatibility

**Description:** Create a version-aware implementation for table UDF registration that works across different DuckDB versions (1.x and 2.x), providing a consistent user experience regardless of the underlying DuckDB version.

**Files Impacted:**
- `sqlflow/core/engines/duckdb_engine.py`

**Subtasks:**
1. Add DuckDB version detection at connection time
2. Implement version-specific UDF registration logic for table functions
3. Design fallback mechanisms for older DuckDB versions
4. Create comprehensive error handling for version-specific API failures
5. Add tests with different DuckDB version compatibility scenarios

**Testing Requirements:**
- Test UDF registration with multiple DuckDB versions (1.x and 2.x)
- Test fallback mechanisms when preferred APIs are unavailable
- Test error reporting under version compatibility issues
- Verify consistent behavior across versions for identical UDFs

**Definition of Done:**
- SQLFlow works consistently with different DuckDB versions
- Version detection and API selection is transparent to users
- Error messages provide clear guidance when version incompatibilities occur
- All tests pass with multiple DuckDB versions
- Documentation includes version compatibility notes

### Task 2.17: Optimize Type Mapping for UDFs

**Description:** Enhance the type mapping between Python and DuckDB to improve UDF robustness and performance, addressing edge cases and ensuring precise type conversions.

**Files Impacted:**
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/udfs/decorators.py`

**Subtasks:**
1. Create comprehensive type mapping between Python and DuckDB types
2. Implement special handling for complex types (JSON, arrays, timestamps)
3. Add proper handling for NULL values and NaN in numeric types
4. Optimize DataFrame conversions for large tables
5. Add validation for unsupported types with clear error messages

**Testing Requirements:**
- Test with various Python data types including dates, times, complex objects
- Test edge cases like NaN, NULL, empty strings, and extreme values
- Test performance with large datasets and type conversions
- Verify error messages for unsupported type conversions

**Definition of Done:**
- Type conversions are reliable across all supported data types
- Edge cases are handled gracefully with clear error messages
- Large dataset performance is optimized for type conversions
- All tests pass with comprehensive type coverage
- Documentation includes type compatibility matrix

### Task 2.18: Performance Profiling and Optimization

**Description:** Profile UDF execution performance across various scenarios and implement optimizations to reduce overhead, particularly for table UDFs handling large datasets.

**Files Impacted:**
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/udfs/decorators.py`
- `sqlflow/core/executors/base_executor.py`

**Subtasks:**
1. Create benchmarking suite for UDF performance measurement
2. Profile data transfer overhead between DuckDB and Python
3. Implement chunking strategies for large DataFrame handling
4. Add caching mechanisms for frequently called UDFs
5. Optimize UDF registration to minimize overhead

**Testing Requirements:**
- Benchmark UDF performance with various dataset sizes
- Compare optimized vs. unoptimized implementations
- Test memory usage during large dataset processing
- Verify correctness of optimized implementations

**Definition of Done:**
- Benchmarking shows measurable performance improvements
- Large dataset handling is memory-efficient
- Optimizations don't sacrifice UDF functionality or reliability
- Documentation includes performance best practices
- All tests pass with performance assertions

### Task 3.1: Parser Updates for Load Modes

**Description:** Extend the SQLFlow parser to support MODE parameter in LOAD and CREATE TABLE statements.

**Files Impacted:**
- `sqlflow/parser/lexer.py`
- `sqlflow/parser/ast.py`
- `sqlflow/parser/parser.py`

**Subtasks:**
1. Add new token types for MODE and related keywords (REPLACE, APPEND, MERGE)
2. Extend AST node structure for LOAD directive to include mode information
3. Update parser to recognize and parse mode syntax
4. Add support for merge key specification for MERGE mode

**Testing Requirements:**
- Test parsing of LOAD statements with different modes
- Test parsing of CREATE TABLE AS statements with modes
- Test merge key specification syntax
- Test error handling for invalid mode specifications
- Test case insensitivity for mode keywords

**Definition of Done:**
- Parser correctly identifies and processes all mode types
- AST representation includes mode and relevant parameters
- Validation logic ensures required parameters (e.g., merge keys for MERGE mode)
- All tests passing with >90% coverage
- Code follows project style guidelines

### Task 3.2: SQL Generator for Load Modes

**Description:** Implement SQL generation strategies for different load modes in SQLEngine implementations.

**Files Impacted:**
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`
- Other engine implementations

**Subtasks:**
1. Extend SQLEngine interface to support different load modes
2. Implement REPLACE mode SQL generation (CREATE OR REPLACE TABLE)
3. Implement APPEND mode SQL generation (INSERT INTO)
4. Implement MERGE mode SQL generation (INSERT INTO ON CONFLICT DO UPDATE)
5. Add transaction handling for modes that require it

**Testing Requirements:**
- Test SQL generation for each mode with various scenarios
- Test transaction handling and atomicity
- Test error handling for each mode
- Test engine-specific optimizations
- Test with edge cases (empty data, schema mismatches)

**Definition of Done:**
- Each mode generates correct SQL for its target engine
- Operations are transactional where required
- Generated SQL is optimized for the target engine
- Error cases are properly handled
- All tests passing with >90% coverage
- Documentation updated to reflect implementation details

### Task 3.3: Schema Compatibility Validation

**Description:** Implement validation logic to ensure schema compatibility for different load modes, especially for APPEND and MERGE.

**Files Impacted:**
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/core/executors/base_executor.py`

**Subtasks:**
1. Implement schema comparison logic for compatibility checks
2. Add pre-execution validation for APPEND mode
3. Add pre-execution validation for MERGE mode
4. Implement clear error reporting for schema incompatibilities
5. Add configuration for validation strictness

**Testing Requirements:**
- Test schema compatibility validation with matching schemas
- Test with schema mismatches (column types, nullability)
- Test with subset/superset schemas
- Test error messages clarity
- Test with different validation strictness settings

**Definition of Done:**
- Schema compatibility is correctly validated before execution
- Clear error messages explain incompatibilities
- Validation logic respects configuration settings
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 3.4: Merge Key Handling

**Description:** Implement special handling for merge keys in MERGE mode, including validation and SQL generation.

**Files Impacted:**
- `sqlflow/parser/parser.py`
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`

**Subtasks:**
1. Extend parse logic to capture merge key specifications
2. Implement validation to ensure merge keys exist in both source and target
3. Add type compatibility checking for merge keys
4. Generate optimized merge SQL based on key specifications
5. Implement clear error reporting for merge key issues

**Testing Requirements:**
- Test merge key parsing with various syntax forms
- Test validation with valid and invalid key specifications
- Test SQL generation with different key combinations
- Test error handling for missing or incompatible keys
- Test with composite (multi-column) keys

**Definition of Done:**
- Merge keys are correctly parsed and validated
- Generated SQL properly uses merge keys for matching
- Clear error messages for key-related issues
- Performance optimized for the merge operation
- All tests passing with >90% coverage
- Documentation updated with merge key examples

### Task 3.5: Load Mode Documentation

**Description:** Create comprehensive documentation for load modes and their usage.

**Files Impacted:**
- `docs/load_modes.md` (new)
- `examples/load_modes/` (new directory)

**Subtasks:**
1. Document REPLACE mode with examples
2. Document APPEND mode with examples
3. Document MERGE mode with merge key syntax
4. Create example pipelines for each mode
5. Document best practices and common patterns

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test all example pipelines to ensure they run correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers edge cases and error scenarios

**Definition of Done:**
- Documentation clearly explains each load mode
- Examples demonstrate practical use cases
- Best practices and limitations are documented
- Example pipelines are functional and well-commented
- Documentation follows project style guidelines

### Task 4.1: Connector Interface Design

**Description:** Design and implement the core Connector interface for SQLFlow.

**Files Impacted:**
- `sqlflow/connectors/base.py`
- `sqlflow/connectors/connector_engine.py`

**Subtasks:**
1. Define the Connector abstract base class with core methods
2. Implement error handling and retry mechanisms
3. Define data exchange formats and protocols
4. Design parameter validation and configuration mechanisms
5. Implement connection testing functionality

**Testing Requirements:**
- Test interface with mock implementations
- Test error handling with simulated failures
- Test retry logic with transient errors
- Test parameter validation
- Test resource management (connection opening/closing)

**Definition of Done:**
- Connector interface is clearly defined with appropriate methods
- Error handling is robust and informative
- Resource management is properly implemented
- Documentation is complete with usage examples
- Tests verify interface contract compliance
- All tests passing with >90% coverage

### Task 4.2: PostgreSQL Connector Implementation

**Description:** Implement a robust PostgreSQL connector for reading data and schema information.

**Files Impacted:**
- `sqlflow/connectors/postgres_connector.py`

**Subtasks:**
1. Implement connection management with pooling
2. Implement schema retrieval using INFORMATION_SCHEMA
3. Implement data reading with efficient batching
4. Add query result parsing and data type mapping
5. Implement error handling specific to PostgreSQL

**Testing Requirements:**
- Test connection to PostgreSQL with various configurations
- Test schema retrieval for tables
- Test data reading with different queries
- Test error handling with various PostgreSQL error scenarios
- Test performance with large datasets
- Test resource cleanup

**Definition of Done:**
- PostgreSQL connector reliably connects and retrieves data
- Schema information is correctly extracted
- Data is efficiently streamed in batches
- Error messages are clear and actionable
- Connection resources are properly managed
- All tests passing with >90% coverage
- Documentation updated with PostgreSQL connection examples

### Task 4.3: S3 Connector Implementation

**Description:** Implement S3 connector for reading and writing CSV and Parquet files.

**Files Impacted:**
- `sqlflow/connectors/s3_connector.py`
- `sqlflow/connectors/csv_connector.py`
- `sqlflow/connectors/parquet_connector.py`

**Subtasks:**
1. Implement S3 authentication and connection management
2. Implement CSV file reading with schema inference
3. Implement Parquet file reading with metadata extraction
4. Implement CSV and Parquet file writing to S3
5. Add error handling and retry logic specific to S3
6. Implement efficient streaming for large files

**Testing Requirements:**
- Test S3 authentication with various methods
- Test reading CSV files with different formats
- Test reading Parquet files with complex schemas
- Test writing data to S3 in different formats
- Test error handling with S3-specific errors
- Test performance with large files
- Test with various AWS regions

**Definition of Done:**
- S3 connector reliably reads and writes data
- Schema inference works correctly for both formats
- Data is efficiently streamed in both directions
- Error messages are clear and actionable
- Connection resources are properly managed
- All tests passing with >90% coverage
- Documentation updated with S3 connection examples

### Task 4.4: Connector Registry and Discovery

**Description:** Implement a registry system to discover and manage connectors.

**Files Impacted:**
- `sqlflow/connectors/registry.py`

**Subtasks:**
1. Design connector registration mechanism
2. Implement connector discovery logic
3. Create type-based connector lookup
4. Add parameter validation for connector types
5. Implement plugin architecture for custom connectors

**Testing Requirements:**
- Test registration of built-in connectors
- Test discovery of all available connectors
- Test type-based lookup with various patterns
- Test parameter validation for different connector types
- Test error handling for missing or invalid connectors

**Definition of Done:**
- Registry correctly manages all available connectors
- Type-based lookup works reliably
- Parameters are validated before connector creation
- Error messages are clear for missing or invalid connectors
- Plugin architecture is documented
- All tests passing with >90% coverage
- Documentation updated with connector registry usage

### Task 4.5: Connector Documentation

**Description:** Create comprehensive documentation for using connectors in SQLFlow.

**Files Impacted:**
- `docs/connectors/` (new directory)
- `examples/connectors/` (new directory)

**Subtasks:**
1. Document connector interface and common patterns
2. Create PostgreSQL connector documentation with examples
3. Create S3 connector documentation with examples
4. Document error handling and retry mechanisms
5. Create example pipelines using various connectors

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test all example pipelines to ensure they run correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers common error scenarios and their resolution

**Definition of Done:**
- Documentation clearly explains connector usage
- Examples demonstrate practical use cases
- Common error scenarios and their resolution are documented
- Example pipelines are functional and well-commented
- Documentation follows project style guidelines

### Task 5.1: Schema Representation and Storage

**Description:** Design and implement schema representation and storage mechanisms.

**Files Impacted:**
- `sqlflow/core/schema.py` (new)
- `sqlflow/core/storage/schema_storage.py` (new)

**Subtasks:**
1. Define schema representation classes (Schema, ColumnSchema)
2. Implement serialization/deserialization for schemas
3. Create schema storage mechanism (file-based for MVP)
4. Implement schema fingerprinting for quick comparison
5. Add metadata tracking (creation time, source, etc.)

**Testing Requirements:**
- Test schema representation with various column types
- Test serialization and deserialization
- Test storage and retrieval of schemas
- Test fingerprinting with schema changes
- Test with large schemas
- Test filesystem interactions

**Definition of Done:**
- Schema representation is comprehensive and extensible
- Serialization/deserialization works correctly
- Storage mechanism is reliable and efficient
- Fingerprinting correctly identifies schema changes
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 5.2: Schema Comparison Algorithm

**Description:** Implement algorithm to compare schemas and identify differences.

**Files Impacted:**
- `sqlflow/core/schema.py`

**Subtasks:**
1. Implement comprehensive schema comparison algorithm
2. Add detection for added and removed columns
3. Add detection for type changes and nullable changes
4. Implement detection for position changes
5. Create structured difference report format
6. Add compatibility assessment based on change types

**Testing Requirements:**
- Test comparison with identical schemas
- Test with added/removed columns
- Test with type changes (compatible and incompatible)
- Test with nullability changes
- Test with position changes
- Test with multiple simultaneous changes
- Test performance with large schemas

**Definition of Done:**
- Comparison algorithm correctly identifies all types of changes
- Difference reports are clear and structured
- Compatibility assessment is accurate
- Algorithm performs efficiently even with large schemas
- All tests passing with >90% coverage
- Documentation clearly explains comparison results

### Task 5.3: Schema Drift Detection Integration

**Description:** Integrate schema drift detection into SQLFlow's execution flow.

**Files Impacted:**
- `sqlflow/connectors/connector_engine.py`
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/planner.py`

**Subtasks:**
1. Add schema capture during initial execution
2. Implement schema comparison before subsequent executions
3. Add configuration for drift detection behavior (alert, permissive)
4. Implement clear error reporting for schema drift
5. Add hooks for schema evolution handling

**Testing Requirements:**
- Test initial schema capture and storage
- Test detection of various schema changes
- Test behavior with different configuration settings
- Test error reporting for schema drift
- Test with real-world schema evolution scenarios
- Test integration with pipeline execution flow

**Definition of Done:**
- Schema drift detection is properly integrated
- Configuration options control behavior as expected
- Error reporting is clear and actionable
- Detection doesn't significantly impact performance
- All tests passing with >90% coverage
- Documentation updated with drift detection information

### Task 5.4: CLI Commands for Schema Management

**Description:** Implement CLI commands for managing and inspecting schemas.

**Files Impacted:**
- `sqlflow/cli/commands/schema.py` (new)
- `sqlflow/cli/main.py`

**Subtasks:**
1. Implement `schema show` command to display current schema
2. Implement `schema history` command to show schema changes
3. Implement `schema accept` command to accept current schema
4. Implement `schema compare` command for manual comparison
5. Add detailed output formatting for schema information

**Testing Requirements:**
- Test each command with various schemas
- Test with valid and invalid pipeline/source combinations
- Test output formatting and readability
- Test command error handling
- Test shell completion for commands

**Definition of Done:**
- CLI commands correctly manage and display schema information
- Output is well-formatted and readable
- Error handling is robust
- Commands are documented in help system
- All tests passing with >90% coverage
- Documentation updated with CLI command examples

### Task 5.5: Schema Management Documentation

**Description:** Create comprehensive documentation for schema management features.

**Files Impacted:**
- `docs/schema_management.md` (new)
- `examples/schema_management/` (new directory)

**Subtasks:**
1. Document schema representation and storage
2. Document drift detection and configuration options
3. Document CLI commands for schema management
4. Create workflow examples for handling schema changes
5. Document best practices for schema evolution

**Testing Requirements:**
- Review documentation for clarity and accuracy
- Test all example workflows to ensure they work correctly
- Verify all syntax and examples are consistent with implementation
- Ensure documentation covers common schema evolution scenarios

**Definition of Done:**
- Documentation clearly explains schema management features
- Examples demonstrate practical use cases
- Best practices provide clear guidance
- Example workflows are functional and well-commented
- Documentation follows project style guidelines

### Task 6.1: DAG Execution Enhancements

**Description:** Enhance the DAG execution model to improve reliability and control.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/dag.py` (new or enhanced)

**Subtasks:**
1. Refine DAG construction from pipeline steps
2. Implement dependency tracking and validation
3. Add cycle detection and prevention
4. Implement execution order determination
5. Add hooks for execution lifecycle events

**Testing Requirements:**
- Test DAG construction with various pipelines
- Test dependency tracking with complex relationships
- Test cycle detection with various graph patterns
- Test execution order determination
- Test with edge cases (empty pipelines, single node)

**Definition of Done:**
- DAG representation correctly captures dependencies
- Cycles are detected and prevented
- Execution order is correctly determined
- Hooks allow for extensibility
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 6.2: Parallel Execution Implementation

**Description:** Implement parallel execution of independent tasks for improved performance.

**Files Impacted:**
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Implement ThreadPoolExecutor with configurable thread count
2. Add task queue management and scheduling
3. Implement dependency-aware task submission
4. Add progress tracking for parallel tasks
5. Implement resource management and limits

**Testing Requirements:**
- Test parallel execution with independent tasks
- Test dependency handling during parallel execution
- Test with varying thread pool sizes
- Test progress tracking accuracy
- Test resource management with memory-intensive tasks
- Test error handling during parallel execution

**Definition of Done:**
- Independent tasks execute in parallel
- Dependencies are correctly respected
- Thread pool size is configurable
- Progress is accurately tracked
- Resources are properly managed
- All tests passing with >90% coverage
- Implementation follows project style guidelines

### Task 6.3: Failure Handling and Recovery

**Description:** Implement robust failure handling and recovery mechanisms.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/errors.py`

**Subtasks:**
1. Define failure policies (FAIL_FAST, CONTINUE)
2. Implement task state tracking
3. Add failure propagation to dependent tasks
4. Implement partial pipeline execution
5. Create detailed error context collection
6. Add recovery mechanisms for transient failures

**Testing Requirements:**
- Test behavior with different failure policies
- Test failure propagation to dependent tasks
- Test partial pipeline execution after failures
- Test error context collection
- Test recovery from transient failures
- Test with various error scenarios

**Definition of Done:**
- Failure policies behave as expected
- Task state is correctly tracked and updated
- Dependent tasks are handled according to policy
- Error contexts provide useful debugging information
- Recovery mechanisms work for supported error types
- All tests passing with >90% coverage
- Documentation clearly explains failure handling

### Task 6.4: Execution Status Tracking

**Description:** Implement comprehensive execution status tracking and reporting.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/status.py` (new)
- `sqlflow/cli/commands/status.py` (new)

**Subtasks:**
1. Define execution status model (PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED)
2. Implement status updates during execution
3. Create status persistence mechanism
4. Add real-time status reporting in console
5. Implement CLI commands for status queries
6. Create detailed execution logs

**Testing Requirements:**
- Test status updates through execution lifecycle
- Test status persistence and retrieval
- Test console reporting accuracy
- Test CLI status commands
- Test log generation and format
- Test with various execution scenarios

**Definition of Done:**
- Status is accurately tracked through execution
- Status updates are persisted and retrievable
- Console reporting provides clear visibility
- CLI commands provide useful status information
- Logs contain comprehensive execution details
- All tests passing with >90% coverage
- Documentation explains status tracking features

### Task 6.5: Execution Metrics Collection

**Description:** Implement collection of execution metrics for monitoring and optimization.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/metrics.py` (new)

**Subtasks:**
1. Define key metrics for collection (execution time, records processed, etc.)
2. Implement metrics collection during execution
3. Add metrics storage mechanism
4. Create metrics reporting capabilities
5. Implement historical metrics comparison
6. Add export capabilities for external monitoring

**Testing Requirements:**
- Test metrics collection accuracy
- Test metrics storage and retrieval
- Test reporting output format
- Test historical comparison functionality
- Test with various execution patterns
- Test export formats

**Definition of Done:**
- Relevant metrics are accurately collected
- Metrics are properly stored and retrievable
- Reporting provides useful insights
- Historical comparison helps identify trends
- Export formats are compatible with monitoring tools
- All tests passing with >90% coverage
- Documentation explains available metrics and their usage 