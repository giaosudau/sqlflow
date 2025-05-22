# SQLFlow Implementation Task Tracker

## Recent Progress (April-June 2024)
- ✅ **Completed core UDF infrastructure** with decorator implementation and engine integration
- ✅ **Standardized Table UDF signatures** with proper validation (commit d12a5eb3)
- ✅ **Enhanced test coverage** for UDF functionality, particularly for table UDFs
- ✅ **Implemented robust UDF discovery** with enhanced metadata and nested module support
- ✅ **Improved error handling and robustness** for UDF management
- ✅ **Enhanced UDF registration system** with improved error reporting and better DuckDB integration
- ✅ **Implemented UDF dependency tracking** in planner for optimized execution
- ✅ **Created technical documentation** for UDF system architecture and dependencies
- ✅ **Completed complex UDF integration testing** with reliable execution

## MVP Focus Areas
The primary focus for MVP release is completing the UDF feature set:
1. ✅ Resolving integration tests for complex UDF pipelines
2. ✅ Improving error reporting for UDF lifecycle
3. ✅ Updating documentation with clear examples
4. ✅ Creating an end-to-end demo showcasing UDF capabilities
5. ✅ Final testing and validation of UDF functionality across different environments

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
| [Task 2.8](#task-28-resolve-complex-udf-integration-test) | Resolve Complex UDF Integration Test | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 2.9](#task-29-enhance-udf-unit-testing-table-udfs--engine) | Enhance UDF Unit Testing (Table UDFs & Engine) | ✅ COMPLETED | | |
| [Task 2.10](#task-210-improve-udf-discovery--metadata) | Improve UDF Discovery & Metadata | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 2.11](#task-211-enhance-udf-lifecycle-error-reporting) | Enhance UDF Lifecycle Error Reporting | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 2.12](#task-212-verify-and-polish-cli-for-udfs) | Verify and Polish CLI for UDFs | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 2.13](#task-213-update-udf-documentation-for-mvp) | Update UDF Documentation for MVP | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 2.14](#task-214-review-and-refine-sqlengine-udf-interface) | Review and Refine SQLEngine UDF Interface | ✅ COMPLETED | | |
| [Task 2.15](#task-215-create-basic-end-to-end-udf-demo-for-mvp) | Create Basic End-to-End UDF Demo for MVP | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 2.16](#task-216-implement-duckdb-version-compatibility) | Implement DuckDB Version Compatibility | ⬜ NOT STARTED | | |
| [Task 2.17](#task-217-optimize-type-mapping-for-udfs) | Optimize Type Mapping for UDFs | ⬜ NOT STARTED | | |
| [Task 2.18](#task-218-performance-profiling-and-optimization) | Performance Profiling and Optimization | ⬜ NOT STARTED | | |

### Epic 3: Enhanced Load Controls (MODE Parameter)

**Goal:** Implement unified data loading controls with various operation modes (REPLACE, APPEND, MERGE) to support common data warehousing patterns.

| Task | Description | Status | Priority | Assignee |
|------|-------------|--------|----------|----------|
| [Task 3.1](#task-31-parser-updates-for-load-modes) | Parser Updates for Load Modes | ✅ COMPLETED | 🔥 MVP Critical | |
| [Task 3.2](#task-32-sql-generator-for-load-modes) | SQL Generator for Load Modes | ✅ COMPLETED | | |
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
- `docs/user/guides/conditionals.md`
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
- `docs/user/reference/python_udfs.md` (new)
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
1. ✅ Analyze the reasons for the original "Test is too complex and needs refactoring" skip.
2. ✅ Refactor the `customer_summary` table UDF in `create_udf_file` (within the test setup) to conform to the `DataFrame + kwargs` standard.
3. ✅ Update the SQL query in `create_pipeline_file` (within the test setup) that calls `PYTHON_FUNC("sales_analysis.customer_summary", ...)` to pass arguments correctly if needed.
4. ✅ Unskip the `test_complex_udf_pipeline` test by removing or commenting out the `@pytest.mark.skip` decorator.
5. ✅ Debug and fix any issues until the test passes consistently.
6. ✅ Ensure the test adequately covers interaction between scalar and table UDFs within a single pipeline.

**Testing Requirements:**
- ✅ The `test_complex_udf_pipeline` must pass without errors.
- ✅ The test should validate data transformations through multiple UDFs (scalar and table) and SQL steps.

**Definition of Done:**
- ✅ The `@pytest.mark.skip` is removed from `test_complex_udf_pipeline`.
- ✅ The test passes reliably in the CI/CD environment.
- ✅ The test serves as a robust example of end-to-end UDF functionality.

**Status:** ✅ COMPLETED (June 2024)

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
1. ✅ Modify `PythonUDFManager.discover_udfs` to search for `*.py` files recursively within the specified `python_udfs_dir`.
2. ✅ Adjust UDF naming convention to reflect module paths if UDFs are in subdirectories (e.g., `subdir.module_name.function_name`).
3. ✅ Ensure `self.udf_info[udf_name]['signature']` in `discover_udfs` captures a user-friendly and accurate representation of the UDF's signature, including parameter names and type hints (if available via `inspect.signature`).
4. ✅ Add better error handling and reporting for UDF discovery issues.
5. ✅ Implement improved metadata extraction with support for parameter details and formatted signatures.

**Testing Requirements:**
- ✅ Unit tests for `PythonUDFManager.discover_udfs` with UDFs placed in direct `python_udfs` folder and in subdirectories.
- ✅ Tests verifying that UDFs from subdirectories are correctly named and their metadata (especially signature) is accurately captured.
- ✅ Test `sqlflow udf list` and `sqlflow udf info` with UDFs in subdirectories.
- ✅ Test error handling for invalid UDF files and modules.

**Definition of Done:**
- ✅ `PythonUDFManager` correctly discovers and namespaces UDFs from subdirectories.
- ✅ UDF metadata stored in `udf_info` (and displayed by CLI) includes accurate and complete signature information.
- ✅ All related tests pass.
- ✅ Error handling is robust and provides helpful diagnostic information.

**Status:** ✅ COMPLETED (May 2024)

### Task 2.11: Enhance UDF Lifecycle Error Reporting

**Description:** Improve error reporting across the UDF lifecycle (discovery, registration, execution) to make it more user-friendly, informative, and actionable.

**Files Impacted:**
- `sqlflow/udfs/manager.py`
- `sqlflow/udfs/decorators.py`
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/core/executors/base_executor.py` (and its implementations)

**Subtasks:**
1. ✅ **Discovery:** In `PythonUDFManager.discover_udfs`, if a Python file in the `python_udfs` directory (or subdirectories) fails to import, log a clear warning detailing the problematic file path and the import error.
2. ✅ **Registration:** Ensure `DuckDBEngine.register_python_udf` (as refined in Task 2.7) throws specific, helpful exceptions if DuckDB rejects a UDF (e.g., `UDFRegistrationError("Failed to register UDF 'my_udf': DuckDB error: ...")`).
3. ✅ **Execution:** In `BaseExecutor` (and its implementations like `LocalExecutor`), when a UDF call within `execute_query` (via the engine) raises an exception, catch it and re-raise it or log it with additional context: the UDF name being executed and the original traceback from the UDF.
4. ✅ Implement enhanced error context collection for UDF execution failures.
5. ✅ Create improved error handling architecture with clearer error classification.

**Testing Requirements:**
- ✅ Test scenarios where a UDF file has syntax errors or missing imports (discovery phase).
- ✅ Test scenarios where a correctly discovered UDF fails DuckDB registration due to an incompatible signature or internal DuckDB issue.
- ✅ Test scenarios where a registered UDF raises an exception during its execution within a pipeline.
- ✅ Verify error messages are clear and point to the source of the problem.

**Definition of Done:**
- ✅ Error messages provided to the user at each stage of the UDF lifecycle (discovery, registration, execution) are informative, clear, and help in debugging.
- ✅ SQLFlow handles these error conditions gracefully without crashing unexpectedly.
- ✅ Error reporting is consistent across all UDF lifecycle stages.

**Status:** ✅ COMPLETED (June 2024)

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

**Status:** ✅ COMPLETED

- Comprehensive Python UDF documentation added in `docs/user/reference/python_udfs.md`.
- Main `README.md` updated with UDF quickstart and best practices.
- Documentation reviewed for best practices by Principal Software Advocates and SMEs (Snowflake, Databricks, dbt, sqlmesh).
- Covers scalar/table UDFs, signature requirements, discovery, usage, CLI, troubleshooting, performance, and best practices.

### Task 2.14: Review and Refine SQLEngine UDF Interface

**Description:** Conduct a comprehensive review of the UDF interface in the SQLEngine base class and implementations, refining it for better consistency, usability, and error handling.

**Files Impacted:**
- `sqlflow/core/engines/base.py`
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/udfs/udf_patch.py` (new)

**Subtasks:**
1. ✅ Review the current SQLEngine UDF registration interface for consistency and usability
2. ✅ Standardize error handling for UDF registration across engine implementations
3. ✅ Improve type compatibility between Python and SQL engine types
4. ✅ Implement version-aware UDF registration mechanisms
5. ✅ Add comprehensive documentation of the UDF interface

**Testing Requirements:**
- ✅ Test UDF registration with various signature types
- ✅ Test error handling for registration failures
- ✅ Test type compatibility between Python and SQL
- ✅ Test with different engine versions if applicable
- ✅ Verify documentation accuracy and completeness

**Definition of Done:**
- ✅ SQLEngine UDF interface is consistent and well-documented
- ✅ Error handling provides clear, actionable feedback
- ✅ Type compatibility is robust across supported types
- ✅ Version-specific behaviors are handled gracefully
- ✅ All tests pass with >90% coverage
- ✅ Documentation is complete and accurate

**Status:** ✅ COMPLETED (June 2024)

### Task 2.15: Create Basic End-to-End UDF Demo for MVP

**Status:** ✅ COMPLETED

- Enhanced and polished the existing `demos/udf_demo` project to serve as the official end-to-end UDF showcase for MVP.
- Improved the comprehensive README with clear instructions, troubleshooting tips, performance best practices, and next steps.
- Ensured proper UDF references in the pipeline files with fully qualified module paths.
- Verified all pipelines work as expected with scalar and table UDFs.
- Created clear SQL examples for both scalar and table UDF usage patterns.
- Added performance tips and best practices for UDF implementation.

### Task 3.1: Parser Updates for Load Modes

**Description:** Add token types and parser support for MODE parameter in LOAD statements.

**Files Impacted:** 
- `sqlflow/parser/lexer.py`
- `sqlflow/parser/ast.py`
- `sqlflow/parser/parser.py`

**Subtasks:**
1. ✅ Add new token types to `TokenType` enum: `MODE`, `REPLACE`, `APPEND`, `MERGE`, `MERGE_KEYS`
2. ✅ Add regex patterns to `Lexer.patterns` for each token type
3. ✅ Update `LoadStep` class to include mode and merge_keys properties
4. ✅ Modify `_parse_load_statement` to handle MODE parameter and MERGE_KEYS
5. ✅ Create unit tests for all load modes
6. ✅ Create example files demonstrating the use of different load modes

**Testing Requirements:**
- ✅ Unit tests for all load modes (REPLACE, APPEND, MERGE)
- ✅ Unit tests for MERGE with multiple merge keys
- ✅ Error handling tests for invalid modes
- ✅ Error handling tests for MERGE without merge keys
- ✅ Integration with existing parsing pipeline

**Definition of Done:**
- ✅ Parser correctly identifies and processes all mode types
- ✅ AST representation includes mode and relevant parameters
- ✅ Validation logic ensures required parameters (e.g., merge keys for MERGE mode)
- ✅ All tests passing with >90% coverage
- ✅ Examples clearly demonstrate the use of different modes

**Status:** ✅ COMPLETED in commit from feature/enhanced-load-controls branch (July 2024)

### Task 3.2: SQL Generator for Load Modes

**Description:** Implement SQL generation strategies for different load modes in SQLEngine implementations.

**Files Impacted:**
- `sqlflow/core/engines/duckdb_engine.py`
- `sqlflow/core/executors/local_executor.py`

**Subtasks:**
1. ✅ Add `generate_load_sql` method to DuckDBEngine to support different load modes
2. ✅ Implement REPLACE mode SQL generation (CREATE OR REPLACE TABLE)
3. ✅ Implement APPEND mode SQL generation (INSERT INTO)
4. ✅ Implement MERGE mode SQL generation with ON clause for merge keys
5. ✅ Add schema compatibility validation for APPEND and MERGE modes
6. ✅ Implement `execute_load_step` method in LocalExecutor for executing LoadStep objects
7. ✅ Add `execute_pipeline` method to LocalExecutor for executing Pipeline objects

**Testing Requirements:**
- ✅ Unit tests for SQL generation for each mode (REPLACE, APPEND, MERGE)
- ✅ Unit tests for MERGE mode with multiple merge keys
- ✅ Tests for table creation when table doesn't exist
- ✅ Tests for schema validation
- ✅ Tests for execution of LoadStep objects
- ✅ Tests for error handling during execution

**Definition of Done:**
- ✅ SQL generation produces correct SQL for each mode
- ✅ SQL is optimized for DuckDB
- ✅ Executors can handle LoadStep objects directly
- ✅ Schema compatibility is validated for APPEND and MERGE modes
- ✅ All tests passing with >90% coverage

**Status:** ✅ COMPLETED in commit from feature/enhanced-load-controls branch (July 2024)

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
- `docs/user/reference/load_modes.md` (new)
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
- `docs/user/reference/connectors/` (new directory)
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
4. Implement `schema reject` command to reject current schema

**Testing Requirements:**
- Test `schema show` command with various schemas
- Test `schema history` command with multiple schema changes
- Test `schema accept` command with valid and invalid schema acceptance
- Test `schema reject` command with valid and invalid schema rejection
- Test error handling for schema management commands

**Definition of Done:**
- CLI commands for schema management are implemented and tested
- Schema management commands are functional and error-free
- All tests passing with >90% coverage
- Documentation updated with schema management CLI usage

**Status:** ✅ COMPLETED (June 2024)

### Task 5.5: Schema Management Documentation

**Description:** Create comprehensive documentation for schema management features.

**Files Impacted:**
- `docs/user/reference/schema_management.md` (new)

**Subtasks:**
1. Document schema management CLI commands
2. Explain schema management concepts and best practices
3. Provide examples of schema management scenarios
4. Create a schema management quickstart guide

**Testing Requirements:**
- Test documentation for clarity and accuracy
- Verify all examples are executable and functional
- Ensure documentation covers all schema management features

**Definition of Done:**
- Schema management documentation is complete and accurate
- Documentation is easy to understand and follow
- All examples are executable and functional
- Documentation covers all schema management features

**Status:** ✅ COMPLETED (June 2024)

### Task 6.1: DAG Execution Enhancements

**Description:** Improve the DAG visualization and execution flow.

**Files Impacted:**
- `sqlflow/visualizer/dag_builder_ast.py`
- `sqlflow/core/planner.py`

**Subtasks:**
1. Implement a more user-friendly and interactive DAG visualization
2. Add interactive features to the DAG visualization
3. Implement DAG execution enhancements
4. Add DAG execution metrics collection

**Testing Requirements:**
- Test DAG visualization with various DAG structures
- Test interactive features with real DAGs
- Test DAG execution with different scenarios
- Test DAG execution metrics collection

**Definition of Done:**
- DAG visualization is more user-friendly and interactive
- Interactive features are implemented
- DAG execution enhancements are implemented
- DAG execution metrics collection is implemented
- All tests passing with >90% coverage

**Status:** ⬜ NOT STARTED

### Task 6.2: Parallel Execution Implementation

**Description:** Implement parallel execution capabilities.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/executors/local_executor.py`
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Implement parallel execution logic
2. Add parallel execution metrics collection
3. Implement parallel execution error handling

**Testing Requirements:**
- Test parallel execution with various DAG structures
- Test parallel execution metrics collection
- Test parallel execution error handling

**Definition of Done:**
- Parallel execution logic is implemented
- Parallel execution metrics collection is implemented
- Parallel execution error handling is implemented
- All tests passing with >90% coverage

**Status:** ⬜ NOT STARTED

### Task 6.3: Failure Handling and Recovery

**Description:** Implement failure handling and recovery mechanisms.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/executors/local_executor.py`
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Implement failure handling logic
2. Implement recovery logic
3. Add failure handling and recovery metrics collection

**Testing Requirements:**
- Test failure handling with various DAG structures
- Test recovery with various DAG structures
- Test failure handling and recovery metrics collection

**Definition of Done:**
- Failure handling logic is implemented
- Recovery logic is implemented
- Failure handling and recovery metrics collection is implemented
- All tests passing with >90% coverage

**Status:** ⬜ NOT STARTED

### Task 6.4: Execution Status Tracking

**Description:** Implement execution status tracking for all tasks and stages.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/executors/local_executor.py`
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Implement execution status tracking logic
2. Add execution status tracking metrics collection
3. Implement execution status tracking error handling

**Testing Requirements:**
- Test execution status tracking with various DAG structures
- Test execution status tracking metrics collection
- Test execution status tracking error handling

**Definition of Done:**
- Execution status tracking logic is implemented
- Execution status tracking metrics collection is implemented
- Execution status tracking error handling is implemented
- All tests passing with >90% coverage

**Status:** ⬜ NOT STARTED

### Task 6.5: Execution Metrics Collection

**Description:** Implement execution metrics collection for all tasks and stages.

**Files Impacted:**
- `sqlflow/core/executors/base_executor.py`
- `sqlflow/core/executors/local_executor.py`
- `sqlflow/core/executors/thread_pool_executor.py`

**Subtasks:**
1. Implement execution metrics collection logic
2. Add execution metrics collection metrics collection
3. Implement execution metrics collection error handling

**Testing Requirements:**
- Test execution metrics collection with various DAG structures
- Test execution metrics collection metrics collection
- Test execution metrics collection error handling

**Definition of Done:**
- Execution metrics collection logic is implemented
- Execution metrics collection metrics collection is implemented
- Execution metrics collection error handling is implemented
- All tests passing with >90% coverage

**Status:** ⬜ NOT STARTED