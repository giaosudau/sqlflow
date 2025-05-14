
# SQLFlow Implementation & Timeline Plan

This plan breaks down the roadmap into epics, milestones, and tasks sized to ~4 hours of work each. Every task includes an **Expected Outcome** and **Definition of Done (DoD)** to guide development and testing.

## Epic 1: Core Engine & DSL

### Milestone 1.1: Parser & AST (Weeks 1–2)

- **Task 1.1.1**: Add `SOURCE` directive grammar  
  - **Outcome**: Parser recognizes `SOURCE <name> TYPE <...> PARAMS {...};`  
  - **DoD**: Unit tests for valid and invalid `SOURCE` statements.

- **Task 1.1.2**: Add `LOAD` directive grammar  
  - **Outcome**: Parser includes `LOAD` in AST nodes.  
  - **DoD**: Tests for `LOAD source INTO table;` with and without `WHERE`.

- **Task 1.1.3**: Add `EXPORT` directive grammar  
  - **Outcome**: AST supports `EXPORT SELECT ... TO ... TYPE ... OPTIONS {...}`.  
  - **DoD**: Tests for missing `OPTIONS`, syntax errors.

- **Task 1.1.4**: Support `INCLUDE` & `SET` directives  
  - **Outcome**: Parser AST nodes for `INCLUDE` and `SET`.  
  - **DoD**: Tests for file path parsing, variable assignment.

- **Task 1.1.5**: Extend AST → DAG builder ✅  
  - **Outcome**: Build node types for each directive.  
  - **DoD**: Integration test assembles DAG from multi-statement script.

### Milestone 1.2: Planner & Executor (Weeks 3–4)

- **Task 1.2.1**: DAG cycle detection ✅  
  - **Outcome**: Compiler raises error on circular dependencies.  
  - **DoD**: Test pipeline with cycle → error.

- **Task 1.2.2**: Operation planner ✅  
  - **Outcome**: Map DAG nodes to execution plan items.  
  - **DoD**: Verify plan order matches dependencies.

- **Task 1.2.3**: Thread-pool task executor ✅  
  - **Outcome**: Execute plan items concurrently.  
  - **DoD**: Integration test with parallelizable tasks logs correct order.

- **Task 1.2.4**: Resume-from-failure logic ✅  
  - **Outcome**: Executor retries failed tasks and can resume.  
  - **DoD**: Simulate failure mid-pipeline and resume.

## Epic 2: Connector Framework (Weeks 5–8)

### Milestone 2.1: Core Connector System (Weeks 5–6)

- **Task 2.1.1**: Define `Connector` & `ExportConnector` interfaces ✅  
  - **Outcome**: Base classes in codebase.  
  - **DoD**: Lint, type-check, and unit tests for method signatures.

- **Task 2.1.2**: Implement plugin registry ✅  
  - **Outcome**: `register_connector` and `register_export_connector` decorators.  
  - **DoD**: Dynamic discovery returns registered classes.

- **Task 2.1.3**: CSV Source connector ✅  
  - **Outcome**: Read CSV in chunks, infer schema.  
  - **DoD**: Integration test reads sample CSV and matches schema.

- **Task 2.1.4**: Parquet Source connector ✅  
  - **Outcome**: Read Parquet via Arrow, infer schema.  
  - **DoD**: Test reading multi-column Parquet file.

### Milestone 2.2: Database & Cloud Connectors (Weeks 7–8)

- **Task 2.2.1**: Postgres Source connector  
  - **Outcome**: Read table via psycopg2, parameterized queries.  
  - **DoD**: CI integration test against test Postgres container.

- **Task 2.2.2**: Postgres Export connector  
  - **Outcome**: Write DataChunk batches to temp table.  
  - **DoD**: Round-trip test: load then export back matches input.

- **Task 2.2.3**: S3 Export connector  
  - **Outcome**: Upload files with multipart, chunk retry.  
  - **DoD**: Mock S3 tests for file presence and integrity.

- **Task 2.2.4**: REST Export connector  
  - **Outcome**: POST JSON batches to endpoint.  
  - **DoD**: Mock HTTP server tests for correct payload.

## Epic 3: CLI, UX & Documentation (Weeks 9–12)

### Milestone 3.1: CLI Core (Weeks 9–10)

- **Task 3.1.1**: `sqlflow init` command ✅  
  - **Outcome**: Scaffolds project structure.  
  - **DoD**: File existence checks, sample files present.

- **Task 3.1.2**: `compile` command ✅  
  - **Outcome**: Produces execution plan output.  
  - **DoD**: CLI test captures and validates plan text.

- **Task 3.1.3**: `run` command ✅  
  - **Outcome**: Executes sample pipeline end-to-end.  
  - **DoD**: Integration test runs a simple CSV→SQL→CSV pipeline.

- **Task 3.1.4**: `list` & `describe` commands ✅  
  - **Outcome**: Lists pipelines and shows metadata.  
  - **DoD**: Tests for correct listings and descriptions.

### Milestone 3.2: Docs & Visualization (Weeks 11–12)

- **Task 3.2.1**: Auto-doc generation  
  - **Outcome**: Convert SQLFlow config and DSL to Markdown docs.  
  - **DoD**: Lint docs, verify link integrity.

- **Task 3.2.2**: Tutorial “Business→Pipeline”  
  - **Outcome**: Written guide with example code and outputs.  
  - **DoD**: Peer review and readability sign-off.

- **Task 3.2.3**: DAG visualization (`viz`)  
  - **Outcome**: Generate Graphviz DOT and PNG.  
  - **DoD**: Snapshot tests for DAG images.

## Weekly Checkpoints & Versioning

- **Weekly Syncs**: Brief stand-ups every Monday.  
- **Sprint Demos**: Fridays, demonstrate completed tasks.  
- **Versioning**:  
  - v0.1.0 at end of Week 12  
  - v0.2.0 after additional connectors (Months 4–6)  
  - v1.0.0 for full enterprise feature set (Months 7–10)
