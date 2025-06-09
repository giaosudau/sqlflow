# SQLFlow Connector Architecture: Revolutionary Refactor Plan (v2)

**Document Version:** 2.0
**Status:** Approved for Implementation
**Lead Architect:** Principal Software Architect
**Reviewed By:** Principal Software Engineer, Principal Product Manager, Principal Data Engineer, Principal Software Manager

---

## 🎯 **Panel Review & Final Decision**

After a comprehensive review by the principal engineering and product panel, we have **APPROVED a revised implementation plan** for the Connector Architecture Refactor.

While the original plan's vision and target architecture were sound, its phasing and task structure were determined to be infeasible under our development and testing constraints (`06_development_workflow.mdc`). Specifically, the requirement for all tests (`pytest`, `run_all_examples.sh`, `run_integration_tests.sh`) to pass after each committed task makes the original large-scale tasks impractical.

The following revised plan is designed to be **pragmatic, executable, and minimize risk** by restructuring the project into logically dependent, committable tasks.

### **Key Decisions & Justifications:**

1.  **Decision**: **Reorder Phases to Be Dependency-Driven.**
    *   **Justification (PSA, PSM)**: The core engine, planner, and executor must be updated *before* most connectors can be migrated. The new plan moves this critical-path work to Phase 1. This is a fundamental change from the original plan and is essential for a successful execution.

2.  **Decision**: **Isolate the "Big Bang" Change.**
    *   **Justification (PSE, PSM)**: The switch to the new architecture is a breaking change. We will isolate this into a single, well-defined (though large) task in **Phase 1**. This task will rewrite the core engine and migrate *only the simplest connector (CSV)*. To keep the build green, all other connectors and their associated tests/examples will be temporarily disabled. This is a pragmatic strategy to manage a revolutionary change.

3.  **Decision**: **Migrate Connectors Incrementally.**
    *   **Justification (PDE, PSE)**: After the core engine is updated, each subsequent connector (PostgreSQL, S3, etc.) can be migrated in its own separate, committable task. Each task will include the connector code refactor, test migration, example updates, and new `README.md` documentation. This makes progress measurable and ensures the system is always in a stable, fully-testable state.

4.  **Decision**: **Enforce Strict Definition of Done (DOD) for Each Task.**
    *   **Justification (PSM)**: To comply with `06_development_workflow.mdc`, the DOD for each task is not just "code complete." It must include passing all unit tests, relevant integration tests, and ensuring `run_all_examples.sh` and `run_integration_tests.sh` succeed. This quality gate is non-negotiable for each task from Phase 1 onwards.

This revised plan represents a more mature and actionable strategy that respects both the architectural vision and the realities of our development process.

---

## **Target Architecture**

_The target architecture remains the same as envisioned in the original plan._

```
sqlflow/connectors/
├── base/
│   ├── source_connector.py      # New: Clean source interface
│   ├── destination_connector.py # New: Clean destination interface
│   └── ...
├── postgres/
│   ├── __init__.py
│   ├── source.py
│   ├── destination.py
│   └── README.md                # New: Connector-specific documentation
├── s3/
│   ├── __init__.py
│   ├── source.py
│   ├── destination.py
│   └── README.md                # New: Connector-specific documentation
└── registry/
    ├── source_registry.py
    └── destination_registry.py
```

---

## 📋 **Revised Implementation Plan**

### **Phase 0: Foundational Scaffolding (Week 1)**

**Goal**: Create the foundational (but disconnected) components of the new architecture. This phase is low-risk and involves only adding new, untested code.

#### **Task 0.1: Create New Directory Structure & Base Classes**
-   **Files to Create**:
    -   `sqlflow/connectors/base/source_connector.py`
    -   `sqlflow/connectors/base/destination_connector.py`
    -   `sqlflow/connectors/registry/source_registry.py`
    -   `sqlflow/connectors/registry/destination_registry.py`
    -   New connector directories: `sqlflow/connectors/postgres/`, `sqlflow/connectors/s3/`, etc.
-   **Implementation**: Use the approved code snippets for `SourceConnector`, `DestinationConnector`, and the registries from the original plan.

#### **Task 0.2: Add Unit Tests for Base Components**
-   **Files to Create**:
    -   `tests/unit/connectors/base/test_source_connector.py`
    -   `tests/unit/connectors/registry/test_source_registry.py`
    -   ...and corresponding tests for destination components.
-   **Goal**: Ensure the new, non-integrated components are logically sound.

#### **DOD for Phase 0:**
-   [x] All new directories and files for the base architecture are created.
-   [x] `SourceConnector` and `DestinationConnector` ABCs are defined.
-   [x] Source/Destination registries are implemented.
-   [x] Unit tests for all new components pass.
-   [x] **Crucially, all existing project checks (`pre-commit`, `pytest`, `run_all_examples.sh`) still pass as no existing code has been modified.**

---

### **Phase 1: Core Engine Overhaul & Foundational Connectors (Weeks 2-4)**

**Goal**: Execute the single, major breaking change required for the new architecture, and migrate the most basic connectors (CSV and In-Memory). This is the highest-risk phase and will be executed on a single, focused feature branch.

#### **Task 1.1: Rewrite Core Engine & Migrate CSV Connector**
-   **Description**: This is the "big bang" task. It rewrites core parts of SQLFlow to use the new connector architecture and migrates the most common connector, CSV.
-   **Core Engine Rewrite**:
    -   Modify `sqlflow/core/planner.py`, `sqlflow/core/executors/local_executor.py`, and related modules to exclusively use the new source/destination registries.
    -   Remove all logic related to the old connector system.
-   **CSV Connector Refactor**:
    -   Implement `CSVSource` and `CSVDestination` in a new `sqlflow/connectors/csv/` directory.
    -   Populate `sqlflow/connectors/csv/README.md` with detailed configuration and usage examples.
-   **Test & Example Migration**:
    -   Refactor all unit and integration tests for the CSV connector.
    -   Update all pipelines in the `examples/` directory that use the CSV connector.
-   **Temporarily Disable Other Connectors**:
    -   To keep the build green, temporarily disable all other connectors (Postgres, S3, etc.).
    -   Skip all non-CSV integration tests using `@pytest.mark.skip`.
    -   Update `run_all_examples.sh` to only execute the now-working CSV-based examples.
-   **Cleanup**: Delete old `csv_connector.py`, `csv_export_connector.py`, and related files.

-   **Affected Examples**: This task has a broad impact. The following examples must be verified:
    -   `examples/conditional_pipelines`
    -   `examples/connector_interface_demo`
    -   `examples/incremental_loading_demo`
    -   `examples/load_modes`
    -   `examples/transform_layer_demo`
    -   `examples/udf_examples`

-   **Definition of Done**:
    -   All unit and integration tests for the CSV connector pass.
    -   All affected example projects run successfully via their respective `run.sh` or `run_demo.sh` scripts.
    -   The main test scripts pass **flawlessly**: `scripts/run_integration_tests.sh` and `scripts/run_all_examples.sh`.
    -   All pre-commit checks pass.

#### **Task 1.2: Migrate In-Memory Connector (Internal)**
-   **Description**: Migrate the internal In-Memory connector used for testing.
-   **Refactor Code**: Implement `InMemorySource` and `InMemoryDestination` in `sqlflow/connectors/in_memory/`.
-   **Migrate Tests**: Refactor all unit tests that rely on the in-memory connector.
-   **Definition of Done**: All relevant unit tests pass.

#### **DOD for Phase 1:**
-   [x] The core engine exclusively uses the new source/destination architecture.
-   [x] The CSV and In-Memory connectors are fully migrated and functional.
-   [x] All old corresponding connector code is deleted.
-   [x] All non-migrated connectors, tests, and examples are verifiably disabled.
-   [x] **The entire test suite passes (`pytest`, `pre-commit`).**
-   [x] **`run_all_examples.sh` and `run_integration_tests.sh` run successfully on the active subset of tests/examples.**
-   [x] The commit is merged. The project is now in a stable state, ready for incremental connector migration.

---

### **Phase 2: Incremental Migration of Production Connectors (Weeks 5-13)**

**Goal**: Migrate the remaining connectors one by one. Each task is self-contained, committable, and must result in a fully passing build.

#### **Task 2.1: Migrate PostgreSQL Connector**
-   **Refactor Code**: Implement `PostgresSource` and `PostgresDestination` in `sqlflow/connectors/postgres/`.
-   **Add Documentation**: Create `sqlflow/connectors/postgres/README.md`.
-   **Migrate Tests & Examples**: Re-enable and refactor all PostgreSQL tests and update the `phase2_integration_demo` example.
-   **Affected Examples**: `examples/phase2_integration_demo`.
-   **Definition of Done**: All Postgres tests pass, `phase2_integration_demo` runs, and the main `run_...sh` scripts succeed.

#### **Task 2.2: Migrate S3 Connector** ✅ **COMPLETED**
-   **Refactor Code**: ✅ Implemented `S3Source` with full Connector interface in `sqlflow/connectors/s3/`.
-   **Enhanced Features**: ✅ Added multi-format support (CSV, JSON, JSONL, Parquet, TSV), cost management, partition awareness, incremental loading.
-   **Backward Compatibility**: ✅ Support both legacy 'uri' interface (s3://bucket/key) and new separate parameters.
-   **Migrate Tests & Examples**: ✅ Re-enabled and refactored all S3 tests, removed module-level skips.
-   **Code Quality**: ✅ Fixed all linting errors, proper imports, comprehensive error handling.
-   **Definition of Done**: ✅ All S3 tests pass, integration tests properly skip when services unavailable, all code style issues resolved.

#### **Task 2.3: Migrate Google Sheets Connector** ✅ **COMPLETED**
-   **Refactor Code**: ✅ Implemented `GoogleSheetsSource` with full Connector interface in `sqlflow/connectors/google_sheets/`.
-   **Enhanced Features**: ✅ Added comprehensive error handling, connection testing, schema discovery, and incremental loading support.
-   **Add Documentation**: ✅ Created comprehensive `sqlflow/connectors/google_sheets/README.md` with setup instructions and troubleshooting.
-   **Migrate Tests & Examples**: ✅ Created new comprehensive test suite and `examples/google_sheets_demo` project with sample pipelines.
-   **Code Quality**: ✅ Fixed all linting errors, proper imports, comprehensive error handling.
-   **Definition of Done**: ✅ All Google Sheets tests pass, new example created with documentation, all code style issues resolved.

#### **Task 2.4: Migrate Parquet Connector**
-   **Refactor Code**: Implement `ParquetSource` in `sqlflow/connectors/parquet/`.
-   **Add Documentation**: Create `sqlflow/connectors/parquet/README.md`.
-   **Migrate Tests & Examples**: Re-enable tests. Create a new `examples/parquet_demo` project.
-   **Definition of Done**: All Parquet tests pass, the new example runs, and the main `run_...sh` scripts succeed.

#### **Task 2.5: Migrate REST API Connector**
-   **Refactor Code**: Implement `RestApiSource` in `sqlflow/connectors/rest/`.
-   **Add Documentation**: Create `sqlflow/connectors/rest/README.md`.
-   **Migrate Tests & Examples**: Re-enable tests. Create a new `examples/rest_demo` project.
-   **Definition of Done**: All REST tests pass, the new example runs, and the main `run_...sh` scripts succeed.

#### **Task 2.6: Migrate Shopify Connector**
-   **Refactor Code**: Implement `ShopifySource` in `sqlflow/connectors/shopify/`.
-   **Add Documentation**: Create `sqlflow/connectors/shopify/README.md`.
-   **Migrate Tests & Examples**: Re-enable tests and update the `shopify_ecommerce_analytics` example.
-   **Affected Examples**: `examples/shopify_ecommerce_analytics`.
-   **Definition of Done**: All Shopify tests pass, the example runs, and the main `run_...sh` scripts succeed.

---

### **Phase 3: Finalization and Documentation (Week 14)**

**Goal**: Ensure the refactor is complete, documented, and the old code is fully removed.

#### **Task 3.1: Update Core Documentation**
-   Rewrite user-facing documentation in `docs/` to reflect the new connector architecture, especially for configuration in profiles.

#### **Task 3.2: Final Project Cleanup & Verification**
-   Perform a full audit of the codebase to ensure no old connector files, test skips, or disabled examples remain.
-   Run all checks one last time to confirm the project is in a clean, fully-functional state.

#### **DOD for Phase 3:**
-   [x] All documentation is updated.
-   [x] There is no remaining disabled or skipped code, tests, or examples.
-   [x] The project is declared fully migrated. 