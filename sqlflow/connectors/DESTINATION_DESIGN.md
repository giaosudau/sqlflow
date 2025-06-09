# Technical Design: SQLFlow Destination Connectors & Write Modes

## 1. Overview & Goal

This document outlines the technical design for enhancing the `EXPORT` directive in SQLFlow. The primary goal is to evolve beyond the current implicit "overwrite" behavior into a robust, safe, and flexible data-writing mechanism.

We will introduce explicit write modes (`REPLACE`, `APPEND`, `UPSERT`) that are predictable, secure, and aligned with modern data engineering best practices. This design ensures that all write operations are atomic, preventing data corruption and requiring minimal permissions from the user's database administrator.

## 2. High-Level Design Principle: "Stage and Swap"

The cornerstone of this design is the **"Stage and Swap"** pattern, which will be applied universally across all destination connectors to guarantee safety and atomicity.

The pattern consists of two phases:

1.  **Stage**: All data is written to a temporary, isolated location (e.g., a temporary table or file). This is the potentially slow, network-dependent phase. A failure here will have no impact on the final destination.
2.  **Swap**: Once the staged data is complete, a single, fast, and atomic (or near-atomic) operation is performed to make the staged data the "live" version. This operation is quick and runs entirely within the destination's ecosystem, minimizing the window for failure.

This pattern ensures that the final destination is never left in a partially written or corrupted state.

## 3. Core System Changes

### 3.1. Parser & AST (`sqlflow/parser/`)

-   **`ast.py`**: The `ExportStep` dataclass will be modified to include `mode` and `upsert_keys`.

    ```python
    # sqlflow/parser/ast.py
    @dataclass
    class ExportStep(PipelineStep):
        # ... existing fields
        sql_query: str
        destination_uri: str
        connector_type: str
        options: Dict[str, Any]
        mode: str = "APPEND" # Default mode
        upsert_keys: List[str] = field(default_factory=list)
        # ... existing fields

        def validate(self) -> List[str]:
            # ... existing validation
            if self.mode.upper() == "UPSERT" and not self.upsert_keys:
                errors.append("UPSERT mode requires the KEY clause to be specified.")
            if self.mode.upper() != "UPSERT" and self.upsert_keys:
                errors.append("KEY clause can only be used with UPSERT mode.")
            return errors
    ```

-   **`parser.py`**: The `_parse_export_statement` method will be updated to parse the optional `MODE <mode>` and `KEY (...)` clauses after the `OPTIONS` block. The logic will be similar to the existing parsers for `LOAD` and `CREATE TABLE` modes.

### 3.2. Base Connector Interface (`sqlflow/connectors/base/`)

-   **`destination_connector.py`**: The `write` method signature in the `DestinationConnector` abstract base class will be changed to pass the mode-specific information.

    ```python
    # sqlflow/connectors/base/destination_connector.py
    @abstractmethod
    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "append",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the destination using a specified mode.
        """
        raise NotImplementedError
    ```

## 4. Connector-Specific Implementation Designs

### 4.1. Database Connectors (e.g., `PostgresDestination`)

-   **Supported Modes**: `REPLACE`, `APPEND`, `UPSERT`.
-   **"Stage and Swap" Implementation**:
    1.  **Stage**: Upon connecting, create a session-scoped temporary table (`CREATE TEMP TABLE ... ON COMMIT PRESERVE ROWS`). Load the DataFrame into this `TEMP` table.
    2.  **Swap**: Execute a single, multi-statement SQL command wrapped in a `BEGIN/COMMIT` transaction block that reads from the `TEMP` table to modify the final table.
        -   `REPLACE`: `TRUNCATE final_table; INSERT INTO final_table SELECT * FROM temp_table;`
        -   `APPEND`: `INSERT INTO final_table SELECT * FROM temp_table;`
        -   `UPSERT`: `INSERT INTO final_table SELECT * FROM temp_table ON CONFLICT (keys) DO UPDATE SET ...;`
    3.  **Cleanup**: The `TEMP` table is automatically dropped by the database when the session ends.
-   **Reasoning**:
    -   **Security**: This approach follows the principle of least privilege. It does **not** require the user to have `CREATE` or `DROP` permissions on permanent tables.
    -   **Safety**: Transactions guarantee atomicity.
    -   **Zero Maintenance**: No risk of orphaned staging tables.

### 4.2. File-System Connectors (e.g., `S3Destination`, `CSVDestination`)

-   **Supported Modes**: `REPLACE` (on single files only), `APPEND`. `UPSERT` is not supported.
-   **"Stage and Swap" Implementation**:
    1.  **Stage**: Write all data to a temporary file/object in the destination directory/prefix (e.g., `._temp_filename_{uuid}`).
    2.  **Swap**:
        -   **`REPLACE`**:
            -   **Local Files**: Use `os.rename(temp_path, final_path)` for an atomic swap.
            -   **S3**: Use `s3_client.copy_object(...)` to copy from the temp key to the final key, then delete the temp object.
        -   **`APPEND`**: This mode is implemented by writing a **new, uniquely named file** to the destination prefix on each run. This is an inherently safe and idempotent pattern for data lakes. The "swap" is simply the successful completion of the file write.
    3.  **Cleanup**: The temporary file/object from a `REPLACE` operation must be deleted in a `finally` block to ensure cleanup on failure.
-   **Reasoning**:
    -   **Safety**: Prevents corrupted, partially written files. `REPLACE` on an S3 prefix is disallowed because it cannot be made atomic and is dangerously destructive.
    -   **Best Practices**: The `APPEND` behavior aligns with data lake principles of immutability.

## 5. Implementation Plan & Task Breakdown

Tasks are ordered by dependency.

#### Phase 1: Core Parser & AST (Blocking)

-   [ ] **Task 1.1**: Modify the `ExportStep` AST dataclass in `sqlflow/parser/ast.py` to add `mode` and `upsert_keys` fields.
-   [ ] **Task 1.2**: Update the `validate()` method in `ExportStep` to validate the new mode-specific rules.
-   [ ] **Task 1.3**: Update the `_parse_export_statement` method in `sqlflow/parser/parser.py` to parse the new `MODE` and `KEY` syntax.

#### Phase 2: System & Interface Integration (Blocking)

-   [ ] **Task 2.1**: Update `_build_export_step` in `sqlflow/core/planner.py` to pass `mode` and `upsert_keys` into the execution plan.
-   [ ] **Task 2.2**: Update the `write` method signature in `sqlflow/connectors/base/destination_connector.py`.
-   [ ] **Task 2.3**: Update `_execute_export` in `sqlflow/core/executors/local_executor.py` to read the new fields from the plan and call the connector's `write` method with the new signature.

#### Phase 3: Connector Implementation (Parallelizable)

-   [ ] **Task 3.1**: Implement safe `REPLACE`, `APPEND`, and `UPSERT` for `PostgresDestination` using the `TEMP TABLE` pattern.
-   [ ] **Task 3.2**: Implement safe `REPLACE` (single object) and `APPEND` (new partitioned file) for `S3Destination` using the temporary object pattern.
-   [ ] **Task 3.3**: Implement safe `REPLACE` and `APPEND` for `CSVDestination` using the temporary file and `os.rename` pattern.
-   [ ] **Task 3.4**: Implement safe `REPLACE` and `APPEND` for `ParquetDestination` using the temporary file and `os.rename` pattern.

#### Phase 4: Documentation (Final)

-   [ ] **Task 4.1**: Update the core SQLFlow documentation to cover the new `EXPORT` syntax and modes.
-   [ ] **Task 4.2**: Add a "Supported Write Modes" section to the `README.md` of each destination connector.
-   [ ] **Task 4.3**: Create a new pipeline in the `examples/` directory to demonstrate the various export modes. 