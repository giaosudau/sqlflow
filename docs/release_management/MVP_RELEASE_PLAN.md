# [WIP] SQLFlow MVP Release Plan

## Overview

The SQLFlow MVP (Minimum Viable Product) release delivers a robust foundation for end-to-end SQL-based data pipelines, with a focus on simplicity, extensibility, and rapid onboarding for data teams. This release prioritizes core value, reliability, and essential integrations, enabling users to build, test, and run pipelines with minimal setup.

---

## Core MVP Features

### 1. Core SQLFlow DSL
- **SQL-based pipeline definition**: Write pipelines using SQLFlow's extended SQL syntax.
- **Variable substitution**: Use variables in SQL and pipeline configuration.
- **Conditional execution**: IF/ELSE blocks for dynamic pipeline logic.
- **Automatic DAG generation**: Dependency graph built from SQL statements.

### 2. Python User-Defined Functions (UDFs)
- **UDF registration and discovery**: Register Python functions for use in SQL.
- **Engine integration**: Execute UDFs within DuckDB pipelines.
- **Error handling and lifecycle management**: Robust error reporting and lifecycle hooks.
- **CLI support**: Register, list, and test UDFs via the command line.
- **End-to-end UDF demo**: Example pipeline showcasing UDF capabilities.

### 3. Execution Engine
- **DuckDB support**: In-memory and persistent DuckDB execution modes.
- **Profile-based configuration**: Easily switch between dev and production environments.

### 4. Connectors
- **File connectors**: Built-in support for CSV and Parquet files (local and S3).
- **PostgreSQL connector**: Production-ready, with industry-standard parameter compatibility.
- **S3 connector**: Import/export data in CSV/Parquet formats.
- **Incremental loading**: Watermark-based incremental sync for supported connectors.

### 5. State Management
- **Watermark management**: Track incremental sync state per pipeline/source/target.
- **DuckDB state backend**: Reliable, atomic state persistence.

### 6. Command-Line Interface (CLI)
- **Pipeline execution**: Run, test, and debug pipelines from the CLI.
- **Profile management**: Configure and select execution environments.
- **Connector and UDF management**: Register, validate, and troubleshoot integrations.

### 7. Testing & Documentation
- **Unit and integration tests**: Core features covered by automated tests.
- **Basic documentation**: Getting started, core concepts, CLI reference, connector/UDF guides.
- **End-to-end demos**: Example projects and data for rapid onboarding.

### 8. Scheduling & Monitoring (Basic)
- **Basic logging**: CLI-based status tracking and error reporting.
- **No advanced UI**: Monitoring via logs and CLI output only.

### 9. Extensible Connector Framework
- **Standardized interface**: Easy to add new connectors.
- **Resilience patterns**: Retry, circuit breaker, and error handling built-in.

---

## Out of Scope for MVP (Planned for Future Releases)
- Advanced scheduling and monitoring UI
- Distributed execution (Celery, Kubernetes, etc.)
- Advanced schema evolution and management
- SaaS connectors beyond PostgreSQL/S3 (e.g., Shopify, Stripe, HubSpot)
- Enterprise features (monitoring dashboards, observability, web UI)
- Comprehensive documentation generation and advanced testing framework

---

## Success Criteria
- All core features above are implemented, tested, and documented.
- End-to-end demo pipelines run successfully with DuckDB, file, and PostgreSQL/S3 connectors.
- UDFs are discoverable, usable, and testable via CLI and in pipelines.
- Incremental loading and state management work as documented.
- Users can onboard and run pipelines with minimal setup (local and Docker).

---

## Next Steps
- Gather user feedback on MVP usability and stability.
- Prioritize post-MVP features based on community and stakeholder input.
- Expand connector ecosystem and add advanced orchestration/monitoring capabilities.
