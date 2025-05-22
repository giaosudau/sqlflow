**SQLFlow Roadmap - Stakeholder One-Pager**

**Overview**
SQLFlow is a SQL-centric ELT and pipeline tool designed for small and mid-sized teams. It bridges the gap between transformation frameworks like dbt and connector-heavy tools like Airbyte, offering end-to-end data movement, transformation, and delivery within a familiar SQL + CLI interface. The initial focus is on ease of use, profile-based config, connector modularity, and local-first deployment.

**Target Users**

* Data analysts and engineers at startups, agencies, and SME SaaS teams
* Teams seeking low-overhead ingestion, transformation, and export
* Devs preferring SQL-first data workflows without orchestrators

**Positioning: Internal Tooling vs. Embedded SaaS**

| Category                         | SQLFlow                                                          | dbt                                       | Airbyte / Meltano                   |
| -------------------------------- | ---------------------------------------------------------------- | ----------------------------------------- | ----------------------------------- |
| **Primary Focus**                | Full ELT: ingest + transform + export                            | SQL transforms only (no ingestion/export) | Ingestion pipelines (ETL/ELT)       |
| **Interface**                    | SQL-based DSL + CLI + UI                                         | SQL (Jinja templating), CLI               | UI-first, config files              |
| **Deployment**                   | Self-hosted, embedded, or CLI-based                              | Self-hosted / SaaS (dbt Cloud)            | Self-hosted or SaaS (Airbyte Cloud) |
| **Extensibility**                | Plugin SDK for connectors + Python UDFs                          | Jinja macros, Python hooks                | Connector SDK (Python, Singer)      |
| **Best For**                     | Lightweight internal data infra or embedded use in SaaS products | Centralized warehouse modeling            | Ingesting SaaS/DBs to warehouse     |
| **Use Case Fit**                 | ✅ Embedded ELT inside products                                   |                                           |                                     |
| ✅ Lightweight analyst ops        |                                                                  |                                           |                                     |
| ✅ Orchestrator-less pipelines    |                                                                  |                                           |                                     |
| ✅ Data movement + transformation | ✅ Complex models in large teams                                  |                                           |                                     |
| ✅ CI/CD + GitOps modeling        | ✅ High-scale ingestion                                           |                                           |                                     |
| ✅ Non-SQL users with GUI needs   |                                                                  |                                           |                                     |

SQLFlow can sit *between* these tools or *replace* them entirely depending on context:

* **Internal tooling**: Ideal for small data teams who need fast delivery without orchestration overhead.
* **Embedded SaaS**: Simple binary + CLI interface, Python SDK, and profile system make it viable for bundling into SaaS data pipelines.
* **Side-by-side with dbt**: Can ingest + export for dbt models, or wrap it entirely.
* **Side-by-side with Airbyte**: Can use SQLFlow for light ingestion and transformation, while delegating scale ingestion to Airbyte when needed.

**MVP Goals (Target: July 2025 - Adjusted Scope)**

* Core engine stability: `SOURCE`, `LOAD`, SQL transform, `EXPORT`, `INCLUDE`
* REST API & PostgreSQL connectors (Export ✅, Source – new)
* Google Sheets Connector (Source – new)
* Profile-based DuckDB engine (in-memory or persistent)
* CLI commands: `init`, `run`, `compile`, `list`, `connect test`, `connect list`
* DAG execution & error handling polish
* SaaS quickstart templates (1–2)

**Phase 2 (Q3–Q4 2025 - Adjusted Scope)**

* Web UI: run status, history, logs, DAG visualization
* Conditional Execution (`IF/ELSE`) for pipeline-level logic
* Python UDFs (`PYTHON_FUNC`) for hybrid SQL+Python logic
* Basic Incremental Loading Patterns
* SQL Macros / Reusable Snippets
* More SaaS connectors (e.g., HubSpot, GA4, Shopify)
* SDK refinement for connector extensibility
* CLI: `sqlflow viz`, `docs generate`
* Basic Lineage Tracking

**Phase 3+ (2026)**

* Serverless/embedded deployment options
* Pipeline versioning & Git integration
* Full Web-Based Pipeline Editor
* Marketplace for templates, connectors
* Distributed execution engine (e.g., Celery)

**Why It Matters**
SQLFlow helps teams go from data extraction to dashboard-ready exports without standing up Airflow, dbt, or multiple systems. Its DSL makes pipelines versionable and simple to audit, while the plugin model ensures extensibility.

---

**Visual Roadmap Slide (Adjusted)**

| Timeline           | Feature Set                                                    | Notes                                                                                                                |
| ------------------ | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Now – Jul 2025** | **MVP: Core Polish, SaaS Ingestion & Python UDFs**             | Polish Engine & Connectors. New REST Source, Google Sheets Connector, `PYTHON_FUNC`, Essential CLI.                  |
| Aug – Dec 2025     | **Phase 2: Monitoring, Extensibility & Advanced Control Flow** | Web UI, **`IF/ELSE` Conditions**, Basic Incrementals, SQL Macros, SDK refinement, more Connectors, CLI `viz`/`docs`. |
| Jan – Jun 2026     | **Phase 3: Enterprise + GUI**                                  | Serverless, Advanced Incrementals, Lineage, Pipeline Versioning, Editor.                                             |

---

**Impact vs Effort Prioritization Matrix (Adjusted)**

| Feature                                       | Impact | Effort       | Phase     | Notes                                                                 |
| --------------------------------------------- | ------ | ------------ | --------- | --------------------------------------------------------------------- |
| REST API Connector (Source capability)        | High   | Large        | MVP       | Export part exists, Source is new.                                    |
| PostgreSQL Connector (Source + Export Polish) | High   | Small        | MVP       | Exists, needs polish.                                                 |
| Google Sheets Connector                       | High   | Medium       | MVP       | New connector.                                                        |
| CLI: `connect test`, `connect list`           | Medium | Small        | MVP       | New CLI commands.                                                     |
| DuckDB Profile-Driven Engine (Polish)         | High   | Small        | MVP       | Exists, needs polish.                                                 |
| Secrets via `env.VAR` (Polish & Formalize)    | High   | Small        | MVP       | Variable system exists, formalize for secrets.                        |
| SaaS Quickstart Templates                     | High   | Small        | MVP       | Effort for templates themselves; depends on new connector completion. |
| **Conditional Execution (`IF/ELSE`)**         | High   | Medium-Large | Phase 2   | Parser may have initial support. Enhances pipeline logic.             |
| Web UI (logs, status, DAG view)               | High   | Large        | Phase 2   | Moved to Phase 2.                                                     |
| Python UDFs (`PYTHON_FUNC`)                   | High   | Medium       | Phase 2   | Follows MVP DAG execution polish.                                     |
| Basic Incremental Loading Patterns            | High   | Medium       | Phase 2   | Effort reduced for "basic patterns".                                  |
| SQL Macros / Reusable Snippets                | Medium | Medium       | Phase 2   |                                                                       |
| `sqlflow viz` (DAG CLI)                       | Medium | Medium       | Phase 2   |                                                                       |
| HubSpot (or similar) Connector                | High   | Medium       | Phase 2   | Moved to Phase 2.                                                     |
| Web UI: Config Editing & Advanced Lineage     | Medium | Large        | Phase 2/3 |                                                                       |
| Full Web-Based Pipeline Editor                | Medium | Large        | Phase 3   |                                                                       |
| Git/Pipeline Versioning                       | Medium | Medium       | Phase 2/3 |                                                                       |
| Marketplace (Templates, Connectors)           | Low    | Large        | Phase 3   |                                                                       |

---

**Wishlist Features - Good Ideas Not in MVP**

These features represent valuable enhancements for future consideration but won't be implemented in the MVP phase:

**CLI Experience Enhancements**

| Feature                                      | Description                                                       | Potential Impact                                      |
| -------------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------- |
| Enhanced CLI Output                          | Color-coding, icons, and rich formatting for command outputs       | Improved readability and user experience              |
| Interactive Repair Mode                      | `connect repair` to walk users through fixing connection issues    | Reduced troubleshooting time and better onboarding    |
| Connection Health Dashboard                  | `connect health` showing metrics for all connections               | Better monitoring and proactive maintenance           |
| Connection Configuration Wizard              | Interactive CLI for creating new connections                       | Simplified onboarding for new connections             |
| Connection Group Management                  | Group connections by environment or purpose                        | Better organization for complex setups                |
| Connection Parameter Inheritance             | Allow connections to inherit from base templates                   | Reduced config duplication and easier management      |

**Core Feature Wishlist**

| Feature                                      | Description                                                       | Potential Impact                                      |
| -------------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------- |
| Schema Evolution Handling                    | Automatic handling of schema changes in data sources              | Reduced pipeline failures from schema changes         |
| Data Quality Checks                          | Built-in validation rules for data quality                         | Improved data reliability and easier debugging        |
| Data Sampling for Development                | Run pipelines on sample data for faster development               | Faster development cycles and testing                 |
| Auto Retries with Backoff                    | Smart retry logic for transient failures                           | Improved reliability for flaky connections            |
| Connection Pooling                           | Reuse connections for better performance                           | Reduced resource usage and better scaling             |
| Notification Integrations                    | Send alerts to Slack, email, etc. on pipeline events              | Better operational awareness                          |
| Cache Layer for API Sources                  | Cache API results to reduce quota usage                            | Cost reduction and performance improvement            |
| Schedule Expression Library                  | Reusable schedule definitions for pipelines                        | Simplified scheduling management                      |
| Pipeline Templates with Parameters           | Parameterized templates for common pipeline patterns              | Faster pipeline creation and standardization          |
| Data Catalog Integration                     | Integrate with external data catalogs                              | Better metadata management and discovery              |
| Multi-Engine Support                         | Support multiple SQL engines in one project                        | More flexibility for different workloads             |

These features would significantly enhance SQLFlow's user experience and capabilities but are being deferred to focus on core MVP functionality. They will be reconsidered for later phases based on user feedback and evolving priorities.

---

