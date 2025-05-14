# SQLFlow

SQLFlow is a SQL-based data pipeline tool that enables users to define, execute, visualize, and manage data transformations using a SQL-based domain-specific language (DSL).

## Features

- Define data sources and transformations using a SQL-based syntax
- Create modular, reusable pipeline components
- Execute pipelines locally or in a distributed environment
- Visualize pipelines as interactive DAGs (Directed Acyclic Graphs)
- Manage project structure with a standardized approach
- Connect to various data sources through an extensible connector framework

## Installation

```bash
pip install sqlflow
```

## Quick Start

```bash
# Initialize a new project
sqlflow init my_project

# Create a pipeline
cd my_project
# Edit pipelines/my_pipeline.sf

# Run the pipeline
sqlflow run pipelines/my_pipeline.sf

# Visualize the pipeline
sqlflow viz pipelines/my_pipeline.sf
```

## Documentation

For more information, see the [documentation](https://sqlflow.readthedocs.io).

## License

MIT
