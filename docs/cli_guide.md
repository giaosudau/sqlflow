# SQLFlow CLI: Command Line Interface Guide

> **Comprehensive guide for using SQLFlow's command line interface for managing data pipelines, connections, and Python UDFs.**

## Overview

The SQLFlow Command Line Interface (CLI) provides a powerful set of tools for creating, managing, and executing SQL-based data pipelines. The CLI enables you to initialize projects, compile and run pipelines, manage connections, and work with Python User-Defined Functions (UDFs).

---

## Global CLI Options

SQLFlow CLI supports the following global options that can be used with any command:

| Option      | Alias | Description                                     |
|-------------|-------|-------------------------------------------------|
| `--verbose` | `-v`  | Enable verbose output with technical details.   |
| `--quiet`   | `-q`  | Reduce output to essential information only.    |

Example:
```bash
sqlflow --verbose pipeline run example
sqlflow -q pipeline list
```

---

## Installation

SQLFlow CLI is automatically installed when you install the SQLFlow package:

```bash
pip install sqlflow-core
```

You can verify the installation by checking the version:

```bash
sqlflow --version
```

---

## Getting Started

### Initializing a New Project

Create a new SQLFlow project with the required directory structure:

```bash
sqlflow init my_project
cd my_project
```

This creates a new project with the following structure:
```
my_project/
├── pipelines/
│   └── example.sf    # Example pipeline file
├── profiles/         # For connection profiles
├── data/             # Data directory (auto-created)
├── python_udfs/      # For Python UDFs
└── output/           # For pipeline outputs
```

---

## Pipeline Management

The `pipeline` command group handles all pipeline-related operations.

### Listing Pipelines

List all available pipelines in your project:

```bash
sqlflow pipeline list
```

### Compiling a Pipeline

Compile a pipeline to validate its syntax and generate the execution plan:

```bash
sqlflow pipeline compile example
```

To save the compiled plan to a specific file:
```bash
sqlflow pipeline compile example --output target/compiled/custom_plan_name.json
```
If `--output` is not provided, the plan is saved to `target/compiled/<pipeline_name>.json` by default.
If compiling all pipelines (no specific pipeline name given), the `--output` flag is ignored, and each plan is saved to its default location.

> **Note:** Like the run command, always use just the pipeline name without path or file extension.

### Running a Pipeline

Execute a pipeline with optional variables:

```bash
sqlflow pipeline run example --profile dev --vars '{"date": "2023-10-25"}'
```

To run a pipeline from a previously compiled plan:
```bash
sqlflow pipeline run example --from-compiled
```
This will use the plan from `target/compiled/example.json` instead of recompiling.

### Pipeline Validation

Validate a pipeline without executing it:

```bash
sqlflow pipeline validate example
```

> **Note:** Like the other commands, always use just the pipeline name without path or file extension.

### Command Options

| Option      | Description                                     |
|-------------|-------------------------------------------------|
| `--profile` | Specify the connection profile to use           |
| `--vars`    | JSON string of variables to pass to the pipeline|
| `--dry-run` | Validate and compile without executing          |
| `--output`  | (compile only) Specify output file for the plan |
| `--from-compiled` | (run only) Use existing compiled plan       |

---

## Connection Management

The `connect`