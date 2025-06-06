# SQLFlow Installation

> **Function**: This document covers the complete SQLFlow installation process, from basic pip installation to development setups. Choose the right installation option for your use case.

**Status**: ✅ Stable  
**Since Version**: 0.1.0  
**Verification Status**: ✅ Verified against SQLFlow v0.1.7+

## Overview

SQLFlow offers fast installation and immediate productivity - from installation to working analytics in under 1 minute. Multiple installation options provide exactly what you need, from basic analytics to full development environments.

### Key Benefits
- **Fast Setup**: Under 1-minute from install to results
- **Flexible Options**: Choose from core analytics to full development with cloud connectors
- **Zero Configuration**: Auto-generated sample data and working pipelines out of the box
- **Cross-Platform**: Optimized for macOS (Intel/Apple Silicon), Windows, and Linux (x86_64/ARM64)

## Quick Example

```bash
# Install SQLFlow with everything for analytics
pip install sqlflow-core

# Get working results immediately
sqlflow init analytics-demo && cd analytics-demo
sqlflow pipeline run customer_analytics  # 0.08 seconds execution!
```

**Expected Output**: Complete customer analytics with 1,000 customers, 5,000 orders analyzed in under 1 minute total.

## Installation Options

### Basic Analytics (Recommended for Most Users)
**Target Users**: Data analysts, business intelligence developers, data scientists
**What's Included**: DuckDB engine, Pandas, Arrow, CSV/Parquet support, CLI

```bash
# Core installation for 90% of users
pip install sqlflow-core
```

**Verification**:
```bash
sqlflow --version && sqlflow init test-project
```

### Database Integration
**Target Users**: Data engineers connecting to existing databases
**What's Included**: Core + database connectors

```bash
# PostgreSQL support
pip install "sqlflow-core[postgres]"

# MySQL support  
pip install "sqlflow-core[mysql]"

# All database connectors
pip install "sqlflow-core[databases]"
```

### Cloud Storage Integration
**Target Users**: Teams using cloud data storage
**What's Included**: Core + cloud storage connectors

```bash
# AWS S3 support
pip install "sqlflow-core[aws]"

# Google Cloud Storage
pip install "sqlflow-core[gcp]"

# Azure Storage
pip install "sqlflow-core[azure]"

# All cloud providers
pip install "sqlflow-core[cloud]"
```

### Complete Installation
**Target Users**: Organizations needing all functionality

```bash
# Everything included
pip install "sqlflow-core[all]"
```

### Development Installation
**Target Users**: Contributors and advanced users

```bash
# Development tools included
pip install "sqlflow-core[dev]"
```

## GitHub Installation

Install directly from the latest source code:

### Latest Release from GitHub
```bash
# Install latest release
pip install git+https://github.com/sqlflow/sqlflow.git

# Install specific version/tag
pip install git+https://github.com/sqlflow/sqlflow.git@v0.1.7
```

### Development Branch
```bash
# Install from development branch
pip install git+https://github.com/sqlflow/sqlflow.git@main

# Install in editable mode for development
git clone https://github.com/sqlflow/sqlflow.git
cd sqlflow
pip install -e .
```

**Verification**:
```bash
sqlflow --version  # Should show installed version
sqlflow init github-test && cd github-test
sqlflow pipeline run customer_analytics
```

## Configuration

### Installation Options Matrix

| Package | Install Command | What's Included |
|---------|-----------------|-----------------|
| **Core** | `pip install sqlflow-core` | DuckDB, Pandas, Arrow, CLI |
| **PostgreSQL** | `pip install "sqlflow-core[postgres]"` | Core + PostgreSQL connector |
| **MySQL** | `pip install "sqlflow-core[mysql]"` | Core + MySQL connector |
| **Databases** | `pip install "sqlflow-core[databases]"` | Core + all database connectors |
| **AWS** | `pip install "sqlflow-core[aws]"` | Core + S3 connector |
| **GCP** | `pip install "sqlflow-core[gcp]"` | Core + GCS connector |
| **Azure** | `pip install "sqlflow-core[azure]"` | Core + Azure connectors |
| **Cloud** | `pip install "sqlflow-core[cloud]"` | Core + all cloud connectors |
| **All** | `pip install "sqlflow-core[all]"` | Everything included |
| **Dev** | `pip install "sqlflow-core[dev]"` | All + testing tools, linting |
| **GitHub** | `pip install git+https://github.com/sqlflow/sqlflow.git` | Latest source code |

### Required Parameters
| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| Python Version | string | Python 3.10 or higher | `3.10`, `3.11`, `3.12` |
| pip | tool | Package installer | Latest version recommended |

### Optional Parameters
| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| Virtual Environment | boolean | Recommended | Isolate dependencies | `python -m venv sqlflow-env` |
| Upgrade pip | boolean | true | Use latest pip features | `pip install --upgrade pip` |

## Advanced Usage

### Virtual Environment Setup (Recommended)
```bash
# Create virtual environment
python -m venv sqlflow-env
source sqlflow-env/bin/activate  # Mac/Linux
# sqlflow-env\Scripts\activate  # Windows

# Install SQLFlow
pip install sqlflow-core

# Verify isolation
which sqlflow  # Should point to virtual environment
```

### Docker Installation
```dockerfile
# Using official Python image
FROM python:3.11-slim

# Install SQLFlow
RUN pip install sqlflow-core

# Verify installation  
RUN sqlflow --version

# Create working directory
WORKDIR /workspace

# Optional: Create sample project
RUN sqlflow init sample-project
```

### Enterprise Setup
```bash
# Complete installation for organizations
pip install "sqlflow-core[all]"

# Verify all connectors
python -c "
from sqlflow.connectors.postgres import PostgresConnector
from sqlflow.connectors.s3 import S3Connector  
print('✅ All connectors available')
"
```

## Troubleshooting

### Common Issues

#### Issue: pip install fails with permission errors
**Symptoms**: `PermissionError: [Errno 13]` during installation
**Cause**: Installing system-wide without proper permissions
**Solution**: 
```bash
# Use virtual environment (recommended)
python -m venv sqlflow-env
source sqlflow-env/bin/activate  # Mac/Linux
pip install sqlflow-core
```

#### Issue: "No module named sqlflow" after installation
**Symptoms**: `ModuleNotFoundError` when running `sqlflow --version`
**Cause**: Installed in different Python environment than executing
**Solution**:
```bash
# Check which Python is running
which python && which pip

# Reinstall in correct environment
pip install sqlflow-core
python -c "import sqlflow; print('✅ Working')"
```

#### Issue: Slow installation on macOS Apple Silicon
**Symptoms**: Installation takes >2 minutes on M1/M2/M3
**Cause**: Building dependencies from source instead of using wheels
**Solution**:
```bash
# Update pip and use binary wheels
pip install --upgrade pip setuptools wheel
pip install --only-binary=all sqlflow-core
```

### Error Messages

| Error Message | Cause | Solution |
|---------------|-------|----------|
| `"No compatible distribution found"` | Unsupported Python version | Upgrade to Python 3.10+ |
| `"Could not find a version that satisfies"` | Network/proxy issues | Use `--trusted-host` flags |
| `"Microsoft Visual C++ required"` | Missing build tools on Windows | Install Visual Studio Build Tools |

## Testing

### Verification Steps
1. **Basic Installation Test**:
   ```bash
   sqlflow --version  # Should show version number
   ```

2. **Functionality Test**:
   ```bash
   sqlflow init test-install && cd test-install
   sqlflow pipeline run customer_analytics
   ls output/  # Should show CSV files
   ```

3. **Performance Test**:
   ```bash
   time sqlflow pipeline run customer_analytics
   # Should complete in <1 second
   ```

### Test Coverage
- **Platform Tests**: macOS (Intel/ARM), Windows, Linux (x86_64/ARM64)
- **Python Versions**: 3.10, 3.11, 3.12
- **Installation Methods**: pip, GitHub, Docker, virtual environments
- **Package Variants**: Core, cloud, databases, all, dev

## Related Features

- **[Quickstart Tutorial](quickstart.md)**: Get from installation to results in under 1 minute
- **[CLI Reference](../reference/cli.md)**: Complete command-line interface documentation
- **[Project Structure](../user-guide/fundamentals/)**: Understanding SQLFlow project organization

## Migration Notes

### From Previous Versions
- **v0.1.6 to v0.1.7**: No breaking changes, direct upgrade supported
- **v0.1.5 to v0.1.7**: Update project templates with `sqlflow init --upgrade`

### Breaking Changes
No breaking changes in current stable releases. All installation methods remain compatible.

## Resources

- **GitHub Repository**: [https://github.com/sqlflow/sqlflow](https://github.com/sqlflow/sqlflow)
- **Installation Scripts**: [`/scripts/install/`](../../scripts/install/)
- **Docker Examples**: [`/examples/docker/`](../../examples/docker/)
- **CI/CD Examples**: [`/.github/workflows/`](../../.github/workflows/)

---

**🎯 Installation Complete!** Choose your path:
- **New Users**: [Quickstart Tutorial](quickstart.md) - Get results in under 1 minute
- **Advanced Users**: [User Guide Fundamentals](../user-guide/fundamentals/)
- **Integration Projects**: [Connectors Documentation](../user-guide/connectors/) 