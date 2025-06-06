# SQLFlow Installation Guide

Get SQLFlow running on your system in minutes with this comprehensive installation guide.

## 🚀 Quick Install (Recommended)

For most users, the quickest way to get started:

```bash
# Install SQLFlow with core functionality
pip install sqlflow-core

# Verify installation
sqlflow --version

# Create your first project
sqlflow init my_first_project
cd my_first_project

# See immediate results
sqlflow pipeline run customer_analytics
```

**That's it!** You now have SQLFlow running with working customer analytics.

## 📦 Installation Options

### Option 1: Basic Installation (90% of users)

```bash
pip install sqlflow-core
```

**Includes:**
- ✅ Core SQLFlow functionality
- ✅ CSV file processing
- ✅ DuckDB engine (in-memory and persistent)
- ✅ Python UDF support
- ✅ CLI tools and validation
- ✅ Auto-generated sample data

**Perfect for:**
- Local development and testing
- CSV-based analytics
- Learning SQLFlow
- Most data analysis tasks

### Option 2: Database Connectivity

```bash
# Add PostgreSQL support
pip install "sqlflow-core[postgres]"

# Add all database connectors
pip install "sqlflow-core[database]"
```

**Additional connectors:**
- ✅ PostgreSQL
- ✅ MySQL (planned)
- ✅ SQLite (planned)

### Option 3: Cloud Storage Support

```bash
# Add AWS S3 support
pip install "sqlflow-core[aws]"

# Add Google Cloud support
pip install "sqlflow-core[gcp]"

# Add all cloud storage
pip install "sqlflow-core[cloud]"
```

**Additional connectors:**
- ✅ AWS S3
- ✅ Google Cloud Storage
- ✅ Azure Blob Storage (planned)

### Option 4: Everything Included

```bash
# Full installation with all connectors
pip install "sqlflow-core[all]"
```

**Includes everything:**
- ✅ All database connectors
- ✅ All cloud storage connectors
- ✅ All optional dependencies
- ✅ Development tools

## 🖥️ Platform-Specific Instructions

### Windows

**Prerequisites:**
- Python 3.10 or higher
- pip package manager

**Installation:**
```powershell
# Using Command Prompt or PowerShell
pip install sqlflow-core

# Verify installation
sqlflow --version
```

**Common Issues:**
- **Path issues**: Add Python Scripts to PATH
- **Permission errors**: Use `--user` flag: `pip install --user sqlflow-core`
- **Long path support**: Enable long path support in Windows settings

### macOS

**Prerequisites:**
- Python 3.10 or higher (recommended: via Homebrew)

**Installation:**
```bash
# Install Python via Homebrew (if needed)
brew install python

# Install SQLFlow
pip install sqlflow-core

# Verify installation
sqlflow --version
```

**Using Homebrew Python:**
```bash
# If using Homebrew Python specifically
/usr/local/bin/pip3 install sqlflow-core
```

### Linux (Ubuntu/Debian)

**Prerequisites:**
```bash
# Update package list
sudo apt update

# Install Python and pip
sudo apt install python3 python3-pip

# Install build tools (if needed)
sudo apt install build-essential python3-dev
```

**Installation:**
```bash
# Install SQLFlow
pip3 install sqlflow-core

# Verify installation
sqlflow --version
```

### Linux (CentOS/RHEL/Fedora)

**Prerequisites:**
```bash
# CentOS/RHEL
sudo yum install python3 python3-pip

# Fedora
sudo dnf install python3 python3-pip
```

**Installation:**
```bash
# Install SQLFlow
pip3 install sqlflow-core

# Verify installation
sqlflow --version
```

## 🐳 Docker Installation

For containerized deployments or isolated environments:

### Quick Start with Docker

```bash
# Pull the official SQLFlow image
docker pull sqlflow/sqlflow:latest

# Run SQLFlow in a container
docker run -it --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  sqlflow/sqlflow:latest \
  sqlflow init my_project

# Run pipelines in container
docker run -it --rm \
  -v $(pwd)/my_project:/workspace \
  -w /workspace \
  sqlflow/sqlflow:latest \
  sqlflow pipeline run customer_analytics
```

### Docker Compose Setup

Create a `docker-compose.yml` file:

```yaml
version: '3.8'
services:
  sqlflow:
    image: sqlflow/sqlflow:latest
    volumes:
      - ./:/workspace
    working_dir: /workspace
    command: sqlflow --help
```

Run with:
```bash
docker-compose run sqlflow init my_project
docker-compose run sqlflow pipeline run customer_analytics
```

## 🔧 Development Installation

For contributors and developers who want to work with SQLFlow source code:

### Clone and Install from Source

```bash
# Clone the repository
git clone https://github.com/sqlflow/sqlflow.git
cd sqlflow

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .

# Install development dependencies
pip install -e ".[dev]"

# Verify installation
sqlflow --version
```

### Development Dependencies

```bash
# Install all development tools
pip install -e ".[dev]"
```

**Includes:**
- ✅ Testing frameworks (pytest, coverage)
- ✅ Code formatting (black, isort)
- ✅ Linting tools (flake8, mypy)
- ✅ Documentation tools (sphinx)
- ✅ Pre-commit hooks

## 🌐 Virtual Environment Setup (Recommended)

### Why Use Virtual Environments?

Virtual environments prevent package conflicts and ensure clean installations:

```bash
# Create virtual environment
python -m venv sqlflow-env

# Activate virtual environment
# On macOS/Linux:
source sqlflow-env/bin/activate

# On Windows:
sqlflow-env\Scripts\activate

# Install SQLFlow
pip install sqlflow-core

# Verify installation
sqlflow --version
```

### Conda Environment

If you prefer conda:

```bash
# Create conda environment
conda create -n sqlflow python=3.10

# Activate environment
conda activate sqlflow

# Install SQLFlow
pip install sqlflow-core
```

## ✅ Verify Installation

### Quick Verification

```bash
# Check SQLFlow version
sqlflow --version

# Check available commands
sqlflow --help

# Create test project
sqlflow init test-project

# Run test pipeline
cd test-project
sqlflow pipeline run customer_analytics

# Check results
ls output/
```

### Comprehensive Verification

```bash
# Validate your installation
sqlflow system check

# Test all core functionality
sqlflow init verification-project
cd verification-project

# Run all example pipelines
sqlflow pipeline run customer_analytics
sqlflow pipeline run data_quality_monitoring
sqlflow pipeline run basic_pipeline

# Check all outputs
ls -la output/
```

## 🚨 Troubleshooting

### Common Installation Issues

#### 1. Python Version Issues

**Problem**: "Python version not supported"
```bash
# Check Python version
python --version

# SQLFlow requires Python 3.10+
# Install newer Python version
```

**Solution**:
- macOS: `brew install python@3.10`
- Windows: Download from python.org
- Linux: Use package manager or pyenv

#### 2. Permission Errors

**Problem**: "Permission denied" during installation
```bash
# Use user installation
pip install --user sqlflow-core

# Or create virtual environment
python -m venv sqlflow-env
source sqlflow-env/bin/activate
pip install sqlflow-core
```

#### 3. Network/Proxy Issues

**Problem**: Cannot connect to PyPI
```bash
# Use proxy
pip install --proxy http://proxy.company.com:8080 sqlflow-core

# Use alternative index
pip install -i https://pypi.org/simple/ sqlflow-core
```

#### 4. Build Dependencies Missing

**Problem**: "Failed building wheel" for dependencies
```bash
# Install build tools
# Ubuntu/Debian:
sudo apt install build-essential python3-dev

# CentOS/RHEL:
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel

# macOS:
xcode-select --install
```

#### 5. DuckDB Installation Issues

**Problem**: DuckDB fails to install
```bash
# Install with pre-built wheels
pip install --only-binary=duckdb sqlflow-core

# Or install DuckDB separately
pip install duckdb
pip install sqlflow-core
```

### Getting Help

If you encounter issues not covered here:

1. **Check GitHub Issues**: [SQLFlow Issues](https://github.com/sqlflow/sqlflow/issues)
2. **Search Documentation**: [SQLFlow Docs](https://sqlflow.readthedocs.io)
3. **Ask the Community**: [GitHub Discussions](https://github.com/sqlflow/sqlflow/discussions)
4. **Report Bugs**: Create a new issue with:
   - Operating system and version
   - Python version (`python --version`)
   - SQLFlow version (`sqlflow --version`)
   - Complete error message
   - Steps to reproduce

## 🔄 Upgrade SQLFlow

### Standard Upgrade

```bash
# Upgrade to latest version
pip install --upgrade sqlflow-core

# Check new version
sqlflow --version
```

### Upgrade with Additional Components

```bash
# Upgrade with all components
pip install --upgrade "sqlflow-core[all]"

# Upgrade specific components
pip install --upgrade "sqlflow-core[postgres,aws]"
```

### Development Upgrade

```bash
# If installed from source
cd sqlflow
git pull origin main
pip install -e .
```

## 🎯 Next Steps

Once SQLFlow is installed:

1. **Start with Quickstart**: [2-Minute Quickstart](quickstart.md)
2. **Learn the Basics**: [Building Analytics Pipelines](user-guides/building-analytics-pipelines.md)
3. **Explore Examples**: Check out the `/examples/` directory
4. **Join Community**: [GitHub Discussions](https://github.com/sqlflow/sqlflow/discussions)

## 📋 System Requirements

### Minimum Requirements
- **Python**: 3.10 or higher
- **RAM**: 1GB minimum, 4GB recommended
- **Storage**: 100MB for installation, additional for data
- **OS**: Windows 10+, macOS 10.14+, Linux (recent distributions)

### Recommended Requirements
- **Python**: 3.11 or 3.12 (latest stable)
- **RAM**: 8GB or more for large datasets
- **Storage**: SSD recommended for better performance
- **OS**: Latest stable versions

---

**Installation complete!** You're ready to start building data pipelines with SQLFlow. Head to the [Quickstart Guide](quickstart.md) to see SQLFlow in action. 