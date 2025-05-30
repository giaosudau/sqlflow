# SQLFlow Installation Guide

SQLFlow delivers the **fastest time to value** in the data pipeline industry - from installation to working analytics in under 2 minutes. This guide covers installation options from basic to advanced.

## 🚀 Quick Start (90% of Users)

**Get working analytics in under 2 minutes:**

```bash
# Install SQLFlow with everything you need for analytics
pip install sqlflow-core

# Create a project with sample data and working pipelines
sqlflow init my_analytics
cd my_analytics

# See immediate results with 1,000 customers and 5,000 orders
sqlflow pipeline run customer_analytics
```

**That's it!** You now have working customer analytics with realistic data.

✅ **What's included in the basic installation:**
- DuckDB engine for lightning-fast analytical queries
- Apache Arrow for high-performance data processing  
- Pandas for data manipulation
- Rich CLI with beautiful output
- Complete sample data (1,000 customers, 5,000 orders, 500 products)
- Ready-to-run analytics pipelines

## 📊 Need Database Connectivity? (8% of Users)

```bash
# Add PostgreSQL support
pip install "sqlflow-core[postgres]"

# Add cloud storage (AWS S3 + Google Cloud)
pip install "sqlflow-core[cloud]"
```

## 🔧 Advanced Installation (2% of Users)

<details>
<summary><strong>Click to expand advanced options</strong></summary>

### Everything Included
```bash
# Complete installation with all features
pip install "sqlflow-core[all]"
```

### Development Installation
```bash
# For contributors - includes testing and linting tools
pip install "sqlflow-core[dev]"
```

### Virtual Environment (Recommended)
```bash
# Create virtual environment (prevents conflicts)
python -m venv sqlflow-env
source sqlflow-env/bin/activate  # Mac/Linux
# sqlflow-env\Scripts\activate  # Windows

# Install SQLFlow
pip install sqlflow-core
```

</details>

## ✅ Verify Installation

```bash
# Check version
sqlflow --version

# Test with sample project
sqlflow init test_project
cd test_project
sqlflow pipeline run customer_analytics

# View results
ls output/
```

## 🐳 Docker Installation

<details>
<summary><strong>Click for Docker setup</strong></summary>

```bash
# Using official Python image
FROM python:3.11-slim

# Install SQLFlow
RUN pip install sqlflow-core

# Verify installation
RUN sqlflow --version
```

</details>

## 🔍 Platform-Specific Optimizations

SQLFlow automatically handles platform differences:

- ✅ **Apple Silicon (M1/M2/M3)**: Optimized ARM compatibility
- ✅ **Intel/AMD Systems**: Pre-compiled binaries for speed
- ✅ **Linux ARM64**: Cloud and edge deployment ready

## 🛠️ Troubleshooting

<details>
<summary><strong>Common Issues and Solutions</strong></summary>

### Installation Fails
```bash
# Update tools first
python -m pip install --upgrade pip setuptools wheel

# Retry installation
pip install sqlflow-core
```

### Permission Errors
```bash
# Use virtual environment (recommended)
python -m venv sqlflow-env
source sqlflow-env/bin/activate
pip install sqlflow-core
```

### Corporate Firewall/Proxy
```bash
# Configure proxy
pip install --proxy http://proxy.company.com:8080 sqlflow-core

# Use trusted hosts
pip install --trusted-host pypi.org --trusted-host pypi.python.org sqlflow-core
```

### Platform-Specific Issues

**macOS:**
```bash
# Install Xcode Command Line Tools if needed
xcode-select --install
```

**Windows:**
```bash
# Install Visual Studio Build Tools if needed
# Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get install build-essential libpq-dev

# CentOS/RHEL  
sudo yum install gcc postgresql-devel
```

</details>

## 🚀 Next Steps

Once installed:

1. **Create your first project:** `sqlflow init my_project`
2. **Run sample analytics:** `sqlflow pipeline run customer_analytics`  
3. **Explore the results:** Check the `output/` directory
4. **Read the quickstart:** [Getting Started Guide](docs/user/getting_started.md)

