"""Setup script for SQLFlow."""

from setuptools import find_packages, setup

setup(
    name="sqlflow",
    version="0.1.0",
    description="SQL-based data pipeline tool",
    author="SQLFlow Team",
    packages=find_packages(),
    install_requires=[
        "duckdb>=1.2.0",  # Core SQL engine
        "pandas>=2.0.0",  # Data manipulation
        "pyarrow>=10.0.0",  # Data format and interchange
        "click>=8.0.0",  # CLI framework
        "networkx>=3.0",  # Dependency graph handling
        "typer>=0.9.0,<0.10.0",  # Modern CLI framework
        "psycopg2-binary>=2.9.0",  # PostgreSQL connector
        "boto3>=1.26.0",  # AWS S3 connector
        "requests>=2.28.0",  # HTTP/REST connector
        "pyyaml>=6.0",  # Configuration handling
        "typing-extensions>=4.0.0",  # Type hints support
    ],
    package_data={
        "sqlflow": ["py.typed"],
    },
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.1.0",
            "black>=22.1.0",
            "isort>=5.10.1",
            "flake8>=4.0.1",
            "autoflake>=2.2.0",
            "pre-commit>=3.0.0",
            "mypy>=1.0.0",  # Type checking
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.1.0",
            "mock>=5.0.0",  # For mocking in tests
        ],
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=1.0.0",
            "myst-parser>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sqlflow=sqlflow.cli.main:cli",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
