"""Setup script for SQLFlow."""

from setuptools import find_packages, setup

setup(
    name="sqlflow",
    version="0.1.0",
    description="SQL-based data pipeline tool",
    author="SQLFlow Team",
    packages=find_packages(),
    install_requires=[
        "duckdb",
        "pandas",
        "pyarrow",
        "click",
        "networkx",
        "typer",
        "psycopg2-binary",  # For PostgreSQL connectors
        "boto3",  # For S3 connector
        "requests",  # For REST connector
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
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sqlflow=sqlflow.cli.main:cli",
        ],
    },
    python_requires=">=3.8",
)
