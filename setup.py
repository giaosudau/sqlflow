"""Setup script for SQLFlow."""

from setuptools import setup, find_packages

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
    ],
    entry_points={
        "console_scripts": [
            "sqlflow=sqlflow.sqlflow.main:main",
        ],
    },
    python_requires=">=3.8",
)
