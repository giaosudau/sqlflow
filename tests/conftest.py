"""Pytest configuration for SQLFlow tests."""

import os
import pytest
import tempfile
from typing import Generator


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for tests.
    
    Yields:
        Path to the temporary directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_source_directive() -> str:
    """Return a sample SOURCE directive for testing.
    
    Returns:
        Sample SOURCE directive text
    """
    return """SOURCE users TYPE POSTGRES PARAMS {
        "connection": "postgresql://user:pass@localhost:5432/db",
        "table": "users"
    };"""


@pytest.fixture
def sample_pipeline() -> str:
    """Return a sample pipeline for testing.
    
    Returns:
        Sample pipeline text
    """
    return """SOURCE users TYPE POSTGRES PARAMS {
        "connection": "postgresql://user:pass@localhost:5432/db",
        "table": "users"
    };
    
    SOURCE sales TYPE CSV PARAMS {
        "path": "data/sales.csv",
        "has_header": true
    };"""
