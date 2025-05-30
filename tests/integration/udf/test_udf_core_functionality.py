"""Core UDF functionality integration tests.

This module tests the fundamental UDF operations:
- UDF discovery and registration
- Basic scalar and table UDF execution
- Query processing and substitution
- Multiple UDF file handling
- Namespace isolation

Tests follow naming convention: test_{component}_{use_case}
Each test represents a real-world scenario users encounter.
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


@pytest.fixture
def core_udf_test_env() -> Generator[Dict[str, Any], None, None]:
    """Create test environment with basic UDF functions for core testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create main UDF file
        main_udf_file = create_basic_udf_file(udf_dir)

        # Create subdirectory with additional UDFs
        sub_dir = os.path.join(udf_dir, "analytics")
        os.makedirs(sub_dir, exist_ok=True)
        sub_udf_file = create_analytics_udf_file(sub_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "main_udf_file": main_udf_file,
            "sub_udf_file": sub_udf_file,
        }


def create_basic_udf_file(udf_dir: str) -> Path:
    """Create UDF file with basic scalar and table functions."""
    udf_file = Path(udf_dir) / "basic_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Basic UDF functions for testing core functionality."""

import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def add_ten(value: float) -> float:
    """Add 10 to a numeric value."""
    return value + 10


@python_scalar_udf  
def format_name(first: str, last: str) -> str:
    """Format full name from first and last name."""
    return f"{first} {last}".strip()


@python_scalar_udf
def calculate_tax(amount: float, rate: float = 0.08) -> float:
    """Calculate tax amount with default rate."""
    # Convert Decimal to float if needed for compatibility with DuckDB
    from decimal import Decimal
    amount_float = float(amount) if isinstance(amount, Decimal) else amount
    rate_float = float(rate) if isinstance(rate, Decimal) else rate
    return amount_float * rate_float


@python_table_udf(output_schema={
    "id": "INTEGER",
    "value": "DOUBLE", 
    "category": "VARCHAR"
})
def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich data with calculated category."""
    result = df.copy()
    result["category"] = result["value"].apply(
        lambda x: "high" if x > 100 else "medium" if x > 50 else "low"
    )
    return result[["id", "value", "category"]]


@python_table_udf(output_schema={
    "customer_id": "INTEGER",
    "total_amount": "DOUBLE",
    "order_count": "INTEGER" 
})
def customer_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize customer orders."""
    summary = df.groupby("customer_id").agg({
        "amount": "sum",
        "order_id": "count"
    }).reset_index()
    
    summary.columns = ["customer_id", "total_amount", "order_count"]
    return summary
'''
        )
    return udf_file


def create_analytics_udf_file(sub_dir: str) -> Path:
    """Create UDF file in subdirectory for namespace testing."""
    udf_file = Path(sub_dir) / "analytics_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Analytics UDFs in subdirectory for namespace testing."""

import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def calculate_growth_rate(current: float, previous: float) -> float:
    """Calculate growth rate between two values."""
    if previous == 0:
        return 0.0
    return (current - previous) / previous * 100


@python_table_udf(output_schema={
    "period": "VARCHAR",
    "revenue": "DOUBLE",
    "growth_rate": "DOUBLE"
})
def revenue_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze revenue growth over periods."""
    result = df.copy()
    result = result.sort_values("period")
    
    # Calculate growth rate compared to previous period
    result["growth_rate"] = result["revenue"].pct_change() * 100
    result["growth_rate"] = result["growth_rate"].fillna(0)
    
    return result[["period", "revenue", "growth_rate"]]
'''
        )
    return udf_file


class TestUDFDiscovery:
    """Test UDF discovery functionality."""

    def test_discovery_single_file_finds_all_udfs(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User runs discovery on project with single UDF file."""
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        udfs = manager.discover_udfs()

        # Should find all UDFs from main file
        udf_names = list(udfs.keys())
        expected_names = [
            "python_udfs.basic_udfs.add_ten",
            "python_udfs.basic_udfs.format_name",
            "python_udfs.basic_udfs.calculate_tax",
            "python_udfs.basic_udfs.enrich_data",
            "python_udfs.basic_udfs.customer_summary",
        ]

        for name in expected_names:
            assert name in udf_names, f"UDF {name} not discovered"

        # Check UDF types from info
        scalar_udfs = [
            name for name in udf_names if manager.get_udf_info(name)["type"] == "scalar"
        ]
        table_udfs = [
            name for name in udf_names if manager.get_udf_info(name)["type"] == "table"
        ]

        assert len(scalar_udfs) >= 3
        assert len(table_udfs) >= 2

    def test_discovery_multiple_files_with_subdirectories(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User has UDFs in multiple files including subdirectories."""
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        udfs = manager.discover_udfs()

        # Should find UDFs from both main file and subdirectory
        udf_names = list(udfs.keys())

        # From main file
        assert any("add_ten" in name for name in udf_names)
        assert any("enrich_data" in name for name in udf_names)

        # From subdirectory
        assert any("calculate_growth_rate" in name for name in udf_names)
        assert any("revenue_analysis" in name for name in udf_names)

        # Verify reasonable total count
        assert len(udfs) >= 7  # At least 5 from main + 2 from sub

    def test_discovery_empty_directory_returns_empty_list(self) -> None:
        """User runs discovery on empty directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = PythonUDFManager(project_dir=tmp_dir)

            udfs = manager.discover_udfs()

            assert udfs == {}

    def test_discovery_ignores_non_python_files(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User has non-Python files in UDF directory."""
        # Add non-Python files
        udf_dir = core_udf_test_env["udf_dir"]
        (Path(udf_dir) / "readme.txt").write_text("Not a Python file")
        (Path(udf_dir) / "config.json").write_text('{"setting": "value"}')

        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Should still find reasonable number of Python UDFs
        assert len(udfs) >= 7  # Same as before


class TestUDFRegistration:
    """Test UDF registration with database engine."""

    def test_registration_scalar_udfs_can_be_called(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User registers scalar UDFs and calls them in SQL."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        # Discover and register UDFs
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Test scalar UDF calls
        result = engine.execute_query("SELECT add_ten(5) as result").fetchdf()
        assert result.iloc[0]["result"] == 15

        result = engine.execute_query(
            "SELECT format_name('John', 'Doe') as full_name"
        ).fetchdf()
        assert result.iloc[0]["full_name"] == "John Doe"

        result = engine.execute_query("SELECT calculate_tax(100, 0.1) as tax").fetchdf()
        assert result.iloc[0]["tax"] == 10.0

    def test_registration_table_udfs_can_be_called(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User registers table UDFs and uses them programmatically."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create test data
        test_data = pd.DataFrame({"id": [1, 2, 3], "value": [75.0, 125.0, 25.0]})

        # Test table UDF call using programmatic execution
        result = engine.execute_table_udf("enrich_data", test_data)

        assert len(result) == 3
        assert result.iloc[0]["category"] == "medium"  # 75 > 50
        assert result.iloc[1]["category"] == "high"  # 125 > 100
        assert result.iloc[2]["category"] == "low"  # 25 <= 50

    def test_registration_handles_duplicate_names_gracefully(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User registers same UDFs multiple times."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        # Discover and register twice - should not error
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)
        manager.register_udfs_with_engine(engine)

        # UDF should still work
        result = engine.execute_query("SELECT add_ten(10) as result").fetchdf()
        assert result.iloc[0]["result"] == 20


class TestUDFExecution:
    """Test UDF execution in realistic scenarios."""

    def test_execution_scalar_udf_with_real_data(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User applies scalar UDF to real dataset."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create realistic customer data
        engine.execute_query(
            """
            CREATE TABLE customers AS
            SELECT * FROM VALUES
                ('John', 'Smith', 1000.0),
                ('Jane', 'Doe', 2500.0),
                ('Bob', 'Johnson', 750.0)
            AS t(first_name, last_name, purchase_amount)
        """
        )

        # Apply multiple UDFs in single query
        result = engine.execute_query(
            """
            SELECT 
                format_name(first_name, last_name) as full_name,
                add_ten(purchase_amount) as adjusted_amount,
                calculate_tax(purchase_amount) as tax_owed
            FROM customers
            ORDER BY purchase_amount DESC
        """
        ).fetchdf()

        assert len(result) == 3
        assert result.iloc[0]["full_name"] == "Jane Doe"
        assert result.iloc[0]["adjusted_amount"] == 2510.0
        assert result.iloc[0]["tax_owed"] == 200.0  # 2500 * 0.08

    def test_execution_table_udf_with_aggregation(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User uses table UDF for data aggregation and analysis."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create order data as DataFrame for table UDF
        order_data = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "customer_id": [101, 101, 102, 101, 103],
                "amount": [250.0, 150.0, 300.0, 100.0, 200.0],
            }
        )

        # Use table UDF for customer summary programmatically
        result = engine.execute_table_udf("customer_summary", order_data)

        assert len(result) == 3

        # Customer 101: 3 orders, $500 total
        customer_101 = result[result["customer_id"] == 101].iloc[0]
        assert customer_101["order_count"] == 3
        assert customer_101["total_amount"] == 500.0

        # Customer 102: 1 order, $300 total
        customer_102 = result[result["customer_id"] == 102].iloc[0]
        assert customer_102["order_count"] == 1
        assert customer_102["total_amount"] == 300.0

    def test_execution_udfs_in_complex_query(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User combines multiple UDFs in complex analytical query."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create sales data as DataFrame for table UDF
        sales_data = pd.DataFrame(
            {
                "period": ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"],
                "revenue": [100000.0, 120000.0, 110000.0, 135000.0],
            }
        )

        # Use table UDF to get enriched data first
        enriched_data = engine.execute_table_udf("revenue_analysis", sales_data)

        # Register the result as a table for SQL query
        engine.register_table("enriched", enriched_data)

        # Complex query using both table and scalar UDFs
        result = engine.execute_query(
            """
            SELECT 
                period,
                revenue,
                growth_rate,
                add_ten(growth_rate) as adjusted_growth_rate,
                calculate_tax(revenue, 0.25) as tax_estimate
            FROM enriched
            ORDER BY period
        """
        ).fetchdf()

        assert len(result) == 4

        # Q2 should show growth compared to Q1
        q2_data = result[result["period"] == "2024-Q2"].iloc[0]
        assert q2_data["growth_rate"] == pytest.approx(
            20.0, rel=1e-6
        )  # (120k - 100k) / 100k * 100
        assert q2_data["adjusted_growth_rate"] == pytest.approx(
            30.0, rel=1e-6
        )  # 20 + 10
        assert q2_data["tax_estimate"] == 30000.0  # 120k * 0.25


class TestUDFQueryProcessing:
    """Test UDF query processing and substitution."""

    def test_query_processing_with_variable_substitution(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User uses variables in UDF calls for dynamic queries."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Create test data
        engine.execute_query(
            """
            CREATE TABLE products AS
            SELECT * FROM VALUES
                (1, 'Widget A', 50.0),
                (2, 'Widget B', 75.0),
                (3, 'Widget C', 120.0)
            AS t(id, name, price)
        """
        )

        # Query with variable for tax rate
        variables = {"tax_rate": 0.1, "bonus": 20}

        query = """
            SELECT 
                name,
                price,
                add_ten(price) + {bonus} as adjusted_price,
                calculate_tax(price, {tax_rate}) as tax
            FROM products
            WHERE price > 60
        """

        # Process query with variables
        processed_query = query.format(**variables)
        result = engine.execute_query(processed_query).fetchdf()

        assert len(result) == 2  # Widget B and C

        widget_b = result[result["name"] == "Widget B"].iloc[0]
        assert widget_b["adjusted_price"] == 105.0  # 75 + 10 + 20
        assert widget_b["tax"] == 7.5  # 75 * 0.1

    def test_query_processing_preserves_udf_functionality(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """User's complex queries with UDFs are processed correctly."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])
        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Use table UDF programmatically to create enriched data
        input_data = pd.DataFrame({"id": [1, 2], "value": [100.0, 200.0]})

        enriched_data = engine.execute_table_udf("enrich_data", input_data)
        engine.register_table("step2", enriched_data)

        # Multi-step query with CTEs and UDFs
        query = """
            SELECT 
                id,
                value,
                category,
                add_ten(value) as enhanced_value,
                format_name(category, 'tier') as tier_name
            FROM step2
            ORDER BY id
        """

        result = engine.execute_query(query).fetchdf()

        assert len(result) == 2
        assert result.iloc[0]["category"] == "medium"  # 100 -> medium
        assert result.iloc[0]["enhanced_value"] == 110.0
        assert result.iloc[0]["tier_name"] == "medium tier"

        assert result.iloc[1]["category"] == "high"  # 200 -> high
        assert result.iloc[1]["enhanced_value"] == 210.0
        assert result.iloc[1]["tier_name"] == "high tier"


class TestUDFNamespaceIsolation:
    """Test UDF namespace isolation and conflict resolution."""

    def test_namespace_isolation_prevents_conflicts(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """UDFs from different files don't interfere with each other."""
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        udfs = manager.discover_udfs()

        # UDFs should have unique names
        udf_names = list(udfs.keys())
        assert len(udf_names) == len(set(udf_names)), "UDF names should be unique"

        # Should have UDFs from both main file and subdirectory
        main_file_udfs = ["add_ten", "format_name", "enrich_data"]
        sub_file_udfs = ["calculate_growth_rate", "revenue_analysis"]

        for udf_name in main_file_udfs + sub_file_udfs:
            assert any(udf_name in full_name for full_name in udf_names)

    def test_namespace_isolation_with_engine_registration(
        self, core_udf_test_env: Dict[str, Any]
    ) -> None:
        """All UDFs from different namespaces work together in engine."""
        engine = DuckDBEngine(":memory:")
        manager = PythonUDFManager(project_dir=core_udf_test_env["project_dir"])

        manager.discover_udfs()
        manager.register_udfs_with_engine(engine)

        # Test UDF from main file
        result1 = engine.execute_query("SELECT add_ten(5) as result").fetchdf()
        assert result1.iloc[0]["result"] == 15

        # Test UDF from subdirectory
        result2 = engine.execute_query(
            "SELECT calculate_growth_rate(120, 100) as growth"
        ).fetchdf()
        assert result2.iloc[0]["growth"] == 20.0

        # Both should work in same query
        result3 = engine.execute_query(
            """
            SELECT 
                add_ten(100) as enhanced,
                calculate_growth_rate(150, 100) as growth
        """
        ).fetchdf()
        assert result3.iloc[0]["enhanced"] == 110
        assert result3.iloc[0]["growth"] == 50.0
