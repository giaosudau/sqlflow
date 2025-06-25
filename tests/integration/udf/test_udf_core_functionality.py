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
    """Test end-to-end execution of UDFs with real data."""

    def test_execution_scalar_udf_with_real_data(
        self, core_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ) -> None:
        """User executes query with scalar UDFs on a real dataset."""
        project_dir = core_udf_test_env["project_dir"]
        test_data = pd.DataFrame(
            {
                "first_name": ["John", "Jane"],
                "last_name": ["Doe", "Smith"],
                "amount": [100.0, 200.0],
            }
        )
        source_path = tmp_path / "test_data.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "steps": [
                {
                    "type": "load",
                    "name": "sales",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "final_data",
                    "query": """
                        SELECT
                            python_udfs.basic_udfs.format_name(first_name, last_name) as full_name,
                            python_udfs.basic_udfs.calculate_tax(amount, 0.1) as tax
                        FROM sales
                    """,
                },
            ]
        }

        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success, "Pipeline execution should succeed"

        # Verify results
        df = coordinator.context.engine.execute_query("SELECT * FROM final_data").df()
        assert len(df) == 2
        assert df.iloc[0]["full_name"] == "John Doe"
        assert df.iloc[1]["tax"] == 20.0

    def test_execution_table_udf_with_aggregation(
        self, core_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ) -> None:
        """User executes table UDF that performs aggregation."""
        project_dir = core_udf_test_env["project_dir"]
        test_data = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "customer_id": [101, 102, 101, 103],
                "amount": [100.0, 150.0, 200.0, 50.0],
            }
        )
        source_path = tmp_path / "test_data.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "steps": [
                {
                    "type": "load",
                    "name": "orders",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "customer_summary",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.basic_udfs.customer_summary", orders)',
                },
            ]
        }

        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success, "Pipeline execution should succeed"

        # Verify results
        df = coordinator.context.engine.execute_query(
            "SELECT * FROM customer_summary ORDER BY customer_id"
        ).df()
        assert len(df) == 3
        summary_101 = df[df["customer_id"] == 101]
        assert summary_101["total_amount"].iloc[0] == 300.0
        assert summary_101["order_count"].iloc[0] == 2

    def test_execution_udfs_in_complex_query(
        self, core_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ) -> None:
        """User combines table UDFs with other SQL operations like JOINs."""
        project_dir = core_udf_test_env["project_dir"]

        # Create test data
        sales_data = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [101, 102, 101],
                "amount": [100.0, 200.0, 50.0],
            }
        )
        sales_path = tmp_path / "sales.csv"
        sales_data.to_csv(sales_path, index=False)

        customer_data = pd.DataFrame(
            {"id": [101, 102], "name": ["John Doe", "Jane Smith"]}
        )
        customers_path = tmp_path / "customers.csv"
        customer_data.to_csv(customers_path, index=False)

        pipeline = {
            "steps": [
                {
                    "type": "load",
                    "name": "sales",
                    "source": str(sales_path),
                },
                {
                    "type": "load",
                    "name": "customers",
                    "source": str(customers_path),
                },
                {
                    "type": "transform",
                    "name": "customer_summary_data",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.basic_udfs.customer_summary", sales)',
                },
                {
                    "type": "transform",
                    "name": "enriched_customer_data",
                    "query": """
                        SELECT
                            c.name,
                            cs.total_amount,
                            cs.order_count
                        FROM customers c
                        JOIN customer_summary_data cs ON c.id = cs.customer_id
                    """,
                },
            ]
        }
        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert result.success, "Pipeline should succeed"

        # Verify results
        df = coordinator.context.engine.execute_query(
            "SELECT * FROM enriched_customer_data"
        ).df()
        assert len(df) == 2
        john_doe = df[df["name"] == "John Doe"].iloc[0]
        assert john_doe["total_amount"] == 150.0
        assert john_doe["order_count"] == 2


class TestUDFQueryProcessing:
    """Test query processing and substitution with UDFs."""

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
        self, core_udf_test_env: Dict[str, Any], v2_pipeline_runner, tmp_path: Path
    ) -> None:
        """User has a query with both variables and UDFs, which should be processed correctly."""
        project_dir = core_udf_test_env["project_dir"]

        test_data = pd.DataFrame({"value": [10, 20, 30]})
        source_path = tmp_path / "test_data.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "variables": {"TAX_RATE": 0.2},
            "steps": [
                {
                    "type": "load",
                    "name": "step1",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "step2",
                    "query": "SELECT calculate_tax(value, ${TAX_RATE}) as tax FROM step1",
                },
            ],
        }

        coordinator = v2_pipeline_runner(
            pipeline["steps"],
            project_dir=project_dir,
            variables=pipeline.get("variables"),
        )
        result = coordinator.result
        assert result.success, "Pipeline execution should succeed"

        # Verify results
        df = coordinator.context.engine.execute_query("SELECT * FROM step2").df()
        assert len(df) == 3
        assert df["tax"].tolist() == [2.0, 4.0, 6.0]


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
