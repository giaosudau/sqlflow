"""Integration tests for Python UDF execution in SQL pipelines."""

import os
import tempfile

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb_engine import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.udfs.manager import PythonUDFManager


@pytest.fixture
def complex_test_environment():
    """Create a test environment with UDFs and SQL pipelines."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create project structure
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        pipeline_dir = os.path.join(tmp_dir, "pipelines")
        data_dir = os.path.join(tmp_dir, "data")
        os.makedirs(udf_dir, exist_ok=True)
        os.makedirs(pipeline_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        # Create sample data CSV
        orders_csv = os.path.join(data_dir, "orders.csv")
        orders_df = pd.DataFrame(
            {
                "order_id": [1001, 1002, 1003, 1004, 1005],
                "customer_id": [1, 2, 1, 3, 2],
                "amount": [100.50, 200.25, 50.75, 300.00, 150.00],
                "status": [
                    "completed",
                    "pending",
                    "completed",
                    "cancelled",
                    "completed",
                ],
                "date": [
                    "2023-01-15",
                    "2023-01-16",
                    "2023-01-17",
                    "2023-01-18",
                    "2023-01-19",
                ],
            }
        )
        orders_df.to_csv(orders_csv, index=False)

        # Create UDF file with various UDFs
        create_udf_file(udf_dir)

        # Create pipeline file
        create_pipeline_file(pipeline_dir, data_dir)

        yield {
            "root_dir": tmp_dir,
            "udf_dir": udf_dir,
            "pipeline_dir": pipeline_dir,
            "data_dir": data_dir,
            "pipeline_file": os.path.join(pipeline_dir, "order_analysis.sf"),
            "orders_csv": orders_csv,
        }


def create_udf_file(udf_dir):
    """Create UDF file with various functions for data processing."""
    sales_udf_file = os.path.join(udf_dir, "sales_analysis.py")
    with open(sales_udf_file, "w") as f:
        f.write(
            """
from sqlflow.udfs import python_scalar_udf, python_table_udf
import pandas as pd
from datetime import datetime


@python_scalar_udf
def calculate_total_with_tax(amount: float, tax_rate: float = 0.1) -> float:
    \"\"\"Calculate the total amount including tax.\"\"\"
    if amount is None:
        return None
    return amount * (1 + tax_rate)


@python_scalar_udf
def format_currency(amount: float, currency: str = "$") -> str:
    \"\"\"Format a number as currency.\"\"\"
    if amount is None:
        return None
    return f"{currency}{amount:.2f}"


@python_scalar_udf
def parse_date(date_str: str) -> str:
    \"\"\"Parse date string to formatted date.\"\"\"
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%B %d, %Y")
    except ValueError:
        return date_str


@python_table_udf
def customer_summary(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Generate customer summary statistics.\"\"\"
    if df.empty:
        return pd.DataFrame()
    
    summary = df.groupby('customer_id').agg(
        total_orders=pd.NamedAgg(column='order_id', aggfunc='count'),
        total_spent=pd.NamedAgg(column='amount', aggfunc='sum'),
        avg_order_value=pd.NamedAgg(column='amount', aggfunc='mean'),
        latest_order=pd.NamedAgg(column='date', aggfunc='max')
    ).reset_index()
    
    # Calculate additional metrics
    summary['status'] = 'active'
    summary.loc[summary['total_orders'] > 2, 'status'] = 'loyal'
    
    return summary
"""
        )

    # Create a second UDF file to test multiple file discovery
    utils_udf_file = os.path.join(udf_dir, "utils.py")
    with open(utils_udf_file, "w") as f:
        f.write(
            """
from sqlflow.udfs import python_scalar_udf
import hashlib


@python_scalar_udf
def generate_hash(text: str) -> str:
    \"\"\"Generate an MD5 hash of the input text.\"\"\"
    if not text:
        return None
    return hashlib.md5(text.encode()).hexdigest()


@python_scalar_udf
def concat_values(*args) -> str:
    \"\"\"Concatenate multiple values with a separator.\"\"\"
    return "|".join(str(arg) for arg in args if arg is not None)
"""
        )

    return [sales_udf_file, utils_udf_file]


def create_pipeline_file(pipeline_dir, data_dir):
    """Create a pipeline file that uses the UDFs."""
    pipeline_file = os.path.join(pipeline_dir, "order_analysis.sf")
    with open(pipeline_file, "w") as f:
        f.write(
            f"""
-- Load orders data
CREATE TABLE raw_orders AS
SELECT *
FROM read_csv('{data_dir}/orders.csv', header=true, auto_detect=true);

-- Apply scalar UDFs to transform data
CREATE TABLE processed_orders AS
SELECT
    order_id,
    customer_id,
    amount,
    PYTHON_FUNC("sales_analysis.calculate_total_with_tax", amount) AS total_with_tax,
    PYTHON_FUNC("sales_analysis.format_currency", amount) AS formatted_amount,
    PYTHON_FUNC("sales_analysis.parse_date", date) AS formatted_date,
    status,
    PYTHON_FUNC("utils.generate_hash", CAST(order_id AS VARCHAR)) AS order_hash,
    PYTHON_FUNC("utils.concat_values", customer_id, status) AS customer_status_key
FROM raw_orders;

-- Apply table UDF to generate summary
CREATE TABLE customer_summary AS
SELECT * FROM PYTHON_FUNC("sales_analysis.customer_summary", raw_orders);

-- Create a final report
CREATE TABLE final_report AS
SELECT
    c.customer_id,
    c.total_orders,
    c.total_spent,
    c.avg_order_value,
    PYTHON_FUNC("sales_analysis.format_currency", c.avg_order_value) AS formatted_avg_value,
    c.status AS customer_status,
    o.order_id AS latest_order_id,
    o.formatted_amount AS latest_order_amount,
    o.formatted_date AS latest_order_date
FROM customer_summary c
LEFT JOIN processed_orders o ON c.customer_id = o.customer_id AND c.latest_order = o.date
ORDER BY c.total_spent DESC;
"""
        )
    return pipeline_file


def test_complex_udf_pipeline(complex_test_environment):
    """Test a complex SQL pipeline with multiple UDFs."""
    # Set up UDF manager
    udf_manager = PythonUDFManager(complex_test_environment["root_dir"])
    udfs = udf_manager.discover_udfs()

    # Verify UDFs were discovered correctly from both files
    assert "sales_analysis.calculate_total_with_tax" in udfs
    assert "sales_analysis.format_currency" in udfs
    assert "sales_analysis.parse_date" in udfs
    assert "sales_analysis.customer_summary" in udfs
    assert "utils.generate_hash" in udfs
    assert "utils.concat_values" in udfs

    # Set up engine and executor
    engine = DuckDBEngine()
    executor = LocalExecutor(engine=engine)

    # Register UDFs with the engine
    udf_manager.register_udfs_with_engine(engine)

    # Execute the pipeline
    pipeline_file = complex_test_environment["pipeline_file"]
    result = executor.execute_file(pipeline_file)

    # Verify execution was successful
    assert result.success, f"Pipeline execution failed: {result.error}"

    # Query results
    processed_orders = engine.query("SELECT * FROM processed_orders")
    customer_summary = engine.query("SELECT * FROM customer_summary")
    final_report = engine.query("SELECT * FROM final_report")

    # Verify scalar UDF results in processed_orders
    assert len(processed_orders) == 5
    for row in processed_orders:
        assert row["total_with_tax"] == pytest.approx(row["amount"] * 1.1)
        assert row["formatted_amount"].startswith("$")
        assert row["order_hash"] is not None
        assert str(row["customer_id"]) in row["customer_status_key"]

    # Verify table UDF results in customer_summary
    assert len(customer_summary) == 3  # 3 unique customers
    customer_total_orders = {
        row["customer_id"]: row["total_orders"] for row in customer_summary
    }
    assert customer_total_orders[1] == 2  # Customer 1 has 2 orders
    assert customer_total_orders[2] == 2  # Customer 2 has 2 orders
    assert customer_total_orders[3] == 1  # Customer 3 has 1 order

    # Verify final report has all customers
    assert len(final_report) == 3
    # The report should be sorted by total_spent in descending order
    assert final_report[0]["total_spent"] >= final_report[1]["total_spent"]
    assert final_report[1]["total_spent"] >= final_report[2]["total_spent"]


def test_incremental_pipeline_with_udfs():
    """Test UDFs in an incremental loading pipeline."""
    # This would test UDFs in an incremental loading scenario
    # where new data is processed and merged with existing data


def test_concurrent_udf_execution():
    """Test concurrent execution of UDFs with ThreadPoolExecutor."""
    # This would test that UDFs work correctly when executed in parallel


def test_udf_with_external_api():
    """Test UDFs that interact with external APIs."""
    # This would test UDFs that make API calls or interact with external systems
    # (would need mocked responses)
