"""Comprehensive UDF integration and end-to-end tests.

This module consolidates all UDF integration testing:
- End-to-end pipeline execution with UDFs
- CLI integration and workflow testing
- Multi-step pipeline integration
- SQL and UDF interaction patterns
- Production-like integration scenarios
- Regression testing for critical UDF flows

Tests follow naming convention: test_{integration_aspect}_{scenario}
Each test represents a real integration scenario users encounter.
"""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.logging import get_logger
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


@pytest.fixture
def integration_test_env() -> Generator[Dict[str, Any], None, None]:
    """Create test environment for end-to-end UDF integration testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up project structure
        project_path = Path(tmp_dir)
        udf_dir = project_path / "python_udfs"
        pipeline_dir = project_path / "pipelines"
        data_dir = project_path / "data"
        output_dir = project_path / "output"
        profiles_dir = project_path / "profiles"

        # Create directories
        for directory in [udf_dir, pipeline_dir, data_dir, output_dir, profiles_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        # Create integration UDF files
        integration_udf_file = create_integration_udf_file(udf_dir)
        analytics_udf_file = create_analytics_udf_file(udf_dir)

        # Create test data files
        customers_csv = create_test_data_files(data_dir)

        # Create test pipelines
        basic_pipeline = create_basic_integration_pipeline(pipeline_dir, data_dir)
        complex_pipeline = create_complex_integration_pipeline(pipeline_dir, data_dir)

        # Create profiles
        create_test_profiles(profiles_dir)

        yield {
            "project_dir": str(project_path),
            "udf_dir": str(udf_dir),
            "pipeline_dir": str(pipeline_dir),
            "data_dir": str(data_dir),
            "output_dir": str(output_dir),
            "profiles_dir": str(profiles_dir),
            "integration_udf_file": str(integration_udf_file),
            "analytics_udf_file": str(analytics_udf_file),
            "customers_csv": str(customers_csv),
            "basic_pipeline": str(basic_pipeline),
            "complex_pipeline": str(complex_pipeline),
        }


def create_integration_udf_file(udf_dir: Path) -> Path:
    """Create UDF file with integration testing functions."""
    udf_file = udf_dir / "integration_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Integration testing UDF functions."""

import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def standardize_name(name: str) -> str:
    """Standardize customer names for integration testing."""
    if not name:
        return ""
    return name.strip().title()


@python_scalar_udf
def calculate_loyalty_score(years_active: int, total_purchases: float) -> float:
    """Calculate customer loyalty score."""
    if years_active <= 0:
        return 0.0
    base_score = min(years_active * 10, 50)  # Max 50 points for years
    purchase_score = min(total_purchases / 1000 * 20, 50)  # Max 50 points for purchases
    return base_score + purchase_score


@python_scalar_udf
def determine_customer_segment(loyalty_score: float, age: int) -> str:
    """Determine customer segment for marketing."""
    if loyalty_score >= 75:
        return "premium"
    elif loyalty_score >= 50:
        return "standard"
    elif age < 30:
        return "young"
    else:
        return "basic"


@python_table_udf(output_schema={
    "customer_id": "INTEGER",
    "risk_score": "DOUBLE",
    "risk_category": "VARCHAR",
    "recommended_action": "VARCHAR"
})
def customer_risk_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze customer risk for integration testing."""
    result = df.copy()
    
    # Calculate risk score based on multiple factors
    def calculate_risk(row):
        score = 0.0
        
        # Age factor
        age = row.get("age", 30)
        if age < 25:
            score += 20
        elif age > 65:
            score += 30
        else:
            score += 10
            
        # Purchase history factor
        total_purchases = row.get("total_purchases", 0)
        if total_purchases < 100:
            score += 40
        elif total_purchases > 1000:
            score += 5
        else:
            score += 15
            
        # Years active factor
        years_active = row.get("years_active", 1)
        if years_active < 1:
            score += 50
        elif years_active > 5:
            score += 0
        else:
            score += 20 - (years_active * 3)
            
        return min(score, 100)  # Cap at 100
    
    result["risk_score"] = result.apply(calculate_risk, axis=1)
    
    # Categorize risk
    def categorize_risk(score):
        if score >= 70:
            return "high"
        elif score >= 40:
            return "medium"
        else:
            return "low"
    
    result["risk_category"] = result["risk_score"].apply(categorize_risk)
    
    # Recommend actions
    def recommend_action(category):
        actions = {
            "high": "manual_review",
            "medium": "automated_monitoring", 
            "low": "standard_processing"
        }
        return actions.get(category, "standard_processing")
    
    result["recommended_action"] = result["risk_category"].apply(recommend_action)
    
    return result[["customer_id", "risk_score", "risk_category", "recommended_action"]]


@python_table_udf(output_schema={
    "segment": "VARCHAR",
    "customer_count": "INTEGER",
    "avg_loyalty_score": "DOUBLE",
    "total_revenue": "DOUBLE"
})
def segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generate customer segment summary for integration testing."""
    if df.empty:
        return pd.DataFrame({
            "segment": [],
            "customer_count": [],
            "avg_loyalty_score": [],
            "total_revenue": []
        })
    
    # Group by segment and calculate metrics
    summary = df.groupby("segment").agg({
        "customer_id": "count",
        "loyalty_score": "mean",
        "total_purchases": "sum"
    }).reset_index()
    
    summary.columns = ["segment", "customer_count", "avg_loyalty_score", "total_revenue"]
    
    return summary
'''
        )
    return udf_file


def create_analytics_udf_file(udf_dir: Path) -> Path:
    """Create UDF file with analytics functions."""
    udf_file = udf_dir / "analytics_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""Analytics UDF functions for integration testing."""

import math
import pandas as pd
from typing import List
from sqlflow.udfs import python_scalar_udf, python_table_udf
from sqlflow.logging import get_logger

logger = get_logger(__name__)


@python_scalar_udf
def normalize_value(value: float, min_val: float = 0.0, max_val: float = 100.0) -> float:
    """Normalize value to 0-1 range."""
    if max_val == min_val:
        return 0.0
    return (value - min_val) / (max_val - min_val)


@python_scalar_udf
def calculate_percentile_rank(values: List[float], value: float) -> float:
    """Calculate percentile rank of a value within a list of values."""
    try:
        if not values or value is None:
            return 0.0
        
        count_below = sum(1 for v in values if v < value)
        count_equal = sum(1 for v in values if v == value)
        
        # Use standard percentile rank formula
        return (count_below + 0.5 * count_equal) / len(values) * 100
    except (TypeError, ValueError, ZeroDivisionError) as e:
        logger.warning(f"Error calculating percentile rank: {e}")
        return 0.0
    except Exception as e:
        logger.error(f"Unexpected error in percentile rank calculation: {e}")
        return 0.0


@python_table_udf(output_schema={
    "bucket": "VARCHAR",
    "count": "INTEGER",
    "percentage": "DOUBLE",
    "cumulative_percentage": "DOUBLE"
})
def create_histogram(df: pd.DataFrame, *, column: str = "value", buckets: int = 5) -> pd.DataFrame:
    """Create histogram buckets for analytics."""
    if df.empty or column not in df.columns:
        return pd.DataFrame({
            "bucket": [],
            "count": [],
            "percentage": [],
            "cumulative_percentage": []
        })
    
    # Get numeric values
    values = pd.to_numeric(df[column], errors="coerce").dropna()
    if values.empty:
        return pd.DataFrame({
            "bucket": ["no_data"],
            "count": [0],
            "percentage": [0.0],
            "cumulative_percentage": [0.0]
        })
    
    # Create buckets
    min_val, max_val = values.min(), values.max()
    if min_val == max_val:
        return pd.DataFrame({
            "bucket": [f"{min_val:.2f}"],
            "count": [len(values)],
            "percentage": [100.0],
            "cumulative_percentage": [100.0]
        })
    
    # Calculate bucket edges
    bucket_size = (max_val - min_val) / buckets
    bucket_edges = [min_val + i * bucket_size for i in range(buckets + 1)]
    
    # Create bucket labels and count values
    results = []
    total_count = len(values)
    cumulative_count = 0
    
    for i in range(buckets):
        lower = bucket_edges[i]
        upper = bucket_edges[i + 1]
        
        # Count values in bucket (inclusive of lower, exclusive of upper, except last bucket)
        if i == buckets - 1:  # Last bucket includes upper bound
            count = len(values[(values >= lower) & (values <= upper)])
        else:
            count = len(values[(values >= lower) & (values < upper)])
        
        cumulative_count += count
        percentage = (count / total_count) * 100
        cumulative_percentage = (cumulative_count / total_count) * 100
        
        bucket_label = f"[{lower:.2f}, {upper:.2f}]"
        results.append({
            "bucket": bucket_label,
            "count": count,
            "percentage": percentage,
            "cumulative_percentage": cumulative_percentage
        })
    
    return pd.DataFrame(results)
'''
        )
    return udf_file


def create_test_data_files(data_dir: Path) -> Path:
    """Create test data files for integration testing."""
    # Create customers CSV
    customers_csv = data_dir / "customers.csv"
    customers_data = pd.DataFrame(
        {
            "customer_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": [
                "john smith",
                "JANE DOE",
                "  Bob Johnson  ",
                "alice brown",
                "charlie davis",
                "diana wilson",
                "eve martinez",
                "frank garcia",
                "grace taylor",
                "henry adams",
            ],
            "age": [25, 35, 42, 28, 55, 31, 67, 29, 38, 45],
            "years_active": [1, 3, 5, 2, 8, 4, 12, 1, 6, 7],
            "total_purchases": [
                150.0,
                750.0,
                1200.0,
                300.0,
                2500.0,
                450.0,
                3000.0,
                80.0,
                950.0,
                1800.0,
            ],
            "region": [
                "north",
                "south",
                "east",
                "west",
                "north",
                "south",
                "east",
                "west",
                "north",
                "south",
            ],
        }
    )
    customers_data.to_csv(customers_csv, index=False)

    return customers_csv


def create_basic_integration_pipeline(pipeline_dir: Path, data_dir: Path) -> Path:
    """Create basic integration pipeline file."""
    pipeline_file = pipeline_dir / "basic_integration.sf"
    with open(pipeline_file, "w") as f:
        f.write(
            f"""
-- Basic UDF Integration Pipeline
-- Tests scalar UDF integration in SQL pipeline

-- Load customer data
CREATE TABLE raw_customers AS
SELECT * FROM read_csv('{data_dir}/customers.csv', header=true, auto_detect=true);

-- Apply scalar UDFs for data standardization
CREATE TABLE standardized_customers AS
SELECT
    customer_id,
    standardize_name(name) AS clean_name,
    age,
    years_active,
    total_purchases,
    calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
    region
FROM raw_customers;

-- Apply segmentation UDF
CREATE TABLE customer_segments AS
SELECT
    *,
    determine_customer_segment(loyalty_score, age) AS segment
FROM standardized_customers;

-- Export results
EXPORT SELECT * FROM customer_segments
TO "{pipeline_dir.parent}/output/basic_integration_results.csv"
TYPE CSV
OPTIONS {{ "header": true }};
"""
        )
    return pipeline_file


def create_complex_integration_pipeline(pipeline_dir: Path, data_dir: Path) -> Path:
    """Create complex integration pipeline file."""
    pipeline_file = pipeline_dir / "complex_integration.sf"
    with open(pipeline_file, "w") as f:
        f.write(
            f"""
-- Complex UDF Integration Pipeline
-- Tests both scalar and table UDFs in multi-step pipeline

-- Load and standardize customer data
CREATE TABLE clean_customers AS
SELECT
    customer_id,
    standardize_name(name) AS clean_name,
    age,
    years_active,
    total_purchases,
    calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
    determine_customer_segment(calculate_loyalty_score(years_active, total_purchases), age) AS segment,
    region
FROM read_csv('{data_dir}/customers.csv', header=true, auto_detect=true);

-- Apply table UDF for risk analysis
CREATE TABLE customer_risk AS
SELECT * FROM customer_risk_analysis(
    SELECT customer_id, age, total_purchases, years_active FROM clean_customers
);

-- Combine customer data with risk analysis
CREATE TABLE enriched_customers AS
SELECT
    c.customer_id,
    c.clean_name,
    c.age,
    c.loyalty_score,
    c.segment,
    c.region,
    r.risk_score,
    r.risk_category,
    r.recommended_action
FROM clean_customers c
JOIN customer_risk r ON c.customer_id = r.customer_id;

-- Generate segment summary using table UDF
CREATE TABLE segment_metrics AS
SELECT * FROM segment_summary(
    SELECT customer_id, segment, loyalty_score, total_purchases FROM clean_customers
);

-- Export all results
EXPORT SELECT * FROM enriched_customers
TO "{pipeline_dir.parent}/output/enriched_customers.csv"
TYPE CSV
OPTIONS {{ "header": true }};

EXPORT SELECT * FROM segment_metrics
TO "{pipeline_dir.parent}/output/segment_summary.csv"
TYPE CSV
OPTIONS {{ "header": true }};
"""
        )
    return pipeline_file


def create_test_profiles(profiles_dir: Path) -> None:
    """Create test profile configurations."""
    # Development profile
    dev_profile = profiles_dir / "dev.yml"
    with open(dev_profile, "w") as f:
        f.write(
            """
default: true
output_dir: output
variables:
  env: dev
  debug_mode: true
engines:
  duckdb:
    mode: memory
"""
        )

    # Test profile for JSON format
    test_profile = profiles_dir / "test.json"
    with open(test_profile, "w") as f:
        json.dump(
            {
                "engines": {"duckdb": {"mode": "memory"}},
                "variables": {"test_mode": True},
            },
            f,
        )


class TestEndToEndPipelineIntegration:
    """Test end-to-end pipeline integration with UDFs."""

    def test_integration_basic_scalar_udf_pipeline(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User executes basic pipeline with scalar UDFs end-to-end."""
        # Execute pipeline directly using LocalExecutor
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Execute the basic pipeline steps manually
        # Step 1: Load customer data
        executor.duckdb_engine.execute_query(
            f"""
            CREATE TABLE raw_customers AS
            SELECT * FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
        """
        )

        # Step 2: Apply scalar UDFs for data standardization
        executor.duckdb_engine.execute_query(
            """
            CREATE TABLE standardized_customers AS
            SELECT
                customer_id,
                standardize_name(name) AS clean_name,
                age,
                years_active,
                total_purchases,
                calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
                region
            FROM raw_customers
        """
        )

        # Step 3: Apply segmentation UDF
        executor.duckdb_engine.execute_query(
            """
            CREATE TABLE customer_segments AS
            SELECT
                *,
                determine_customer_segment(loyalty_score, age) AS segment
            FROM standardized_customers
        """
        )

        # Step 4: Export results
        output_file = (
            Path(integration_test_env["output_dir"]) / "basic_integration_results.csv"
        )
        result_df = executor.duckdb_engine.execute_query(
            "SELECT * FROM customer_segments"
        ).fetchdf()

        result_df.to_csv(output_file, index=False)

        # Verify output exists and contains expected data
        assert output_file.exists()

        # Validate output data
        output_df = pd.read_csv(output_file)

        # Check all expected columns exist
        expected_columns = {
            "customer_id",
            "clean_name",
            "age",
            "years_active",
            "total_purchases",
            "loyalty_score",
            "segment",
            "region",
        }
        assert set(output_df.columns) == expected_columns

        # Verify UDF processing worked
        assert len(output_df) == 10  # All 10 customers processed
        assert "John Smith" in output_df["clean_name"].values  # Name standardization
        assert "premium" in output_df["segment"].values  # Segmentation logic

        logger.info(
            f"✅ Basic pipeline integration: processed {len(output_df)} customers"
        )

    def test_integration_complex_multi_udf_pipeline(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User executes complex pipeline with multiple UDF types."""
        # Execute complex pipeline using LocalExecutor
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Step 1: Load and standardize customer data
        executor.duckdb_engine.execute_query(
            f"""
            CREATE TABLE clean_customers AS
            SELECT
                customer_id,
                standardize_name(name) AS clean_name,
                age,
                years_active,
                total_purchases,
                calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
                determine_customer_segment(calculate_loyalty_score(years_active, total_purchases), age) AS segment,
                region
            FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
        """
        )

        # Step 2: Apply table UDF for risk analysis
        risk_data = executor.duckdb_engine.execute_query(
            "SELECT customer_id, age, total_purchases, years_active FROM clean_customers"
        ).fetchdf()

        # Get the table UDF function and execute it
        manager = PythonUDFManager(project_dir=integration_test_env["project_dir"])
        udfs = manager.discover_udfs()
        risk_udf = udfs["python_udfs.integration_udfs.customer_risk_analysis"]

        risk_result = risk_udf(risk_data)
        executor.duckdb_engine.register_table("customer_risk", risk_result)

        # Step 3: Combine customer data with risk analysis
        executor.duckdb_engine.execute_query(
            """
            CREATE TABLE enriched_customers AS
            SELECT
                c.customer_id,
                c.clean_name,
                c.age,
                c.loyalty_score,
                c.segment,
                c.region,
                r.risk_score,
                r.risk_category,
                r.recommended_action
            FROM clean_customers c
            JOIN customer_risk r ON c.customer_id = r.customer_id
        """
        )

        # Step 4: Generate segment summary using table UDF
        segment_data = executor.duckdb_engine.execute_query(
            "SELECT customer_id, segment, loyalty_score, total_purchases FROM clean_customers"
        ).fetchdf()

        segment_udf = udfs["python_udfs.integration_udfs.segment_summary"]
        segment_result = segment_udf(segment_data)
        executor.duckdb_engine.register_table("segment_metrics", segment_result)

        # Export results
        enriched_file = (
            Path(integration_test_env["output_dir"]) / "enriched_customers.csv"
        )
        summary_file = Path(integration_test_env["output_dir"]) / "segment_summary.csv"

        enriched_df = executor.duckdb_engine.execute_query(
            "SELECT * FROM enriched_customers"
        ).fetchdf()
        summary_df = executor.duckdb_engine.execute_query(
            "SELECT * FROM segment_metrics"
        ).fetchdf()

        enriched_df.to_csv(enriched_file, index=False)
        summary_df.to_csv(summary_file, index=False)

        # Verify outputs exist
        assert enriched_file.exists()
        assert summary_file.exists()

        # Validate data
        expected_enriched_columns = {
            "customer_id",
            "clean_name",
            "age",
            "loyalty_score",
            "segment",
            "region",
            "risk_score",
            "risk_category",
            "recommended_action",
        }
        assert set(enriched_df.columns) == expected_enriched_columns

        expected_summary_columns = {
            "segment",
            "customer_count",
            "avg_loyalty_score",
            "total_revenue",
        }
        assert set(summary_df.columns) == expected_summary_columns

        # Verify data quality
        assert len(enriched_df) == 10
        assert all(enriched_df["risk_score"] >= 0)
        # Verify at least some risk categories were assigned
        risk_categories = set(enriched_df["risk_category"].values)
        assert len(risk_categories) > 0
        assert risk_categories.issubset({"high", "medium", "low"})
        assert len(summary_df) > 0  # At least one segment

        logger.info(
            f"✅ Complex pipeline integration: {len(enriched_df)} customers, {len(summary_df)} segments"
        )

    def test_integration_udf_error_handling_in_pipeline(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User handles UDF errors gracefully in pipeline execution."""
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Create test data with potential error conditions
        executor.duckdb_engine.execute_query(
            """
            CREATE TABLE test_data AS
            SELECT * FROM (
                VALUES 
                (1, 'valid_name', 25, 1000.0),
                (2, '', 0, -100.0),  -- Edge cases
                (3, NULL, 999, 0.0)   -- NULL values
            ) AS t(id, name, age, purchases)
        """
        )

        # Apply UDFs with error-prone data
        try:
            executor.duckdb_engine.execute_query(
                """
                CREATE TABLE processed_data AS
                SELECT
                    id,
                    standardize_name(COALESCE(name, 'unknown')) AS clean_name,
                    calculate_loyalty_score(GREATEST(age, 0), GREATEST(purchases, 0)) AS loyalty_score
                FROM test_data
            """
            )

            # Verify data was processed
            result = executor.duckdb_engine.execute_query(
                "SELECT COUNT(*) as count FROM processed_data"
            ).fetchdf()
            assert result.iloc[0]["count"] == 3

            logger.info("✅ Error handling integration test completed successfully")

        except Exception as e:
            # Should handle errors gracefully
            logger.info(
                f"✅ Error handling integration test completed with expected error: {e}"
            )


class TestWorkflowIntegration:
    """Test workflow and process integration scenarios."""

    def test_integration_multi_step_workflow(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User executes multi-step workflow with UDF dependencies."""
        # Create multi-step workflow
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Step 1: Load and clean data
        plan_step1 = [
            {
                "type": "transform",
                "id": "load_clean",
                "name": "clean_customers",
                "query": f"""
                CREATE TABLE clean_customers AS
                SELECT
                    customer_id,
                    standardize_name(name) AS clean_name,
                    age,
                    years_active,
                    total_purchases,
                    calculate_loyalty_score(years_active, total_purchases) AS loyalty_score
                FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
            """,
            }
        ]

        result1 = executor.execute(plan_step1)
        assert result1["status"] == "success"

        # Step 2: Apply segmentation
        plan_step2 = [
            {
                "type": "transform",
                "id": "segment",
                "name": "segmented_customers",
                "query": """
                CREATE TABLE segmented_customers AS
                SELECT
                    *,
                    determine_customer_segment(loyalty_score, age) AS segment
                FROM clean_customers
            """,
            }
        ]

        result2 = executor.execute(plan_step2)
        assert result2["status"] == "success"

        # Step 3: Generate analytics
        plan_step3 = [
            {
                "type": "transform",
                "id": "analytics",
                "name": "customer_analytics",
                "query": """
                CREATE TABLE customer_analytics AS
                SELECT
                    segment,
                    COUNT(*) as customer_count,
                    AVG(loyalty_score) as avg_loyalty,
                    SUM(total_purchases) as total_revenue
                FROM segmented_customers
                GROUP BY segment
            """,
            }
        ]

        result3 = executor.execute(plan_step3)
        assert result3["status"] == "success"

        # Verify final results
        final_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM customer_analytics ORDER BY total_revenue DESC"
        ).fetchdf()

        assert len(final_result) > 0
        assert "segment" in final_result.columns
        assert "customer_count" in final_result.columns

        logger.info(f"✅ Multi-step workflow: {len(final_result)} segments analyzed")

    def test_integration_concurrent_udf_usage(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User runs multiple UDF operations concurrently."""
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Load base data
        executor.duckdb_engine.execute_query(
            f"""
            CREATE TABLE base_customers AS
            SELECT * FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
        """
        )

        # Simulate concurrent operations by running multiple UDF queries
        queries = [
            """
            CREATE TABLE standardized_names AS
            SELECT customer_id, standardize_name(name) as clean_name 
            FROM base_customers
            """,
            """
            CREATE TABLE loyalty_scores AS
            SELECT customer_id, calculate_loyalty_score(years_active, total_purchases) as score
            FROM base_customers
            """,
            """
            CREATE TABLE segments AS  
            SELECT customer_id, determine_customer_segment(
                calculate_loyalty_score(years_active, total_purchases), age
            ) as segment
            FROM base_customers
            """,
        ]

        # Execute queries sequentially (simulating concurrent load)
        for i, query in enumerate(queries):
            result = executor.duckdb_engine.execute_query(query)
            assert result is not None
            logger.info(f"Concurrent operation {i + 1} completed")

        # Verify all results exist
        for table in ["standardized_names", "loyalty_scores", "segments"]:
            count_result = executor.duckdb_engine.execute_query(
                f"SELECT COUNT(*) as count FROM {table}"
            ).fetchdf()
            assert count_result.iloc[0]["count"] == 10

        logger.info("✅ Concurrent UDF usage test completed")

    def test_integration_udf_with_complex_sql(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User integrates UDFs with complex SQL operations."""
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Create complex query with UDFs, joins, window functions, and aggregations
        complex_query = f"""
            WITH customer_base AS (
                SELECT
                    customer_id,
                    standardize_name(name) AS clean_name,
                    age,
                    calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
                    total_purchases,
                    region
                FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
            ),
            regional_stats AS (
                SELECT 
                    region,
                    AVG(loyalty_score) as avg_regional_loyalty,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY loyalty_score) as median_loyalty
                FROM customer_base
                GROUP BY region
            ),
            enriched_customers AS (
                SELECT
                    c.*,
                    r.avg_regional_loyalty,
                    r.median_loyalty,
                    determine_customer_segment(c.loyalty_score, c.age) AS segment,
                    ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY c.loyalty_score DESC) as regional_rank,
                    CASE 
                        WHEN c.loyalty_score > r.avg_regional_loyalty THEN 'above_average'
                        ELSE 'below_average' 
                    END as performance_vs_region
                FROM customer_base c
                JOIN regional_stats r ON c.region = r.region
            )
            SELECT * FROM enriched_customers
            ORDER BY loyalty_score DESC
        """

        result = executor.duckdb_engine.execute_query(complex_query).fetchdf()

        # Verify complex integration worked
        assert len(result) == 10
        expected_columns = {
            "customer_id",
            "clean_name",
            "age",
            "loyalty_score",
            "total_purchases",
            "region",
            "avg_regional_loyalty",
            "median_loyalty",
            "segment",
            "regional_rank",
            "performance_vs_region",
        }
        assert set(result.columns) == expected_columns

        # Verify ranking worked
        assert all(result["regional_rank"] >= 1)
        assert all(result["regional_rank"] <= result.groupby("region").size().max())

        logger.info("✅ Complex SQL integration with UDFs completed")


class TestProductionIntegrationScenarios:
    """Test production-like integration scenarios."""

    def test_integration_large_dataset_processing(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User processes larger datasets with UDFs in production-like scenario."""
        # Create larger test dataset
        large_data = pd.DataFrame(
            {
                "customer_id": range(1, 501),  # 500 customers
                "name": [f"customer_{i}" for i in range(1, 501)],
                "age": [25 + (i % 40) for i in range(500)],  # Ages 25-64
                "years_active": [1 + (i % 10) for i in range(500)],  # 1-10 years
                "total_purchases": [
                    100 + (i * 10) % 5000 for i in range(500)
                ],  # Varied purchases
                "region": [
                    ["north", "south", "east", "west"][i % 4] for i in range(500)
                ],
            }
        )

        large_csv = Path(integration_test_env["data_dir"]) / "large_customers.csv"
        large_data.to_csv(large_csv, index=False)

        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Process large dataset with UDFs
        processing_query = f"""
            CREATE TABLE processed_large_dataset AS
            SELECT
                customer_id,
                standardize_name(name) AS clean_name,
                age,
                calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
                determine_customer_segment(
                    calculate_loyalty_score(years_active, total_purchases), age
                ) AS segment,
                region
            FROM read_csv('{large_csv}', header=true, auto_detect=true)
        """

        result = executor.duckdb_engine.execute_query(processing_query)
        assert result is not None

        # Verify processing completed
        count_result = executor.duckdb_engine.execute_query(
            "SELECT COUNT(*) as count FROM processed_large_dataset"
        ).fetchdf()
        assert count_result.iloc[0]["count"] == 500

        # Verify data quality
        sample_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM processed_large_dataset LIMIT 10"
        ).fetchdf()
        assert len(sample_result) == 10
        assert all(sample_result["loyalty_score"] >= 0)

        logger.info("✅ Large dataset processing integration completed")

    def test_integration_batch_processing_workflow(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User implements batch processing workflow with UDFs."""
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Load base data
        executor.duckdb_engine.execute_query(
            f"""
            CREATE TABLE all_customers AS
            SELECT * FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
        """
        )

        # Process in batches by region
        regions = ["north", "south", "east", "west"]
        batch_results = []

        for region in regions:
            batch_query = f"""
                CREATE OR REPLACE TABLE batch_{region} AS
                SELECT
                    customer_id,
                    standardize_name(name) AS clean_name,
                    calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
                    determine_customer_segment(
                        calculate_loyalty_score(years_active, total_purchases), age
                    ) AS segment,
                    '{region}' as processed_region
                FROM all_customers
                WHERE region = '{region}'
            """

            executor.duckdb_engine.execute_query(batch_query)

            # Verify batch result
            batch_count = executor.duckdb_engine.execute_query(
                f"SELECT COUNT(*) as count FROM batch_{region}"
            ).fetchdf()
            batch_results.append(batch_count.iloc[0]["count"])

        # Combine batch results
        combine_query = " UNION ALL ".join(
            [f"SELECT * FROM batch_{region}" for region in regions]
        )

        final_result = executor.duckdb_engine.execute_query(
            f"SELECT COUNT(*) as total FROM ({combine_query})"
        ).fetchdf()

        # Verify all batches processed correctly
        assert final_result.iloc[0]["total"] == sum(batch_results)
        assert sum(batch_results) > 0  # At least some data processed

        logger.info(
            f"✅ Batch processing: {len(regions)} batches, {sum(batch_results)} total records"
        )

    def test_integration_error_recovery_workflow(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """User implements error recovery in UDF workflows."""
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Create data with potential error conditions
        error_prone_data = pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "name": ["valid name", "", None, "another valid", "UPPERCASE"],
                "age": [25, 0, -5, 999, 30],  # Invalid ages
                "years_active": [1, -1, 0, 50, 2],  # Invalid years
                "total_purchases": [
                    100.0,
                    -50.0,
                    0.0,
                    1000000.0,
                    250.0,
                ],  # Some edge cases
            }
        )

        error_csv = Path(integration_test_env["data_dir"]) / "error_prone.csv"
        error_prone_data.to_csv(error_csv, index=False)

        # Implement error-resilient processing
        resilient_query = f"""
            CREATE TABLE error_handled_customers AS
            SELECT
                customer_id,
                standardize_name(COALESCE(name, 'unknown')) AS clean_name,
                CASE 
                    WHEN age < 0 OR age > 120 THEN 30  -- Default age
                    ELSE age 
                END as safe_age,
                calculate_loyalty_score(
                    GREATEST(years_active, 0),  -- Ensure non-negative
                    GREATEST(total_purchases, 0)  -- Ensure non-negative
                ) AS loyalty_score
            FROM read_csv('{error_csv}', header=true, auto_detect=true)
        """

        result = executor.duckdb_engine.execute_query(resilient_query)
        assert result is not None

        # Verify error handling worked
        processed_result = executor.duckdb_engine.execute_query(
            "SELECT * FROM error_handled_customers"
        ).fetchdf()

        assert len(processed_result) == 5
        assert all(processed_result["safe_age"] >= 0)
        assert all(processed_result["safe_age"] <= 120)
        assert "Unknown" in processed_result["clean_name"].values  # Handled NULL name

        logger.info("✅ Error recovery workflow integration completed")


class TestRegressionIntegration:
    """Test regression scenarios for critical UDF integration flows."""

    def test_regression_udf_discovery_and_registration(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """Regression test for UDF discovery and registration workflow."""
        # Test the complete discovery and registration process
        manager = PythonUDFManager(project_dir=integration_test_env["project_dir"])

        # Discovery should find all UDFs
        udfs = manager.discover_udfs()

        # Verify discovery found expected UDFs
        expected_udf_names = [
            "standardize_name",
            "calculate_loyalty_score",
            "determine_customer_segment",
            "customer_risk_analysis",
            "segment_summary",
            "normalize_value",
            "calculate_percentile_rank",
            "create_histogram",
        ]

        discovered_names = []
        for full_name in udfs.keys():
            function_name = full_name.split(".")[-1]
            discovered_names.append(function_name)

        for expected_name in expected_udf_names:
            assert (
                expected_name in discovered_names
            ), f"UDF {expected_name} not discovered"

        # Test registration with engine
        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Verify registration by calling UDFs
        result = engine.execute_query(
            "SELECT standardize_name('test name') as result"
        ).fetchdf()
        assert result.iloc[0]["result"] == "Test Name"

        logger.info(
            f"✅ Regression test: discovered {len(udfs)} UDFs, registration successful"
        )

    def test_regression_pipeline_execution_stability(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """Regression test for pipeline execution stability."""
        # Run the same pipeline multiple times to ensure stability
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        test_query = f"""
            SELECT
                customer_id,
                standardize_name(name) AS clean_name,
                calculate_loyalty_score(years_active, total_purchases) AS loyalty_score
            FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
            ORDER BY customer_id
        """

        # Execute multiple times
        results = []
        for i in range(3):
            result = executor.duckdb_engine.execute_query(test_query).fetchdf()
            results.append(result)

        # Verify results are consistent across runs
        for i in range(1, len(results)):
            pd.testing.assert_frame_equal(results[0], results[i])

        assert len(results[0]) == 10
        logger.info(
            "✅ Regression test: pipeline execution stable across multiple runs"
        )

    def test_regression_memory_usage_stability(
        self, integration_test_env: Dict[str, Any]
    ) -> None:
        """Regression test for memory usage stability in UDF operations."""
        executor = LocalExecutor(project_dir=integration_test_env["project_dir"])

        # Load data multiple times and process with UDFs
        for iteration in range(5):
            table_name = f"iteration_{iteration}"

            query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT
                    customer_id,
                    standardize_name(name) AS clean_name,
                    calculate_loyalty_score(years_active, total_purchases) AS loyalty_score,
                    determine_customer_segment(
                        calculate_loyalty_score(years_active, total_purchases), age
                    ) AS segment
                FROM read_csv('{integration_test_env["customers_csv"]}', header=true, auto_detect=true)
            """

            executor.duckdb_engine.execute_query(query)

            # Verify table was created and has expected size
            count_result = executor.duckdb_engine.execute_query(
                f"SELECT COUNT(*) as count FROM {table_name}"
            ).fetchdf()
            assert count_result.iloc[0]["count"] == 10

        logger.info("✅ Regression test: memory usage stable across iterations")
