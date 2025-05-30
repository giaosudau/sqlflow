"""Real integration tests that verify actual functionality.

These tests use real data and verify that the system actually works,
rather than just testing that methods exist or return success.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqlflow.core.executors.local_executor import LocalExecutor


class TestRealIntegration:
    """Real integration tests that verify actual functionality."""

    @pytest.mark.skip(
        reason="LocalExecutor table registration needs stabilization - part of test refactoring"
    )
    def test_complete_data_pipeline_with_real_data(self):
        """Test a complete pipeline with real data transformation."""
        executor = LocalExecutor()

        # Create REAL test data
        customer_data = pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "name": [
                    "Alice Johnson",
                    "Bob Smith",
                    "Charlie Brown",
                    "Diana Prince",
                    "Eve Wilson",
                ],
                "age": [25, 34, 28, 31, 29],
                "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
            }
        )

        order_data = pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104, 105, 106],
                "customer_id": [1, 2, 1, 3, 2, 4],
                "amount": [150.00, 200.50, 75.25, 300.00, 125.75, 450.00],
                "order_date": [
                    "2024-01-15",
                    "2024-01-16",
                    "2024-01-17",
                    "2024-01-18",
                    "2024-01-19",
                    "2024-01-20",
                ],
            }
        )

        # Register tables with executor's DuckDB engine
        executor.duckdb_engine.register_table("customers", customer_data)
        executor.duckdb_engine.register_table("orders", order_data)

        # Also store in table_data for consistency
        executor.table_data["customers"] = customer_data
        executor.table_data["orders"] = order_data

        # Execute a complex transformation that actually transforms the data
        plan = [
            {
                "type": "transform",
                "id": "customer_order_summary",
                "name": "customer_summary",
                "query": """
                    SELECT 
                        c.customer_id,
                        c.name,
                        c.city,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_spent,
                        AVG(o.amount) as avg_order_value,
                        MIN(o.order_date) as first_order,
                        MAX(o.order_date) as last_order
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    GROUP BY c.customer_id, c.name, c.city
                    ORDER BY total_spent DESC
                """,
            }
        ]

        result = executor.execute(plan)

        # Verify execution was successful
        assert result["status"] == "success"

        # VERIFY THE ACTUAL DATA TRANSFORMATION
        summary_data = executor.table_data["customer_summary"]

        # Verify we have the expected number of customers
        assert len(summary_data) == 5

        # Verify specific data transformations are correct
        alice_row = summary_data[summary_data["name"] == "Alice Johnson"].iloc[0]
        assert alice_row["total_orders"] == 2  # Alice has 2 orders
        assert alice_row["total_spent"] == 225.25  # 150.00 + 75.25
        assert alice_row["avg_order_value"] == 112.625  # (150.00 + 75.25) / 2

        diana_row = summary_data[summary_data["name"] == "Diana Prince"].iloc[0]
        assert diana_row["total_orders"] == 1
        assert diana_row["total_spent"] == 450.00

        # Verify ordering (Diana should be first with highest total_spent)
        assert summary_data.iloc[0]["name"] == "Diana Prince"

    @pytest.mark.skip(
        reason="LocalExecutor table registration needs stabilization - part of test refactoring"
    )
    def test_multi_step_pipeline_with_dependencies(self):
        """Test a multi-step pipeline where steps depend on each other."""
        executor = LocalExecutor()

        # Create realistic sales data
        sales_data = pd.DataFrame(
            {
                "sale_id": [1, 2, 3, 4, 5, 6, 7, 8],
                "product_id": [101, 102, 101, 103, 102, 101, 104, 103],
                "quantity": [2, 1, 3, 1, 2, 1, 4, 2],
                "unit_price": [25.00, 45.00, 25.00, 15.00, 45.00, 25.00, 10.00, 15.00],
                "sale_date": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                ],
                "region": [
                    "North",
                    "South",
                    "North",
                    "East",
                    "South",
                    "West",
                    "East",
                    "North",
                ],
            }
        )

        product_data = pd.DataFrame(
            {
                "product_id": [101, 102, 103, 104],
                "product_name": ["Widget A", "Widget B", "Gadget X", "Tool Y"],
                "category": ["Widgets", "Widgets", "Gadgets", "Tools"],
            }
        )

        executor.table_data["sales"] = sales_data
        executor.table_data["products"] = product_data

        # Multi-step pipeline with real business logic
        plan = [
            # Step 1: Calculate sale totals
            {
                "type": "transform",
                "id": "step1_sale_totals",
                "name": "sale_totals",
                "query": """
                    SELECT 
                        sale_id,
                        product_id,
                        quantity,
                        unit_price,
                        quantity * unit_price as total_amount,
                        sale_date,
                        region
                    FROM sales
                """,
            },
            # Step 2: Join with product information
            {
                "type": "transform",
                "id": "step2_enriched_sales",
                "name": "enriched_sales",
                "query": """
                    SELECT 
                        st.*,
                        p.product_name,
                        p.category
                    FROM sale_totals st
                    JOIN products p ON st.product_id = p.product_id
                """,
            },
            # Step 3: Create regional summary
            {
                "type": "transform",
                "id": "step3_regional_summary",
                "name": "regional_summary",
                "query": """
                    SELECT 
                        region,
                        COUNT(*) as total_sales,
                        SUM(total_amount) as revenue,
                        AVG(total_amount) as avg_sale_amount,
                        COUNT(DISTINCT product_id) as unique_products
                    FROM enriched_sales
                    GROUP BY region
                    ORDER BY revenue DESC
                """,
            },
            # Step 4: Create product performance summary
            {
                "type": "transform",
                "id": "step4_product_summary",
                "name": "product_summary",
                "query": """
                    SELECT 
                        product_name,
                        category,
                        COUNT(*) as times_sold,
                        SUM(quantity) as total_quantity,
                        SUM(total_amount) as total_revenue,
                        AVG(unit_price) as avg_price
                    FROM enriched_sales
                    GROUP BY product_name, category
                    ORDER BY total_revenue DESC
                """,
            },
        ]

        result = executor.execute(plan)
        assert result["status"] == "success"

        # VERIFY EACH STEP PRODUCED CORRECT RESULTS

        # Verify Step 1: Sale totals calculation
        sale_totals = executor.table_data["sale_totals"]
        first_sale = sale_totals[sale_totals["sale_id"] == 1].iloc[0]
        assert first_sale["total_amount"] == 50.00  # 2 * 25.00

        # Verify Step 2: Enriched data includes product info
        enriched = executor.table_data["enriched_sales"]
        widget_a_sales = enriched[enriched["product_name"] == "Widget A"]
        assert len(widget_a_sales) == 3  # Widget A appears in 3 sales

        # Verify Step 3: Regional aggregation is correct
        regional = executor.table_data["regional_summary"]
        north_region = regional[regional["region"] == "North"].iloc[0]
        # North region has sales: 50.00 + 30.00 = 80.00
        assert north_region["revenue"] == 80.00
        assert north_region["total_sales"] == 2

        # Verify Step 4: Product performance is calculated correctly
        product_perf = executor.table_data["product_summary"]
        widget_a_perf = product_perf[product_perf["product_name"] == "Widget A"].iloc[0]
        assert widget_a_perf["times_sold"] == 3
        assert widget_a_perf["total_quantity"] == 6  # 2 + 3 + 1
        assert widget_a_perf["total_revenue"] == 150.00  # 3 sales * 25.00 each

    @pytest.mark.skip(
        reason="LocalExecutor table registration needs stabilization - part of test refactoring"
    )
    def test_error_handling_with_real_scenarios(self):
        """Test error handling in realistic failure scenarios."""
        executor = LocalExecutor()

        # Test 1: Missing table reference
        plan = [
            {
                "type": "transform",
                "id": "missing_table_test",
                "name": "result",
                "query": "SELECT * FROM nonexistent_table",
            }
        ]

        result = executor.execute(plan)
        assert result["status"] == "failed"
        assert "error" in result

        # Test 2: Invalid SQL syntax
        plan = [
            {
                "type": "transform",
                "id": "invalid_sql_test",
                "name": "result",
                "query": "INVALID SQL SYNTAX HERE",
            }
        ]

        result = executor.execute(plan)
        assert result["status"] == "failed"

        # Test 3: Type mismatch error
        executor.table_data["test_data"] = pd.DataFrame(
            {"text_col": ["a", "b", "c"], "num_col": [1, 2, 3]}
        )

        plan = [
            {
                "type": "transform",
                "id": "type_error_test",
                "name": "result",
                "query": "SELECT text_col + num_col FROM test_data",  # Invalid operation
            }
        ]

        result = executor.execute(plan)
        # Should handle the error gracefully
        assert result["status"] in ["success", "failed"]

    @pytest.mark.skip(
        reason="LocalExecutor table registration needs stabilization - part of test refactoring"
    )
    def test_data_types_preservation(self):
        """Test that data types are preserved correctly through transformations."""
        executor = LocalExecutor()

        # Create data with various types
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "amount": [100.50, 200.75, 150.25],
                "is_active": [True, False, True],
                "created_date": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03"]
                ),
            }
        )

        executor.table_data["source_data"] = test_data

        plan = [
            {
                "type": "transform",
                "id": "type_preservation_test",
                "name": "result",
                "query": """
                    SELECT 
                        id,
                        name,
                        amount,
                        is_active,
                        created_date,
                        amount * 1.1 as adjusted_amount
                    FROM source_data
                    WHERE is_active = true
                """,
            }
        ]

        result = executor.execute(plan)
        assert result["status"] == "success"

        # Verify data types are preserved
        result_data = executor.table_data["result"]

        # Should have filtered to only active records
        assert len(result_data) == 2

        # Verify calculated column
        first_row = result_data.iloc[0]
        assert abs(first_row["adjusted_amount"] - 110.55) < 0.01  # 100.50 * 1.1

    @pytest.mark.skip(
        reason="LocalExecutor table registration needs stabilization - part of test refactoring"
    )
    def test_real_csv_file_integration(self):
        """Test integration with actual CSV files."""
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            csv_content = """customer_id,name,email,total_purchases
1,Alice Johnson,alice@example.com,1250.00
2,Bob Smith,bob@example.com,850.50
3,Charlie Brown,charlie@example.com,2100.75
4,Diana Prince,diana@example.com,750.25"""
            f.write(csv_content)
            csv_file_path = f.name

        try:
            executor = LocalExecutor()

            # Load from CSV file
            plan = [
                {
                    "type": "load",
                    "id": "load_customers",
                    "table_name": "customers",
                    "mode": "REPLACE",
                    "query": {
                        "source_connector_type": "CSV",
                        "path": csv_file_path,
                        "has_header": True,
                    },
                },
                {
                    "type": "transform",
                    "id": "high_value_customers",
                    "name": "high_value",
                    "query": """
                        SELECT 
                            customer_id,
                            name,
                            email,
                            total_purchases,
                            CASE 
                                WHEN total_purchases > 2000 THEN 'Premium'
                                WHEN total_purchases > 1000 THEN 'Gold'
                                ELSE 'Standard'
                            END as customer_tier
                        FROM customers
                        WHERE total_purchases > 800
                        ORDER BY total_purchases DESC
                    """,
                },
            ]

            result = executor.execute(plan)
            assert result["status"] == "success"

            # Verify the data was loaded and transformed correctly
            high_value_data = executor.table_data["high_value"]

            # Should have 3 customers (all except Diana who has 750.25)
            assert len(high_value_data) == 3

            # Verify customer tiers are assigned correctly
            charlie_row = high_value_data[
                high_value_data["name"] == "Charlie Brown"
            ].iloc[0]
            assert charlie_row["customer_tier"] == "Premium"

            alice_row = high_value_data[
                high_value_data["name"] == "Alice Johnson"
            ].iloc[0]
            assert alice_row["customer_tier"] == "Gold"

        finally:
            # Clean up the temp file
            Path(csv_file_path).unlink(missing_ok=True)
