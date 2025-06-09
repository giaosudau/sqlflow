#!/usr/bin/env python3
"""Demonstrates advanced UDF processing patterns.

This script covers two main approaches for complex data transformations:
1.  **Programmatic Table UDFs**: Calling a @python_table_udf-decorated
    function directly from Python for easy, in-process transformations.
2.  **External Processing**: A pattern for more complex workflows where
    data is explicitly fetched from a database, processed using pure
    Python/pandas, and then registered back for further SQL analysis.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    """Run the advanced UDF processing demonstrations."""
    # Ensure project root is in the path to allow UDF and sqlflow imports
    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

    print("🚀 Running Advanced UDF Processing Demonstrations...\n")

    # --- Part 1: Programmatic Table UDF Call ---
    demonstrate_programmatic_udf_call()

    # --- Part 2: External Processing Pattern ---
    demonstrate_external_processing()

    print("\n✅ Advanced UDF processing demonstrations complete.")


def demonstrate_programmatic_udf_call():
    """
    Shows how to import and call a @python_table_udf directly.
    This is useful for reusing transformation logic within a Python application.
    """
    print("--- Part 1: Programmatic Table UDF Call ---")
    try:
        # Import a UDF decorated with @python_table_udf
        from examples.udf_examples.python_udfs.data_transforms import (
            add_sales_metrics,
        )

        print("  - Imported 'add_sales_metrics' UDF.")

        # Load data from CSV into a pandas DataFrame
        sales_df = pd.read_csv(Path("data/sales.csv"))
        print(f"  - Loaded {len(sales_df)} sales records.")

        # Call the table UDF programmatically, just like any other Python function
        processed_df = add_sales_metrics(sales_df)
        print("  - Executed UDF programmatically.")

        # Save the results
        output_path = Path("output/programmatic_udf_results.csv")
        processed_df.to_csv(output_path, index=False)
        print(f"  - Results saved to '{output_path}'.")
        print("-----------------------------------------\n")

    except Exception as e:
        print(f"  - ❌ Error in programmatic UDF demo: {e}")
        sys.exit(1)


def demonstrate_external_processing():
    """
    Shows a pattern for processing data outside of the database.
    Pattern: Fetch -> Process -> Register
    """
    print("--- Part 2: External Processing Pattern ---")
    try:
        from sqlflow.core.engines.duckdb import DuckDBEngine

        # This is a plain Python function, NOT a UDF.
        # It contains the core logic that could be inside a table UDF.
        def detect_outliers_external(df: pd.DataFrame) -> pd.DataFrame:
            """Detects outliers in the 'price' column."""
            result = df.copy()
            prices = result["price"]
            mean = prices.mean()
            std = prices.std()
            if std > 0:
                result["z_score"] = (prices - mean) / std
                result["is_outlier"] = np.abs(result["z_score"]) > 3
            else:
                result["z_score"] = 0
                result["is_outlier"] = False
            return result

        # 1. FETCH: Load data and initialize a database engine
        engine = DuckDBEngine(":memory:")
        sales_df = pd.read_csv(Path("data/sales.csv"))
        engine.connection.register("sales_data", sales_df)
        print("  - 1. FETCH: Initialized in-memory DB and loaded sales data.")

        # For this demo, we'll just fetch it back out. In a real scenario,
        # you might do a more complex SQL query here.
        fetched_df = engine.execute_query("SELECT * FROM sales_data").fetchdf()

        # 2. PROCESS: Use the plain Python function to process the DataFrame
        processed_df = detect_outliers_external(fetched_df)
        print("  - 2. PROCESS: Applied outlier detection logic in Python.")

        # 3. REGISTER: Register the processed DataFrame back into the DB
        engine.connection.register("processed_sales", processed_df)
        print("  - 3. REGISTER: Registered processed data back into DB.")

        # Now you can query the processed data with SQL
        final_result = engine.execute_query(
            "SELECT COUNT(*) as outlier_count FROM processed_sales WHERE is_outlier"
        ).fetchdf()
        outlier_count = final_result["outlier_count"].iloc[0]
        print(f"  - Analyzed processed data with SQL: Found {outlier_count} outliers.")

        # Save the results
        output_path = Path("output/external_processing_results.csv")
        processed_df.to_csv(output_path, index=False)
        print(f"  - Results saved to '{output_path}'.")
        print("---------------------------------------")

    except Exception as e:
        print(f"  - ❌ Error in external processing demo: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
