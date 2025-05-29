"""
Integration tests for advanced table UDF functionality.

This test suite validates the sophisticated table UDF registration
strategies and capabilities including schema management, fallback
strategies, and error handling.
"""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.udfs.decorators import python_table_udf


class TestAdvancedTableUDFFunctionality:
    """Test suite validating advanced table UDF capabilities."""

    @python_table_udf(
        output_schema={
            "id": "INTEGER",
            "name": "VARCHAR",
            "computed_value": "DOUBLE",
            "category": "VARCHAR",
        }
    )
    def advanced_table_processor(self, df: pd.DataFrame) -> pd.DataFrame:
        """Advanced table UDF with explicit schema for testing."""
        result = df.copy()

        # Ensure basic columns exist
        if "id" not in result.columns:
            result["id"] = range(1, len(result) + 1)
        if "name" not in result.columns:
            result["name"] = [f"item_{i}" for i in result["id"]]
        if "value" not in result.columns:
            result["value"] = [10.0, 20.0, 30.0][: len(result)]

        # Advanced processing with schema validation
        result["computed_value"] = result["value"] * 2.5 + 5.0
        result["category"] = result["value"].apply(
            lambda x: "low" if x < 20 else "medium" if x < 30 else "high"
        )

        # Return DataFrame explicitly with proper typing
        final_result: pd.DataFrame = result[
            ["id", "name", "computed_value", "category"]
        ]
        return final_result

    @python_table_udf(infer=True)
    def schema_inference_processor(self, df: pd.DataFrame) -> pd.DataFrame:
        """Table UDF with schema inference for testing."""
        result = df.copy()

        # Add columns that should be inferred automatically
        if "score" in result.columns:
            result["normalized_score"] = result["score"] / result["score"].max()
            result["grade"] = result["normalized_score"].apply(
                lambda x: "A" if x >= 0.9 else "B" if x >= 0.8 else "C"
            )

        return result

    def test_explicit_schema_registration(self):
        """Test that table UDFs with explicit schemas register correctly."""
        engine = DuckDBEngine(":memory:")

        # This should use the advanced structured schema registration
        try:
            engine.register_python_udf(
                "advanced_processor", self.advanced_table_processor
            )

            # Verify registration succeeded
            assert "advanced_processor" in engine.registered_udfs
            registered_func = engine.registered_udfs["advanced_processor"]

            # Verify metadata preservation
            assert hasattr(registered_func, "_output_schema")
            assert hasattr(registered_func, "_udf_type")
            assert registered_func._udf_type == "table"

            print("✅ Explicit schema registration successful")

        except Exception as e:
            pytest.fail(f"Explicit schema registration failed: {e}")

    def test_schema_inference_registration(self):
        """Test that table UDFs with schema inference register correctly."""
        engine = DuckDBEngine(":memory:")

        # This should use the schema inference strategy
        try:
            engine.register_python_udf(
                "inference_processor", self.schema_inference_processor
            )

            # Verify registration succeeded
            assert "inference_processor" in engine.registered_udfs
            registered_func = engine.registered_udfs["inference_processor"]

            # Verify inference metadata
            assert hasattr(registered_func, "_infer_schema") or hasattr(
                registered_func, "_infer"
            )

            print("✅ Schema inference registration successful")

        except Exception as e:
            pytest.fail(f"Schema inference registration failed: {e}")

    def test_fallback_strategy_handles_simple_udfs(self):
        """Test that simple table UDFs without metadata still register via fallback."""
        engine = DuckDBEngine(":memory:")

        # Create a UDF without explicit metadata to test fallback
        def simple_table_transform(df: pd.DataFrame) -> pd.DataFrame:
            if "value" in df.columns:
                return df.assign(doubled_value=df["value"] * 2)
            else:
                # Handle case where value column doesn't exist
                return df.assign(doubled_value=0)

        # Mark as table UDF using setattr to avoid linter errors
        setattr(simple_table_transform, "_udf_type", "table")

        try:
            engine.register_python_udf("simple_transform", simple_table_transform)

            # Should succeed even without schema metadata
            assert "simple_transform" in engine.registered_udfs

            print("✅ Fallback strategy handles simple UDFs")

        except Exception as e:
            pytest.fail(f"Fallback strategy failed: {e}")

    def test_wrapped_function_metadata_preserved(self):
        """Test that metadata is preserved when functions are wrapped."""
        from functools import wraps

        # Create a wrapped function that preserves metadata
        def wrapper_decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            # Preserve metadata
            for attr in ["_output_schema", "_udf_type", "_infer_schema", "_infer"]:
                if hasattr(func, attr):
                    setattr(wrapper, attr, getattr(func, attr))
            return wrapper

        # Apply wrapper to our UDF
        wrapped_udf = wrapper_decorator(self.advanced_table_processor)

        engine = DuckDBEngine(":memory:")

        try:
            engine.register_python_udf("wrapped_processor", wrapped_udf)

            # Should successfully extract metadata from wrapped function
            assert "wrapped_processor" in engine.registered_udfs

            print("✅ Wrapped function metadata preserved")

        except Exception as e:
            pytest.fail(f"Wrapped function registration failed: {e}")

    def test_struct_type_generation_from_schema(self):
        """Test that STRUCT types are correctly generated from output schemas."""
        from sqlflow.core.engines.duckdb.udf.registration import (
            AdvancedTableUDFStrategy,
        )

        strategy = AdvancedTableUDFStrategy()

        # Test schema with various types
        test_schema = {
            "user_id": "INTEGER",
            "username": "VARCHAR",
            "score": "DOUBLE",
            "is_active": "BOOLEAN",
            "custom_type": "DECIMAL",
        }

        struct_type = strategy._build_struct_type_from_schema(test_schema)

        # Verify STRUCT format
        assert struct_type.startswith("STRUCT(")
        assert struct_type.endswith(")")

        # Verify type normalization
        assert "user_id INTEGER" in struct_type
        assert "username VARCHAR" in struct_type
        assert "score DOUBLE" in struct_type
        assert "is_active BOOLEAN" in struct_type

        print(f"✅ STRUCT type generation works: {struct_type}")

    def test_registration_error_handling(self):
        """Test that registration errors are properly handled with informative messages."""
        engine = DuckDBEngine(":memory:")

        # Create a problematic UDF that might cause registration issues
        def problematic_udf(df: pd.DataFrame) -> pd.DataFrame:
            return df

        # Add invalid schema to trigger error handling using setattr
        setattr(problematic_udf, "_udf_type", "table")
        setattr(problematic_udf, "_output_schema", {"invalid*column": "INVALID_TYPE"})

        # Should handle errors gracefully and provide useful error messages
        try:
            engine.register_python_udf("problematic_udf", problematic_udf)
            print("⚠️  Problematic UDF registered (fallback worked)")

        except Exception as e:
            # Error should be informative and properly formatted
            assert "Failed to register table UDF" in str(e)
            assert "problematic_udf" in str(e)
            print(
                f"✅ Error handling provides informative messages: {type(e).__name__}"
            )

    def test_multiple_registration_strategies_attempted(self):
        """Test that multiple registration strategies are tried when needed."""
        engine = DuckDBEngine(":memory:")

        # Create UDF that should trigger multiple strategy attempts
        def complex_udf(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(processed=True)

        # Add metadata using setattr to avoid linter errors
        setattr(complex_udf, "_udf_type", "table")
        setattr(complex_udf, "_output_schema", {"processed": "BOOLEAN"})
        setattr(complex_udf, "_infer_schema", True)

        try:
            engine.register_python_udf("complex_udf", complex_udf)

            # Should succeed with one of the strategies
            assert "complex_udf" in engine.registered_udfs

            print("✅ Multiple registration strategies work correctly")

        except Exception as e:
            pytest.fail(f"Multi-strategy registration failed: {e}")


class TestBackwardCompatibility:
    """Test that legacy UDF patterns continue to work."""

    def test_legacy_strategy_redirection(self):
        """Test that legacy registration strategies redirect correctly."""
        from sqlflow.core.engines.duckdb.udf.registration import (
            VectorizedTableUDFStrategy,
        )

        engine = DuckDBEngine(":memory:")
        strategy = VectorizedTableUDFStrategy()

        def legacy_udf(df: pd.DataFrame) -> pd.DataFrame:
            return df

        setattr(legacy_udf, "_udf_type", "table")

        # Should redirect without errors
        try:
            strategy.register("legacy_udf", legacy_udf, engine.connection)
            print("✅ Legacy strategy redirection works")

        except Exception as e:
            pytest.fail(f"Legacy strategy redirection failed: {e}")

    def test_existing_decorator_patterns_compatible(self):
        """Test that existing UDF decorator patterns continue to work."""
        engine = DuckDBEngine(":memory:")

        # Test existing decorator pattern
        @python_table_udf(output_schema={"result": "VARCHAR"})
        def existing_pattern_udf(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(result="processed")

        try:
            engine.register_python_udf("existing_udf", existing_pattern_udf)
            assert "existing_udf" in engine.registered_udfs

            print("✅ Existing decorator patterns remain compatible")

        except Exception as e:
            pytest.fail(f"Existing decorator pattern compatibility broken: {e}")


if __name__ == "__main__":
    # Run tests directly for development
    test_class = TestAdvancedTableUDFFunctionality()

    print("🔧 Testing Advanced Table UDF Functionality...")

    try:
        test_class.test_explicit_schema_registration()
        test_class.test_schema_inference_registration()
        test_class.test_fallback_strategy_handles_simple_udfs()
        test_class.test_wrapped_function_metadata_preserved()
        test_class.test_struct_type_generation_from_schema()
        test_class.test_registration_error_handling()
        test_class.test_multiple_registration_strategies_attempted()

        print("\n🎉 All functionality tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
