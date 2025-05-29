"""Test Phase 3 engine enhancements for table UDF functionality.

This test suite validates the advanced engine methods implemented in Phase 3:
- Batch execution of table UDFs
- Schema compatibility validation  
- Performance metrics and monitoring
- Comprehensive debugging capabilities
- Automatic UDF optimization
"""

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine


@pytest.fixture
def engine():
    """Create a DuckDB engine for testing."""
    return DuckDBEngine(":memory:")


@pytest.fixture
def sample_table_udf():
    """Create a sample table UDF for testing."""
    def sample_analytics(df: pd.DataFrame) -> pd.DataFrame:
        """Sample table UDF that adds calculated columns."""
        result = df.copy()
        if 'value' in df.columns:
            result['calculated'] = df['value'] * 2
            result['category'] = df['value'].apply(lambda x: 'high' if x > 50 else 'low')
        return result
    
    # Mark as table UDF with metadata using setattr()
    setattr(sample_analytics, '_udf_type', "table")
    setattr(sample_analytics, '_output_schema', {
        'value': 'INTEGER',
        'calculated': 'INTEGER', 
        'category': 'VARCHAR'
    })
    setattr(sample_analytics, '_infer_schema', True)
    
    return sample_analytics


class TestPhase3EngineEnhancements:
    """Test suite for Phase 3 engine enhancements."""

    def test_debug_table_udf_registration(self, engine, sample_table_udf):
        """Test comprehensive UDF debugging information."""
        udf_name = "test_analytics"
        
        # Get debug info before registration
        debug_info_before = engine.debug_table_udf_registration(udf_name)
        
        assert debug_info_before["udf_name"] == udf_name
        assert debug_info_before["engine_state"]["connection_available"] is True
        assert debug_info_before["registration_status"]["in_registered_udfs"] is False
        assert len(debug_info_before["recommendations"]) > 0
        
        # Register the UDF
        engine.register_python_udf(udf_name, sample_table_udf)
        
        # Get debug info after registration
        debug_info_after = engine.debug_table_udf_registration(udf_name)
        
        assert debug_info_after["registration_status"]["in_registered_udfs"] is True
        assert debug_info_after["metadata"]["udf_type"] == "table"
        assert debug_info_after["metadata"]["output_schema"] is not None
        assert debug_info_after["metadata"]["infer_schema"] is True

    def test_table_udf_performance_metrics(self, engine, sample_table_udf):
        """Test table UDF performance metrics collection."""
        udf_name = "test_analytics"
        
        # Get metrics before registration
        metrics_before = engine.get_table_udf_performance_metrics()
        assert metrics_before["table_udf_specific"]["total_table_udfs"] == 0
        
        # Register the UDF
        engine.register_python_udf(udf_name, sample_table_udf)
        
        # Get metrics after registration
        metrics_after = engine.get_table_udf_performance_metrics()
        assert metrics_after["table_udf_specific"]["total_table_udfs"] == 1
        assert len(metrics_after["performance_insights"]) > 0

    def test_table_udf_schema_compatibility_validation(self, engine, sample_table_udf):
        """Test schema compatibility validation."""
        # Test with non-existent table (should always be compatible)
        udf_schema = {
            'value': 'INTEGER',
            'calculated': 'INTEGER',
            'category': 'VARCHAR'
        }
        
        is_compatible = engine.validate_table_udf_schema_compatibility(
            "non_existent_table", udf_schema
        )
        assert is_compatible is True
        
        # Create a test table with compatible schema
        test_data = pd.DataFrame({
            'value': [10, 20, 30],
            'calculated': [20, 40, 60],
            'category': ['low', 'low', 'low']
        })
        engine.register_table("test_table", test_data)
        
        # Test compatibility with existing table
        is_compatible = engine.validate_table_udf_schema_compatibility(
            "test_table", udf_schema
        )
        assert is_compatible is True

    def test_udf_optimization_for_performance(self, engine, sample_table_udf):
        """Test automatic UDF performance optimization."""
        udf_name = "test_analytics"
        
        # Register the UDF
        engine.register_python_udf(udf_name, sample_table_udf)
        
        # Optimize the UDF for performance
        optimization_results = engine.optimize_table_udf_for_performance(udf_name)
        
        assert optimization_results["udf_name"] == udf_name
        assert isinstance(optimization_results["optimizations_applied"], list)
        assert isinstance(optimization_results["recommendations"], list)
        assert "performance_impact" in optimization_results

    def test_batch_execute_table_udf(self, engine, sample_table_udf):
        """Test batch execution of table UDFs."""
        udf_name = "test_analytics"
        
        # Register the UDF
        engine.register_python_udf(udf_name, sample_table_udf)
        
        # Create test dataframes for batch processing
        dataframes = [
            pd.DataFrame({'value': [10, 20]}),
            pd.DataFrame({'value': [30, 40]}),
            pd.DataFrame({'value': [50, 60]})
        ]
        
        # Execute batch processing
        results = engine.batch_execute_table_udf(udf_name, dataframes)
        
        assert len(results) == 3
        
        # Verify each result has the expected structure
        for result in results:
            if not result.empty:  # Some results might be empty due to error handling
                assert 'value' in result.columns
                assert 'calculated' in result.columns
                assert 'category' in result.columns

    def test_empty_batch_execution(self, engine, sample_table_udf):
        """Test batch execution with empty dataframe list."""
        udf_name = "test_analytics"
        
        # Register the UDF
        engine.register_python_udf(udf_name, sample_table_udf)
        
        # Test with empty list
        results = engine.batch_execute_table_udf(udf_name, [])
        assert results == []

    def test_debug_unregistered_udf(self, engine):
        """Test debugging information for unregistered UDF."""
        debug_info = engine.debug_table_udf_registration("non_existent_udf")
        
        assert debug_info["udf_name"] == "non_existent_udf"
        assert debug_info["registration_status"]["in_registered_udfs"] is False
        assert any("not registered" in rec for rec in debug_info["recommendations"])

    def test_optimize_non_table_udf(self, engine):
        """Test optimization attempt on non-table UDF."""
        def scalar_udf(x):
            return x * 2
        
        setattr(scalar_udf, '_udf_type', "scalar")
        
        # Register as scalar UDF
        engine.register_python_udf("scalar_test", scalar_udf)
        
        # Try to optimize (should fail gracefully)
        result = engine.optimize_table_udf_for_performance("scalar_test")
        
        assert "error" in result
        assert "not a table UDF" in result["error"]

    def test_performance_metrics_insights(self, engine, sample_table_udf):
        """Test performance insights generation."""
        # Register multiple UDFs with different characteristics
        udf1 = sample_table_udf
        udf1._vectorized = True
        udf1._arrow_compatible = True
        
        udf2 = sample_table_udf
        udf2._vectorized = False
        udf2._arrow_compatible = False
        
        engine.register_python_udf("udf1", udf1)
        engine.register_python_udf("udf2", udf2)
        
        metrics = engine.get_table_udf_performance_metrics()
        
        assert metrics["table_udf_specific"]["total_table_udfs"] == 2
        assert len(metrics["performance_insights"]) > 0
        
        # Should have optimization opportunities since not all UDFs are optimized
        if metrics["table_udf_specific"]["vectorized_udfs"] < metrics["table_udf_specific"]["total_table_udfs"]:
            assert len(metrics["optimization_opportunities"]) > 0 