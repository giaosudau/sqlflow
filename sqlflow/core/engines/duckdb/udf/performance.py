"""Zero-copy performance optimization for table UDFs.

Phase 2 enhancement implementing high-performance data exchange between
Python UDFs and DuckDB using Apache Arrow zero-copy operations.
"""

import time
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class ArrowPerformanceOptimizer:
    """Zero-copy performance optimization for table UDFs.
    
    Phase 2 enhancement providing:
    - Apache Arrow zero-copy data exchange
    - Serialization overhead minimization
    - Vectorized processing enablement
    - Batch processing optimization
    - Performance monitoring and profiling
    """

    def __init__(self):
        """Initialize the performance optimizer."""
        self.performance_metrics: Dict[str, Any] = {}
        self.optimization_cache: Dict[str, Callable] = {}
        self.batch_size_cache: Dict[str, int] = {}
        
        # Check Arrow availability
        self.arrow_available = self._check_arrow_availability()
        
        logger.info(f"ArrowPerformanceOptimizer initialized (Arrow available: {self.arrow_available})")

    def optimize_data_exchange(self, input_data: Any) -> pa.Table:
        """Optimize data exchange using Arrow zero-copy operations.
        
        Phase 2 enhancement converting various input formats to Arrow Tables
        with minimal copying and maximum performance.
        
        Args:
            input_data: Input data in various formats (DataFrame, Arrow, etc.)
            
        Returns:
            Optimized Arrow Table for zero-copy operations
        """
        start_time = time.time()
        
        try:
            # Handle different input types
            if isinstance(input_data, pa.Table):
                # Already an Arrow table - no conversion needed
                logger.debug("Input already Arrow Table - zero copy optimization")
                optimized_table = input_data
                
            elif isinstance(input_data, pd.DataFrame):
                # Convert pandas DataFrame to Arrow with optimal settings
                logger.debug("Converting pandas DataFrame to Arrow Table")
                optimized_table = self._optimize_pandas_to_arrow(input_data)
                
            elif hasattr(input_data, "to_arrow"):
                # Handle objects with to_arrow method (e.g., Polars)
                logger.debug("Converting to Arrow using to_arrow() method")
                optimized_table = input_data.to_arrow()
                
            else:
                # Fallback: try to convert via pandas
                logger.debug("Fallback conversion via pandas DataFrame")
                df = pd.DataFrame(input_data)
                optimized_table = self._optimize_pandas_to_arrow(df)
            
            # Record performance metrics
            conversion_time = time.time() - start_time
            self._record_optimization_metric("data_exchange", {
                "conversion_time": conversion_time,
                "input_type": type(input_data).__name__,
                "output_rows": optimized_table.num_rows,
                "output_columns": optimized_table.num_columns
            })
            
            logger.debug(f"Data exchange optimized in {conversion_time:.4f}s")
            return optimized_table
            
        except Exception as e:
            logger.error(f"Error optimizing data exchange: {e}")
            # Fallback to pandas conversion
            if not isinstance(input_data, pd.DataFrame):
                input_data = pd.DataFrame(input_data)
            return pa.Table.from_pandas(input_data)

    def minimize_serialization_overhead(self, function: Callable) -> Callable:
        """Minimize serialization overhead in UDF execution.
        
        Phase 2 enhancement wrapping UDF functions with optimized
        serialization and data transfer mechanisms.
        
        Args:
            function: Original UDF function
            
        Returns:
            Optimized function with reduced serialization overhead
        """
        function_name = getattr(function, "__name__", "unknown")
        
        # Check cache first
        if function_name in self.optimization_cache:
            logger.debug(f"Using cached optimization for {function_name}")
            return self.optimization_cache[function_name]
        
        def optimized_wrapper(*args, **kwargs):
            """Optimized wrapper that minimizes serialization overhead."""
            start_time = time.time()
            
            # Optimize input arguments
            optimized_args = []
            for arg in args:
                if self._should_optimize_arg(arg):
                    optimized_arg = self._optimize_argument(arg)
                    optimized_args.append(optimized_arg)
                else:
                    optimized_args.append(arg)
            
            # Execute the function with optimized inputs
            try:
                result = function(*optimized_args, **kwargs)
                
                # Optimize the output if it's a large dataset
                if self._should_optimize_output(result):
                    result = self._optimize_output(result)
                
                # Record performance metrics
                execution_time = time.time() - start_time
                self._record_optimization_metric("serialization", {
                    "function_name": function_name,
                    "execution_time": execution_time,
                    "input_args": len(args),
                    "optimization_applied": True
                })
                
                return result
                
            except Exception as e:
                logger.error(f"Error in optimized function {function_name}: {e}")
                # Fallback to original function
                return function(*args, **kwargs)
        
        # Copy function metadata
        optimized_wrapper.__name__ = function_name
        optimized_wrapper.__doc__ = getattr(function, "__doc__", "")
        
        # Copy UDF metadata
        for attr in ["_udf_type", "_output_schema", "_infer_schema", "_table_dependencies"]:
            if hasattr(function, attr):
                setattr(optimized_wrapper, attr, getattr(function, attr))
        
        # Cache the optimized function
        self.optimization_cache[function_name] = optimized_wrapper
        
        logger.debug(f"Created optimized wrapper for function {function_name}")
        return optimized_wrapper

    def enable_vectorized_processing(self, function: Callable) -> Callable:
        """Enable advanced vectorized processing for large datasets.
        
        Phase 2 enhancement enabling batch processing and vectorization
        for table UDFs working with large datasets.
        
        Args:
            function: UDF function to vectorize
            
        Returns:
            Vectorized function with batch processing capabilities
        """
        function_name = getattr(function, "__name__", "unknown")
        
        def vectorized_wrapper(*args, **kwargs):
            """Vectorized wrapper that processes data in optimized batches."""
            
            # Check if we should use vectorized processing
            if not self._should_vectorize(args):
                logger.debug(f"Vectorization not beneficial for {function_name}")
                return function(*args, **kwargs)
            
            start_time = time.time()
            
            # Find the primary DataFrame argument
            primary_df = self._find_primary_dataframe(args)
            if primary_df is None:
                logger.debug(f"No DataFrame found for vectorization in {function_name}")
                return function(*args, **kwargs)
            
            # Determine optimal batch size
            batch_size = self._get_optimal_batch_size(function_name, primary_df)
            
            if len(primary_df) <= batch_size:
                # Dataset is small enough to process in one batch
                logger.debug(f"Dataset size {len(primary_df)} <= batch size {batch_size}")
                return function(*args, **kwargs)
            
            # Process in batches
            logger.info(f"Processing {len(primary_df)} rows in batches of {batch_size}")
            results = []
            
            for i in range(0, len(primary_df), batch_size):
                batch_end = min(i + batch_size, len(primary_df))
                batch_df = primary_df.iloc[i:batch_end]
                
                # Replace the primary DataFrame with the batch
                batch_args = self._replace_primary_dataframe(args, batch_df)
                
                # Process the batch
                batch_result = function(*batch_args, **kwargs)
                results.append(batch_result)
            
            # Combine results
            if results:
                final_result = pd.concat(results, ignore_index=True)
            else:
                final_result = pd.DataFrame()
            
            # Record performance metrics
            processing_time = time.time() - start_time
            self._record_optimization_metric("vectorization", {
                "function_name": function_name,
                "total_rows": len(primary_df),
                "batch_size": batch_size,
                "num_batches": len(results),
                "processing_time": processing_time
            })
            
            logger.info(f"Vectorized processing completed in {processing_time:.4f}s")
            return final_result
        
        # Copy function metadata
        vectorized_wrapper.__name__ = function_name
        vectorized_wrapper.__doc__ = getattr(function, "__doc__", "")
        
        # Copy UDF metadata and mark as vectorized
        for attr in ["_udf_type", "_output_schema", "_infer_schema", "_table_dependencies"]:
            if hasattr(function, attr):
                setattr(vectorized_wrapper, attr, getattr(function, attr))
        
        # Mark as vectorized
        setattr(vectorized_wrapper, "_vectorized", True)
        
        logger.debug(f"Enabled vectorized processing for function {function_name}")
        return vectorized_wrapper

    def _check_arrow_availability(self) -> bool:
        """Check if Apache Arrow is available and functional.
        
        Returns:
            True if Arrow is available
        """
        try:
            # Test basic Arrow functionality
            test_data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
            df = pd.DataFrame(test_data)
            table = pa.Table.from_pandas(df)
            return table.num_rows == 3
        except Exception as e:
            logger.warning(f"Arrow not fully available: {e}")
            return False

    def _optimize_pandas_to_arrow(self, df: pd.DataFrame) -> pa.Table:
        """Optimize pandas DataFrame to Arrow Table conversion.
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            Optimized Arrow Table
        """
        # Use optimal conversion settings
        return pa.Table.from_pandas(
            df,
            preserve_index=False,  # Usually not needed for UDF processing
            safe=False,  # Allow unsafe conversions for performance
        )

    def _should_optimize_arg(self, arg: Any) -> bool:
        """Check if an argument should be optimized.
        
        Args:
            arg: Function argument
            
        Returns:
            True if argument should be optimized
        """
        # Optimize large DataFrames and Arrow tables
        if isinstance(arg, pd.DataFrame):
            return len(arg) > 1000  # Optimize for larger datasets
        elif isinstance(arg, pa.Table):
            return arg.num_rows > 1000
        return False

    def _optimize_argument(self, arg: Any) -> Any:
        """Optimize a function argument.
        
        Args:
            arg: Argument to optimize
            
        Returns:
            Optimized argument
        """
        if isinstance(arg, pd.DataFrame):
            # Convert to Arrow for zero-copy operations
            return self.optimize_data_exchange(arg)
        return arg

    def _should_optimize_output(self, result: Any) -> bool:
        """Check if output should be optimized.
        
        Args:
            result: Function result
            
        Returns:
            True if result should be optimized
        """
        if isinstance(result, pd.DataFrame):
            return len(result) > 1000
        elif isinstance(result, pa.Table):
            return result.num_rows > 1000
        return False

    def _optimize_output(self, result: Any) -> Any:
        """Optimize function output.
        
        Args:
            result: Function result
            
        Returns:
            Optimized result
        """
        if isinstance(result, pd.DataFrame) and self.arrow_available:
            # Convert to Arrow for downstream zero-copy operations
            return self.optimize_data_exchange(result)
        return result

    def _should_vectorize(self, args: tuple) -> bool:
        """Check if vectorized processing should be used.
        
        Args:
            args: Function arguments
            
        Returns:
            True if vectorization is beneficial
        """
        # Look for large DataFrames in arguments
        for arg in args:
            if isinstance(arg, pd.DataFrame) and len(arg) > 10000:
                return True
        return False

    def _find_primary_dataframe(self, args: tuple) -> Optional[pd.DataFrame]:
        """Find the primary DataFrame argument for vectorization.
        
        Args:
            args: Function arguments
            
        Returns:
            Primary DataFrame or None
        """
        # Return the first DataFrame found
        for arg in args:
            if isinstance(arg, pd.DataFrame):
                return arg
        return None

    def _get_optimal_batch_size(self, function_name: str, df: pd.DataFrame) -> int:
        """Get optimal batch size for a function and dataset.
        
        Args:
            function_name: Name of the function
            df: Input DataFrame
            
        Returns:
            Optimal batch size
        """
        # Check cache first
        if function_name in self.batch_size_cache:
            return self.batch_size_cache[function_name]
        
        # Calculate based on dataset characteristics
        num_rows = len(df)
        num_cols = len(df.columns)
        
        # Heuristic: balance memory usage and processing efficiency
        if num_rows > 100000:
            batch_size = 10000  # Large datasets: smaller batches
        elif num_rows > 10000:
            batch_size = 5000   # Medium datasets: medium batches
        else:
            batch_size = num_rows  # Small datasets: process all at once
        
        # Adjust for wide datasets (many columns)
        if num_cols > 50:
            batch_size = batch_size // 2
        
        # Ensure minimum batch size
        batch_size = max(batch_size, 1000)
        
        # Cache the result
        self.batch_size_cache[function_name] = batch_size
        
        logger.debug(f"Optimal batch size for {function_name}: {batch_size}")
        return batch_size

    def _replace_primary_dataframe(self, args: tuple, new_df: pd.DataFrame) -> tuple:
        """Replace the primary DataFrame in arguments with a new one.
        
        Args:
            args: Original arguments
            new_df: New DataFrame to use
            
        Returns:
            Modified arguments tuple
        """
        new_args = []
        df_replaced = False
        
        for arg in args:
            if isinstance(arg, pd.DataFrame) and not df_replaced:
                new_args.append(new_df)
                df_replaced = True
            else:
                new_args.append(arg)
        
        return tuple(new_args)

    def _record_optimization_metric(self, optimization_type: str, metrics: Dict[str, Any]) -> None:
        """Record optimization performance metrics.
        
        Args:
            optimization_type: Type of optimization
            metrics: Metrics to record
        """
        if optimization_type not in self.performance_metrics:
            self.performance_metrics[optimization_type] = []
        
        metrics["timestamp"] = time.time()
        self.performance_metrics[optimization_type].append(metrics)
        
        # Keep only recent metrics (last 100 per type)
        if len(self.performance_metrics[optimization_type]) > 100:
            self.performance_metrics[optimization_type] = \
                self.performance_metrics[optimization_type][-100:]

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics.
        
        Returns:
            Dictionary with performance metrics and statistics
        """
        metrics_summary = {}
        
        for optimization_type, metrics_list in self.performance_metrics.items():
            if metrics_list:
                # Calculate summary statistics
                times = [m.get("execution_time", m.get("conversion_time", m.get("processing_time", 0))) 
                        for m in metrics_list]
                
                metrics_summary[optimization_type] = {
                    "total_operations": len(metrics_list),
                    "avg_time": sum(times) / len(times) if times else 0,
                    "min_time": min(times) if times else 0,
                    "max_time": max(times) if times else 0,
                    "recent_operations": len([m for m in metrics_list 
                                            if time.time() - m["timestamp"] < 3600])  # Last hour
                }
        
        return {
            "arrow_available": self.arrow_available,
            "optimization_metrics": metrics_summary,
            "cached_optimizations": len(self.optimization_cache),
            "cached_batch_sizes": len(self.batch_size_cache)
        }

    def clear_caches(self) -> None:
        """Clear optimization caches."""
        self.optimization_cache.clear()
        self.batch_size_cache.clear()
        logger.debug("Cleared optimization caches")

    def get_recommended_optimizations(self, function: Callable) -> List[str]:
        """Get recommended optimizations for a UDF function.
        
        Args:
            function: UDF function to analyze
            
        Returns:
            List of recommended optimization strategies
        """
        recommendations = []
        
        # Check function metadata for optimization hints
        if getattr(function, "_udf_type", None) == "table":
            recommendations.append("serialization_optimization")
            
            # Check if function processes large datasets
            if getattr(function, "_large_dataset", False):
                recommendations.append("vectorization")
                recommendations.append("batch_processing")
            
            # Check if function is Arrow-compatible
            if getattr(function, "_arrow_compatible", False):
                recommendations.append("arrow_optimization")
        
        # Default recommendations for table UDFs
        if not recommendations and getattr(function, "_udf_type", None) == "table":
            recommendations.extend(["serialization_optimization", "arrow_optimization"])
        
        return recommendations 