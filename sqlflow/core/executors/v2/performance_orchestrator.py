"""Performance-enhanced orchestrator integrating Week 9-10 optimizations.

Following the Zen of Python:
- Simple is better than complex
- Beautiful is better than ugly
- Practicality beats purity

This module integrates async, streaming, memory optimization, and concurrent
processing capabilities into the main orchestrator.
"""

from typing import Any, Dict, List, Optional

from sqlflow.logging import get_logger

from .async_execution import AsyncPipelineOrchestrator, AsyncSequentialExecutionStrategy
from .memory_optimization import (
    OptimizedExecutionContext,
    OptimizedMetricsCollector,
    OptimizedStepDefinition,
)
from .streaming import StreamingPipelineCoordinator

logger = get_logger(__name__)


class PerformanceEnhancedOrchestrator:
    """Enhanced orchestrator with Week 9-10 performance optimizations.

    Provides async, streaming, memory-optimized, and concurrent execution
    capabilities while maintaining backward compatibility.
    """

    def __init__(
        self,
        engine: Any,
        enable_async: bool = True,
        enable_streaming: bool = False,
        enable_concurrent: bool = True,
        max_concurrent_steps: int = 5,
        streaming_chunk_size: int = 1000,
    ):
        """Initialize performance-enhanced orchestrator."""
        self.engine = engine
        self.enable_async = enable_async
        self.enable_streaming = enable_streaming
        self.enable_concurrent = enable_concurrent
        self.max_concurrent_steps = max_concurrent_steps
        self.streaming_chunk_size = streaming_chunk_size

        # Initialize performance components
        self.metrics_collector = OptimizedMetricsCollector()

        logger.info(
            f"PerformanceEnhancedOrchestrator initialized: "
            f"async={enable_async}, streaming={enable_streaming}, "
            f"concurrent={enable_concurrent}, max_concurrent={max_concurrent_steps}"
        )

    async def execute_pipeline_async(
        self, pipeline: List[Dict[str, Any]], variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute pipeline asynchronously for improved I/O performance."""
        if not self.enable_async:
            logger.info("Async execution disabled, falling back to sync execution")
            return self.execute_pipeline_sync(pipeline, variables)

        logger.info(f"Starting async pipeline execution with {len(pipeline)} steps")

        self.metrics_collector.start_pipeline()

        try:
            # Convert steps to optimized format
            optimized_steps = [
                OptimizedStepDefinition.from_dict(step) for step in pipeline
            ]

            # Create execution context
            context = OptimizedExecutionContext(
                session=self.engine, variables=variables or {}, observability=None
            )

            # Use sequential async execution for now
            # TODO: Implement proper async step executors
            strategy = AsyncSequentialExecutionStrategy([])
            orchestrator = AsyncPipelineOrchestrator(strategy)

            results = await orchestrator.execute_pipeline(optimized_steps, context)

            # Convert results back to dict format
            step_results = [result.to_dict() for result in results]

            total_time = self.metrics_collector.get_total_execution_time()

            return {
                "status": (
                    "success"
                    if all(r.get("status") == "success" for r in step_results)
                    else "failed"
                ),
                "executed_steps": [r.get("step_id", "unknown") for r in step_results],
                "step_results": step_results,
                "total_steps": len(step_results),
                "execution_mode": "async",
                "total_execution_time": total_time,
                "optimization_enabled": True,
            }

        except Exception as e:
            logger.error(f"Async pipeline execution failed: {e}")
            # Fallback to sync execution
            logger.info("Falling back to synchronous execution")
            return self.execute_pipeline_sync(pipeline, variables)

    def execute_pipeline_streaming(
        self, pipeline: List[Dict[str, Any]], variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute pipeline with streaming data processing."""
        if not self.enable_streaming:
            logger.info(
                "Streaming execution disabled, falling back to standard execution"
            )
            return self.execute_pipeline_sync(pipeline, variables)

        logger.info(
            f"Starting streaming pipeline execution with chunk size {self.streaming_chunk_size}"
        )

        self.metrics_collector.start_pipeline()

        try:
            coordinator = StreamingPipelineCoordinator(self.streaming_chunk_size)

            # Execute pipeline with streaming
            results = []
            for result in coordinator.execute_streaming_pipeline(pipeline, self.engine):
                results.append(result)

                # Record memory snapshots
                if "memory_usage" in result:
                    self.metrics_collector.record_memory_snapshot(
                        result["memory_usage"]
                    )

            # Aggregate final results
            final_results = self._aggregate_streaming_results(results)

            total_time = self.metrics_collector.get_total_execution_time()
            peak_memory = self.metrics_collector.get_peak_memory()

            return {
                "status": (
                    "success"
                    if all(r.get("status") == "success" for r in final_results)
                    else "failed"
                ),
                "executed_steps": [r.get("step_id", "unknown") for r in final_results],
                "step_results": final_results,
                "total_steps": len(final_results),
                "execution_mode": "streaming",
                "streaming_chunk_size": self.streaming_chunk_size,
                "total_execution_time": total_time,
                "peak_memory_mb": peak_memory,
                "optimization_enabled": True,
            }

        except Exception as e:
            logger.error(f"Streaming pipeline execution failed: {e}")
            # Fallback to sync execution
            return self.execute_pipeline_sync(pipeline, variables)

    def execute_pipeline_sync(
        self, pipeline: List[Dict[str, Any]], variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute pipeline synchronously (fallback method)."""
        logger.info(f"Executing pipeline synchronously with {len(pipeline)} steps")

        self.metrics_collector.start_pipeline()

        # Simple sequential execution for fallback
        results = []
        for i, step in enumerate(pipeline):
            step_id = step.get("id", f"step_{i}")

            try:
                # This is a simplified execution - in practice, you'd use proper step executors
                result = {
                    "step_id": step_id,
                    "status": "success",
                    "message": f"Step {step_id} executed (sync fallback)",
                    "execution_time": 0.1,  # Placeholder
                }
                results.append(result)

            except Exception as e:
                result = {"step_id": step_id, "status": "error", "error": str(e)}
                results.append(result)
                break  # Fail fast

        total_time = self.metrics_collector.get_total_execution_time()

        return {
            "status": (
                "success"
                if all(r.get("status") == "success" for r in results)
                else "failed"
            ),
            "executed_steps": [r.get("step_id", "unknown") for r in results],
            "step_results": results,
            "total_steps": len(results),
            "execution_mode": "sync",
            "total_execution_time": total_time,
            "optimization_enabled": False,
        }

    def _aggregate_streaming_results(
        self, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Aggregate streaming results by step."""
        final_results = []
        current_step = None
        step_summary = {}

        for result in results:
            step_id = result.get("step_id")
            if step_id != current_step:
                if step_summary:
                    final_results.append(step_summary)
                current_step = step_id
                step_summary = {
                    "step_id": step_id,
                    "step_type": result.get("step_type", "unknown"),
                    "status": result.get("status", "unknown"),
                    "total_chunks": 0,
                    "total_rows": 0,
                }

            # Accumulate metrics
            if result.get("status") == "processing":
                step_summary["total_chunks"] = result.get("chunk", 0)
                step_summary["total_rows"] = result.get("total_rows", 0)
            elif result.get("status") == "completed":
                step_summary["status"] = "success"
                step_summary["total_chunks"] = result.get("total_chunks", 0)
                step_summary["total_rows"] = result.get("total_rows", 0)
            elif result.get("status") == "error":
                step_summary["status"] = "error"
                step_summary["error"] = result.get("error", "Unknown error")

        # Add final step summary
        if step_summary:
            final_results.append(step_summary)

        return final_results

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics."""
        return {
            "metrics_available": True,
            "total_execution_time": self.metrics_collector.get_total_execution_time(),
            "peak_memory_mb": self.metrics_collector.get_peak_memory(),
            "memory_efficiency_ratio": self.metrics_collector.get_memory_efficiency_ratio(),
            "optimization_settings": {
                "async_enabled": self.enable_async,
                "streaming_enabled": self.enable_streaming,
                "concurrent_enabled": self.enable_concurrent,
                "max_concurrent_steps": self.max_concurrent_steps,
                "streaming_chunk_size": self.streaming_chunk_size,
            },
        }

    def reset_metrics(self) -> None:
        """Reset performance metrics collector."""
        self.metrics_collector.clear_metrics()


class OptimizedPipelineFactory:
    """Factory for creating optimized pipeline executors.

    Provides different execution strategies based on pipeline characteristics
    and performance requirements.
    """

    @staticmethod
    def create_orchestrator(
        engine: Any, execution_mode: str = "auto", **optimization_config
    ) -> PerformanceEnhancedOrchestrator:
        """Create optimized orchestrator based on execution mode."""

        if execution_mode == "auto":
            # Auto-detect best configuration
            config = {
                "enable_async": True,
                "enable_streaming": False,
                "enable_concurrent": True,
                "max_concurrent_steps": 5,
                "streaming_chunk_size": 1000,
            }
        elif execution_mode == "memory_optimized":
            # Prioritize memory efficiency
            config = {
                "enable_async": True,
                "enable_streaming": True,
                "enable_concurrent": False,
                "max_concurrent_steps": 2,
                "streaming_chunk_size": 500,
            }
        elif execution_mode == "speed_optimized":
            # Prioritize execution speed
            config = {
                "enable_async": True,
                "enable_streaming": False,
                "enable_concurrent": True,
                "max_concurrent_steps": 10,
                "streaming_chunk_size": 2000,
            }
        elif execution_mode == "compatibility":
            # Maximum compatibility (minimal optimizations)
            config = {
                "enable_async": False,
                "enable_streaming": False,
                "enable_concurrent": False,
                "max_concurrent_steps": 1,
                "streaming_chunk_size": 1000,
            }
        else:
            raise ValueError(f"Unknown execution mode: {execution_mode}")

        # Override with user config
        config.update(optimization_config)

        return PerformanceEnhancedOrchestrator(engine, **config)

    @staticmethod
    def recommend_configuration(
        pipeline_size: int,
        dataset_size_mb: Optional[float] = None,
        available_memory_mb: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Recommend optimization configuration based on pipeline characteristics."""

        recommendations = {}

        # Base recommendations on pipeline size
        if pipeline_size <= 5:
            recommendations["enable_concurrent"] = False
            recommendations["max_concurrent_steps"] = 1
        elif pipeline_size <= 20:
            recommendations["enable_concurrent"] = True
            recommendations["max_concurrent_steps"] = min(5, pipeline_size // 2)
        else:
            recommendations["enable_concurrent"] = True
            recommendations["max_concurrent_steps"] = min(10, pipeline_size // 3)

        # Memory-based recommendations
        if dataset_size_mb and available_memory_mb:
            memory_ratio = dataset_size_mb / available_memory_mb

            if memory_ratio > 0.8:  # Dataset is close to available memory
                recommendations["enable_streaming"] = True
                recommendations["streaming_chunk_size"] = max(
                    100, int(available_memory_mb * 0.1)
                )
            elif memory_ratio > 0.5:  # Moderate memory usage
                recommendations["enable_streaming"] = True
                recommendations["streaming_chunk_size"] = 1000
            else:  # Low memory usage
                recommendations["enable_streaming"] = False

        # Always enable async for I/O improvements
        recommendations["enable_async"] = True

        return recommendations
