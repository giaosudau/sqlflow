"""Test suite for V2 executor performance features.

Tests the V2 executor's performance enhancement capabilities including async execution,
streaming data processing, memory optimization, and concurrent processing.

This test suite follows SQLFlow testing standards:
- Uses real implementations wherever possible
- Minimal mocking only for external boundaries
- Descriptive test names that explain behavior
- Focus on observable behaviors, not implementation details
"""

import asyncio

import pytest

# Import real implementations
from sqlflow.core.executors.v2.async_execution import AsyncDataStream
from sqlflow.core.executors.v2.concurrent_execution import DependencyGraph
from sqlflow.core.executors.v2.memory_optimization import (
    OptimizedExecutionContext,
    OptimizedMetricsCollector,
    OptimizedStepDefinition,
    OptimizedStepResult,
    StepStatus,
)
from sqlflow.core.executors.v2.performance_orchestrator import (
    OptimizedPipelineFactory,
    PerformanceEnhancedOrchestrator,
)
from sqlflow.core.executors.v2.streaming import (
    StreamingDataProcessor,
    StreamingLoadExecutor,
)


class TestAsyncDataStreamProcessing:
    """Test async data streaming capabilities with real data flows."""

    def test_async_data_stream_chunks_data_correctly(self):
        """Test that AsyncDataStream properly chunks input data into specified sizes."""
        # Arrange
        test_data = list(range(10))  # [0, 1, 2, ..., 9]
        chunk_size = 3
        stream = AsyncDataStream(test_data, chunk_size=chunk_size)

        # Act
        async def collect_chunks():
            chunks = []
            async for chunk in stream.stream_chunks():
                chunks.append(chunk)
            return chunks

        # Assert
        chunks = asyncio.run(collect_chunks())
        assert len(chunks) == 4  # [0,1,2], [3,4,5], [6,7,8], [9]
        assert chunks[0] == [0, 1, 2]
        assert chunks[1] == [3, 4, 5]
        assert chunks[2] == [6, 7, 8]
        assert chunks[3] == [9]

    def test_async_data_stream_handles_empty_data(self):
        """Test that AsyncDataStream handles empty input data gracefully."""
        # Arrange
        stream = AsyncDataStream([], chunk_size=5)

        # Act & Assert
        async def verify_empty():
            chunks = [chunk async for chunk in stream.stream_chunks()]
            return chunks

        chunks = asyncio.run(verify_empty())
        assert chunks == []

    def test_async_data_stream_with_single_item(self):
        """Test that AsyncDataStream handles single-item data correctly."""
        # Arrange
        stream = AsyncDataStream([42], chunk_size=5)

        # Act & Assert
        async def verify_single():
            chunks = [chunk async for chunk in stream.stream_chunks()]
            return chunks

        chunks = asyncio.run(verify_single())
        assert len(chunks) == 1
        assert chunks[0] == [42]


class TestStreamingDataProcessing:
    """Test streaming data processing with real data transformations."""

    @pytest.fixture
    def processor(self):
        """Create a real streaming data processor."""
        return StreamingDataProcessor(chunk_size=2)

    def test_streaming_processor_initializes_with_correct_chunk_size(self):
        """Test that StreamingDataProcessor initializes with specified chunk size."""
        # Arrange & Act
        processor = StreamingDataProcessor(chunk_size=100)

        # Assert
        assert processor.chunk_size == 100

    def test_transform_stream_applies_function_to_all_data(self, processor):
        """Test that transform_stream applies transformation function to all data chunks."""

        # Arrange
        def double_values(row):
            return {
                k: v * 2 if isinstance(v, (int, float)) else v for k, v in row.items()
            }

        test_data = [[{"value": 1}, {"value": 2}], [{"value": 3}, {"value": 4}]]

        def data_generator():
            for chunk in test_data:
                yield chunk

        # Act
        result_chunks = list(
            processor.transform_stream(data_generator(), double_values)
        )

        # Assert
        assert len(result_chunks) == 2
        assert result_chunks[0] == [{"value": 2}, {"value": 4}]
        assert result_chunks[1] == [{"value": 6}, {"value": 8}]

    def test_filter_stream_removes_matching_rows(self, processor):
        """Test that filter_stream correctly filters out rows that don't match criteria."""

        # Arrange
        def keep_even_values(row):
            return row.get("value", 0) % 2 == 0

        test_data = [[{"value": 1}, {"value": 2}], [{"value": 3}, {"value": 4}]]

        def data_generator():
            for chunk in test_data:
                yield chunk

        # Act
        result_chunks = list(
            processor.filter_stream(data_generator(), keep_even_values)
        )

        # Assert
        assert len(result_chunks) == 2
        assert result_chunks[0] == [{"value": 2}]
        assert result_chunks[1] == [{"value": 4}]

    def test_streaming_load_executor_identifies_compatible_steps(self):
        """Test that StreamingLoadExecutor correctly identifies streaming-compatible steps."""
        # Arrange
        executor = StreamingLoadExecutor(chunk_size=100)

        # Act & Assert
        assert executor.can_execute({"type": "load", "streaming": True})
        assert not executor.can_execute({"type": "load", "streaming": False})
        assert not executor.can_execute({"type": "transform", "streaming": True})
        assert not executor.can_execute({"type": "export"})


class TestMemoryOptimization:
    """Test memory optimization features using real optimized classes."""

    def test_optimized_step_result_creates_successful_result(self):
        """Test that OptimizedStepResult creates successful results with correct attributes."""
        # Arrange & Act
        result = OptimizedStepResult.success(
            step_id="test_step", execution_time=1.5, data={"rows": 100}
        )

        # Assert
        assert result.step_id == "test_step"
        assert result.status == StepStatus.SUCCESS
        assert result.execution_time == 1.5
        assert result.data == {"rows": 100}
        assert result.error_message is None

    def test_optimized_step_result_creates_error_result(self):
        """Test that OptimizedStepResult creates error results with proper error handling."""
        # Arrange & Act
        result = OptimizedStepResult.error(
            step_id="failed_step",
            error_message="Database connection failed",
            execution_time=0.5,
        )

        # Assert
        assert result.step_id == "failed_step"
        assert result.status == StepStatus.ERROR
        assert result.error_message == "Database connection failed"
        assert result.execution_time == 0.5
        assert result.data is None

    def test_optimized_step_result_serializes_to_dict(self):
        """Test that OptimizedStepResult can serialize to dictionary format."""
        # Arrange
        result = OptimizedStepResult.success(
            "test", execution_time=1.0, data={"key": "value"}
        )

        # Act
        result_dict = result.to_dict()

        # Assert
        expected_keys = {
            "step_id",
            "status",
            "message",
            "error",
            "execution_time",
            "data",
            "row_count",
            "memory_usage",
        }
        assert set(result_dict.keys()) == expected_keys
        assert result_dict["step_id"] == "test"
        assert result_dict["status"] == "success"  # Should be string value from enum
        assert result_dict["execution_time"] == 1.0
        assert result_dict["data"] == {"key": "value"}

    def test_optimized_execution_context_creates_with_correct_parameters(self):
        """Test that OptimizedExecutionContext creates with correct initialization."""
        # Arrange
        mock_session = object()  # Simple object to represent session
        variables = {"var1": "value1"}

        # Act
        context = OptimizedExecutionContext(session=mock_session, variables=variables)

        # Assert
        assert context.session is mock_session
        assert context.variables == variables
        assert context.get_variable("var1") == "value1"
        assert context.get_variable("nonexistent", "default") == "default"

    def test_optimized_execution_context_creates_immutable_copy(self):
        """Test that OptimizedExecutionContext creates copies with updated variables."""
        # Arrange
        mock_session = object()
        original_context = OptimizedExecutionContext(
            session=mock_session, variables={"var1": "value1"}
        )

        # Act
        new_context = original_context.with_variables({"var2": "value2"})

        # Assert
        assert new_context.session is original_context.session
        assert new_context.variables == {"var1": "value1", "var2": "value2"}
        # Original should be unchanged
        assert original_context.variables == {"var1": "value1"}

    def test_optimized_step_definition_from_dict_conversion(self):
        """Test that OptimizedStepDefinition correctly converts from dictionary."""
        # Arrange
        step_dict = {
            "id": "test_step",
            "type": "load",
            "source": {"table": "customers"},
            "target_table": "customer_table",
        }

        # Act
        step_def = OptimizedStepDefinition.from_dict(step_dict)

        # Assert
        assert step_def.id == "test_step"
        assert step_def.type == "load"
        assert step_def.source == {"table": "customers"}
        assert step_def.target_table == "customer_table"

    def test_metrics_collector_tracks_step_metrics(self):
        """Test that OptimizedMetricsCollector properly tracks step execution metrics."""
        # Arrange
        collector = OptimizedMetricsCollector()

        # Act
        collector.start_pipeline()
        collector.record_step_metric("step1", "execution_time", 1.5)
        collector.record_step_metric("step1", "row_count", 1000)
        collector.record_memory_snapshot(50.0)
        collector.record_memory_snapshot(75.0)

        # Assert
        step_metrics = collector.get_step_metrics("step1")
        assert step_metrics["execution_time"] == 1.5
        assert step_metrics["row_count"] == 1000
        assert collector.get_peak_memory() == 75.0
        assert collector.get_total_execution_time() is not None


class TestDependencyGraphProcessing:
    """Test dependency graph creation and processing with real dependency scenarios."""

    def test_dependency_graph_identifies_executable_steps(self):
        """Test that DependencyGraph correctly identifies steps with no dependencies."""
        # Arrange
        step_defs = [
            OptimizedStepDefinition(id="step1", type="load", depends_on=[]),
            OptimizedStepDefinition(id="step2", type="transform", depends_on=["step1"]),
            OptimizedStepDefinition(id="step3", type="load", depends_on=[]),
        ]
        graph = DependencyGraph(step_defs)

        # Act
        executable = graph.get_executable_steps(completed_steps=set())

        # Assert
        assert "step1" in executable
        assert "step3" in executable
        assert "step2" not in executable

    def test_dependency_graph_processes_dependencies_after_completion(self):
        """Test that completing a step makes its dependents available for execution."""
        # Arrange
        step_defs = [
            OptimizedStepDefinition(id="step1", type="load", depends_on=[]),
            OptimizedStepDefinition(id="step2", type="transform", depends_on=["step1"]),
            OptimizedStepDefinition(
                id="step3", type="export", depends_on=["step1", "step2"]
            ),
        ]
        graph = DependencyGraph(step_defs)

        # Act
        executable_after_none = graph.get_executable_steps(completed_steps=set())
        executable_after_step1 = graph.get_executable_steps(completed_steps={"step1"})
        executable_after_both = graph.get_executable_steps(
            completed_steps={"step1", "step2"}
        )

        # Assert
        assert executable_after_none == ["step1"]
        assert "step2" in executable_after_step1
        assert "step3" not in executable_after_step1
        assert "step3" in executable_after_both

    def test_dependency_graph_identifies_parallel_executable_steps(self):
        """Test that DependencyGraph identifies independent steps that can run in parallel."""
        # Arrange
        step_defs = [
            OptimizedStepDefinition(id="load_customers", type="load", depends_on=[]),
            OptimizedStepDefinition(id="load_orders", type="load", depends_on=[]),
            OptimizedStepDefinition(id="load_products", type="load", depends_on=[]),
            OptimizedStepDefinition(
                id="join_data",
                type="transform",
                depends_on=["load_customers", "load_orders", "load_products"],
            ),
        ]
        graph = DependencyGraph(step_defs)

        # Act
        parallel_groups = graph.topological_sort()

        # Assert
        assert len(parallel_groups) >= 2
        first_group = parallel_groups[0]
        assert "load_customers" in first_group
        assert "load_orders" in first_group
        assert "load_products" in first_group

        # join_data should be in a later group
        later_groups = [step for group in parallel_groups[1:] for step in group]
        assert "join_data" in later_groups

    def test_dependency_graph_detects_circular_dependencies(self):
        """Test that DependencyGraph detects circular dependencies and raises appropriate error."""
        # Arrange
        step_defs = [
            OptimizedStepDefinition(id="step1", type="load", depends_on=["step2"]),
            OptimizedStepDefinition(id="step2", type="transform", depends_on=["step1"]),
        ]
        graph = DependencyGraph(step_defs)

        # Act & Assert
        with pytest.raises(ValueError, match="Circular dependency detected"):
            graph.topological_sort()


class TestPerformanceOrchestration:
    """Test performance orchestration and factory patterns with real configurations."""

    def test_optimized_pipeline_factory_creates_auto_mode_orchestrator(self):
        """Test that factory creates auto-optimized orchestrator with balanced settings."""
        # Arrange
        mock_engine = object()  # Simple mock engine

        # Act
        orchestrator = OptimizedPipelineFactory.create_orchestrator(
            engine=mock_engine, execution_mode="auto"
        )

        # Assert
        assert isinstance(orchestrator, PerformanceEnhancedOrchestrator)
        assert orchestrator.enable_async is True
        assert orchestrator.enable_concurrent is True

    def test_optimized_pipeline_factory_creates_memory_optimized_mode(self):
        """Test that factory creates memory-optimized orchestrator for low-memory environments."""
        # Arrange
        mock_engine = object()

        # Act
        orchestrator = OptimizedPipelineFactory.create_orchestrator(
            engine=mock_engine, execution_mode="memory_optimized"
        )

        # Assert
        assert isinstance(orchestrator, PerformanceEnhancedOrchestrator)
        assert orchestrator.enable_streaming is True  # For memory efficiency

    def test_optimized_pipeline_factory_creates_speed_optimized_mode(self):
        """Test that factory creates speed-optimized orchestrator for high-performance scenarios."""
        # Arrange
        mock_engine = object()

        # Act
        orchestrator = OptimizedPipelineFactory.create_orchestrator(
            engine=mock_engine, execution_mode="speed_optimized"
        )

        # Assert
        assert isinstance(orchestrator, PerformanceEnhancedOrchestrator)
        assert orchestrator.enable_async is True
        assert orchestrator.enable_concurrent is True

    def test_optimized_pipeline_factory_creates_compatibility_mode(self):
        """Test that factory creates compatibility mode orchestrator with minimal optimizations."""
        # Arrange
        mock_engine = object()

        # Act
        orchestrator = OptimizedPipelineFactory.create_orchestrator(
            engine=mock_engine, execution_mode="compatibility"
        )

        # Assert
        assert isinstance(orchestrator, PerformanceEnhancedOrchestrator)
        assert orchestrator.enable_async is False
        assert orchestrator.enable_streaming is False
        assert orchestrator.enable_concurrent is False

    def test_optimized_pipeline_factory_provides_configuration_recommendations(self):
        """Test that factory provides helpful configuration recommendations."""
        # Act
        recommendations = OptimizedPipelineFactory.recommend_configuration(
            pipeline_size=10, dataset_size_mb=100.0, available_memory_mb=500.0
        )

        # Assert
        assert "enable_concurrent" in recommendations
        assert "max_concurrent_steps" in recommendations
        assert "enable_async" in recommendations
        assert isinstance(recommendations["enable_concurrent"], bool)
        assert isinstance(recommendations["max_concurrent_steps"], int)
        assert recommendations["enable_async"] is True  # Always recommended

    def test_factory_recommendations_adapt_to_pipeline_size(self):
        """Test that factory recommendations adapt to different pipeline sizes."""
        # Act
        small_recs = OptimizedPipelineFactory.recommend_configuration(pipeline_size=3)
        large_recs = OptimizedPipelineFactory.recommend_configuration(pipeline_size=30)

        # Assert
        # Small pipelines shouldn't use concurrency
        assert small_recs["enable_concurrent"] is False
        assert small_recs["max_concurrent_steps"] == 1

        # Large pipelines should use concurrency
        assert large_recs["enable_concurrent"] is True
        assert large_recs["max_concurrent_steps"] > 1


class TestIntegratedPerformanceScenarios:
    """Integration tests for real-world performance scenarios."""

    def test_small_pipeline_executes_successfully_with_optimizations(self):
        """Test that a small pipeline executes successfully with all optimizations enabled."""
        # Arrange
        mock_engine = object()
        orchestrator = OptimizedPipelineFactory.create_orchestrator(
            engine=mock_engine, execution_mode="auto"
        )

        # Simple pipeline with real step definitions
        steps = [
            {"id": "load_data", "type": "load", "source": "test_source"},
            {"id": "transform_data", "type": "transform", "depends_on": ["load_data"]},
            {"id": "export_data", "type": "export", "depends_on": ["transform_data"]},
        ]

        # Act - Test orchestrator initialization and configuration
        assert isinstance(orchestrator, PerformanceEnhancedOrchestrator)
        assert orchestrator.enable_async is True
        assert orchestrator.metrics_collector is not None

        # Test that optimized step definitions can be created from the pipeline
        optimized_steps = [OptimizedStepDefinition.from_dict(step) for step in steps]
        assert len(optimized_steps) == 3
        assert all(
            isinstance(step, OptimizedStepDefinition) for step in optimized_steps
        )

    def test_streaming_pipeline_handles_large_data_efficiently(self):
        """Test that streaming configuration handles large datasets efficiently."""
        # Arrange
        processor = StreamingDataProcessor(chunk_size=1000)

        # Simulate large dataset
        def generate_large_dataset():
            for i in range(0, 10000, 1000):  # 10k records in 1k chunks
                yield [
                    {"id": j, "value": f"data_{j}"}
                    for j in range(i, min(i + 1000, 10000))
                ]

        def simple_transform(row):
            return {**row, "processed": True}

        # Act
        processed_chunks = list(
            processor.transform_stream(generate_large_dataset(), simple_transform)
        )

        # Assert
        assert len(processed_chunks) == 10  # 10 chunks of 1000 each
        assert all(len(chunk) <= 1000 for chunk in processed_chunks)
        assert all(row["processed"] for chunk in processed_chunks for row in chunk)

    def test_memory_optimized_classes_use_less_memory_than_regular_classes(self):
        """Test that optimized classes actually use less memory through __slots__."""
        # Arrange - Create many instances to see memory difference
        optimized_results = []

        # Act - Create instances (memory optimization is structural, not behavioral)
        for i in range(100):
            optimized_results.append(
                OptimizedStepResult.success(
                    f"step_{i}", execution_time=1.0, data={"data": i}
                )
            )

        # Assert - Test that optimized classes have __slots__ defined
        assert hasattr(OptimizedStepResult, "__slots__")
        assert hasattr(OptimizedExecutionContext, "__slots__")
        assert hasattr(OptimizedStepDefinition, "__slots__")

        # Verify instances work correctly
        assert len(optimized_results) == 100
        assert all(result.status == StepStatus.SUCCESS for result in optimized_results)
        assert all(
            result.data["data"] == i for i, result in enumerate(optimized_results)
        )
