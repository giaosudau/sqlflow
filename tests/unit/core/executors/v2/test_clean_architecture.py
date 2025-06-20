"""Test Clean Architecture with Dependency Injection.

This module tests the new PipelineExecutor class that demonstrates
clean architecture principles with proper dependency injection.
"""

from unittest.mock import Mock, patch

import pytest

from sqlflow.core.executors.v2.orchestrator import (
    EngineFactory,
    PipelineExecutor,
    ProfileManager,
    UDFRegistry,
)


class TestCleanArchitecture:
    """Test the clean architecture implementation."""

    def test_engine_factory_creates_duckdb_engine(self):
        """Test that EngineFactory creates DuckDB engines correctly."""
        with patch("sqlflow.core.engines.duckdb.DuckDBEngine") as mock_engine:
            engine = EngineFactory.create_engine("duckdb", memory_mode=True)
            mock_engine.assert_called_once_with(memory_mode=True)

    def test_engine_factory_raises_for_invalid_engine(self):
        """Test that EngineFactory raises error for invalid engine types."""
        with pytest.raises(ValueError, match="Unsupported engine type: invalid"):
            EngineFactory.create_engine("invalid")

    def test_profile_manager_stores_and_retrieves_profiles(self):
        """Test ProfileManager profile management."""
        manager = ProfileManager()

        # Initially empty
        assert manager.load_profile("test") == {}

        # Set and retrieve profile
        test_config = {"database": "test.db", "mode": "memory"}
        manager.set_profile("test", test_config)

        assert manager.load_profile("test") == test_config

    def test_udf_registry_manages_udfs(self):
        """Test UDFRegistry UDF management."""
        registry = UDFRegistry()

        # Initially empty
        assert registry.list_udfs() == []
        assert registry.get_udf("test_func") is None

        # Register UDF
        def test_func(x):
            return x * 2

        registry.register_udf("test_func", test_func)

        assert "test_func" in registry.list_udfs()
        assert registry.get_udf("test_func") == test_func

    def test_pipeline_executor_dependency_injection(self):
        """Test PipelineExecutor with dependency injection."""
        # Create dependencies
        engine_factory = EngineFactory()
        profile_manager = ProfileManager()
        udf_registry = UDFRegistry()

        # Create executor with injected dependencies
        executor = PipelineExecutor(
            engine_factory=engine_factory,
            profile_manager=profile_manager,
            udf_registry=udf_registry,
            enable_optimizations=True,
        )

        # Verify dependencies are injected
        assert executor.engine_factory is engine_factory
        assert executor.profile_manager is profile_manager
        assert executor.udf_registry is udf_registry
        assert executor.enable_optimizations is True

    @patch("sqlflow.core.engines.duckdb.DuckDBEngine")
    def test_pipeline_executor_configures_engine(self, mock_engine_class):
        """Test that PipelineExecutor configures engines correctly."""
        mock_engine = Mock()
        mock_engine_class.return_value = mock_engine

        # Create executor
        engine_factory = EngineFactory()
        profile_manager = ProfileManager()
        udf_registry = UDFRegistry()

        executor = PipelineExecutor(
            engine_factory=engine_factory,
            profile_manager=profile_manager,
            udf_registry=udf_registry,
        )

        # Configure engine
        executor.configure_engine("duckdb", memory_mode=True)

        # Verify engine creation
        mock_engine_class.assert_called_once_with(memory_mode=True)
        assert executor._engine is mock_engine

    @patch("sqlflow.core.engines.duckdb.DuckDBEngine")
    def test_pipeline_executor_registers_udfs(self, mock_engine_class):
        """Test that PipelineExecutor registers UDFs with the engine."""
        mock_engine = Mock()
        mock_engine.register_udf = Mock()
        mock_engine_class.return_value = mock_engine

        # Create executor with UDF
        engine_factory = EngineFactory()
        profile_manager = ProfileManager()
        udf_registry = UDFRegistry()

        def test_udf(x):
            return x + 1

        udf_registry.register_udf("test_udf", test_udf)

        executor = PipelineExecutor(
            engine_factory=engine_factory,
            profile_manager=profile_manager,
            udf_registry=udf_registry,
        )

        # Configure engine (should register UDFs)
        executor.configure_engine("duckdb")

        # Verify UDF registration
        mock_engine.register_udf.assert_called_once_with("test_udf", test_udf)

    def test_pipeline_executor_loads_profile(self):
        """Test that PipelineExecutor loads profiles correctly."""
        engine_factory = EngineFactory()
        profile_manager = ProfileManager()
        udf_registry = UDFRegistry()

        # Set up profile
        profile_config = {"engine": {"engine_type": "duckdb", "memory_mode": True}}
        profile_manager.set_profile("test_profile", profile_config)

        executor = PipelineExecutor(
            engine_factory=engine_factory,
            profile_manager=profile_manager,
            udf_registry=udf_registry,
        )

        with patch.object(executor, "configure_engine") as mock_configure:
            executor.load_profile("test_profile")

            # Verify profile loading and engine configuration
            assert executor._current_profile == profile_config
            mock_configure.assert_called_once_with(
                engine_type="duckdb", memory_mode=True
            )

    @patch("sqlflow.core.executors.v2.orchestrator.LocalOrchestrator")
    def test_pipeline_executor_executes_pipeline(self, mock_orchestrator_class):
        """Test that PipelineExecutor executes pipelines correctly."""
        # Mock the legacy orchestrator
        mock_orchestrator = Mock()
        mock_orchestrator._execute_load.return_value = {
            "step_id": "test_load",
            "status": "success",
        }
        mock_orchestrator_class.return_value = mock_orchestrator

        # Create executor
        engine_factory = EngineFactory()
        profile_manager = ProfileManager()
        udf_registry = UDFRegistry()

        executor = PipelineExecutor(
            engine_factory=engine_factory,
            profile_manager=profile_manager,
            udf_registry=udf_registry,
        )

        # Mock engine configuration
        with patch.object(executor, "configure_engine"):
            # Execute pipeline
            pipeline = [
                {
                    "type": "load",
                    "id": "test_load",
                    "source": "test.csv",
                    "target_table": "test_table",
                }
            ]

            result = executor.execute_pipeline(pipeline)

            # Verify execution
            assert result["status"] == "success"
            assert result["executed_steps"] == ["test_load"]
            assert result["total_steps"] == 1
            assert result["optimization_enabled"] is True

            # Verify legacy orchestrator was used
            mock_orchestrator._execute_load.assert_called_once()

    def test_clean_architecture_principles_demonstrated(self):
        """Test that the implementation demonstrates clean architecture principles."""
        # Dependency Injection: Dependencies are injected via constructor
        engine_factory = EngineFactory()
        profile_manager = ProfileManager()
        udf_registry = UDFRegistry()

        executor = PipelineExecutor(
            engine_factory=engine_factory,
            profile_manager=profile_manager,
            udf_registry=udf_registry,
        )

        # Single Responsibility: Each component has one responsibility
        assert hasattr(engine_factory, "create_engine")  # Creates engines
        assert hasattr(profile_manager, "load_profile")  # Manages profiles
        assert hasattr(udf_registry, "register_udf")  # Manages UDFs
        assert hasattr(executor, "execute_pipeline")  # Executes pipelines

        # Interface Segregation: Clean, focused interfaces
        # Each class has a specific, well-defined interface

        # Inversion of Control: High-level modules don't depend on low-level modules
        # PipelineExecutor depends on abstractions (injected dependencies)
        assert executor.engine_factory is not None
        assert executor.profile_manager is not None
        assert executor.udf_registry is not None
