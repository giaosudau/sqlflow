"""Tests for StepHandlerFactory handler registration and creation behavior.

This test suite validates the factory pattern implementation for step handlers,
ensuring proper registration, creation, and error handling of step handlers.
"""

from datetime import datetime

import pytest

from sqlflow.core.executors.v2.handlers.base import StepHandler
from sqlflow.core.executors.v2.handlers.factory import StepHandlerFactory
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import BaseStep


class MockStepHandler(StepHandler):
    """Mock step handler for testing."""

    STEP_TYPE = "mock"

    def execute(self, step, context):
        """Simple mock execution."""
        return StepExecutionResult.success(
            step_id=step.id, step_type=step.type, start_time=datetime.utcnow()
        )


class MockStep(BaseStep):
    """Mock step for testing."""

    def __init__(self, step_id="test_step", step_type="mock"):
        super().__init__(
            id=step_id,
            type=step_type,
            criticality="normal",
            expected_duration_ms=1000.0,
        )


class TestStepHandlerFactory:
    """Test suite for StepHandlerFactory behavior."""

    def test_handler_registration(self):
        """Test that handlers can be registered and retrieved."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        # Register a handler
        StepHandlerFactory.register_handler("mock", MockStepHandler)

        # Test registration
        assert StepHandlerFactory.is_step_type_supported("mock")

        # Test handler creation
        handler = StepHandlerFactory.create_handler("mock")
        assert isinstance(handler, MockStepHandler)

    def test_handler_creation_with_invalid_type(self):
        """Test error handling for invalid step types."""
        with pytest.raises(ValueError, match="Unsupported step type"):
            StepHandlerFactory.create_handler("invalid_type")

    def test_duplicate_handler_registration(self):
        """Test that duplicate registrations are handled appropriately."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        # Register handler twice
        StepHandlerFactory.register_handler("duplicate", MockStepHandler)
        StepHandlerFactory.register_handler("duplicate", MockStepHandler)

        # Should still work
        assert StepHandlerFactory.is_step_type_supported("duplicate")

    def test_handler_creation_returns_cached_instances(self):
        """Test that factory returns cached instances for performance (stateless handlers)."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        StepHandlerFactory.register_handler("instance_test", MockStepHandler)

        handler1 = StepHandlerFactory.create_handler("instance_test")
        handler2 = StepHandlerFactory.create_handler("instance_test")

        # Handlers are stateless, so caching is safe and improves performance
        assert handler1 is handler2
        assert isinstance(handler1, MockStepHandler)
        assert isinstance(handler2, MockStepHandler)

    def test_get_handler_info(self):
        """Test handler information retrieval."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        StepHandlerFactory.register_handler("mock", MockStepHandler)

        # Get all handler info
        all_info = StepHandlerFactory.get_handler_info()
        assert "mock" in all_info
        assert all_info["mock"] == "MockStepHandler"

    def test_handler_class_validation(self):
        """Test that only valid handler classes can be registered."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        # Test invalid handler class
        class InvalidHandler:
            pass

        with pytest.raises(TypeError, match="must be a subclass of StepHandler"):
            StepHandlerFactory.register_handler("invalid", InvalidHandler)  # type: ignore

    def test_step_type_must_be_string(self):
        """Test that step type must be a string."""
        with pytest.raises(TypeError, match="Step type must be a string"):
            StepHandlerFactory.register_handler(123, MockStepHandler)  # type: ignore

    def test_handler_execution_through_factory(self):
        """Test that handlers created by factory can execute steps."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        StepHandlerFactory.register_handler("executable", MockStepHandler)

        handler = StepHandlerFactory.create_handler("executable")
        step = MockStep()
        context = None  # MockStepHandler doesn't use context  # type: ignore

        result = handler.execute(step, context)  # type: ignore

        assert result.is_successful()
        assert result.step_id == "test_step"

    def test_available_step_types(self):
        """Test getting list of available step types."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        StepHandlerFactory.register_handler("mock1", MockStepHandler)
        StepHandlerFactory.register_handler("mock2", MockStepHandler)

        available_types = StepHandlerFactory.get_available_step_types()

        assert "mock1" in available_types
        assert "mock2" in available_types

    def test_handler_registration_preserves_step_type(self):
        """Test that registered handlers maintain their step type."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        StepHandlerFactory.register_handler("preserve_test", MockStepHandler)

        handler = StepHandlerFactory.create_handler("preserve_test")
        assert hasattr(handler, "STEP_TYPE") and handler.STEP_TYPE == "mock"  # type: ignore

    def test_step_type_support_check(self):
        """Test checking if step types are supported."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        StepHandlerFactory.register_handler("supported", MockStepHandler)

        assert StepHandlerFactory.is_step_type_supported("supported")
        assert not StepHandlerFactory.is_step_type_supported("unsupported")

    def test_factory_state_isolation(self):
        """Test that factory state is properly isolated between tests."""
        # Clear any existing registrations
        StepHandlerFactory.clear_registry()

        # Register handlers
        StepHandlerFactory.register_handler("isolated1", MockStepHandler)

        # Check state
        assert len(StepHandlerFactory.get_available_step_types()) == 1

        # Clear and verify
        StepHandlerFactory.clear_registry()
        assert len(StepHandlerFactory.get_available_step_types()) == 0
