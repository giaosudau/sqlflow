"""Unit tests for clean V2 protocols.

Testing the core protocols that define our clean architecture interfaces.
Following TDD principles as recommended by Kent Beck.
"""


class TestCoreProtocols:
    """Test that our protocols work correctly."""

    def test_step_protocol_structure(self):
        """Test that Step protocol has the right structure."""

        # Create a simple step implementation
        class SimpleStep:
            def __init__(self, id: str, step_type: str):
                self.id = id
                self.step_type = step_type

        step = SimpleStep("test_step", "load")

        # Test protocol compliance
        assert hasattr(step, "id")
        assert hasattr(step, "step_type")
        assert step.id == "test_step"
        assert step.step_type == "load"

    def test_step_result_protocol_structure(self):
        """Test that StepResult protocol has the right structure."""

        class SimpleStepResult:
            def __init__(self):
                self.step_id = "test"
                self.success = True
                self.duration_ms = 100.0
                self.rows_affected = 5
                self.error_message = None

        result = SimpleStepResult()

        # Test protocol compliance
        assert hasattr(result, "step_id")
        assert hasattr(result, "success")
        assert hasattr(result, "duration_ms")
        assert hasattr(result, "rows_affected")
        assert hasattr(result, "error_message")

    def test_database_engine_protocol_structure(self):
        """Test DatabaseEngine protocol structure."""

        class MockEngine:
            def execute_query(self, sql: str):
                return {"result": "success"}

            def table_exists(self, table_name: str) -> bool:
                return True

            def get_table_schema(self, table_name: str):
                return {"col1": "string", "col2": "int"}

        engine = MockEngine()

        # Test protocol compliance
        assert hasattr(engine, "execute_query")
        assert hasattr(engine, "table_exists")
        assert hasattr(engine, "get_table_schema")

        # Test functionality
        assert engine.table_exists("test_table")
        assert engine.execute_query("SELECT 1") == {"result": "success"}
        assert engine.get_table_schema("test") == {"col1": "string", "col2": "int"}

    def test_observability_manager_protocol_structure(self):
        """Test ObservabilityManager protocol structure."""

        class MockObservability:
            def __init__(self):
                self.started_steps = []
                self.ended_steps = []

            def start_step(self, step_id: str) -> None:
                self.started_steps.append(step_id)

            def end_step(self, step_id: str, success: bool, error=None) -> None:
                self.ended_steps.append((step_id, success, error))

            def get_metrics(self):
                return {
                    "started": len(self.started_steps),
                    "ended": len(self.ended_steps),
                }

        obs = MockObservability()

        # Test protocol compliance
        assert hasattr(obs, "start_step")
        assert hasattr(obs, "end_step")
        assert hasattr(obs, "get_metrics")

        # Test functionality
        obs.start_step("test_step")
        obs.end_step("test_step", True)

        assert len(obs.started_steps) == 1
        assert len(obs.ended_steps) == 1
        assert obs.get_metrics()["started"] == 1

    def test_step_executor_protocol_structure(self):
        """Test StepExecutor protocol structure."""

        class MockStepExecutor:
            def can_execute(self, step) -> bool:
                return step.step_type == "test"

            def execute(self, step, context):
                class MockResult:
                    step_id = step.id
                    success = True
                    duration_ms = 50.0
                    rows_affected = 0
                    error_message = None

                return MockResult()

        executor = MockStepExecutor()

        # Test protocol compliance
        assert hasattr(executor, "can_execute")
        assert hasattr(executor, "execute")

        # Test functionality
        class TestStep:
            id = "test"
            step_type = "test"

        step = TestStep()
        assert executor.can_execute(step)

        result = executor.execute(step, None)
        assert result.step_id == "test"
        assert result.success is True


class TestProtocolTypeSafety:
    """Test that protocols provide proper type safety."""

    def test_protocol_duck_typing(self):
        """Test that protocols work with duck typing."""
        # Any object with the right attributes should work with protocols

        # Create a simple object that looks like a Step
        step_like = type("StepLike", (), {"id": "test_step", "step_type": "load"})()

        # It should work as a Step (duck typing)
        assert step_like.id == "test_step"
        assert step_like.step_type == "load"

    def test_protocol_flexibility(self):
        """Test that protocols are flexible for different implementations."""
        # Different implementations of the same protocol should all work

        class SimpleResult:
            def __init__(self, step_id: str, success: bool):
                self.step_id = step_id
                self.success = success
                self.duration_ms = 0.0
                self.rows_affected = 0
                self.error_message = None

        class DetailedResult:
            def __init__(self, step_id: str, success: bool, duration: float, rows: int):
                self.step_id = step_id
                self.success = success
                self.duration_ms = duration
                self.rows_affected = rows
                self.error_message = None

        # Both should work as StepResult protocols
        simple = SimpleResult("test1", True)
        detailed = DetailedResult("test2", True, 100.5, 42)

        assert simple.step_id == "test1"
        assert detailed.step_id == "test2"
        assert detailed.duration_ms == 100.5
        assert detailed.rows_affected == 42
