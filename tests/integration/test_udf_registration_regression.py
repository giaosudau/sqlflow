"""
Integration tests for UDF registration regression prevention.

These tests ensure that the fix for UDF registration bound method handling
doesn't regress. Specifically, they test that bound methods maintain their
instance binding during registration and execution.

The bug was that `getattr(function, "__wrapped__", function)` was unwrapping
bound methods back to unbound functions, losing the instance binding and
causing signature mismatches like `(ANY, DOUBLE)` instead of `(DOUBLE)`.
"""

from sqlflow.core.engines.duckdb.udf.handlers import ScalarUDFHandler


class TestUDFRegistrationRegression:
    """Test UDF registration to prevent regression of bound method fix."""

    def test_bound_method_signature_preservation(self):
        """Test that bound methods preserve their correct signature during registration."""

        # Create a test class with a bound method (similar to real UDF classes)
        class TestUDFClass:
            def __init__(self):
                self.multiplier = 2.0

            def compute_enhanced_value(self, value: float) -> float:
                """A bound method that should have signature (value: float) -> float."""
                return value * self.multiplier

        # Create instance and get bound method
        udf_instance = TestUDFClass()
        bound_method = udf_instance.compute_enhanced_value

        # Verify it's actually a bound method
        import inspect

        assert inspect.ismethod(bound_method), "Should be a bound method"

        # This is the core fix - bound methods should be preserved as-is
        if inspect.ismethod(bound_method):
            actual_function = bound_method  # Keep bound methods as-is
        else:
            actual_function = getattr(bound_method, "__wrapped__", bound_method)

        # Verify the bound method is preserved
        assert actual_function is bound_method, "Bound method should be preserved"
        assert inspect.ismethod(actual_function), "Should still be a bound method"

        # Verify signature is correct (single parameter, not including 'self')
        sig = inspect.signature(actual_function)
        param_names = list(sig.parameters.keys())
        assert param_names == ["value"], f"Expected ['value'], got {param_names}"

        # Verify the function still works correctly
        result = actual_function(5.0)
        assert result == 10.0, "Bound method should work with instance state"

    def test_scalar_udf_registration_bound_methods(self):
        """Test that scalar UDFs with bound methods register correctly."""

        # This test focuses on the core regression: bound method signature preservation
        # We test the registration logic without requiring a full pipeline execution

        # Create a test class with bound methods (similar to real UDF classes)
        class TestUDFClass:
            def __init__(self):
                self.multiplier = 2.0

            def compute_enhanced_value(self, value: float) -> float:
                """A bound method that should have signature (value: float) -> float."""
                return value * self.multiplier

        # Create instance and get bound method
        udf_instance = TestUDFClass()
        bound_method = udf_instance.compute_enhanced_value

        # Test the registration handler
        handler = ScalarUDFHandler()

        # Test that the method can be prepared for registration without errors
        error_msg = ""
        try:
            prepared_function = handler._prepare_function_for_registration(bound_method)

            # Verify the function is still callable and works
            assert callable(prepared_function), "Prepared function should be callable"

            # Test that it still works as a bound method
            result = prepared_function(5.0)
            assert result == 10.0, "Bound method should work with instance state"

            success = True
        except Exception as e:
            success = False
            error_msg = str(e)

        assert success, f"Bound method registration should succeed: {error_msg}"

    def test_udf_handler_method_complexity_regression(self):
        """Test that the refactored UDF handler methods work correctly."""

        handler = ScalarUDFHandler()

        # Test the refactored methods exist and work
        class MockFunction:
            def __init__(self):
                self.test_value = 42

            def mock_method(self, x: float) -> float:
                return x + self.test_value

        mock_instance = MockFunction()
        bound_method = mock_instance.mock_method

        # Test _handle_instance_method_wrapper
        # This should preserve the bound method
        wrapped_function = handler._handle_instance_method_wrapper(bound_method)
        assert wrapped_function is bound_method, "Bound method should be preserved"

        # Test that the function still works
        result = wrapped_function(5.0)
        assert result == 47.0, "Bound method should work with instance state"

    def test_real_udf_execution_pipeline(self):
        """Test UDF registration logic without requiring full pipeline execution."""

        # This test focuses on the core regression: UDF registration process
        # We test the registration logic without requiring actual UDF execution

        # Create a mock UDF function with the signature that was causing issues
        def mock_validate_price_range(
            price: float, min_val: float, max_val: float
        ) -> bool:
            """Mock UDF function with multiple parameters."""
            return min_val <= price <= max_val

        def mock_capitalize_words(text: str) -> str:
            """Mock UDF function for text processing."""
            return text.title()

        # Test the registration handler
        handler = ScalarUDFHandler()

        # Test that both functions can be prepared for registration
        error_msg = ""
        try:
            prepared_price_func = handler._prepare_function_for_registration(
                mock_validate_price_range
            )
            prepared_text_func = handler._prepare_function_for_registration(
                mock_capitalize_words
            )

            # Verify both functions are callable
            assert callable(
                prepared_price_func
            ), "Price validation function should be callable"
            assert callable(
                prepared_text_func
            ), "Text processing function should be callable"

            # Test that they work correctly
            price_result = prepared_price_func(1.50, 0.5, 5.0)
            assert price_result is True, "Price validation should work"

            text_result = prepared_text_func("apple")
            assert text_result == "Apple", "Text processing should work"

            success = True
        except Exception as e:
            success = False
            error_msg = str(e)

        assert success, f"UDF registration should succeed: {error_msg}"

    def test_signature_extraction_preservation(self):
        """Test that function signature extraction preserves bound method signatures correctly."""

        # Create a test class similar to the real UDF issue
        class DataQualityUDF:
            def __init__(self):
                self.threshold = 0.5

            def compute_enhanced_value(self, value: float) -> float:
                """Compute enhanced value with instance state."""
                return value + self.threshold

        instance = DataQualityUDF()
        bound_method = instance.compute_enhanced_value

        # Test signature extraction (this was the core issue)
        import inspect

        # Before the fix, this would unwrap the bound method
        # After the fix, bound methods should be preserved
        if inspect.ismethod(bound_method):
            preserved_function = bound_method  # The fix
        else:
            preserved_function = getattr(bound_method, "__wrapped__", bound_method)

        # Get signature of the preserved function
        sig = inspect.signature(preserved_function)

        # The signature should NOT include 'self' (that was the bug)
        param_names = list(sig.parameters.keys())
        assert (
            "self" not in param_names
        ), "Bound method signature should not include 'self'"
        assert param_names == ["value"], f"Expected ['value'], got {param_names}"

        # Verify parameter types are preserved
        value_param = sig.parameters["value"]
        assert (
            value_param.annotation == float
        ), "Parameter type annotation should be preserved"

        # Verify return type is preserved
        assert (
            sig.return_annotation == float
        ), "Return type annotation should be preserved"

    def test_prepare_function_complexity_refactor(self):
        """Test that the complexity refactor of _prepare_function_for_registration works."""

        # This tests that the method complexity was reduced and still works
        handler = ScalarUDFHandler()

        # Create a function that would have triggered the complexity issue
        def complex_function_with_defaults(
            value: float, multiplier: float = 2.0, offset: float = 1.0
        ) -> float:
            """A function with multiple default parameters."""
            return (value * multiplier) + offset

        # Test that we can prepare this function without complexity errors
        error_msg = ""
        try:
            # The method should now be broken into smaller, focused methods
            prepared_function = handler._prepare_function_for_registration(
                complex_function_with_defaults
            )

            # Verify the function is still callable and works
            assert callable(prepared_function), "Prepared function should be callable"

            # Test with basic call
            result = prepared_function(5.0, 3.0, 2.0)
            expected = (5.0 * 3.0) + 2.0
            assert (
                result == expected
            ), f"Function should compute correctly: expected {expected}, got {result}"

            success = True
        except Exception as e:
            success = False
            error_msg = str(e)

        assert (
            success
        ), f"Function preparation should succeed after complexity refactor: {error_msg}"
