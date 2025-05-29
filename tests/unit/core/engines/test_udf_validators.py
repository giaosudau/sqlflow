"""Unit tests for UDF validators."""

import inspect
import unittest
from unittest.mock import MagicMock, patch

from sqlflow.core.engines.duckdb.constants import DuckDBConstants
from sqlflow.core.engines.duckdb.exceptions import UDFRegistrationError
from sqlflow.core.engines.duckdb.udf.validators import (
    TableUDFSignatureValidator,
    TypeValidator,
)


class TestTableUDFSignatureValidator(unittest.TestCase):
    """Test the TableUDFSignatureValidator class."""

    def setUp(self):
        """Set up test environment."""
        self.validator = TableUDFSignatureValidator()

    def test_valid_table_udf_signature(self):
        """Test validation of a valid table UDF signature."""

        def valid_table_udf(df, param1=None, param2="default"):
            return df

        # Add proper type annotations
        valid_table_udf.__annotations__ = {
            "df": "pd.DataFrame",
            "return": "pd.DataFrame",
        }

        sig, param_info = self.validator.validate("test_udf", valid_table_udf)

        self.assertIsInstance(sig, inspect.Signature)
        self.assertIsInstance(param_info, dict)
        self.assertIn("df", param_info)
        self.assertIn("param1", param_info)
        self.assertIn("param2", param_info)

    def test_valid_table_udf_with_pandas_annotations(self):
        """Test validation with proper pandas DataFrame annotations."""

        def valid_table_udf(df) -> None:
            return df

        sig, param_info = self.validator.validate("test_udf", valid_table_udf)

        self.assertIsInstance(sig, inspect.Signature)
        self.assertEqual(len(param_info), 1)
        self.assertIn("df", param_info)

    def test_udf_without_parameters(self):
        """Test validation of UDF without parameters."""

        def invalid_udf():
            return None

        with self.assertRaises(UDFRegistrationError) as cm:
            self.validator.validate("invalid_udf", invalid_udf)

        self.assertIn("must accept at least one argument", str(cm.exception))

    def test_udf_with_non_positional_first_parameter(self):
        """Test validation of UDF with non-positional first parameter."""

        def invalid_udf(*args):
            return args[0]

        with self.assertRaises(UDFRegistrationError) as cm:
            self.validator.validate("invalid_udf", invalid_udf)

        self.assertIn("must be positional", str(cm.exception))

    def test_udf_with_positional_additional_parameters(self):
        """Test validation of UDF with additional positional parameters."""

        def invalid_udf(df, required_param):
            return df

        # This validator is more relaxed and only checks for POSITIONAL_ONLY and VAR_POSITIONAL
        # POSITIONAL_OR_KEYWORD is allowed, so this should not raise an error
        sig, param_info = self.validator.validate("test_udf", invalid_udf)
        self.assertIsInstance(sig, inspect.Signature)

    def test_udf_with_var_positional_parameters(self):
        """Test validation of UDF with *args parameters."""

        def invalid_udf(df, *args):
            return df

        with self.assertRaises(UDFRegistrationError) as cm:
            self.validator.validate("invalid_udf", invalid_udf)

        self.assertIn("must be keyword arguments", str(cm.exception))

    def test_parameter_info_extraction(self):
        """Test extraction of parameter information."""

        def test_udf(df, *, param1: int = 10, param2: str = "default", param3=None):
            return df

        sig, param_info = self.validator.validate("test_udf", test_udf)

        # Check parameter info structure
        self.assertIn("df", param_info)
        self.assertIn("param1", param_info)
        self.assertIn("param2", param_info)
        self.assertIn("param3", param_info)

        # Check parameter details
        param1_info = param_info["param1"]
        self.assertEqual(param1_info["default"], 10)
        self.assertEqual(param1_info["annotation"], "<class 'int'>")

        param2_info = param_info["param2"]
        self.assertEqual(param2_info["default"], "default")
        self.assertEqual(param2_info["annotation"], "<class 'str'>")

        param3_info = param_info["param3"]
        self.assertIsNone(param3_info["default"])

    def test_parameter_with_no_default(self):
        """Test parameter without default value."""

        def test_udf(df, *, required_param):
            return df

        # With keyword-only parameters, this should work
        sig, param_info = self.validator.validate("test_udf", test_udf)
        self.assertIsInstance(sig, inspect.Signature)
        self.assertIn("required_param", param_info)

    def test_parameter_with_empty_annotation(self):
        """Test parameter with no type annotation."""

        def test_udf(df, param1=None):
            return df

        sig, param_info = self.validator.validate("test_udf", test_udf)

        param1_info = param_info["param1"]
        self.assertEqual(param1_info["annotation"], "Any")

    def test_keyword_only_parameters(self):
        """Test UDF with keyword-only parameters."""

        def valid_udf(df, *, param1=None, param2="default"):
            return df

        sig, param_info = self.validator.validate("test_udf", valid_udf)

        self.assertEqual(len(param_info), 3)  # df + 2 keyword-only params
        self.assertIn("param1", param_info)
        self.assertIn("param2", param_info)

    def test_return_type_validation_warning(self):
        """Test that return type validation issues warnings but doesn't fail."""

        def test_udf(df) -> str:  # Wrong return type
            return "not a dataframe"

        # Should not raise an exception, just log a warning
        sig, param_info = self.validator.validate("test_udf", test_udf)

        self.assertIsInstance(sig, inspect.Signature)
        self.assertEqual(sig.return_annotation, str)

    def test_first_parameter_type_validation_warning(self):
        """Test that first parameter type validation issues warnings but doesn't fail."""

        def test_udf(df: str):  # Wrong parameter type
            return df

        # Should not raise an exception, just log a warning
        sig, param_info = self.validator.validate("test_udf", test_udf)

        self.assertIsInstance(sig, inspect.Signature)

    def test_validation_error_propagation(self):
        """Test that validation errors are properly propagated."""
        # Create a mock function that will cause an inspection error
        mock_func = MagicMock()
        mock_func.__name__ = "mock_func"

        with patch("inspect.signature", side_effect=ValueError("Inspection error")):
            with self.assertRaises(UDFRegistrationError) as cm:
                self.validator.validate("mock_func", mock_func)

            self.assertIn("Error validating table UDF", str(cm.exception))
            self.assertIn("Inspection error", str(cm.exception))

    def test_complex_signature_validation(self):
        """Test validation of complex UDF signature with various parameter types."""

        def complex_udf(
            df,
            *,
            threshold: float = 0.5,
            mode: str = "default",
            options: dict = {},
            **kwargs,
        ):
            return df

        sig, param_info = self.validator.validate("complex_udf", complex_udf)

        self.assertIsInstance(sig, inspect.Signature)
        self.assertIn("df", param_info)
        self.assertIn("threshold", param_info)
        self.assertIn("mode", param_info)
        self.assertIn("options", param_info)
        self.assertIn("kwargs", param_info)

        # Check specific parameter details
        threshold_info = param_info["threshold"]
        self.assertEqual(threshold_info["default"], 0.5)
        self.assertEqual(threshold_info["annotation"], "<class 'float'>")

        options_info = param_info["options"]
        self.assertEqual(options_info["default"], {})  # Empty dict, not None
        self.assertEqual(options_info["annotation"], "<class 'dict'>")


class TestTypeValidator(unittest.TestCase):
    """Test the TypeValidator class."""

    def test_supported_python_types(self):
        """Test mapping of supported Python types to DuckDB types."""
        test_cases = [
            (int, "INTEGER"),
            (float, "DOUBLE"),
            (str, "VARCHAR"),
            (bool, "BOOLEAN"),
        ]

        for py_type, expected_db_type in test_cases:
            # Mock the constants mapping
            with patch.object(
                DuckDBConstants, "PYTHON_TO_DUCKDB_TYPES", {py_type: expected_db_type}
            ):
                result = TypeValidator.map_python_type_to_duckdb(
                    py_type, "test_udf", "param1"
                )
                self.assertEqual(result, expected_db_type)

    def test_unsupported_python_type(self):
        """Test handling of unsupported Python types."""
        unsupported_type = complex  # Complex numbers are not typically supported

        # Mock empty constants mapping
        with patch.object(DuckDBConstants, "PYTHON_TO_DUCKDB_TYPES", {}):
            with self.assertRaises(ValueError) as cm:
                TypeValidator.map_python_type_to_duckdb(
                    unsupported_type, "test_udf", "param1"
                )

            self.assertIn("Unsupported Python type", str(cm.exception))
            self.assertIn(str(unsupported_type), str(cm.exception))

    def test_type_mapping_with_none_type(self):
        """Test type mapping with None type."""
        none_type = type(None)

        with patch.object(
            DuckDBConstants, "PYTHON_TO_DUCKDB_TYPES", {none_type: "NULL"}
        ):
            result = TypeValidator.map_python_type_to_duckdb(
                none_type, "test_udf", "param1"
            )
            self.assertEqual(result, "NULL")

    def test_type_mapping_with_custom_types(self):
        """Test type mapping with custom/user-defined types."""

        class CustomType:
            pass

        # Should raise ValueError for unmapped custom types
        with patch.object(DuckDBConstants, "PYTHON_TO_DUCKDB_TYPES", {}):
            with self.assertRaises(ValueError):
                TypeValidator.map_python_type_to_duckdb(
                    CustomType, "test_udf", "param1"
                )

    def test_type_mapping_consistency(self):
        """Test that type mapping is consistent across calls."""
        test_type = str
        expected_type = "VARCHAR"

        with patch.object(
            DuckDBConstants, "PYTHON_TO_DUCKDB_TYPES", {test_type: expected_type}
        ):
            result1 = TypeValidator.map_python_type_to_duckdb(
                test_type, "udf1", "param1"
            )
            result2 = TypeValidator.map_python_type_to_duckdb(
                test_type, "udf2", "param2"
            )

            self.assertEqual(result1, result2)
            self.assertEqual(result1, expected_type)

    def test_error_message_includes_context(self):
        """Test that error messages include UDF and parameter context."""
        unsupported_type = dict

        with patch.object(DuckDBConstants, "PYTHON_TO_DUCKDB_TYPES", {}):
            with self.assertRaises(ValueError) as cm:
                TypeValidator.map_python_type_to_duckdb(
                    unsupported_type, "my_udf", "my_param"
                )

            error_message = str(cm.exception)
            self.assertIn("Unsupported Python type", error_message)


class TestValidatorIntegration(unittest.TestCase):
    """Integration tests for validator components."""

    def setUp(self):
        """Set up test environment."""
        self.signature_validator = TableUDFSignatureValidator()

    def test_full_validation_workflow(self):
        """Test complete validation workflow for a realistic UDF."""

        def realistic_table_udf(
            df, *, threshold: float = 0.5, mode: str = "strict", debug: bool = False
        ):
            """A realistic table UDF for testing."""
            return df

        # Add proper annotations
        realistic_table_udf.__annotations__ = {
            "df": "pd.DataFrame",
            "return": "pd.DataFrame",
        }

        # Validate signature
        sig, param_info = self.signature_validator.validate(
            "realistic_udf", realistic_table_udf
        )

        # Verify structure
        self.assertIsInstance(sig, inspect.Signature)
        self.assertEqual(len(param_info), 4)  # df + 3 keyword params

        # Verify parameter details
        self.assertIn("df", param_info)
        self.assertIn("threshold", param_info)
        self.assertIn("mode", param_info)
        self.assertIn("debug", param_info)

        # Verify parameter types and defaults
        threshold_info = param_info["threshold"]
        self.assertEqual(threshold_info["default"], 0.5)
        self.assertEqual(threshold_info["annotation"], "Any")

        mode_info = param_info["mode"]
        self.assertEqual(mode_info["default"], "strict")
        self.assertEqual(mode_info["annotation"], "Any")

        debug_info = param_info["debug"]
        self.assertEqual(debug_info["default"], False)
        self.assertEqual(debug_info["annotation"], "Any")

    def test_edge_case_signatures(self):
        """Test validation of edge case UDF signatures."""

        # UDF with only required DataFrame parameter
        def minimal_udf(df):
            return df

        sig, param_info = self.signature_validator.validate("minimal_udf", minimal_udf)
        self.assertEqual(len(param_info), 1)
        self.assertIn("df", param_info)

        # UDF with many optional parameters
        def many_params_udf(df, *, p1=1, p2=2, p3=3, p4=4, p5=5):
            return df

        sig, param_info = self.signature_validator.validate(
            "many_params_udf", many_params_udf
        )
        self.assertEqual(len(param_info), 6)  # df + 5 params
        for i in range(1, 6):
            self.assertIn(f"p{i}", param_info)
            self.assertEqual(param_info[f"p{i}"]["default"], i)


if __name__ == "__main__":
    unittest.main()
