"""Test export destination URI parsing in SQLFlow parser.

This module tests the parsing of destination URIs in EXPORT statements,
ensuring that both single and double quotes are properly stripped from
destination paths.
"""

from sqlflow.parser.ast import ExportStep
from sqlflow.parser.parser import Parser


class TestExportDestinationUriParsing:
    """Test class for export destination URI parsing behavior."""

    def test_export_destination_with_double_quotes_stripped(self):
        """Test that double quotes are properly stripped from export destination URIs.

        This ensures that when users specify destinations with double quotes like
        "output/file.csv", the actual path used is output/file.csv without quotes.
        """
        sql = (
            'EXPORT SELECT * FROM test_table TO "output/test.csv" TYPE CSV OPTIONS {};'
        )
        parser = Parser()

        pipeline = parser.parse(sql, validate=False)
        assert len(pipeline.steps) == 1

        export_step = pipeline.steps[0]
        assert isinstance(export_step, ExportStep)
        assert export_step.destination_uri == "output/test.csv"  # No quotes
        assert export_step.connector_type == "csv"

    def test_export_destination_with_single_quotes_stripped(self):
        """Test that single quotes are properly stripped from export destination URIs.

        This ensures that when users specify destinations with single quotes like
        'output/file.csv', the actual path used is output/file.csv without quotes.
        This was the bug that caused files to be created with quote characters
        in the filename.
        """
        sql = (
            "EXPORT SELECT * FROM test_table TO 'output/test.csv' TYPE CSV OPTIONS {};"
        )
        parser = Parser()

        pipeline = parser.parse(sql, validate=False)
        assert len(pipeline.steps) == 1

        export_step = pipeline.steps[0]
        assert isinstance(export_step, ExportStep)
        assert export_step.destination_uri == "output/test.csv"  # No quotes
        assert export_step.connector_type == "csv"

    def test_export_destination_with_mixed_quotes_in_path(self):
        """Test destination paths that contain quote characters within the actual path.

        This tests edge cases where the path itself might contain quote characters
        that should be preserved while only stripping the outer quotes.
        """
        sql = """EXPORT SELECT * FROM test_table TO "output/client's_data.csv" TYPE CSV OPTIONS {};"""
        parser = Parser()

        pipeline = parser.parse(sql, validate=False)
        assert len(pipeline.steps) == 1

        export_step = pipeline.steps[0]
        assert isinstance(export_step, ExportStep)
        assert (
            export_step.destination_uri == "output/client's_data.csv"
        )  # Inner quotes preserved
        assert export_step.connector_type == "csv"

    def test_export_destination_s3_uri_with_quotes(self):
        """Test S3 URI destination paths with quotes are properly handled.

        This ensures that S3 URIs like 's3://bucket/path/file.csv' are parsed
        correctly without the outer quotes affecting the bucket/path parsing.
        """
        sql = "EXPORT SELECT * FROM test_table TO 's3://test-bucket/exports/data.csv' TYPE CSV OPTIONS {};"
        parser = Parser()

        pipeline = parser.parse(sql, validate=False)
        assert len(pipeline.steps) == 1

        export_step = pipeline.steps[0]
        assert isinstance(export_step, ExportStep)
        assert (
            export_step.destination_uri == "s3://test-bucket/exports/data.csv"
        )  # No outer quotes
        assert export_step.connector_type == "csv"

    def test_export_destination_with_variable_references(self):
        """Test that variable references in destination paths work with quote stripping.

        This ensures that variable substitution still works correctly after
        the quote stripping enhancement.
        """
        sql = "EXPORT SELECT * FROM test_table TO 'output/${env}_results.csv' TYPE CSV OPTIONS {};"
        parser = Parser()

        # Note: Variable substitution happens at runtime, so the parsed destination
        # will still contain the variable reference
        pipeline = parser.parse(sql, validate=False)
        assert len(pipeline.steps) == 1

        export_step = pipeline.steps[0]
        assert isinstance(export_step, ExportStep)
        assert (
            export_step.destination_uri == "output/${env}_results.csv"
        )  # Variable preserved, quotes stripped

    def test_export_destination_quote_stripping_with_options(self):
        """Test quote stripping works correctly when export includes options.

        This ensures that the quote stripping doesn't interfere with option
        parsing in more complex export statements.
        """
        sql = """EXPORT SELECT * FROM test_table 
                 TO 'output/complex_export.csv' 
                 TYPE CSV 
                 OPTIONS { "header": true, "delimiter": "," };"""
        parser = Parser()

        pipeline = parser.parse(sql, validate=False)
        assert len(pipeline.steps) == 1

        export_step = pipeline.steps[0]
        assert isinstance(export_step, ExportStep)
        assert export_step.destination_uri == "output/complex_export.csv"  # No quotes
        assert export_step.connector_type == "csv"
        assert export_step.options["header"] is True
        assert export_step.options["delimiter"] == ","
