"""Tests for variable reference formatting in conditional expressions."""

import unittest

from sqlflow.parser.parser import Parser


class TestVariableFormatting(unittest.TestCase):
    """Test that variable references are properly formatted in conditions."""

    def test_condition_variable_formatting(self):
        """Test that variables in conditions are properly formatted without spaces."""
        sql = """
        IF $ { env | prod } == 'production' THEN
            CREATE TABLE users AS SELECT * FROM prod_users;
        ELSE IF $ { region | us-east } == 'us-west' THEN
            CREATE TABLE users AS SELECT * FROM west_users;
        ELSE
            CREATE TABLE users AS SELECT * FROM default_users;
        END IF;
        """

        parser = Parser(sql)
        pipeline = parser.parse()

        # Should have 1 step
        self.assertEqual(1, len(pipeline.steps))
        conditional_step = pipeline.steps[0]

        # Check that the variable references don't have spaces
        # First branch condition
        branch1 = conditional_step.branches[0]
        self.assertIn("${env|prod}", branch1.condition)
        self.assertNotIn("$ { env", branch1.condition)
        self.assertNotIn("env | prod }", branch1.condition)

        # Second branch condition
        branch2 = conditional_step.branches[1]
        self.assertIn("${region|us - east}", branch2.condition)
        self.assertNotIn("$ { region", branch2.condition)
        self.assertNotIn("region | us - east }", branch2.condition)

    def test_variable_in_export_paths(self):
        """Test that variables in export paths are properly formatted."""
        sql = """
        EXPORT
          SELECT * FROM sales
        TO "output/$ { run_date | today }_report.csv"
        TYPE CSV
        OPTIONS { "header": true };
        """

        parser = Parser(sql)
        pipeline = parser.parse()

        # Should have 1 step
        self.assertEqual(1, len(pipeline.steps))
        export_step = pipeline.steps[0]

        # Check the destination URI for proper variable formatting
        self.assertIn("${run_date|today}", export_step.destination_uri)
        self.assertNotIn("$ { run_date", export_step.destination_uri)


if __name__ == "__main__":
    unittest.main()
