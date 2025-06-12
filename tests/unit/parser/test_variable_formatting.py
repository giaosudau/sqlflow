"""Tests for variable reference formatting in conditional expressions."""

import unittest

from sqlflow.core.planner_main import Planner
from sqlflow.parser.parser import Parser


class TestVariableFormatting(unittest.TestCase):
    """Test that variable references are properly formatted in conditions."""

    def test_condition_variable_formatting(self):
        """Test that variables in conditions are properly formatted without spaces, and invalid defaults raise errors."""
        sql = """
        IF $ { env | prod } == 'production' THEN
            CREATE TABLE users AS SELECT * FROM prod_users;
        ELSE IF $ { region | us - east } == 'us-west' THEN
            CREATE TABLE users AS SELECT * FROM west_users;
        ELSE
            CREATE TABLE users AS SELECT * FROM default_users;
        END IF;
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()
        # Should raise error due to invalid default value with spaces
        with self.assertRaises(Exception) as cm:
            planner.create_plan(pipeline)
        self.assertIn("Invalid default values for variables", str(cm.exception))
        self.assertIn("${region|us - east}", str(cm.exception))
        self.assertIn("must be quoted", str(cm.exception))

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

    def test_variable_in_sql_conditions(self):
        """Test that variables in SQL WHERE conditions are properly formatted."""
        sql = """
        CREATE TABLE filtered_sales AS
        SELECT * FROM sales_enriched
        WHERE 
            ('${customer_segment}' = 'all' OR customer_tier = '${customer_segment}') AND
            ('${target_region}' = 'global' OR region = '${target_region}') AND
            total_amount >= ${min_order_amount};
        """

        parser = Parser(sql)
        pipeline = parser.parse()

        # Should have 1 step
        self.assertEqual(1, len(pipeline.steps))
        sql_step = pipeline.steps[0]

        # Check that variables are properly formatted in the SQL
        formatted_sql = sql_step.sql_query

        # Variables in string comparisons should be quoted
        self.assertIn("'${customer_segment}'", formatted_sql)
        self.assertIn("'${target_region}'", formatted_sql)

        # Numeric comparison should not be quoted
        self.assertIn("total_amount >= ${min_order_amount}", formatted_sql)

        # No extra spaces in variable references
        self.assertNotIn("$ {", formatted_sql)
        self.assertNotIn("} ", formatted_sql)

    def test_variable_with_quoted_defaults(self):
        """Test that variables with quoted default values are handled correctly."""
        sql = """
        SOURCE data_source TYPE CSV PARAMS {
          "path": "data.csv",
          "has_header": true
        };
        
        LOAD data FROM data_source;
        
        CREATE TABLE filtered_data AS
        SELECT * FROM data
        WHERE region = ${region|"us east"} AND
              status = ${status|'active'} AND
              score >= ${min_score|0};
        """

        parser = Parser(sql)
        pipeline = parser.parse()
        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Should not raise error for quoted default values with spaces
        self.assertIsNotNone(plan)

    def test_variable_numeric_comparisons(self):
        """Test that numeric comparisons with variables are handled correctly."""
        sql = """
        CREATE TABLE high_value_sales AS
        SELECT * FROM sales
        WHERE amount > ${min_amount} AND
              quantity >= ${min_quantity} AND
              price BETWEEN ${min_price} AND ${max_price};
        """

        parser = Parser(sql)
        pipeline = parser.parse()

        # Should have 1 step
        self.assertEqual(1, len(pipeline.steps))
        sql_step = pipeline.steps[0]

        # Check that numeric comparisons are formatted correctly
        formatted_sql = sql_step.sql_query

        # Basic comparison operators
        self.assertIn("amount > ${min_amount}", formatted_sql)
        self.assertIn("quantity >= ${min_quantity}", formatted_sql)

        # BETWEEN operator - both variables should be unquoted
        self.assertIn("price BETWEEN ${min_price}", formatted_sql)  # First part
        self.assertIn("AND ${max_price}", formatted_sql)  # Second part
        self.assertNotIn("'${min_price}'", formatted_sql)  # Should not be quoted
        self.assertNotIn("'${max_price}'", formatted_sql)  # Should not be quoted


if __name__ == "__main__":
    unittest.main()
