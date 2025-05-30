"""Regression tests for planner fixes.

These tests ensure that specific planner issues are never reintroduced:
1. source_connector_type vs connector_type field naming
2. Dependency resolution fixes
3. Load step structure generation
"""

from sqlflow.core.planner import Planner
from sqlflow.parser.parser import Parser


class TestPlannerRegression:
    """Unit tests for planner regression fixes."""

    def test_source_definition_uses_source_connector_type(self):
        """Test that source definition steps use 'source_connector_type' field.

        Regression test for: "Unknown connector type: source_definition" error
        This ensures the executor gets the expected field name.
        """
        pipeline_text = """
        SOURCE test_source TYPE CSV PARAMS {
            "path": "data/test.csv",
            "has_header": true
        };
        
        LOAD test_table FROM test_source;
        """

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Find the source definition step
        source_steps = [
            step for step in plan if step.get("type") == "source_definition"
        ]
        assert len(source_steps) == 1

        source_step = source_steps[0]

        # Must have source_connector_type (not just connector_type)
        assert "source_connector_type" in source_step
        assert source_step["source_connector_type"] == "CSV"

        # Should NOT have a bare 'connector_type' that would confuse the executor
        assert (
            "connector_type" not in source_step
            or source_step.get("connector_type") == source_step["source_connector_type"]
        )

    def test_load_step_structure_contains_required_fields(self):
        """Test that load steps have the correct structure.

        Regression test for: Load step creation using wrong field names
        """
        pipeline_text = """
        SOURCE customers TYPE CSV PARAMS {
            "path": "data/customers.csv",
            "has_header": true
        };
        
        LOAD raw_customers FROM customers;
        """

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Find the load step
        load_steps = [step for step in plan if step.get("type") == "load"]
        assert len(load_steps) == 1

        load_step = load_steps[0]

        # Must have correct top-level fields for executor compatibility
        assert "source_name" in load_step
        assert "target_table" in load_step
        assert "source_connector_type" in load_step

        # Verify values
        assert load_step["source_name"] == "customers"
        assert load_step["target_table"] == "raw_customers"
        assert load_step["source_connector_type"] == "CSV"

        # Should have query dict as well for backward compatibility
        assert "query" in load_step
        query = load_step["query"]
        assert "source_name" in query
        assert "table_name" in query

    def test_dependency_resolution_generates_valid_step_ids(self):
        """Test that dependency resolution creates valid step IDs.

        Regression test for: Invalid dependency IDs like "4844783456"
        """
        pipeline_text = """
        SOURCE source1 TYPE CSV PARAMS {"path": "data1.csv"};
        SOURCE source2 TYPE CSV PARAMS {"path": "data2.csv"};
        
        LOAD table1 FROM source1;
        LOAD table2 FROM source2;
        
        CREATE TABLE combined AS
        SELECT * FROM table1
        UNION ALL
        SELECT * FROM table2;
        """

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Check that all step IDs are valid (not numeric object IDs)
        for step in plan:
            step_id = step.get("id", "")

            # Step IDs should be descriptive strings, not raw object IDs
            assert isinstance(step_id, str)
            assert len(step_id) > 0
            assert not step_id.isdigit()  # Should not be pure numeric

            # Should follow expected patterns
            if step.get("type") == "source_definition":
                assert step_id.startswith("source_")
            elif step.get("type") == "load":
                assert step_id.startswith("load_")
            elif step.get("type") == "transform":
                assert step_id.startswith("transform_")

        # Check dependencies reference valid step IDs
        for step in plan:
            depends_on = step.get("depends_on", [])
            for dependency_id in depends_on:
                # Dependencies should be valid step IDs, not object IDs
                assert isinstance(dependency_id, str)
                assert len(dependency_id) > 0
                assert not dependency_id.isdigit()

                # Should reference an existing step
                dependency_exists = any(s.get("id") == dependency_id for s in plan)
                assert dependency_exists, (
                    f"Dependency {dependency_id} not found in plan"
                )

    def test_load_step_mode_and_merge_keys_preserved(self):
        """Test that load step mode and merge keys are preserved.

        Regression test for: Load step attributes being lost during planning
        """
        pipeline_text = """
        SOURCE customers TYPE CSV PARAMS {"path": "customers.csv"};
        
        LOAD customer_data FROM customers MODE MERGE MERGE_KEYS id, email;
        """

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Find the load step
        load_steps = [step for step in plan if step.get("type") == "load"]
        assert len(load_steps) == 1

        load_step = load_steps[0]

        # Mode should be preserved
        assert "mode" in load_step
        assert load_step["mode"] == "MERGE"

        # Merge keys should be preserved
        assert "merge_keys" in load_step
        assert load_step["merge_keys"] == ["id", "email"]


class TestPlannerExecutionOrderRegression:
    """Tests for execution order and dependency resolution regressions."""

    def test_source_before_load_dependency(self):
        """Test that SOURCE steps come before LOAD steps in execution order.

        Regression test for: Load steps executing before source definitions
        """
        pipeline_text = """
        SOURCE data_source TYPE CSV PARAMS {"path": "data.csv"};
        LOAD data_table FROM data_source;
        """

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Find step indices
        source_index = None
        load_index = None

        for i, step in enumerate(plan):
            if step.get("type") == "source_definition":
                source_index = i
            elif step.get("type") == "load":
                load_index = i

        assert source_index is not None, "Source step not found"
        assert load_index is not None, "Load step not found"
        assert source_index < load_index, "Source step must come before load step"

        # Also check dependencies
        load_step = plan[load_index]
        depends_on = load_step.get("depends_on", [])
        source_step_id = plan[source_index]["id"]
        assert source_step_id in depends_on, "Load step must depend on source step"

    def test_load_before_transform_dependency(self):
        """Test that LOAD steps come before transform steps that use them.

        Regression test for: Transform steps executing before data is loaded
        """
        pipeline_text = """
        SOURCE customers TYPE CSV PARAMS {"path": "customers.csv"};
        LOAD raw_customers FROM customers;
        
        CREATE TABLE processed_customers AS
        SELECT id, UPPER(name) as name_upper
        FROM raw_customers;
        """

        parser = Parser(pipeline_text)
        pipeline = parser.parse()

        planner = Planner()
        plan = planner.create_plan(pipeline)

        # Find step indices
        load_index = None
        transform_index = None

        for i, step in enumerate(plan):
            if (
                step.get("type") == "load"
                and step.get("target_table") == "raw_customers"
            ):
                load_index = i
            elif (
                step.get("type") == "transform"
                and step.get("name") == "processed_customers"
            ):
                transform_index = i

        assert load_index is not None, "Load step not found"
        assert transform_index is not None, "Transform step not found"
        assert load_index < transform_index, "Load step must come before transform step"

        # Check dependencies
        transform_step = plan[transform_index]
        depends_on = transform_step.get("depends_on", [])
        load_step_id = plan[load_index]["id"]
        assert load_step_id in depends_on, "Transform step must depend on load step"
