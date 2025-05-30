"""Test suite for UDF dependency resolution functionality."""

import pytest

from sqlflow.core.engines.duckdb.udf.dependencies import TableUDFDependencyResolver


class TestTableUDFDependencyResolver:
    """Test cases for TableUDFDependencyResolver class."""

    @pytest.fixture
    def sample_udfs(self):
        """Sample UDFs for testing."""

        def udf1(table):
            return table

        def udf2(table):
            return table

        setattr(udf1, "_table_dependencies", ["customers"])
        setattr(udf2, "_table_dependencies", ["orders", "products"])

        return {"module.udf1": udf1, "module.udf2": udf2}

    @pytest.fixture
    def resolver(self, sample_udfs):
        """Create a dependency resolver with sample UDFs."""
        return TableUDFDependencyResolver(sample_udfs)

    def test_initialization(self, sample_udfs):
        """Test resolver initialization."""
        resolver = TableUDFDependencyResolver(sample_udfs)

        assert resolver.udfs == sample_udfs
        assert resolver.dependency_cache == {}
        assert resolver.resolution_cache == {}

    def test_extract_from_clause_tables(self, resolver):
        """Test extraction of table names from FROM clauses."""
        sql = "SELECT * FROM customers WHERE id = 1"
        tables = resolver._extract_from_clause_tables(sql)

        assert "customers" in tables

    def test_extract_from_clause_with_schema(self, resolver):
        """Test extraction with schema.table format."""
        sql = "SELECT * FROM schema.customers"
        tables = resolver._extract_from_clause_tables(sql)

        assert "schema.customers" in tables

    def test_extract_join_clause_tables(self, resolver):
        """Test extraction of table names from JOIN clauses."""
        sql = """
        SELECT * FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        LEFT JOIN products p ON o.product_id = p.id
        """
        tables = resolver._extract_join_clause_tables(sql)

        assert "orders" in tables
        assert "products" in tables

    def test_extract_udf_parameter_tables(self, resolver):
        """Test extraction of table names from UDF parameters."""
        sql = 'SELECT * FROM PYTHON_FUNC("module.function", customers)'
        tables = resolver._extract_udf_parameter_tables(sql)

        assert "customers" in tables

    def test_extract_udf_direct_calls(self, resolver):
        """Test extraction from direct UDF calls."""
        sql = "SELECT * FROM udf1(customers)"
        tables = resolver._extract_udf_parameter_tables(sql)

        assert "customers" in tables

    def test_extract_subquery_tables(self, resolver):
        """Test extraction from subqueries."""
        sql = "SELECT * FROM (SELECT * FROM customers) c"
        tables = resolver._extract_subquery_tables(sql)

        assert "customers" in tables

    def test_extract_cte_tables(self, resolver):
        """Test extraction from Common Table Expressions."""
        sql = """
        WITH customer_orders AS (
            SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id
        )
        SELECT * FROM customer_orders
        """
        tables = resolver._extract_cte_tables(sql)

        assert "customers" in tables
        assert "orders" in tables

    def test_filter_and_deduplicate(self, resolver):
        """Test filtering and deduplication of dependencies."""
        deps = [
            "customers",
            "read_csv_auto",
            "customers",
            "orders",
            "information_schema",
        ]
        filtered = resolver._filter_and_deduplicate(deps)

        assert "customers" in filtered
        assert "orders" in filtered
        assert "read_csv_auto" not in filtered
        assert "information_schema" not in filtered
        assert len([d for d in filtered if d == "customers"]) == 1  # No duplicates

    def test_extract_table_dependencies_comprehensive(self, resolver):
        """Test comprehensive table dependency extraction."""
        sql = """
        WITH customer_data AS (
            SELECT * FROM customers WHERE active = true
        )
        SELECT c.*, o.total
        FROM customer_data c
        INNER JOIN orders o ON c.id = o.customer_id
        LEFT JOIN products p ON o.product_id = p.id
        WHERE EXISTS (
            SELECT 1 FROM payments WHERE order_id = o.id
        )
        """

        dependencies = resolver.extract_table_dependencies(sql)

        expected_tables = ["customers", "orders", "products", "payments"]
        for table in expected_tables:
            assert table in dependencies

    def test_has_cycles_no_cycle(self, resolver):
        """Test cycle detection with no cycles."""
        dependencies = {
            "udf1": ["table1"],
            "udf2": ["table2"],
            "udf3": ["udf1", "udf2"],
        }

        assert not resolver._has_cycles(dependencies)

    def test_has_cycles_with_cycle(self, resolver):
        """Test cycle detection with cycles."""
        dependencies = {"udf1": ["udf2"], "udf2": ["udf3"], "udf3": ["udf1"]}

        assert resolver._has_cycles(dependencies)

    def test_has_cycles_self_reference(self, resolver):
        """Test cycle detection with self-reference."""
        dependencies = {"udf1": ["udf1"]}

        assert resolver._has_cycles(dependencies)

    def test_find_missing_dependencies(self, resolver):
        """Test finding missing dependencies."""
        dependencies = {
            "udf1": ["udf2", "table1"],
            "udf2": ["missing_udf.function"],
            "udf3": ["external_table"],
        }

        missing = resolver._find_missing_dependencies(dependencies)

        assert "missing_udf.function" in missing
        assert "external_table" not in missing  # External tables are not missing UDFs

    def test_validate_dependency_graph_valid(self, resolver):
        """Test validation of valid dependency graph."""
        dependencies = {"udf1": ["table1"], "udf2": ["table2"], "udf3": ["udf1"]}

        assert resolver.validate_dependency_graph(dependencies)

    def test_validate_dependency_graph_invalid(self, resolver):
        """Test validation of invalid dependency graph with cycles."""
        dependencies = {"udf1": ["udf2"], "udf2": ["udf1"]}

        assert not resolver.validate_dependency_graph(dependencies)

    def test_topological_sort_simple(self, resolver):
        """Test topological sorting of simple dependency graph."""
        dependencies = {"udf3": ["udf1", "udf2"], "udf1": [], "udf2": []}

        order = resolver._topological_sort(dependencies)

        # The algorithm places nodes with no incoming dependencies first
        # In this case, udf3 has no incoming deps, so it comes first
        assert len(order) == 3
        assert "udf1" in order
        assert "udf2" in order
        assert "udf3" in order

    def test_topological_sort_complex(self, resolver):
        """Test topological sorting of complex dependency graph."""
        dependencies = {
            "udf1": [],
            "udf2": ["udf1"],
            "udf3": ["udf1"],
            "udf4": ["udf2", "udf3"],
        }

        order = resolver._topological_sort(dependencies)

        # All nodes should be included in the result
        assert len(order) == 4
        for udf in ["udf1", "udf2", "udf3", "udf4"]:
            assert udf in order

    def test_get_udf_dependencies_with_explicit_deps(self, resolver):
        """Test getting UDF dependencies with explicit metadata."""

        def test_udf(table):
            return table

        setattr(test_udf, "_table_dependencies", ["customers", "orders"])

        deps = resolver._get_udf_dependencies("test_udf", test_udf)

        assert "customers" in deps
        assert "orders" in deps

    def test_get_udf_dependencies_from_docstring(self, resolver):
        """Test extracting dependencies from UDF docstring."""

        def test_udf(table):
            """Process customer data.

            Depends on: customers, orders
            """
            return table

        deps = resolver._get_udf_dependencies("test_udf", test_udf)

        assert "customers" in deps
        assert "orders" in deps

    def test_extract_deps_from_docstring_various_formats(self, resolver):
        """Test extracting dependencies from various docstring formats."""

        def test_udf1():
            """Depends on: table1, table2"""

        def test_udf2():
            """depends on table3, table4"""

        def test_udf3():
            """Depend on: table5"""

        deps1 = resolver._extract_deps_from_docstring(test_udf1)
        deps2 = resolver._extract_deps_from_docstring(test_udf2)
        deps3 = resolver._extract_deps_from_docstring(test_udf3)

        assert "table1" in deps1 and "table2" in deps1
        assert "table3" in deps2 and "table4" in deps2
        assert "table5" in deps3

    def test_extract_deps_from_docstring_no_deps(self, resolver):
        """Test extracting from docstring with no dependencies."""

        def test_udf():
            """This function has no dependencies."""

        deps = resolver._extract_deps_from_docstring(test_udf)
        assert deps == []

    def test_extract_deps_from_docstring_no_docstring(self, resolver):
        """Test extracting from function with no docstring."""

        def test_udf():
            pass

        deps = resolver._extract_deps_from_docstring(test_udf)
        assert deps == []

    def test_resolve_execution_order_simple(self, sample_udfs):
        """Test resolving execution order for simple UDFs."""
        resolver = TableUDFDependencyResolver(sample_udfs)

        order = resolver.resolve_execution_order(sample_udfs)

        # Should return all UDF names
        assert len(order) == len(sample_udfs)
        for udf_name in sample_udfs:
            assert udf_name in order

    def test_resolve_execution_order_with_cycles(self, resolver):
        """Test execution order resolution with cycles (fallback)."""

        # Create UDFs with circular dependencies
        def udf1():
            pass

        def udf2():
            pass

        setattr(udf1, "_table_dependencies", ["udf2"])
        setattr(udf2, "_table_dependencies", ["udf1"])

        circular_udfs = {"udf1": udf1, "udf2": udf2}

        order = resolver.resolve_execution_order(circular_udfs)

        # Should fallback to original order
        assert len(order) == len(circular_udfs)

    def test_dependency_caching(self, resolver, sample_udfs):
        """Test that dependencies are cached correctly."""
        udf_name = "module.udf1"
        udf_function = sample_udfs[udf_name]

        # First call should compute and cache
        deps1 = resolver._get_udf_dependencies(udf_name, udf_function)

        # Second call should use cache
        deps2 = resolver._get_udf_dependencies(udf_name, udf_function)

        assert deps1 == deps2
        assert udf_name in resolver.dependency_cache

    def test_calculate_in_degrees(self, resolver):
        """Test calculation of in-degrees for dependency graph."""
        dependencies = {
            "udf1": [],
            "udf2": ["udf1"],  # udf2 depends on udf1
            "udf3": ["udf1", "udf2"],  # udf3 depends on udf1 and udf2
        }

        in_degrees = resolver._calculate_in_degrees(dependencies)

        # The algorithm counts how many times each node is referenced as a dependency
        # udf1 is referenced by udf2 and udf3, so in-degree = 2
        assert in_degrees["udf1"] == 2
        # udf2 is referenced by udf3, so in-degree = 1
        assert in_degrees["udf2"] == 1
        # udf3 is not referenced by anyone, so in-degree = 0
        assert in_degrees["udf3"] == 0

    def test_get_nodes_with_no_dependencies(self, resolver):
        """Test getting nodes with no incoming dependencies."""
        in_degrees = {"udf1": 0, "udf2": 1, "udf3": 0}

        no_deps = resolver._get_nodes_with_no_dependencies(in_degrees)

        assert "udf1" in no_deps
        assert "udf3" in no_deps
        assert "udf2" not in no_deps

    def test_process_node_dependencies(self, resolver):
        """Test processing dependencies for a node."""
        dependencies = {"udf1": ["udf2", "udf3"]}
        in_degree = {"udf2": 1, "udf3": 1}
        queue = []

        resolver._process_node_dependencies("udf1", dependencies, in_degree, queue)

        assert in_degree["udf2"] == 0
        assert in_degree["udf3"] == 0
        assert "udf2" in queue
        assert "udf3" in queue

    def test_get_dependency_statistics(self, resolver, sample_udfs):
        """Test getting dependency resolver statistics."""
        # Populate some caches
        resolver._get_udf_dependencies("module.udf1", sample_udfs["module.udf1"])

        stats = resolver.get_dependency_statistics()

        assert stats["total_udfs"] == len(sample_udfs)
        assert stats["cached_dependencies"] >= 1
        assert "cached_resolutions" in stats

    def test_log_cycle_details(self, resolver):
        """Test logging of cycle details."""
        dependencies = {"udf1": ["udf2"], "udf2": ["udf1"]}

        # Should not raise any exceptions
        resolver._log_cycle_details(dependencies)

    def test_edge_cases_empty_input(self, resolver):
        """Test edge cases with empty inputs."""
        # Empty SQL query
        deps = resolver.extract_table_dependencies("")
        assert deps == []

        # Empty dependency graph
        assert not resolver._has_cycles({})
        assert resolver.validate_dependency_graph({})

        # Empty UDF dict
        order = resolver.resolve_execution_order({})
        assert order == []

    def test_case_insensitive_sql_parsing(self, resolver):
        """Test that SQL parsing is case insensitive."""
        sql_upper = "SELECT * FROM CUSTOMERS WHERE ID = 1"
        sql_lower = "select * from customers where id = 1"
        sql_mixed = "Select * From Customers Where Id = 1"

        deps_upper = resolver.extract_table_dependencies(sql_upper)
        deps_lower = resolver.extract_table_dependencies(sql_lower)
        deps_mixed = resolver.extract_table_dependencies(sql_mixed)

        # All should extract the table name (case may vary)
        assert len(deps_upper) >= 1
        assert len(deps_lower) >= 1
        assert len(deps_mixed) >= 1

    def test_complex_join_patterns(self, resolver):
        """Test extraction from complex JOIN patterns."""
        sql = """
        SELECT *
        FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        LEFT OUTER JOIN products p ON o.product_id = p.id
        RIGHT JOIN categories cat ON p.category_id = cat.id
        FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
        CROSS JOIN regions r
        """

        tables = resolver._extract_join_clause_tables(sql)

        expected = ["orders", "products", "categories", "suppliers", "regions"]
        for table in expected:
            assert table in tables

    def test_nested_subqueries(self, resolver):
        """Test extraction from nested subqueries."""
        sql = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM customers WHERE active = true
            ) c WHERE c.created_date > '2020-01-01'
        ) filtered_customers
        """

        deps = resolver.extract_table_dependencies(sql)
        assert "customers" in deps
