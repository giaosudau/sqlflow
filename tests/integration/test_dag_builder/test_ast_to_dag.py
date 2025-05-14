"""Integration tests for AST to DAG conversion."""

from sqlflow.parser.ast import (
    ExportStep,
    LoadStep,
    Pipeline,
    SourceDefinitionStep,
)
from sqlflow.parser.parser import Parser
from sqlflow.visualizer.dag_builder_ast import ASTDAGBuilder


class TestASTToDAG:
    """Test AST to DAG conversion."""

    def test_simple_pipeline_to_dag(self):
        """Test converting a simple pipeline to a DAG."""
        pipeline = Pipeline()

        pipeline.add_step(
            SourceDefinitionStep(
                name="users",
                connector_type="POSTGRES",
                params={"connection": "${DB_CONN}", "table": "users"},
                line_number=1,
            )
        )

        pipeline.add_step(
            LoadStep(table_name="users_table", source_name="users", line_number=5)
        )

        pipeline.add_step(
            ExportStep(
                sql_query="SELECT * FROM users_table",
                destination_uri="s3://bucket/users.csv",
                connector_type="CSV",
                options={"delimiter": ",", "header": True},
                line_number=10,
            )
        )

        dag_builder = ASTDAGBuilder()
        dag = dag_builder.build_dag_from_ast(pipeline)

        assert len(dag.get_all_nodes()) == 3

        source_node = None
        load_node = None
        export_node = None

        for node_id in dag.get_all_nodes():
            attrs = dag.get_node_attributes(node_id)
            if attrs.get("type") == "SOURCE":
                source_node = node_id
            elif attrs.get("type") == "LOAD":
                load_node = node_id
            elif attrs.get("type") == "EXPORT":
                export_node = node_id

        assert source_node is not None
        assert load_node is not None
        assert export_node is not None

        assert source_node in dag.get_predecessors(load_node)
        assert load_node in dag.get_predecessors(export_node)

    def test_multi_statement_script(self):
        """Test assembling a DAG from a multi-statement script."""
        script = """
        SOURCE users TYPE POSTGRES PARAMS {
            "connection": "${DB_CONN}",
            "table": "users"
        };

        LOAD users_table FROM users;

        EXPORT
          SELECT * FROM users_table
        TO "s3://bucket/users.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true
        };
        """

        parser = Parser(script)
        pipeline = parser.parse()

        dag_builder = ASTDAGBuilder()
        dag = dag_builder.build_dag_from_ast(pipeline)

        assert len(dag.get_all_nodes()) == 3

        source_node = None
        load_node = None
        export_node = None

        for node_id in dag.get_all_nodes():
            attrs = dag.get_node_attributes(node_id)
            if attrs.get("type") == "SOURCE":
                source_node = node_id
            elif attrs.get("type") == "LOAD":
                load_node = node_id
            elif attrs.get("type") == "EXPORT":
                export_node = node_id

        assert source_node is not None
        assert load_node is not None
        assert export_node is not None

        assert source_node in dag.get_predecessors(load_node)
        assert load_node in dag.get_predecessors(export_node)

    def test_sql_block_in_dag(self):
        """Test assembling a DAG with SQL blocks."""
        script = """
        SOURCE users TYPE POSTGRES PARAMS {
            "connection": "${DB_CONN}",
            "table": "users"
        };

        LOAD users_table FROM users;

        CREATE TABLE customer_ltv AS
        SELECT
          customer_id,
          SUM(amount) AS total_spent
        FROM users_table;

        EXPORT
          SELECT * FROM customer_ltv
        TO "s3://bucket/customer_ltv.csv"
        TYPE CSV
        OPTIONS {
            "delimiter": ",",
            "header": true
        };
        """

        parser = Parser(script)
        pipeline = parser.parse()

        dag_builder = ASTDAGBuilder()
        dag = dag_builder.build_dag_from_ast(pipeline)

        assert len(dag.get_all_nodes()) == 4

        source_node = None
        load_node = None
        sql_block_node = None
        export_node = None

        for node_id in dag.get_all_nodes():
            attrs = dag.get_node_attributes(node_id)
            if attrs.get("type") == "SOURCE":
                source_node = node_id
            elif attrs.get("type") == "LOAD":
                load_node = node_id
            elif attrs.get("type") == "SQL_BLOCK":
                sql_block_node = node_id
            elif attrs.get("type") == "EXPORT":
                export_node = node_id

        assert source_node is not None
        assert load_node is not None
        assert sql_block_node is not None
        assert export_node is not None

        assert source_node in dag.get_predecessors(load_node)
        assert load_node in dag.get_predecessors(sql_block_node)
        assert sql_block_node in dag.get_predecessors(export_node)
