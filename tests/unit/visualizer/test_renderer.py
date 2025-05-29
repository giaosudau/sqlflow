"""Tests for visualizer renderer functionality."""

import tempfile
from unittest.mock import MagicMock

import pytest

from sqlflow.visualizer.dag_builder import PipelineDAG
from sqlflow.visualizer.renderer import Renderer


class TestRenderer:
    """Test Renderer functionality."""

    @pytest.fixture
    def renderer(self):
        """Create a Renderer instance for testing."""
        return Renderer()

    @pytest.fixture
    def mock_dag(self):
        """Create a mock PipelineDAG for testing."""
        dag = MagicMock(spec=PipelineDAG)

        # Configure the mock to return test data
        dag.get_all_nodes.return_value = ["source1", "transform1", "export1"]

        def mock_get_node_attributes(node_id):
            attrs = {
                "source1": {"label": "User Data", "type": "source"},
                "transform1": {"label": "Clean Data", "type": "transform"},
                "export1": {"label": "Save Results", "type": "export"},
            }
            return attrs.get(node_id, {})

        def mock_get_successors(node_id):
            successors = {
                "source1": ["transform1"],
                "transform1": ["export1"],
                "export1": [],
            }
            return successors.get(node_id, [])

        dag.get_node_attributes.side_effect = mock_get_node_attributes
        dag.get_successors.side_effect = mock_get_successors

        return dag

    @pytest.fixture
    def simple_dag(self):
        """Create a simple mock DAG for basic testing."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = ["node1"]
        dag.get_node_attributes.return_value = {"label": "Node 1", "type": "source"}
        dag.get_successors.return_value = []
        return dag

    @pytest.fixture
    def empty_dag(self):
        """Create an empty mock DAG for edge case testing."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = []
        dag.get_node_attributes.return_value = {}
        dag.get_successors.return_value = []
        return dag

    def test_render_dot_basic_structure(self, renderer, simple_dag):
        """Test basic DOT rendering structure."""
        result = renderer.render_dot(simple_dag)

        # Should contain basic DOT structure
        assert result.startswith("digraph G {")
        assert result.endswith("}")
        assert "node1" in result

    def test_render_dot_complete_pipeline(self, renderer, mock_dag):
        """Test DOT rendering with a complete pipeline."""
        result = renderer.render_dot(mock_dag)

        # Should contain all nodes
        assert "source1" in result
        assert "transform1" in result
        assert "export1" in result

        # Should contain node labels
        assert "User Data" in result
        assert "Clean Data" in result
        assert "Save Results" in result

        # Should contain edges
        assert 'source1" -> "transform1"' in result
        assert 'transform1" -> "export1"' in result

    def test_render_dot_node_styling(self, renderer, mock_dag):
        """Test that different node types get appropriate styling."""
        result = renderer.render_dot(mock_dag)

        # Source nodes should have blue styling
        assert 'fillcolor="#e6f3ff"' in result

        # Transform nodes should have green styling
        assert 'fillcolor="#e6ffe6"' in result

        # Export nodes should have orange styling
        assert 'fillcolor="#fff0e6"' in result

    def test_render_dot_empty_dag(self, renderer, empty_dag):
        """Test DOT rendering with an empty DAG."""
        result = renderer.render_dot(empty_dag)

        # Should still have basic structure
        assert result.startswith("digraph G {")
        assert result.endswith("}")

        # Should only contain the digraph wrapper
        lines = [line.strip() for line in result.split("\n") if line.strip()]
        assert lines == ["digraph G {", "}"]

    def test_render_dot_unknown_node_type(self, renderer):
        """Test DOT rendering with unknown node type."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = ["unknown_node"]
        dag.get_node_attributes.return_value = {"label": "Unknown", "type": "unknown"}
        dag.get_successors.return_value = []

        result = renderer.render_dot(dag)

        # Should handle unknown types gracefully (no styling)
        assert "unknown_node" in result
        assert "Unknown" in result
        # Should not contain any fillcolor for unknown type
        assert "fillcolor=" not in result

    def test_render_dot_node_without_label(self, renderer):
        """Test DOT rendering when node has no label attribute."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = ["node_id"]
        dag.get_node_attributes.return_value = {"type": "source"}  # No label
        dag.get_successors.return_value = []

        result = renderer.render_dot(dag)

        # Should use node_id as label when no label is provided
        assert 'label="node_id"' in result

    def test_render_dot_node_without_type(self, renderer):
        """Test DOT rendering when node has no type attribute."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = ["node1"]
        dag.get_node_attributes.return_value = {"label": "Test Node"}  # No type
        dag.get_successors.return_value = []

        result = renderer.render_dot(dag)

        # Should handle missing type gracefully (no styling)
        assert "node1" in result
        assert "Test Node" in result
        # Should not have any fillcolor for unknown/missing type
        assert "fillcolor=" not in result

    def test_render_dot_complex_dependencies(self, renderer):
        """Test DOT rendering with complex dependency structure."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = ["a", "b", "c", "d"]

        def mock_get_node_attributes(node_id):
            return {"label": f"Node {node_id.upper()}", "type": "transform"}

        def mock_get_successors(node_id):
            # Complex dependency: a -> b, a -> c, b -> d, c -> d
            dependencies = {
                "a": ["b", "c"],
                "b": ["d"],
                "c": ["d"],
                "d": [],
            }
            return dependencies.get(node_id, [])

        dag.get_node_attributes.side_effect = mock_get_node_attributes
        dag.get_successors.side_effect = mock_get_successors

        result = renderer.render_dot(dag)

        # Should contain all edges
        assert '"a" -> "b"' in result
        assert '"a" -> "c"' in result
        assert '"b" -> "d"' in result
        assert '"c" -> "d"' in result

    def test_render_html_method_exists(self, renderer, mock_dag):
        """Test that render_html method exists and can be called."""
        with tempfile.NamedTemporaryFile(suffix=".html") as tmp_file:
            # Should not raise an exception (method is defined but not implemented)
            result = renderer.render_html(mock_dag, tmp_file.name)
            assert result is None  # Currently returns None

    def test_render_png_method_exists(self, renderer, mock_dag):
        """Test that render_png method exists and can be called."""
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp_file:
            # Should not raise an exception (method is defined but not implemented)
            result = renderer.render_png(mock_dag, tmp_file.name)
            assert result is None  # Currently returns None

    def test_renderer_instantiation(self):
        """Test that Renderer can be instantiated."""
        renderer = Renderer()
        assert renderer is not None
        assert isinstance(renderer, Renderer)

    def test_render_dot_with_special_characters(self, renderer):
        """Test DOT rendering with special characters in labels."""
        dag = MagicMock(spec=PipelineDAG)
        dag.get_all_nodes.return_value = ["node1"]
        dag.get_node_attributes.return_value = {
            "label": 'Node with "quotes" and symbols!@#$%',
            "type": "source",
        }
        dag.get_successors.return_value = []

        result = renderer.render_dot(dag)

        # Should handle special characters in labels
        assert "node1" in result
        assert 'Node with "quotes" and symbols!@#$%' in result

    def test_render_dot_multiple_calls_same_dag(self, renderer, mock_dag):
        """Test that multiple calls to render_dot produce consistent results."""
        result1 = renderer.render_dot(mock_dag)
        result2 = renderer.render_dot(mock_dag)

        # Should produce identical results
        assert result1 == result2

    def test_render_dot_dag_method_calls(self, renderer, mock_dag):
        """Test that render_dot calls the expected DAG methods."""
        renderer.render_dot(mock_dag)

        # Should call get_all_nodes to get the node list
        mock_dag.get_all_nodes.assert_called()

        # Should call get_node_attributes for each node
        assert mock_dag.get_node_attributes.call_count >= 3

        # Should call get_successors for each node
        assert mock_dag.get_successors.call_count >= 3
