"""DAG builder for SQLFlow pipelines."""

from typing import Any, Dict, List, Optional

import networkx as nx


class PipelineDAG:
    """Directed Acyclic Graph representation of a pipeline."""

    def __init__(self):
        """Initialize a PipelineDAG."""
        self.graph = nx.DiGraph()
        self.node_attributes: Dict[str, Dict[str, str]] = {}

    def add_node(self, node_id: str, **attrs) -> None:
        """Add a node to the DAG.

        Args:
        ----
            node_id: Unique identifier for the node
            **attrs: Node attributes

        """
        self.graph.add_node(node_id)
        self.node_attributes[node_id] = attrs

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add an edge between nodes.

        Args:
        ----
            from_node: Source node ID
            to_node: Target node ID

        """
        self.graph.add_edge(from_node, to_node)

    def get_node_attributes(self, node_id: str) -> Dict[str, str]:
        """Get attributes for a node.

        Args:
        ----
            node_id: Node ID

        Returns:
        -------
            Dict of node attributes

        """
        return self.node_attributes.get(node_id, {})

    def get_all_nodes(self) -> List[str]:
        """Get all node IDs.

        Returns
        -------
            List of node IDs

        """
        return list(self.graph.nodes())

    def get_predecessors(self, node_id: str) -> List[str]:
        """Get predecessors of a node.

        Args:
        ----
            node_id: Node ID

        Returns:
        -------
            List of predecessor node IDs

        """
        return list(self.graph.predecessors(node_id))

    def get_successors(self, node_id: str) -> List[str]:
        """Get successors of a node.

        Args:
        ----
            node_id: Node ID

        Returns:
        -------
            List of successor node IDs

        """
        return list(self.graph.successors(node_id))

    def has_cycles(self) -> bool:
        """Check if the DAG has cycles.

        Returns
        -------
            True if the DAG has cycles, False otherwise

        """
        return not nx.is_directed_acyclic_graph(self.graph)

    def find_cycle(self) -> Optional[List[str]]:
        """Find a cycle in the DAG.

        Returns:
            A list of nodes representing the cycle, or None if no cycle exists.
        """
        try:
            # find_cycle returns edges, so we extract the nodes.
            cycle_edges = list(nx.find_cycle(self.graph))
            # To get a clean node path, we take the source node of each edge.
            return [u for u, v in cycle_edges]
        except nx.NetworkXNoCycle:
            return None

    def get_topological_sort(self) -> List[str]:
        """Get a topological sort of the DAG.

        Returns:
            A list of nodes in topological order.
        """
        return list(nx.topological_sort(self.graph))

    def get_independent_nodes(self) -> List[str]:
        """Get all nodes with no incoming dependencies."""
        return [node for node, in_degree in self.graph.in_degree if in_degree == 0]

    def remove_node(self, node_id: str):
        """Remove a node and its outgoing edges from the graph."""
        if self.graph.has_node(node_id):
            self.graph.remove_node(node_id)


class DAGBuilder:
    """Builds a DAG from a pipeline."""

    def build_dag(self, pipeline_steps: List[Dict[str, Any]]) -> PipelineDAG:
        """Build a DAG from pipeline steps.

        Args:
        ----
            pipeline_steps: List of pipeline steps

        Returns:
        -------
            PipelineDAG

        """
        dag = PipelineDAG()

        for step in pipeline_steps:
            dag.add_node(
                step["id"],
                label=step.get("name", step["id"]),
                type=step.get("type", "unknown"),
            )

        for step in pipeline_steps:
            if "depends_on" in step:
                for dependency in step["depends_on"]:
                    dag.add_edge(dependency, step["id"])

        return dag
