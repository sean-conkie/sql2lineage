"""LineageGraph.

This module defines a LineageGraph class that uses NetworkX to represent.
"""

from typing import List, Literal, Optional, Set

import networkx as nx

from sql2lineage.model import (
    ColumnLineage,
    LineageResult,
    ParsedExpression,
    SourceTable,
)


class LineageGraph:
    """LineageGraph.

    This class represents a directed graph for lineage tracking using NetworkX.
    """

    _attrs = (
        "type",
        "action",
    )

    def __init__(self):
        self.graph = nx.DiGraph()

    def add_table_edges(self, table_edges: Set[SourceTable]):
        """Add edges representing table relationships to the graph.

        This method takes a set of `SourceTable` objects, where each object
        represents a relationship between a source table and a target table.
        It then adds these relationships as edges to the graph with the edge
        type set to "TABLE".

        Args:
            table_edges (Set[SourceTable]): A set of `SourceTable` objects
                representing the edges to be added to the graph. Each edge
                contains a source table and a target table.

        """
        for edge in table_edges:
            self.graph.add_edge(edge.source, edge.target, type="TABLE")

    def add_column_edges(self, column_edges: Set[ColumnLineage]):
        """Add edges representing column-level lineage to the graph.

        Each edge connects a source node to a target node with a specific column
        and includes metadata such as the type of edge and the action performed.

        Args:
            column_edges (Set[ColumnLineage]): A set of ColumnLineage objects,
                where each object represents a lineage relationship between a
                source and a target column, along with the action performed.

        """
        for edge in column_edges:
            self.graph.add_edge(
                edge.source,
                f"{edge.target}.{edge.column}",
                type="COLUMN",
                action=edge.action,
            )

    def pretty_string(self) -> str:
        """Generate a human-readable string representation of the graph's edges.

        This method iterates through all edges in the graph and constructs a string
        representation for each edge, including optional attributes such as "type"
        and "action" if they are present in the edge's data.

        Returns:
            str: A string where each line represents an edge in the format:
                 "source_node --> target_node [attribute: value, ...]"
                 If no attributes are present, the edge is represented as:
                 "source_node --> target_node".

        """
        _str = []

        for u, v, d in self.graph.edges(data=True):

            types_string = ""
            _types = []
            for attr in self._attrs:
                if d.get(attr):
                    _types.append(f"{attr}: {d[attr]}")

            if _types:
                types_string = f" [{', '.join(_types)}]"

            _str.append(f"{u} --> {v}{types_string}")

        return "\n".join(_str)

    def pretty_print(self):
        """Print the graph in a human-readable format."""
        print(self.pretty_string())

    def print_neighbourhood(self, paths: List[List[LineageResult]]):
        """Print the neighborhood of nodes for each path in the provided list of paths.

        Args:
            paths (List[List[LineageResult]]): A list of paths, where each path is a list of
                LineageResult objects representing nodes in the graph.

        Each path is printed with its nodes in sequence, and each node is displayed using its
        `model_dump()` representation.

        """
        print("Neighbourhood:")
        for path in paths:
            for node in path:
                print("  ↳", node.model_dump())

    def from_parsed(self, parsed_expressions: List[ParsedExpression]):
        """Create a lineage graph from parsed expressions.

        This method takes a list of `ParsedExpression` objects, which represent
        SQL expressions that have been parsed. It extracts table and column
        lineage information from these expressions and adds them to the graph.

        Args:
            parsed_expressions (List[ParsedExpression]): A list of `ParsedExpression`
                objects representing parsed SQL expressions.

        """
        for expression in parsed_expressions:
            self.add_table_edges(expression.tables)
            self.add_column_edges(expression.columns)

    def is_root_node(
        self, node: str, node_type: Literal["COLUMN", "TABLE"] = "COLUMN"
    ) -> bool:
        """Determine if a given node is a root node in the graph.

        A root node is defined as a node that has no incoming edges of the specified type.

        Args:
            node (str): The name of the node to check.
            node_type (Literal["COLUMN", "TABLE"], optional): The type of the node to check for.
                Defaults to "COLUMN".

        Returns:
            bool: True if the node is a root node of the specified type, False otherwise.

        """
        return all(
            self.graph.edges[u, node].get("type") != node_type
            for u in self.graph.predecessors(node)
        )

    def is_leaf_node(
        self, node: str, node_type: Literal["COLUMN", "TABLE"] = "COLUMN"
    ) -> bool:
        """Determine if a given node in the graph is a leaf node.

        A leaf node is defined as a node that does not have any outgoing edges
        of the specified type ("COLUMN" or "TABLE").

        Args:
            node (str): The identifier of the node to check.
            node_type (Literal["COLUMN", "TABLE"], optional): The type of edge to consider
                when determining if the node is a leaf. Defaults to "COLUMN".

        Returns:
            bool: True if the node is a leaf node of the specified type, False otherwise.

        """
        return all(
            self.graph.edges[node, u].get("type") != node_type
            for u in self.graph.successors(node)
        )

    def get_node_lineage(
        self,
        node: str,
        node_type: Literal["COLUMN", "TABLE"] = "COLUMN",
        max_steps: Optional[int] = None,
    ) -> List[List[LineageResult]]:
        """Retrieve the lineage of a specific node in the graph.

        This method identifies all possible paths from the root nodes (true sources)
        to the specified node and provides detailed information about each step in
        the lineage.

        Args:
            node (str): The target node for which lineage is to be retrieved.
            node_type (Literal["COLUMN", "TABLE"], optional): The type of the node.
                Defaults to "COLUMN".
            max_steps (int, optional): The maximum number of steps to extract from each path.
                Defaults to None.

        Returns:
            List[List[LineageResult]]: A list of chains, where each chain is a list
            of dictionaries representing the steps in the lineage.

        """
        chains = []
        ancestors = nx.ancestors(self.graph, node)
        # Step 1: Identify root nodes (true sources)
        root_nodes = [node for node in ancestors if self.is_root_node(node, node_type)]

        for source in root_nodes:
            for path in nx.all_simple_paths(self.graph, source, node):
                step_info = self._extract_path_steps(path, max_steps)
                chains.append(step_info)

        return chains

    def get_node_descendants(
        self,
        source_node: str,
        node_type: Literal["COLUMN", "TABLE"] = "COLUMN",
        max_steps: Optional[int] = None,
    ) -> List[List[LineageResult]]:
        """Retrieve all descendant nodes of a given source node in the graph, grouped by paths.

        This method identifies all descendant nodes of the specified `source_node` in the graph
        and organizes them into chains of paths. Each path represents a sequence of nodes
        from the `source_node` to a root node of the specified `node_type`.

        Args:
            source_node (str): The starting node in the graph from which to find descendants.
            node_type (Literal["COLUMN", "TABLE"], optional): The type of node to consider as
                root nodes in the graph. Defaults to "COLUMN".
            max_steps (int, optional): The maximum number of steps to extract from each path.
                Defaults to None.

        Returns:
            List[List[LineageResult]]: A list of chains, where each chain is a list of node names
            representing a path from the `source_node` to a root node of the specified type.

        """
        descendents = nx.descendants(self.graph, source_node)

        root_nodes = [
            node for node in descendents if self.is_leaf_node(node, node_type)
        ]
        chains = []
        for source in root_nodes:
            for path in nx.all_simple_paths(self.graph, source_node, source):
                step_info = self._extract_path_steps(path, max_steps)
                chains.append(step_info)
        return chains

    def get_node_neighbours(
        self,
        node: str,
        node_type: Literal["COLUMN", "TABLE"] = "COLUMN",
        max_steps: Optional[int] = None,
    ) -> List[List[LineageResult]]:
        """Retrieve the neighboring nodes of a given node in the lineage graph.

        This method fetches both the lineage (ancestors) and descendants of the specified node
        up to a certain number of steps, if specified.

        Args:
            node (str): The name of the node for which neighbors are to be retrieved.
            node_type (Literal["COLUMN", "TABLE"], optional): The type of the node, either "COLUMN" or "TABLE".
                Defaults to "COLUMN".
            max_steps (Optional[int], optional): The maximum number of steps to traverse in the graph.
                If None, the traversal is unbounded. Defaults to None.

        Returns:
            List[List[LineageResult]]: A list of chains, where each chain is a list of LineageResult objects
            representing the lineage or descendant paths of the given node.

        """
        chains = []
        chains.extend(self.get_node_lineage(node, node_type, max_steps))
        chains.extend(self.get_node_descendants(node, node_type, max_steps))
        return chains

    def _extract_path_steps(
        self, path, max_steps: Optional[int] = None
    ) -> List[LineageResult]:
        """Extract detailed information about each step in a given path within the graph.

        Args:
            path (list): A list of nodes representing a path in the graph.
            max_steps (int, optional): The maximum number of steps to extract from the path.
                Defaults to None.

        Returns:
            list: A list of dictionaries, where each dictionary contains information
                  about the source node, target node, and any additional attributes
                  associated with the edge connecting them.

        """
        step_info = []
        for i in range(max_steps or len(path) - 1):
            u, v = path[i], path[i + 1]
            edge = self.graph.get_edge_data(u, v)

            lineage_result = {
                "source": u,
                "target": v,
            }

            for attr in self._attrs:
                if edge.get(attr):
                    lineage_result[attr] = edge[attr]

            step_info.append(LineageResult.model_validate(lineage_result))

        return step_info
