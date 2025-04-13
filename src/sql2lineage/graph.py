"""LineageGraph.

This module defines a LineageGraph class that uses NetworkX to represent.
"""

from typing import List, Literal, Set

import networkx as nx

from sql2lineage.model import ColumnLineage, ParsedExpression, SourceTable


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
            self.graph.edges[u, node]["type"] != node_type
            for u in self.graph.predecessors(node)
        )

    def get_node_lineage(
        self, node: str, node_type: Literal["COLUMN", "TABLE"] = "COLUMN"
    ):
        """Retrieve the lineage of a specific node in the graph.

        This method identifies all possible paths from the root nodes (true sources)
        to the specified node and provides detailed information about each step in
        the lineage.

        Args:
            node (str): The target node for which lineage is to be retrieved.
            node_type (Literal["COLUMN", "TABLE"], optional): The type of the node.
                Defaults to "COLUMN".

        Returns:
            List[List[Dict[str, Any]]]: A list of chains, where each chain is a list
            of dictionaries representing the steps in the lineage. Each dictionary
            contains:
                - "from" (str): The source node of the edge.
                - "to" (str): The destination node of the edge.
                - Additional attributes of the edge, if present in the graph.

        """
        chains = []
        ancestors = nx.ancestors(self.graph, node)
        # Step 1: Identify root nodes (true sources)
        root_nodes = [node for node in ancestors if self.is_root_node(node, node_type)]

        for source in root_nodes:
            for path in nx.all_simple_paths(self.graph, source, node):
                step_info = []
                for i in range(len(path) - 1):
                    u, v = path[i], path[i + 1]
                    edge = self.graph.get_edge_data(u, v)

                    lineage_result = {
                        "source": u,
                        "target": v,
                    }

                    for attr in self._attrs:
                        if edge.get(attr):
                            lineage_result[attr] = edge[attr]

                    step_info.append(lineage_result)
                chains.append(step_info)

        return chains
