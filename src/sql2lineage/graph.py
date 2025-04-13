"""LineageGraph.

This module defines a LineageGraph class that uses NetworkX to represent.
"""

from typing import List, Set

import networkx as nx

from sql2lineage.model import ColumnLineage, ParsedExpression, SourceTable


class LineageGraph:
    """LineageGraph.

    This class represents a directed graph for lineage tracking using NetworkX.
    """

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
        _attrs = (
            "type",
            "action",
        )

        _str = []

        for u, v, d in self.graph.edges(data=True):

            types_string = ""
            _types = []
            for attr in _attrs:
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
