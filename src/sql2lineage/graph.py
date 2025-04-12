"""LineageGraph.

This module defines a LineageGraph class that uses NetworkX to represent.
"""

from typing import List, Tuple

import networkx as nx


class LineageGraph:
    """A class to represent lineage information as a directed graph."""

    def __init__(self):
        self.graph = nx.DiGraph()

    def add_table_lineage(self, output_table: str, source_tables: List[str]):
        """Add a lineage relationship between a source table and an output table in the graph.

        This method creates directed edges in the graph, representing the dependency
        of the output table on the source tables.

        Args:
            output_table (str): The name of the output table.
            source_tables (List[str]): A list of source table names that the output table depends on.

        Returns:
            None

        """
        for src in source_tables:
            self.graph.add_edge(src, output_table, type="table")

    def add_column_lineage(
        self, output_table: str, column_mappings: List[Tuple[str, str]]
    ):
        """Add column-level lineage information to the graph.

        This method creates edges in the graph to represent the lineage
        between source columns and output columns for a specific output table.

        Args:
            output_table (str): The name of the output table to which the columns belong.
            column_mappings (List[Tuple[str, str]]): A list of tuples where each tuple
                contains the source column name (str) and the corresponding output column name (str).

        Example:
            Given an output table "table1" and column mappings [("source_col1", "output_col1")],
            this method will create an edge in the graph from "source_col1" to "table1.output_col1".

        """
        for output_col, source_col in column_mappings:
            src_node = f"{source_col}"
            tgt_node = f"{output_table}.{output_col}"
            self.graph.add_edge(src_node, tgt_node, type="column")

    def print_graph(self):
        """Print the edges of the graph along with their types.

        For each edge in the graph, this method prints the source node,
        destination node, and the type of the edge in the format:
        "source_node --> destination_node [edge_type]".

        Example:
            If the graph contains an edge from 'A' to 'B' with type 'dependency',
            the output will be:
            A --> B [dependency]

        """
        for u, v, data in self.graph.edges(data=True):
            print(f"{u} --> {v} [{data['type']}]")
