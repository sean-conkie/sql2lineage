"""Test the graph module."""

from pathlib import Path
from typing import Generator

import pytest

from sql2lineage.graph import LineageGraph
from sql2lineage.model import ParsedResult
from sql2lineage.parser import SQLLineageParser
from sql2lineage.types.model import LineageNode


class TestGraph:
    """Test the graph module."""

    @pytest.fixture(scope="class")
    def expression(self) -> Generator[ParsedResult, None, None]:
        """Fixture for the expression."""

        parser = SQLLineageParser(dialect="bigquery")

        with Path("tests/sql/example.sql").open("r", encoding="utf-8") as src:
            parsed_result = parser.extract_lineage(src.read())

        yield parsed_result

    def test_from_parsed(self, expression: ParsedResult):
        """Test the from_parsed method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        assert len(graph.graph.nodes) == 16

    def test_print_neighbourhood(
        self, expression: ParsedResult, capsys: pytest.CaptureFixture
    ):
        """Test the print_neighbourhood method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        graph.print_neighbourhood(
            graph.get_node_neighbours(node="big_orders", node_type="TABLE")
        )
        captured = capsys.readouterr()
        assert "big_orders" in captured.out

    def test_pretty_string(self, expression: ParsedResult):
        """Test the pretty_string method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        assert (
            "orders_with_tax --> filtered_orders [target_type: CTE, source_type: CTE, node_type: TABLE]"
            in graph.pretty_string()
        )

    def test_pretty_print(
        self, expression: ParsedResult, capsys: pytest.CaptureFixture
    ):
        """Test the pretty_print method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        graph.pretty_print()
        captured = capsys.readouterr()
        assert (
            "orders_with_tax --> filtered_orders [target_type: CTE, source_type: CTE, node_type: TABLE]"
            in captured.out
        )

    def test_get_node_lineage(self, expression: ParsedResult):
        """Test the get_node_lineage method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        nodes = graph.get_node_lineage(node="big_orders", node_type="TABLE")
        assert nodes == [
            [
                LineageNode(
                    source="raw.orders",
                    target="orders_with_tax",
                    node_type="TABLE",
                    source_type="TABLE",
                    target_type="CTE",
                ),
                LineageNode(
                    source="orders_with_tax",
                    target="filtered_orders",
                    node_type="TABLE",
                    source_type="CTE",
                    target_type="CTE",
                ),
                LineageNode(
                    source="filtered_orders",
                    target="big_orders",
                    node_type="TABLE",
                    source_type="CTE",
                    target_type="TABLE",
                ),
            ]
        ]

    def test_get_node_descendents(self, expression: ParsedResult):
        """Test the get_node_descendants method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        nodes = graph.get_node_descendants(node="orders_with_tax", node_type="TABLE")
        assert nodes == [
            [
                LineageNode(
                    source="orders_with_tax",
                    target="filtered_orders",
                    node_type="TABLE",
                    source_type="CTE",
                    target_type="CTE",
                ),
                LineageNode(
                    source="filtered_orders",
                    target="big_orders",
                    node_type="TABLE",
                    source_type="CTE",
                    target_type="TABLE",
                ),
            ]
        ]

    @pytest.mark.parametrize(
        "node, physical_only, expected",
        [
            pytest.param(
                "filtered_orders",
                False,
                [
                    [
                        LineageNode(
                            source="raw.orders",
                            target="orders_with_tax",
                            node_type="TABLE",
                            source_type="TABLE",
                            target_type="CTE",
                        ),
                        LineageNode(
                            source="orders_with_tax",
                            target="filtered_orders",
                            node_type="TABLE",
                            source_type="CTE",
                            target_type="CTE",
                        ),
                    ],
                    [
                        LineageNode(
                            source="filtered_orders",
                            target="big_orders",
                            node_type="TABLE",
                            source_type="CTE",
                            target_type="TABLE",
                        )
                    ],
                ],
                id="all_nodes",
            ),
            pytest.param(
                "filtered_orders",
                True,
                [
                    [
                        LineageNode(
                            source="raw.orders",
                            target="big_orders",
                            node_type="TABLE",
                            source_type="TABLE",
                            target_type="TABLE",
                        )
                    ]
                ],
                id="physical_only",
            ),
        ],
    )
    def test_get_node_neighbours(
        self,
        node: str,
        physical_only: bool,
        expected,
        expression: ParsedResult,
    ):
        """Test the get_node_neighbours method."""

        graph = LineageGraph()
        graph.from_parsed(expression.expressions)

        nodes = graph.get_node_neighbours(
            node=node, node_type="TABLE", physical_nodes_only=physical_only
        )
        assert nodes == expected
