"""Test the graph module."""

import pytest

from sql2lineage.graph import LineageGraph
from sql2lineage.model import (
    ColumnLineage,
    LineageResult,
    ParsedExpression,
    SourceTable,
)
from sql2lineage.parser import SQLLineageParser


class TestLinageGraph:
    """TestLinageGraph."""

    @pytest.fixture(scope="class")
    def graph(self):
        """Fixture for graph."""
        with open("tests/sql/example.sql", encoding="utf-8") as f:
            sql = f.read()

        parser = SQLLineageParser(dialect="bigquery")
        r = parser.extract_lineage(sql)

        graph = LineageGraph()
        graph.from_parsed(r.expressions)
        yield graph

    def test_add_table_edges(self):
        """Test add table edges."""
        graph = LineageGraph()
        graph.add_table_edges(
            {
                SourceTable(
                    source="source_table", target="target_table", alias="alias_name"
                ),
                SourceTable(
                    source="source_table2", target="target_table2", alias="alias_name2"
                ),
            }
        )

        assert len(graph.graph.edges) == 2
        assert ("source_table", "target_table") in graph.graph.edges
        assert ("source_table2", "target_table2") in graph.graph.edges

    def test_add_column_edges(self):
        """Test add column edges."""
        graph = LineageGraph()
        graph.add_column_edges(
            {
                ColumnLineage(
                    source="source_table",
                    target="target_table",
                    column="column_name",
                    action="COPY",
                ),
                ColumnLineage(
                    source="source_table2",
                    target="target_table2",
                    column="column_name2",
                    action="COPY",
                ),
            }
        )

        assert len(graph.graph.edges) == 2
        assert ("source_table", "target_table.column_name") in graph.graph.edges
        assert ("source_table2", "target_table2.column_name2") in graph.graph.edges

    def test_pretty_string(self):
        """Test pretty string."""
        graph = LineageGraph()
        graph.add_table_edges(
            {
                SourceTable(
                    source="source_table", target="target_table", alias="alias_name"
                ),
                SourceTable(
                    source="source_table2", target="target_table2", alias="alias_name2"
                ),
            }
        )
        graph.add_column_edges(
            {
                ColumnLineage(
                    source="source_table",
                    target="target_table",
                    column="column_name",
                    action="COPY",
                ),
                ColumnLineage(
                    source="source_table2",
                    target="target_table2",
                    column="column_name2",
                    action="COPY",
                ),
            }
        )

        expected = (
            (
                "source_table2 --> target_table2 [type: TABLE]\n"
                "source_table2 --> target_table2.column_name2 [type: COLUMN, action: COPY]\n"
                "source_table --> target_table [type: TABLE]\n"
                "source_table --> target_table.column_name [type: COLUMN, action: COPY]"
            )
            .split("\n")
            .sort()
        )

        result = graph.pretty_string().split("\n").sort()

        assert expected == result

    def test_from_parsed(self):
        """Test from parsed."""
        graph = LineageGraph()
        graph.from_parsed(
            [
                ParsedExpression.model_validate(
                    {
                        "target": "expr000",
                        "columns": [
                            {
                                "target": "expr000",
                                "column": "age",
                                "source": "raw.user_details.age",
                                "action": "COPY",
                            },
                            {
                                "target": "expr000",
                                "column": "id",
                                "source": "raw.users.id",
                                "action": "COPY",
                            },
                            {
                                "target": "expr000",
                                "column": "name",
                                "source": "raw.users.name",
                                "action": "COPY",
                            },
                        ],
                        "tables": [
                            {
                                "target": "expr000",
                                "source": "raw.user_details",
                                "alias": None,
                            }
                        ],
                        "subqueries": {},
                    }
                )
            ]
        )

        assert len(graph.graph.edges) == 4

    @pytest.mark.parametrize(
        "node, node_type, expected",
        [
            pytest.param(
                "orders_with_tax.order_id",
                "COLUMN",
                [
                    [
                        LineageResult(
                            source="raw.orders.order_id",
                            target="orders_with_tax.order_id",
                            type="COLUMN",
                            action="COPY",
                        )
                    ]
                ],
                id="COLUMN",
            ),
            pytest.param(
                "big_orders",
                "TABLE",
                [
                    [
                        LineageResult(
                            source="raw.orders",
                            target="orders_with_tax",
                            type="TABLE",
                            action=None,
                        ),
                        LineageResult(
                            source="orders_with_tax",
                            target="filtered_orders",
                            type="TABLE",
                            action=None,
                        ),
                        LineageResult(
                            source="filtered_orders",
                            target="big_orders",
                            type="TABLE",
                            action=None,
                        ),
                    ]
                ],
                id="TABLE",
            ),
        ],
    )
    def test_get_node_lineage(self, graph, node, node_type, expected):
        """Test get node lineage."""
        assert graph.get_node_lineage(node, node_type) == expected

    @pytest.mark.parametrize(
        "node, node_type, expected",
        [
            pytest.param(
                "raw.orders.order_id",
                "COLUMN",
                [
                    [
                        LineageResult(
                            source="raw.orders.order_id",
                            target="orders_with_tax.order_id",
                            type="COLUMN",
                            action="COPY",
                        ),
                        LineageResult(
                            source="orders_with_tax.order_id",
                            target="filtered_orders.order_id",
                            type="COLUMN",
                            action="COPY",
                        ),
                        LineageResult(
                            source="filtered_orders.order_id",
                            target="big_orders.order_id",
                            type="COLUMN",
                            action="COPY",
                        ),
                    ]
                ],
                id="COLUMN",
            ),
            pytest.param(
                "raw.orders",
                "TABLE",
                [
                    [
                        LineageResult(
                            source="raw.orders",
                            target="orders_with_tax",
                            type="TABLE",
                            action=None,
                        ),
                        LineageResult(
                            source="orders_with_tax",
                            target="filtered_orders",
                            type="TABLE",
                            action=None,
                        ),
                        LineageResult(
                            source="filtered_orders",
                            target="big_orders",
                            type="TABLE",
                            action=None,
                        ),
                    ]
                ],
                id="TABLE",
            ),
        ],
    )
    def test_get_node_descendants(self, graph, node, node_type, expected):
        """Test get node descendants."""
        assert graph.get_node_descendants(node, node_type) == expected

    @pytest.mark.parametrize(
        "node, node_type, expected",
        [
            pytest.param(
                "filtered_orders.order_id",
                "COLUMN",
                [
                    [
                        LineageResult(
                            source="raw.orders.order_id",
                            target="orders_with_tax.order_id",
                            type="COLUMN",
                            action="COPY",
                        ),
                        LineageResult(
                            source="orders_with_tax.order_id",
                            target="filtered_orders.order_id",
                            type="COLUMN",
                            action="COPY",
                        ),
                    ],
                    [
                        LineageResult(
                            source="filtered_orders.order_id",
                            target="big_orders.order_id",
                            type="COLUMN",
                            action="COPY",
                        ),
                    ],
                ],
                id="COLUMN",
            ),
            pytest.param(
                "filtered_orders",
                "TABLE",
                [
                    [
                        LineageResult(
                            source="raw.orders",
                            target="orders_with_tax",
                            type="TABLE",
                            action=None,
                        ),
                        LineageResult(
                            source="orders_with_tax",
                            target="filtered_orders",
                            type="TABLE",
                            action=None,
                        ),
                    ],
                    [
                        LineageResult(
                            source="filtered_orders",
                            target="big_orders",
                            type="TABLE",
                            action=None,
                        ),
                    ],
                ],
                id="TABLE",
            ),
        ],
    )
    def test_get_node_neighbours(self, graph, node, node_type, expected):
        """Test get node neighbours."""
        assert graph.get_node_neighbours(node, node_type) == expected
