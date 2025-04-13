"""Test the graph module."""

from sql2lineage.graph import LineageGraph
from sql2lineage.model import ColumnLineage, ParsedExpression, SourceTable


class TestLinageGraph:
    """TestLinageGraph."""

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
