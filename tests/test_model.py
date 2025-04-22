"""Test model."""

# pylint: disable=no-member

from sql2lineage.model import ColumnLineage, ParsedExpression, ParsedResult, SourceTable


class TestColumnLineage:
    """TestColumnLineage."""

    def test_serialize(self):
        """Test column lineage."""
        c = ColumnLineage(
            source="source_table.column_name",
            target="target_table",
            action="COPY",
            table_type="TABLE",
        )

        assert c.model_dump() == {
            "source": "source_table.column_name",
            "target": "target_table",
            "action": "COPY",
            "table_type": "TABLE",
            "node_type": "COLUMN",
        }


class TestSourceTable:
    """TestSourceTable."""

    def test_serialize(self):
        """Test column lineage."""
        source_table = SourceTable(
            source="source_table",
            target="target_table",
            alias="alias_name",
            type="TABLE",
        )

        assert source_table.model_dump() == {
            "source": "source_table",
            "target": "target_table",
            "alias": "alias_name",
            "node_type": "TABLE",
            "type": "TABLE",
        }


class TestParsedExpression:
    """TestParsedExpression."""

    def test_serialize(self):
        """Test column lineage."""
        parsed_expression = ParsedExpression(target="target_table", expression="")
        parsed_expression.columns.add(
            ColumnLineage(
                source="source_table.column_name",
                target="target_table",
                action="COPY",
                table_type="TABLE",
            )
        )

        parsed_expression.tables.add(
            SourceTable(
                source="source_table",
                target="target_table",
                alias="alias_name",
                type="TABLE",
            )
        )

        assert parsed_expression.model_dump() == {
            "target": "target_table",
            "columns": [
                {
                    "source": "source_table.column_name",
                    "target": "target_table",
                    "action": "COPY",
                    "table_type": "TABLE",
                }
            ],
            "tables": [
                {
                    "source": "source_table",
                    "target": "target_table",
                    "alias": "alias_name",
                    "table_type": "TABLE",
                }
            ],
            "subqueries": {},
            "expression": "",
        }


class TestParsedResult:
    """TestParsedResult."""

    def test_serialize(self):
        """Test column lineage."""
        parsed_expression = ParsedExpression(target="target_table", expression="")
        parsed_expression.columns.add(
            ColumnLineage(
                source="source_table.column_name",
                target="target_table",
                action="COPY",
                table_type="TABLE",
            )
        )

        parsed_expression.tables.add(
            SourceTable(
                source="source_table",
                target="target_table",
                alias="alias_name",
                type="TABLE",
            )
        )

        parsed_result = ParsedResult()
        parsed_result.add(parsed_expression)

        assert parsed_result.model_dump() == {
            "expressions": [
                {
                    "target": "target_table",
                    "columns": [
                        {
                            "source": "source_table.column_name",
                            "target": "target_table",
                            "action": "COPY",
                            "table_type": "TABLE",
                        }
                    ],
                    "tables": [
                        {
                            "source": "source_table",
                            "target": "target_table",
                            "alias": "alias_name",
                            "table_type": "TABLE",
                        }
                    ],
                    "subqueries": {},
                    "expression": "",
                }
            ],
            "tables": [
                {
                    "source": "source_table",
                    "target": "target_table",
                    "alias": "alias_name",
                    "table_type": "TABLE",
                }
            ],
            "columns": [
                {
                    "source": "source_table.column_name",
                    "target": "target_table",
                    "action": "COPY",
                    "table_type": "TABLE",
                }
            ],
        }
