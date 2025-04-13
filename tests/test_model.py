"""Test model."""

# pylint: disable=no-member

from sql2lineage.model import ColumnLineage, ParsedExpression, ParsedResult, SourceTable


class TestColumnLineage:
    """TestColumnLineage."""

    def test_serialize(self):
        """Test column lineage."""
        c = ColumnLineage(
            source="source_table",
            target="target_table",
            column="column_name",
            action="COPY",
        )

        assert c.model_dump() == {
            "source": "source_table",
            "target": "target_table",
            "column": "column_name",
            "action": "COPY",
        }


class TestSourceTable:
    """TestSourceTable."""

    def test_serialize(self):
        """Test column lineage."""
        source_table = SourceTable(
            source="source_table",
            target="target_table",
            alias="alias_name",
        )

        assert source_table.model_dump() == {
            "source": "source_table",
            "target": "target_table",
            "alias": "alias_name",
        }


class TestParsedExpression:
    """TestParsedExpression."""

    def test_serialize(self):
        """Test column lineage."""
        parsed_expression = ParsedExpression(target="target_table")
        parsed_expression.columns.add(
            ColumnLineage(
                source="source_table",
                target="target_table",
                column="column_name",
                action="COPY",
            )
        )

        parsed_expression.tables.add(
            SourceTable(
                source="source_table",
                target="target_table",
                alias="alias_name",
            )
        )

        assert parsed_expression.model_dump() == {
            "target": "target_table",
            "columns": [
                {
                    "source": "source_table",
                    "target": "target_table",
                    "column": "column_name",
                    "action": "COPY",
                }
            ],
            "tables": [
                {
                    "source": "source_table",
                    "target": "target_table",
                    "alias": "alias_name",
                }
            ],
            "subqueries": {},
        }


class TestParsedResult:
    """TestParsedResult."""

    def test_serialize(self):
        """Test column lineage."""
        parsed_expression = ParsedExpression(target="target_table")
        parsed_expression.columns.add(
            ColumnLineage(
                source="source_table",
                target="target_table",
                column="column_name",
                action="COPY",
            )
        )

        parsed_expression.tables.add(
            SourceTable(
                source="source_table",
                target="target_table",
                alias="alias_name",
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
                            "source": "source_table",
                            "target": "target_table",
                            "column": "column_name",
                            "action": "COPY",
                        }
                    ],
                    "tables": [
                        {
                            "source": "source_table",
                            "target": "target_table",
                            "alias": "alias_name",
                        }
                    ],
                    "subqueries": {},
                }
            ],
            "tables": [
                {
                    "source": "source_table",
                    "target": "target_table",
                    "alias": "alias_name",
                }
            ],
            "columns": [
                {
                    "source": "source_table",
                    "target": "target_table",
                    "column": "column_name",
                    "action": "COPY",
                }
            ],
        }
