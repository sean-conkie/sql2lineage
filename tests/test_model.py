"""Test model."""

# pylint: disable=no-member
import pytest
import sqlglot
from sqlglot import Expression

from sql2lineage.model import (
    ColumnLineage,
    DataColumn,
    DataTable,
    ParsedExpression,
    ParsedResult,
    TableLineage,
)
from sql2lineage.utils import SimpleTupleStore

EXPRESSION = sqlglot.parse("select current_timestamp", read="bigquery")[0]
assert EXPRESSION is not None, "Failed to parse expression"
PARSED_RESULT = ParsedResult()
PARSED_RESULT.add(
    ParsedExpression(
        target=DataTable(name="target", type="TABLE"),
        columns={
            ColumnLineage(
                target=DataColumn(
                    table=DataTable(name="target", type="TABLE"),
                    name="target_column",
                ),
                source=DataColumn(
                    table=DataTable(name="source", type="TABLE"),
                    name="source_column",
                ),
                action="COPY",
            )
        },
        tables={
            TableLineage(
                target=DataTable(name="target", type="TABLE"),
                source=DataTable(name="source", type="TABLE"),
                alias="alias",
            )
        },
        expression=EXPRESSION,
    )  # type: ignore
)


class TestSerialisation:
    """Test serialisation."""

    @pytest.mark.parametrize(
        "obj, expected",
        [
            pytest.param(
                ParsedExpression(
                    target=DataTable(name="target", type="TABLE"),
                    columns={
                        ColumnLineage(
                            target=DataColumn(
                                table=DataTable(name="target", type="TABLE"),
                                name="target_column",
                            ),
                            source=DataColumn(
                                table=DataTable(name="source", type="TABLE"),
                                name="source_column",
                            ),
                            action="COPY",
                        )
                    },
                    tables={
                        TableLineage(
                            target=DataTable(name="target", type="TABLE"),
                            source=DataTable(name="source", type="TABLE"),
                            alias="alias",
                        )
                    },
                    expression=EXPRESSION,
                ),  # type: ignore
                {
                    "expression": EXPRESSION.sql(pretty=True),
                    "target": {
                        "name": "target",
                        "type": "TABLE",
                    },
                    "columns": [
                        {
                            "target": {
                                "table": {
                                    "name": "target",
                                    "type": "TABLE",
                                },
                                "name": "target_column",
                            },
                            "source": {
                                "table": {
                                    "name": "source",
                                    "type": "TABLE",
                                },
                                "name": "source_column",
                            },
                            "action": "COPY",
                            "source_type": "TABLE",
                            "target_type": "TABLE",
                            "node_type": "COLUMN",
                        }
                    ],
                    "tables": [
                        {
                            "target": {
                                "name": "target",
                                "type": "TABLE",
                            },
                            "source": {
                                "name": "source",
                                "type": "TABLE",
                            },
                            "alias": "alias",
                            "source_type": "TABLE",
                            "target_type": "TABLE",
                            "node_type": "TABLE",
                        }
                    ],
                    "subqueries": {},
                },
                id="ParsedExpression",
            ),
            pytest.param(
                PARSED_RESULT,
                {
                    "expressions": [
                        {
                            "expression": EXPRESSION.sql(pretty=True),
                            "target": {
                                "name": "target",
                                "type": "TABLE",
                            },
                            "columns": [
                                {
                                    "target": {
                                        "table": {
                                            "name": "target",
                                            "type": "TABLE",
                                        },
                                        "name": "target_column",
                                    },
                                    "source": {
                                        "table": {
                                            "name": "source",
                                            "type": "TABLE",
                                        },
                                        "name": "source_column",
                                    },
                                    "action": "COPY",
                                    "source_type": "TABLE",
                                    "target_type": "TABLE",
                                    "node_type": "COLUMN",
                                }
                            ],
                            "tables": [
                                {
                                    "target": {
                                        "name": "target",
                                        "type": "TABLE",
                                    },
                                    "source": {
                                        "name": "source",
                                        "type": "TABLE",
                                    },
                                    "alias": "alias",
                                    "source_type": "TABLE",
                                    "target_type": "TABLE",
                                    "node_type": "TABLE",
                                }
                            ],
                            "subqueries": {},
                        }
                    ],
                    "columns": [
                        {
                            "target": {
                                "table": {
                                    "name": "target",
                                    "type": "TABLE",
                                },
                                "name": "target_column",
                            },
                            "source": {
                                "table": {
                                    "name": "source",
                                    "type": "TABLE",
                                },
                                "name": "source_column",
                            },
                            "action": "COPY",
                            "source_type": "TABLE",
                            "target_type": "TABLE",
                            "node_type": "COLUMN",
                        }
                    ],
                    "tables": [
                        {
                            "target": {
                                "name": "target",
                                "type": "TABLE",
                            },
                            "source": {
                                "name": "source",
                                "type": "TABLE",
                            },
                            "alias": "alias",
                            "source_type": "TABLE",
                            "target_type": "TABLE",
                            "node_type": "TABLE",
                        }
                    ],
                },
                id="ParsedResult",
            ),
        ],
    )
    def test_serialisation(self, obj, expected):
        """Test serialisation."""
        assert obj.model_dump() == expected


# classes to represent Column from sqlglot


class Table:
    """Table class."""

    def __init__(self, alias=None, source=None):
        self.alias = alias
        self.source = source


class Part:
    """Part class to represent a part of a column."""

    def __init__(self, name):
        self.name = name


class Column:
    """Class to represent a Column in sqlglot."""

    def __init__(
        self, name: str, table: str | None = None, parts: list[Part] | None = None
    ):
        self.name = name
        self.table = table
        self.parts = parts or []


class TestParsedExpression:
    """Test ParsedExpression."""

    @pytest.fixture(scope="class")
    def parsed_expression(self):
        """Fixture for ParsedExpression."""
        yield ParsedExpression(  # type: ignore[assignment]
            target=DataTable(name="target", type="TABLE"),
            expression=EXPRESSION,
            tables={
                TableLineage(
                    target=DataTable(name="target.table", type="TABLE"),
                    source=DataTable(name="source.table", type="TABLE"),
                    alias="alias",
                )
            },
            subqueries={
                "subquery": ParsedExpression(
                    target=DataTable(name="subquery.table", type="TABLE"),
                    expression=EXPRESSION,
                    columns={
                        ColumnLineage(
                            target=DataColumn(
                                table=DataTable(name="subquery.table", type="TABLE"),
                                name="target_column",
                            ),
                            source=DataColumn(
                                table=DataTable(name="source.table", type="TABLE"),
                                name="source_column",
                            ),
                            action="COPY",
                        )
                    },
                ),  # type: ignore
            },
        )

    @pytest.mark.parametrize(
        "column, expected, source_table, table_store",
        [
            pytest.param(
                Column(
                    name="target_column",
                    table="source.table",
                ),
                "source.table.target_column",
                DataTable(name="source.table", type="TABLE"),
                None,
                id="No table alias",
            ),
            pytest.param(
                Column(
                    name="target_column",
                    table="alias",
                ),
                "source.table.target_column",
                DataTable(name="source.table", type="TABLE"),
                None,
                id="Table alias",
            ),
            pytest.param(
                Column(
                    name="target_column",
                    table="subquery",
                ),
                "subquery.table.target_column",
                DataTable(name="source.table", type="TABLE"),
                None,
                id="From subquery",
            ),
            pytest.param(
                Column(
                    name="target_column",
                    parts=[
                        Part(name="source"),
                        Part(name="table"),
                        Part(name="target_column"),
                    ],
                ),
                "source.table.target_column",
                DataTable(name="source.table", type="TABLE"),
                None,
                id="From parts with source table",
            ),
            pytest.param(
                Column(
                    name="target_column",
                    parts=[
                        Part(name="source"),
                        Part(name="table"),
                        Part(name="target_column"),
                    ],
                ),
                "source.table.target_column",
                None,
                SimpleTupleStore(
                    [("source.table", DataTable(name="source.table", type="TABLE"))]
                ),
                id="From parts with table store",
            ),
        ],
    )
    def test__get_source_column(
        self,
        column: Column,
        source_table,
        table_store,
        expected: str,
        parsed_expression,
    ):
        """Test _get_source_column."""
        actual = parsed_expression._get_source_column(column, source_table, table_store)
        assert (
            f"{actual.table.name}.{actual.name}" == expected
        ), f"Expected {expected}, got {actual}"
