"""Test model."""

# pylint: disable=no-member
import pytest

from sql2lineage.model import (
    ColumnLineage,
    DataColumn,
    DataTable,
    ParsedExpression,
    ParsedResult,
    TableLineage,
)

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
        expression="a + b",
    )
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
                    expression="a + b",
                ),
                {
                    "expression": "a + b",
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
                            "expression": "a + b",
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
