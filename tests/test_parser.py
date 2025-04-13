"""Test SQL parser."""

from pathlib import Path

import pytest

from sql2lineage.parser import SQLLineageParser


class TestParser:
    """TestParser."""

    @pytest.mark.parametrize(
        "file_path, expected",
        [
            pytest.param(
                "tests/sql/example.sql",
                {
                    "target": "big_orders",
                    "columns": [
                        {
                            "target": "big_orders",
                            "column": "orders_with_tax.customer_id",
                            "source": "filtered_orders.orders_with_tax.customer_id",
                            "action": "COPY",
                        },
                        {
                            "target": "big_orders",
                            "column": "orders_with_tax.order_id",
                            "source": "filtered_orders.orders_with_tax.order_id",
                            "action": "COPY",
                        },
                        {
                            "target": "big_orders",
                            "column": "orders_with_tax.total_with_tax",
                            "source": "filtered_orders.orders_with_tax.total_with_tax",
                            "action": "COPY",
                        },
                        {
                            "target": "filtered_orders",
                            "column": "customer_id",
                            "source": "orders_with_tax.customer_id",
                            "action": "COPY",
                        },
                        {
                            "target": "filtered_orders",
                            "column": "order_id",
                            "source": "orders_with_tax.order_id",
                            "action": "COPY",
                        },
                        {
                            "target": "filtered_orders",
                            "column": "total_with_tax",
                            "source": "orders_with_tax.total_with_tax",
                            "action": "COPY",
                        },
                        {
                            "target": "orders_with_tax",
                            "column": "customer_id",
                            "source": "raw.orders.customer_id",
                            "action": "COPY",
                        },
                        {
                            "target": "orders_with_tax",
                            "column": "order_id",
                            "source": "raw.orders.order_id",
                            "action": "COPY",
                        },
                        {
                            "target": "orders_with_tax",
                            "column": "order_total",
                            "source": "raw.orders.order_total",
                            "action": "TRANSFORM",
                        },
                    ],
                    "tables": [
                        {
                            "target": "big_orders",
                            "source": "filtered_orders",
                            "alias": "filtered_orders",
                        },
                        {
                            "target": "filtered_orders",
                            "source": "orders_with_tax",
                            "alias": "orders_with_tax",
                        },
                        {
                            "target": "orders_with_tax",
                            "source": "raw.orders",
                            "alias": "orders",
                        },
                    ],
                    "subqueries": {},
                },
                id="example",
            ),
            pytest.param(
                "tests/sql/join_example.sql",
                {
                    "target": "join_example",
                    "columns": [
                        {
                            "target": "join_example",
                            "column": "age",
                            "source": "raw.user_details.age",
                            "action": "COPY",
                        },
                        {
                            "target": "join_example",
                            "column": "id",
                            "source": "raw.users.id",
                            "action": "COPY",
                        },
                        {
                            "target": "join_example",
                            "column": "name",
                            "source": "raw.users.name",
                            "action": "COPY",
                        },
                    ],
                    "tables": [
                        {
                            "target": "join_example",
                            "source": "raw.user_details",
                            "alias": "user_details",
                        },
                        {"target": "join_example", "source": "raw.users", "alias": "a"},
                    ],
                    "subqueries": {
                        "b": {
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
                                    "source": "raw.user_details.id",
                                    "action": "COPY",
                                },
                            ],
                            "tables": [
                                {
                                    "target": "expr000",
                                    "source": "raw.user_details",
                                    "alias": "user_details",
                                }
                            ],
                            "subqueries": {},
                        }
                    },
                },
                id="join_example",
            ),
            pytest.param("tests/sql/empty.sql", [], id="empty"),
            pytest.param(
                "tests/sql/join_no_subquery.sql",
                {
                    "target": "join_example",
                    "columns": [
                        {
                            "target": "join_example",
                            "column": "age",
                            "source": "raw.user_details.age",
                            "action": "COPY",
                        },
                        {
                            "target": "join_example",
                            "column": "id",
                            "source": "raw.users.id",
                            "action": "COPY",
                        },
                        {
                            "target": "join_example",
                            "column": "name",
                            "source": "raw.users.name",
                            "action": "COPY",
                        },
                    ],
                    "tables": [
                        {
                            "target": "join_example",
                            "source": "raw.user_details",
                            "alias": "b",
                        },
                        {"target": "join_example", "source": "raw.users", "alias": "a"},
                    ],
                    "subqueries": {},
                },
                id="join_no_subquery",
            ),
        ],
    )
    def test_parse_sql(self, file_path, expected):
        """Test the SQL parser with various SQL files.

        Args:
            file_path (str): The path to the SQL file to be parsed.
            expected (dict): The expected output of the parser.

        """
        parser = SQLLineageParser(dialect="bigquery")

        with Path(file_path).open("r", encoding="utf-8") as src:
            parsed_result = parser.extract_lineage(src.read())

        if parsed_result.expressions:
            result = parsed_result.expressions[0].model_dump()
            assert result == expected
        else:
            assert parsed_result.expressions == expected
