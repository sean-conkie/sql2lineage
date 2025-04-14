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
                            "column": "customer_id",
                            "source": "filtered_orders.customer_id",
                            "action": "COPY",
                        },
                        {
                            "target": "big_orders",
                            "column": "order_id",
                            "source": "filtered_orders.order_id",
                            "action": "COPY",
                        },
                        {
                            "target": "big_orders",
                            "column": "total_with_tax",
                            "source": "filtered_orders.total_with_tax",
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
                    "expression": "WITH orders_with_tax AS (\n  SELECT\n    order_id,\n    customer_id,\n    order_total * 1.2 AS total_with_tax\n  FROM raw.orders\n), filtered_orders AS (\n  SELECT\n    order_id,\n    customer_id,\n    total_with_tax\n  FROM orders_with_tax\n)\nCREATE TABLE big_orders AS\nSELECT\n  *\nFROM filtered_orders",
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
                            "target": "",
                            "columns": [
                                {
                                    "target": "",
                                    "column": "age",
                                    "source": "raw.user_details.age",
                                    "action": "COPY",
                                },
                                {
                                    "target": "",
                                    "column": "id",
                                    "source": "raw.user_details.id",
                                    "action": "COPY",
                                },
                            ],
                            "tables": [
                                {
                                    "target": "",
                                    "source": "raw.user_details",
                                    "alias": "user_details",
                                }
                            ],
                            "subqueries": {},
                            "expression": "(\n  SELECT\n    id,\n    age\n  FROM raw.user_details\n) AS b",
                        }
                    },
                    "expression": "CREATE TABLE join_example AS\nSELECT\n  a.id AS id,\n  a.name,\n  b.age AS age\nFROM raw.users AS a\nJOIN (\n  SELECT\n    id,\n    age\n  FROM raw.user_details\n) AS b\n  ON a.id = b.id\nWHERE\n  NOT a.name IS NULL AND b.age > 18",
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
                    "expression": "CREATE TABLE join_example AS\nSELECT\n  a.id AS id,\n  a.name,\n  b.age AS age\nFROM raw.users AS a\nJOIN raw.user_details AS b\n  ON a.id = b.id\nWHERE\n  NOT a.name IS NULL AND b.age > 18",
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
