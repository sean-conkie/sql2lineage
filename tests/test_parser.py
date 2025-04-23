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
                            "target": "big_orders.customer_id",
                            "source": "filtered_orders.customer_id",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "big_orders.order_id",
                            "source": "filtered_orders.order_id",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "big_orders.total_with_tax",
                            "source": "filtered_orders.total_with_tax",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "filtered_orders.customer_id",
                            "source": "orders_with_tax.customer_id",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "filtered_orders.order_id",
                            "source": "orders_with_tax.order_id",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "filtered_orders.total_with_tax",
                            "source": "orders_with_tax.total_with_tax",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "orders_with_tax.customer_id",
                            "source": "raw.orders.customer_id",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "orders_with_tax.order_id",
                            "source": "raw.orders.order_id",
                            "node_type": "COLUMN",
                            "action": "COPY",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "orders_with_tax.total_with_tax",
                            "source": "raw.orders.order_total",
                            "node_type": "COLUMN",
                            "action": "TRANSFORM",
                            "table_type": "TABLE",
                        },
                    ],
                    "tables": [
                        {
                            "target": "big_orders",
                            "source": "filtered_orders",
                            "alias": "filtered_orders",
                            "node_type": "TABLE",
                            "type": "TABLE",
                        },
                        {
                            "target": "filtered_orders",
                            "source": "orders_with_tax",
                            "alias": "orders_with_tax",
                            "node_type": "TABLE",
                            "type": "CTE",
                        },
                        {
                            "target": "orders_with_tax",
                            "source": "raw.orders",
                            "alias": "orders",
                            "node_type": "TABLE",
                            "type": "CTE",
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
                            "target": "join_example.age",
                            "source": "raw.user_details.age",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "SUBQUERY",
                        },
                        {
                            "target": "join_example.id",
                            "source": "raw.users.id",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "join_example.name",
                            "source": "raw.users.name",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                    ],
                    "tables": [
                        {
                            "target": "join_example",
                            "source": "raw.user_details",
                            "alias": "user_details",
                            "type": "SUBQUERY",
                            "node_type": "TABLE",
                        },
                        {
                            "target": "join_example",
                            "source": "raw.users",
                            "alias": "a",
                            "type": "TABLE",
                            "node_type": "TABLE",
                        },
                    ],
                    "subqueries": {
                        "b": {
                            "target": "",
                            "columns": [
                                {
                                    "target": "age",
                                    "source": "raw.user_details.age",
                                    "action": "COPY",
                                    "node_type": "COLUMN",
                                    "table_type": "TABLE",
                                },
                                {
                                    "target": "id",
                                    "source": "raw.user_details.id",
                                    "action": "COPY",
                                    "node_type": "COLUMN",
                                    "table_type": "TABLE",
                                },
                            ],
                            "tables": [
                                {
                                    "target": "",
                                    "source": "raw.user_details",
                                    "alias": "user_details",
                                    "type": "TABLE",
                                    "node_type": "TABLE",
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
                            "target": "join_example.age",
                            "source": "raw.user_details.age",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "join_example.id",
                            "source": "raw.users.id",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "join_example.name",
                            "source": "raw.users.name",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                    ],
                    "tables": [
                        {
                            "target": "join_example",
                            "source": "raw.user_details",
                            "alias": "b",
                            "type": "TABLE",
                            "node_type": "TABLE",
                        },
                        {
                            "alias": "a",
                            "node_type": "TABLE",
                            "target": "join_example",
                            "source": "raw.users",
                            "type": "TABLE",
                        },
                    ],
                    "subqueries": {},
                    "expression": "CREATE TABLE join_example AS\nSELECT\n  a.id AS id,\n  a.name,\n  b.age AS age\nFROM raw.users AS a\nJOIN raw.user_details AS b\n  ON a.id = b.id\nWHERE\n  NOT a.name IS NULL AND b.age > 18",
                },
                id="join_no_subquery",
            ),
            pytest.param(
                "tests/sql/unnest.sql",
                {
                    "target": "unnest_example",
                    "columns": [
                        {
                            "target": "unnest_example.address1",
                            "source": "raw_orders.address1",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "unnest_example.address2",
                            "source": "raw_orders.address2",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "unnest_example.city",
                            "source": "raw_orders.city",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "unnest_example.order_id",
                            "source": "raw_orders.orderid",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "unnest_example.product",
                            "source": "raw_orders.product",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "unnest_example.state",
                            "source": "raw_orders.state",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "unnest_example.zip",
                            "source": "raw_orders.zip",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                    ],
                    "tables": [
                        {
                            "target": "unnest_example",
                            "source": "raw_orders",
                            "alias": "ad",
                            "type": "TABLE",
                            "node_type": "TABLE",
                        },
                        {
                            "target": "unnest_example",
                            "source": "raw_orders",
                            "alias": "li",
                            "type": "TABLE",
                            "node_type": "TABLE",
                        },
                        {
                            "target": "unnest_example",
                            "source": "raw_orders",
                            "alias": "src",
                            "type": "TABLE",
                            "node_type": "TABLE",
                        },
                    ],
                    "subqueries": {},
                    "expression": "CREATE OR REPLACE TABLE unnest_example AS\nSELECT\n  orderid AS order_id,\n  ad.address1,\n  ad.address2,\n  ad.city,\n  ad.state,\n  ad.zip,\n  li.product\nFROM raw_orders AS src, UNNEST(src.address) AS _t0(ad), UNNEST(lines_items) AS _t1(li)",
                },
                id="unnest",
            ),
            pytest.param(
                "tests/sql/from_subquery.sql",
                {
                    "target": "from_subquery",
                    "columns": [
                        {
                            "target": "from_subquery.age",
                            "source": "raw.user_details.age",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "SUBQUERY",
                        },
                        {
                            "target": "from_subquery.id",
                            "source": "raw.user_details.id",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "SUBQUERY",
                        },
                    ],
                    "tables": [
                        {
                            "target": "from_subquery",
                            "source": "raw.user_details",
                            "alias": "user_details",
                            "type": "SUBQUERY",
                            "node_type": "TABLE",
                        }
                    ],
                    "subqueries": {
                        "a": {
                            "target": "",
                            "columns": [
                                {
                                    "target": "age",
                                    "source": "raw.user_details.age",
                                    "action": "COPY",
                                    "node_type": "COLUMN",
                                    "table_type": "TABLE",
                                },
                                {
                                    "target": "id",
                                    "source": "raw.user_details.id",
                                    "action": "COPY",
                                    "node_type": "COLUMN",
                                    "table_type": "TABLE",
                                },
                            ],
                            "tables": [
                                {
                                    "target": "",
                                    "source": "raw.user_details",
                                    "alias": "user_details",
                                    "type": "TABLE",
                                    "node_type": "TABLE",
                                }
                            ],
                            "subqueries": {},
                            "expression": "(\n  SELECT\n    id,\n    age\n  FROM raw.user_details\n) AS a",
                        }
                    },
                    "expression": "CREATE TABLE from_subquery AS\nSELECT\n  a.id AS id,\n  a.age AS age\nFROM (\n  SELECT\n    id,\n    age\n  FROM raw.user_details\n) AS a",
                },
                id="from_subquery",
            ),
            pytest.param(
                "tests/sql/truncate_table.sql",
                {
                    "target": "truncaate_table",
                    "columns": [],
                    "tables": [],
                    "subqueries": {},
                    "expression": "TRUNCATE TABLE   truncaate_table",
                },
                id="truncate",
            ),
            pytest.param(
                "tests/sql/cte_with_subquery.sql",
                {
                    "target": "expr000",
                    "columns": [
                        {
                            "target": "cte_with_subquery.id",
                            "source": "test_table.id",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "cte_with_subquery.name",
                            "source": "test_table.name",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "cte_with_subquery.total_count",
                            "source": "test_table.total_count",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "expr000.id",
                            "source": "cte_with_subquery.id",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "expr000.name",
                            "source": "cte_with_subquery.name",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                        {
                            "target": "expr000.total_count",
                            "source": "cte_with_subquery.total_count",
                            "action": "COPY",
                            "node_type": "COLUMN",
                            "table_type": "TABLE",
                        },
                    ],
                    "tables": [
                        {
                            "target": "cte_with_subquery",
                            "source": "test_table",
                            "alias": "test_table",
                            "node_type": "TABLE",
                            "type": "CTE",
                        },
                        {
                            "target": "expr000",
                            "source": "cte_with_subquery",
                            "alias": "cte_with_subquery",
                            "node_type": "TABLE",
                            "type": "TABLE",
                        },
                    ],
                    "subqueries": {},
                    "expression": "WITH cte_with_subquery AS (\n  SELECT\n    id,\n    name,\n    total_count\n  FROM (\n    SELECT\n      id,\n      name,\n      COUNT(*) AS total_count\n    FROM test_table\n    GROUP BY\n      1,\n      2\n  )\n)\nSELECT\n  id,\n  name,\n  total_count\nFROM cte_with_subquery\nWHERE\n  total_count > 0\nORDER BY\n  id",
                },
                id="cte_with_subquery",
            ),
        ],
    )
    def test_extract_lineage(self, file_path, expected):
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

    def test_extract_lineage_error(self):
        """Test the SQL parser with an invalid SQL file.

        Args:
            file_path (str): The path to the SQL file to be parsed.
            expected (dict): The expected output of the parser.

        """
        parser = SQLLineageParser(dialect="bigquery")

        parsed_result = parser.extract_lineage("def not_sql(): pass")
        assert parsed_result.expressions == []

    def test_extract_lineages_from_file(self):
        """Test the SQL parser with various SQL files."""
        parser = SQLLineageParser(dialect="bigquery")
        result = parser.extract_lineages_from_file("tests/sql", glob="*.sql")
        assert len(result.expressions) == 7

    @pytest.mark.asyncio
    async def test_aextract_lineages_from_file(self):
        """Test the SQL parser with various SQL files."""
        parser = SQLLineageParser(dialect="bigquery")
        result = await parser.aextract_lineages_from_file("tests/sql", glob="*.sql")
        assert len(result.expressions) == 7
