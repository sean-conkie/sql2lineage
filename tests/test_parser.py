"""Test SQL parser."""

from pathlib import Path
from typing import Callable

import pytest

from sql2lineage.model import ParsedResult
from sql2lineage.parser import SQLLineageParser


# functions to test query results
def example(result: ParsedResult) -> bool:
    """Test the example SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 9
    assert len(result.expressions[0].tables) == 3
    return True


def cte_with_subquery(result: ParsedResult) -> bool:
    """Test the CTE with subquery SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 6
    assert len(result.expressions[0].tables) == 3
    return True


def from_subquery(result: ParsedResult) -> bool:
    """Test the subquery SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 4
    assert len(result.expressions[0].tables) == 2
    return True


def join_example(result: ParsedResult) -> bool:
    """Test the subquery SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 5
    assert len(result.expressions[0].tables) == 3
    return True


def join_no_subquery(result: ParsedResult) -> bool:
    """Test the subquery SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 3
    assert len(result.expressions[0].tables) == 2
    return True


def empty(result: ParsedResult) -> bool:
    """Test the empty SQL file."""

    assert len(result.expressions) == 0
    return True


def truncate_table(result: ParsedResult) -> bool:
    """Test the empty SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 0
    assert len(result.expressions[0].tables) == 0
    return True


def unnest(result: ParsedResult) -> bool:
    """Test the empty SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 7
    assert len(result.expressions[0].tables) == 3
    return True


def query_with_struct(result: ParsedResult) -> bool:
    """Test the struct SQL file."""

    assert len(result.expressions) == 1
    assert len(result.expressions[0].columns) == 9
    assert len(result.expressions[0].tables) == 2

    column_names = [col.target.name for col in result.expressions[0].columns]
    assert "country_of_birth.continent" in column_names

    return True


def multi_expression_with_struct(result: ParsedResult) -> bool:
    """Test the struct SQL file."""

    assert len(result.expressions) == 2
    assert len(result.expressions[0].columns) == 12
    assert len(result.expressions[1].columns) == 6

    return True


class TestParser:
    """TestParser."""

    @pytest.mark.parametrize(
        "file_path, test_callable",
        [
            pytest.param(
                "tests/sql/example.sql",
                example,
                id="example",
            ),
            pytest.param(
                "tests/sql/cte_with_subquery.sql",
                cte_with_subquery,
                id="cte_with_subquery",
            ),
            pytest.param(
                "tests/sql/empty.sql",
                empty,
                id="empty",
            ),
            pytest.param(
                "tests/sql/from_subquery.sql",
                from_subquery,
                id="from_subquery",
            ),
            pytest.param(
                "tests/sql/join_example.sql",
                join_example,
                id="join_example",
            ),
            pytest.param(
                "tests/sql/join_no_subquery.sql",
                join_no_subquery,
                id="join_no_subquery",
            ),
            pytest.param(
                "tests/sql/truncate_table.sql",
                truncate_table,
                id="truncate_table",
            ),
            pytest.param(
                "tests/sql/unnest.sql",
                unnest,
                id="unnest",
            ),
            pytest.param(
                "tests/sql/query_with_struct.sql",
                query_with_struct,
                id="query_with_struct",
            ),
            pytest.param(
                "tests/sql/multi_expression_with_struct.sql",
                multi_expression_with_struct,
                id="multi_expression_with_struct",
            ),
        ],
    )
    def test_extract_lineage(
        self, file_path, test_callable: Callable[[ParsedResult], bool]
    ):
        """Test the SQL parser with various SQL files.

        Args:
            file_path (str): The path to the SQL file to be parsed.
            expected (dict): The expected output of the parser.

        """
        parser = SQLLineageParser(dialect="bigquery")

        with Path(file_path).open("r", encoding="utf-8") as src:
            parsed_result = parser.extract_lineage(src.read())

        assert test_callable(parsed_result)

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
        assert len(result.expressions) == 10

    @pytest.mark.asyncio
    async def test_aextract_lineages_from_file(self):
        """Test the SQL parser with various SQL files."""
        parser = SQLLineageParser(dialect="bigquery")
        result = await parser.aextract_lineages_from_file("tests/sql", glob="*.sql")
        assert len(result.expressions) == 10

    def test_extract_lineages_with_schema(self):
        """Test the SQL parser with various SQL files."""
        parser = SQLLineageParser(dialect="bigquery")
        result = parser.extract_lineages(
            [
                """select *
  from new_customers
;"""
            ],
            schema={
                "tables": [
                    {
                        "name": "new_customers",
                        "type": "TABLE",
                        "columns": [
                            {"name": "id", "type": "INTEGER"},
                            {"name": "name", "type": "STRING"},
                            {
                                "name": "address",
                                "type": "STRUCT",
                                "fields": [
                                    {"name": "address_line_1", "type": "STRING"},
                                    {"name": "address_line_2", "type": "STRING"},
                                    {"name": "city", "type": "STRING"},
                                    {"name": "zip", "type": "STRING"},
                                ],
                            },
                        ],
                    }
                ]
            },
        )
        assert len(result.expressions) == 1
        assert len(result.expressions[0].columns) == 6
