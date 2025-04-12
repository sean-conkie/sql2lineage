"""SQL Lineage Parser.

This module provides a class to parse SQL queries and extract lineage information.
"""

from typing import List, Optional, Tuple

import sqlglot
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import Column, Create, Table


class SQLLineageParser:
    """A class to parse SQL queries and extract lineage information."""

    def __init__(self, dialect: Optional[DialectType] = None):
        self._dialect = dialect

    def extract_lineage(
        self, sql: str, dialect: Optional[DialectType] = None
    ) -> Tuple[str | None, List[str], List[Tuple[str, str]]]:
        """Extract lineage information from a parsed SQL query.

        This method analyzes the parsed SQL query and extracts the following:
        - The output table name (if a CREATE statement is present).
        - A list of source table names referenced in the query.
        - A list of column lineage mappings, where each mapping is a tuple containing
          the column alias or name and its fully qualified name.

        Returns:
            Tuple[str, List[str], List[Tuple[str, str]]]: A tuple containing:
                - The name of the output table (str) or None if not found.
                - A list of source table names (List[str]).
                - A list of column lineage mappings (List[Tuple[str, str]]).

        """
        assert any(
            [dialect, self._dialect]
        ), "Either 'dialect' or 'self._dialect' must be provided."
        parsed = sqlglot.parse(sql, read=dialect or self._dialect)

        output_table = None
        source_tables = set()
        column_lineage = []

        for node in parsed:
            if not node:
                continue

            for expression in node.walk():

                if isinstance(expression, Create):
                    output_table = expression.this.name

                if isinstance(expression, Table):
                    source_tables.add(expression.name)

                if isinstance(expression, Column):
                    column_lineage.append(
                        (
                            expression.alias_or_name,
                            ".".join([part.name for part in expression.parts]),
                        )
                    )

        return output_table, list(source_tables), column_lineage
