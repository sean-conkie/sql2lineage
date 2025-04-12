"""SQL Lineage Parser.

This module provides a class to parse SQL queries and extract lineage information.
"""

from typing import List, Optional, Tuple

import sqlglot
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import CTE, Column, Create, Subquery, Table


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
        column_lineage = set()

        def process_expression(expression):
            for node in expression.walk():
                if isinstance(node, Table):
                    source_tables.add(node.name)

                if isinstance(node, Column):
                    col_name = node.alias_or_name
                    if len(node.parts) >= 2:
                        source = f"{node.parts[-2]}.{node.parts[-1]}"
                    else:
                        source = node.name
                    column_lineage.add((col_name, source))

        for node in parsed:
            if isinstance(node, Create):
                output_table = node.this.name
                process_expression(node)

            elif isinstance(node, (CTE, Subquery)):
                process_expression(node)

        return output_table, list(source_tables), list(column_lineage)
