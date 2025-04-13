"""SQL Lineage Parser.

This module provides a class to parse SQL queries and extract lineage information.
"""

# pylint: disable=no-member

from typing import Optional

import sqlglot
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import (
    CTE,
    Expression,
    From,
    Join,
    Subquery,
)

from sql2lineage.model import ParsedExpression, ParsedResult, SourceTable


class SQLLineageParser:
    """A class to parse SQL queries and extract lineage information."""

    def __init__(self, dialect: Optional[DialectType] = None):
        self._dialect = dialect

    def _parse_expression(self, expression: Expression, index: int) -> ParsedExpression:
        parsed_expression = ParsedExpression(
            output_table=expression.this.name or f"expr{index:03}"
        )

        for cte in expression.find_all(CTE):

            # CTE creates an alias for the table
            cte_output_table = cte.alias_or_name

            # find the source table for the CTE
            for source in cte.find_all(From):
                parsed_expression.source_tables.add(
                    SourceTable(
                        output_table=cte_output_table,
                        source_table=".".join(
                            [identifier.name for identifier in source.this.parts]
                        ),
                        alias=source.alias_or_name,
                    )
                )

            # create a default source table
            source_table = cte.find(From)
            if source_table:
                source_table = ".".join(
                    [identifier.name for identifier in source_table.this.parts]
                )

            parsed_expression.update_column_lineage(cte, source_table)

        # find joins for the main query
        for join in expression.find_all(Join):
            subqueries = list(join.find_all(Subquery))
            if subqueries:
                for subquery in subqueries:

                    parsed_expression.subqueries[subquery.alias_or_name] = (
                        self._parse_expression(subquery, index)
                    )

                    for source in subquery.find_all(From):
                        parsed_expression.source_tables.add(
                            SourceTable(
                                output_table=parsed_expression.output_table,
                                source_table=".".join(
                                    [
                                        identifier.name
                                        for identifier in source.this.parts
                                    ]
                                ),
                                alias=source.name,
                            )
                        )

            else:
                source = join.find(From)
                if not source:
                    continue
                source_table = ".".join(
                    [identifier.name for identifier in source.this.parts]
                )
                parsed_expression.source_tables.add(
                    SourceTable(
                        output_table=parsed_expression.output_table,
                        source_table=source_table,
                        alias=join.alias_or_name,
                    ),
                )

        # find the source tables for the main query
        source_table = None
        source = expression.find(From)
        if source:
            source_table = ".".join(
                [identifier.name for identifier in source.this.parts]
            )
            parsed_expression.source_tables.add(
                SourceTable(
                    output_table=parsed_expression.output_table,
                    source_table=source_table,
                    alias=source.alias_or_name,
                ),
            )

        # find the columns for the main query
        parsed_expression.update_column_lineage(expression, source_table)

        return parsed_expression

    def extract_lineage(self, sql: str, dialect: Optional[DialectType] = None):
        parsed = sqlglot.parse(sql, read=dialect or self._dialect)
        result = ParsedResult()

        for i, expression in enumerate(parsed):
            if expression is None:
                continue

            result.add(self._parse_expression(expression, i))

        return result
