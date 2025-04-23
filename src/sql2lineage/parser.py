"""SQL Lineage Parser.

This module provides a class to parse SQL queries and extract lineage information.
"""

# pylint: disable=no-member
import asyncio
import logging
from os import PathLike
from pathlib import Path
from typing import Callable, List, Optional, Sequence, TypeAlias

import sqlglot
import sqlglot.errors
from anyio import open_file
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import (
    CTE,
    Column,
    Expression,
    From,
    Identifier,
    Join,
    Subquery,
    Table,
    TruncateTable,
    Unnest,
)

from sql2lineage.model import ParsedExpression, ParsedResult, SourceTable
from sql2lineage.types.model import TableType
from sql2lineage.utils import SimpleTupleStore

StrPath: TypeAlias = str | PathLike[str]

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


class SQLLineageParser:  # noqa: D101 # pylint: disable=missing-class-docstring

    def __init__(self, dialect: Optional[DialectType] = None):
        """Class to parse SQL queries and extract lineage information.

        Args:
            dialect (Optional[DialectType]): The SQL dialect to be used by the parser.
                If not provided, a default dialect may be used.

        """
        self._dialect = dialect
        self._table_store = SimpleTupleStore[str, TableType]()

    def _extract_target(self, expression: Expression, index: int) -> str:
        """Extract the target from the expression.

        Args:
            expression (Expression): The SQL expression to extract the target from.
            index (int): The index of the expression in the parsed SQL.

        Returns:
            str: The target name extracted from the expression.

        """
        fallback = f"expr{index:03}"

        # is a truncate table statement
        if isinstance(expression, TruncateTable):
            table = expression.find(Table)
            if table:
                return self._join_parts(table.parts)

        elif expression.this:

            if hasattr(expression.this, "parts"):

                return self._join_parts(expression.this.parts)
            else:

                return expression.this.name

        return fallback

    def _extract_source(self, expression: Expression) -> Optional[Expression]:
        """Extract the source expression from the given SQL expression.

        This method analyzes the provided SQL expression and attempts to identify
        the source of the data, such as a table, subquery, or a "FROM" clause.

        Args:
            expression (Expression): The SQL expression to analyze.

        Returns:
            Optional[Expression]: The extracted source expression if found,
            otherwise None.

        """
        if isinstance(expression, TruncateTable):
            return expression.find(Table)

        # elif isinstance(expression, Insert):
        #     source = expression.find(From)
        #     if source:
        #         return source.this

        source = expression.find(From)
        if source:
            source = source.this

        if isinstance(source, (Table, Subquery)):
            return source

        elif isinstance(expression.this, (Table, Subquery)):
            return expression.this

        if source:
            return source.this

    def _join_parts(self, parts: Sequence[Expression | Identifier]) -> str:
        """Join the parts of an expression into a string.

        Args:
            parts (List[Expression]): The list of expressions to join.

        Returns:
            str: The joined string representation of the expressions.

        """
        return ".".join([identifier.name for identifier in parts])

    def _parse_expression(self, expression: Expression, index: int) -> ParsedExpression:

        parsed_expression = ParsedExpression(
            target=self._extract_target(expression, index),
            expression=expression.sql(pretty=True, dialect=self._dialect),
        )

        for cte in expression.find_all(CTE):
            # CTE creates an alias for the table
            cte_target = cte.alias_or_name

            # find the source tables for the CTE
            # parse the CTE
            parsed_cte = self._parse_expression(cte.this, index)

            for source in parsed_cte.tables:
                parsed_expression.tables.add(
                    SourceTable(
                        target=cte_target,
                        source=source.source,
                        alias=source.alias,
                        type=source.type,
                    )
                )

            # create a default source table
            if source_table := cte.find(From):
                if hasattr(source_table.this, "parts"):
                    source_table = self._join_parts(source_table.this.parts)
                elif source_table_table := source_table.find(Table):
                    source_table = self._join_parts(source_table_table.parts)
                else:
                    source_table = None

            if cte_target not in self._table_store:
                self._table_store[cte_target] = "CTE"

            parsed_expression.update_column_lineage(
                cte, source_table, cte_target, self._table_store
            )

        unnests = set()

        # find joins for the main query
        for join in expression.find_all(Join):
            subqueries = list(join.find_all(Subquery))
            if subqueries:
                for subquery in subqueries:

                    parsed_expression.subqueries[subquery.alias_or_name] = (
                        self._parse_expression(subquery, index)
                    )

                    for table in parsed_expression.subqueries[
                        subquery.alias_or_name
                    ].tables:
                        parsed_expression.tables.add(
                            SourceTable(
                                target=parsed_expression.target,
                                source=table.source,
                                alias=table.alias,
                                type="SUBQUERY",
                            )
                        )

            elif isinstance(join.this, Unnest):
                # the table is stored in a column and alias will be the alias_column_names
                # unnest is not a new table, store it and reprocess it later
                source = join.this.find(Column)
                assert source, f"Unable to find unnest table source {join}"
                source_table = self._join_parts(source.parts)
                alias = (
                    join.this.alias_column_names[0]
                    if len(join.this.alias_column_names) > 0
                    else join.this.alias_or_name
                )

                unnests.add(
                    SourceTable(
                        target=parsed_expression.target,
                        source=source_table,
                        alias=alias,
                        type="UNNEST",
                    ),
                )

            else:
                # source_table = self._join_parts(join.this.parts)
                # parsed_expression.tables.add(
                #     SourceTable(
                #         target=parsed_expression.target,
                #         source=source_table,
                #         alias=join.alias_or_name,
                #         type=self._table_store.get(source_table, "TABLE"),
                #     ),
                # )
                self._add_table_to_expression(
                    parsed_expression,
                    join.this.parts,
                    parsed_expression.target,
                    join.alias_or_name,
                )

        # find the source tables for the main query
        source_table = None
        source_table_type = None
        source = self._extract_source(expression)
        if source is None:
            raise sqlglot.errors.ParseError(f"Unable to find table source {expression}")

        if isinstance(expression, TruncateTable):
            # skip
            pass

        elif isinstance(source, Table):
            # source_table = self._join_parts(source.parts)
            # source_table_type = "TABLE"
            # if source_table not in self._table_store:
            #     self._table_store[source_table] = source_table_type
            # parsed_expression.tables.add(
            #     SourceTable(
            #         target=parsed_expression.target,
            #         source=source_table,
            #         alias=source.alias_or_name,
            #         type=source_table_type,
            #     ),
            # )

            self._add_table_to_expression(
                parsed_expression,
                source.parts,
                parsed_expression.target,
                source.alias_or_name,
            )
        elif isinstance(source, Subquery):
            # parse the subquery
            subquery = self._parse_expression(source, index)
            parsed_expression.subqueries[source.alias_or_name] = subquery
            for subquery_source in subquery.tables:
                if source_table is None:
                    source_table = subquery_source.source
                parsed_expression.tables.add(
                    SourceTable(
                        target=parsed_expression.target,
                        source=subquery_source.source,
                        alias=subquery_source.alias,
                        type="SUBQUERY",
                    )
                )

        # process the unnests
        for unnest in unnests:
            # the unnest will be a column so if it has 2 parts we know the alias
            # otherwise use the default source
            if len(unnest.source.split(".")) == 2:
                alias = unnest.source.split(".")[0]
                for table in parsed_expression.tables:
                    if alias == table.alias:
                        unnest.source = table.source
                        unnest.type = table.type
                        break

            else:
                # use the default source
                unnest.source = source_table
                unnest.type = source_table_type or "TABLE"

            parsed_expression.tables.add(unnest)

        # find the columns for the main query
        parsed_expression.update_column_lineage(
            expression,
            source_table,
            target=parsed_expression.target,
            table_store=self._table_store,
        )

        return parsed_expression

    def _add_table_to_expression(
        self,
        parsed_expression: ParsedExpression,
        parts: List[Expression],
        target: str,
        alias: Optional[str] = None,
    ):
        source_table = self._join_parts(parts)
        if source_table not in self._table_store:
            self._table_store[source_table] = "TABLE"
        parsed_expression.tables.add(
            SourceTable(
                target=target,
                source=source_table,
                alias=alias,
                type=self._table_store.get(source_table, "TABLE"),
            ),
        )

    def extract_lineage(
        self,
        sql: str,
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
    ):
        """Extract the lineage information from the given SQL query.

        This method parses the provided SQL string using the specified SQL dialect
        (or the default dialect if none is provided) and processes each parsed
        expression to extract lineage details.

        Args:
            sql (str): The SQL query string to extract lineage from.
            dialect (Optional[DialectType]): The SQL dialect to use for parsing.
                If not provided, the default dialect of the instance is used.
            pre_transform (Optional[Callable[[str], str]]): A callable function
                that takes a SQL string as input and returns a transformed SQL string.
                This can be used to preprocess the SQL statement before parsing.
                If not provided, no transformation is applied.

        Returns:
            ParsedResult: An object containing the extracted lineage information.

        """
        return self.extract_lineages(
            [sql],
            dialect=dialect,
            pre_transform=pre_transform,
        )

    def extract_lineages(
        self,
        sqls: List[str],
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
    ):
        """Extract lineage information from a list of SQL statements.

        This method parses the provided SQL statements and extracts lineage
        information such as table dependencies and column mappings. The lineage
        information is aggregated into a `ParsedResult` object.

        Args:
            sqls (List[str]): A list of SQL statements to be parsed.
            dialect (Optional[DialectType]): The SQL dialect to use for parsing.
                If not provided, the default dialect of the parser is used.
            pre_transform (Optional[Callable[[str], str]]): A callable function
                that takes a SQL string as input and returns a transformed SQL string.
                This can be used to preprocess the SQL statements before parsing.
                If not provided, no transformation is applied.

        Returns:
            ParsedResult: An object containing the extracted lineage information.

        """
        result = ParsedResult()
        for sql in sqls:
            if pre_transform:
                sql = pre_transform(sql)
            try:
                parsed = sqlglot.parse(sql, read=dialect or self._dialect)

                for i, expression in enumerate(parsed):
                    if expression is None:
                        continue

                    result.add(self._parse_expression(expression, i))

            except sqlglot.errors.ParseError as error:
                logger.error("Error parsing: %s", error)
                continue
        return result

    async def aextract_lineages_from_file(
        self,
        path: StrPath,
        glob: Optional[str] = None,
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
    ):
        """Asynchronously extract lineages from SQL files in a given directory.

        This method searches for SQL files in the specified directory (and its subdirectories)
        matching the provided glob pattern, reads their contents asynchronously, and extracts
        lineage information from the SQL content.

        Args:
            path (StrPath): The path to the directory or file to process.
            glob (Optional[str], optional): A glob pattern to match specific files. Defaults to "*.sql".
            dialect (Optional[DialectType], optional): The SQL dialect to use for parsing. Defaults to None.
            pre_transform (Optional[Callable[[str], str]], optional): A callable to pre-process the SQL content
                before extracting lineages. Defaults to None.

        Returns:
            List[Lineage]: A list of extracted lineage objects.

        Raises:
            Any exceptions raised during file reading or lineage extraction.

        Notes:
            - This function uses asynchronous file I/O for better performance when processing multiple files.
            - The `extract_lineages` method is called internally to perform the actual lineage extraction.

        """

        async def read(path):
            async with await open_file(path, "r", encoding="utf-8") as f:
                return await f.read()

        # find all .sql files in the directory
        if glob is None:
            glob = "*.sql"

        tasks = [read(pth) for pth in Path(path).rglob(glob)]
        contents = await asyncio.gather(*tasks)

        return self.extract_lineages(
            [content for content in contents if content],
            dialect=dialect,
            pre_transform=pre_transform,
        )

    def extract_lineages_from_file(
        self,
        path: StrPath,
        glob: Optional[str] = None,
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
    ):
        """Extract lineage information from SQL files in a specified directory.

        This method searches for SQL files in the given directory (and its subdirectories) matching
        the specified glob pattern, reads their contents, and extracts lineage information.

        Args:
            path (StrPath): The path to the directory containing SQL files.
            glob (Optional[str]): A glob pattern to match specific SQL files. Defaults to "*.sql".
            dialect (Optional[DialectType]): The SQL dialect to use for parsing. Defaults to None.
            pre_transform (Optional[Callable[[str], str]]): A callable to preprocess the SQL content
                before extracting lineage. Defaults to None.

        Returns:
            List[Lineage]: A list of lineage objects extracted from the SQL files.

        """
        # find all .sql files in the directory
        if glob is None:
            glob = "*.sql"

        contents = [
            pth.open("r", encoding="utf-8").read() for pth in Path(path).rglob(glob)
        ]

        return self.extract_lineages(
            [content for content in contents if content],
            dialect=dialect,
            pre_transform=pre_transform,
        )
