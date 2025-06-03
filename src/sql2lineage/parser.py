"""SQL Lineage Parser.

This module provides a class to parse SQL queries and extract lineage information.
"""

# pylint: disable=no-member
import asyncio
import logging
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, TypeAlias

import sqlglot
import sqlglot.errors
from anyio import open_file
from pydantic import ValidationError
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import (
    CTE,
    Column,
    Expression,
    From,
    Identifier,
    Join,
    Select,
    Subquery,
    Table,
    TruncateTable,
    Unnest,
)

from sql2lineage.model import (
    ColumnLineage,
    DataColumn,
    DataTable,
    ParsedExpression,
    ParsedResult,
    TableLineage,
)
from sql2lineage.types.model import Schema, TableType
from sql2lineage.types.parser import DATATABLE_DEFAULT
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
        self._table_store = SimpleTupleStore[str, DataTable]()
        self._schema = Schema()

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

    def _get_or_create_source_table(
        self,
        name: str,
    ) -> DataTable:

        if name in self._table_store:
            source_table = self._table_store[name]
        else:
            source_table = DataTable(name=name, type="TABLE")
            self._table_store[name] = source_table

        return source_table

    def _process_subquery(
        self,
        subquery: Subquery,
        parsed_expression: ParsedExpression,
    ):
        """Process a subquery and return its target table.

        Args:
            subquery (Subquery): The subquery to process.
            parsed_expression (ParsedExpression): The parsed expression to update.
            struct_override (Optional[dict[str, List[str]]]): A dictionary that can be used to override
                the structure of the parsed expressions, allowing for custom handling of specific SQL constructs.

        Returns:
            DataTable: The target table of the subquery.

        """
        index = len(parsed_expression.subqueries)

        processed_subquery = self._parse_expression(
            subquery,
            index,
            target=f"subquery{index:03}",
            type="SUBQUERY",
        )
        parsed_expression.subqueries[subquery.alias_or_name] = processed_subquery
        for table in processed_subquery.tables:
            if table.source.name not in self._table_store:
                self._table_store[table.source.name] = table.source
            parsed_expression.tables.add(table)

        source_table = processed_subquery.target
        if source_table.name not in self._table_store:
            self._table_store[source_table.name] = source_table

        for column in processed_subquery.columns:
            parsed_expression.columns.add(column)

        parsed_expression.tables.add(
            TableLineage(
                target=parsed_expression.target,
                source=source_table,
                alias=subquery.alias_or_name,
            )
        )

    def _extract_tables_from_expression(
        self,
        expression: Expression,
        index: int,
        target: Optional[str] = None,
        type: Optional[TableType] = None,  # pylint: disable=redefined-builtin
    ) -> ParsedExpression:

        target = target or self._extract_target(expression, index)

        if type is None and isinstance(expression, Select):
            type = "QUERY"
        elif type is None:
            type = self._table_store.get(target, DATATABLE_DEFAULT).type

        parsed_expression = ParsedExpression(
            target=DataTable(name=target, type=type),
            expression=expression,
            dialect=self._dialect,
        )

        for cte in expression.find_all(CTE):
            # CTE creates an alias for the table
            cte_target = DataTable(name=cte.alias_or_name, type="CTE")
            if cte_target.name not in self._table_store:
                self._table_store[cte_target.name] = cte_target

            # find the source tables for the CTE
            # parse the CTE
            parsed_cte = self._parse_expression(
                cte.this,
                index,
                target=cte_target.name,
                type=cte_target.type,
            )

            for table in parsed_cte.tables:
                parsed_expression.tables.add(table)

            # create a default source table
            tbl_name = f"expr{index:03}"
            if cte_source_table := cte.find(From):
                if hasattr(cte_source_table.this, "parts"):
                    tbl_name = self._join_parts(cte_source_table.this.parts)
                elif source_table_table := cte_source_table.find(Table):
                    tbl_name = self._join_parts(source_table_table.parts)

            source_table = self._get_or_create_source_table(tbl_name)

            # parsed_expression.update_column_lineage(
            #     cte,
            #     source_table,
            #     cte_target,
            #     self._table_store,
            # )

        unnests = set()

        # find joins for the main query
        for join in expression.find_all(Join):
            subqueries = list(join.find_all(Subquery))
            if subqueries:
                for subquery in subqueries:
                    # parse the subquery
                    self._process_subquery(subquery, parsed_expression)

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

                unnests.add((source_table, alias))

            else:
                source_table = self._add_table_to_expression(
                    parsed_expression,
                    join.this.parts,
                    parsed_expression.target,
                    join.alias_or_name,
                )

        # find the source tables for the main query
        source_table = None
        source = self._extract_source(expression)
        if source is None:
            raise sqlglot.errors.ParseError(f"Unable to find table source {expression}")

        if isinstance(expression, TruncateTable):
            # skip
            pass

        elif isinstance(source, Table):
            source_table = self._add_table_to_expression(
                parsed_expression,
                source.parts,
                parsed_expression.target,
                source.alias_or_name,
            )
        elif isinstance(source, Subquery):
            # parse the subquery
            self._process_subquery(source, parsed_expression)

        if source_table is None:
            source_table = self._get_or_create_source_table(f"expr{index:03}")

        # process the unnests
        for unnest in unnests:
            # the unnest will be a column so if it has 2 parts we know the alias
            # otherwise use the default source

            unnest_source, unnest_alias = unnest

            if len(unnest_source.split(".")) == 2:
                alias, col = unnest_source.split(".")
                for table in parsed_expression.tables:
                    if alias == table.alias:
                        new_source = f"{table.source.name}.{col}"
                        unnest_source = DataTable(name=new_source, type="UNNEST")
                        break

            else:
                # use the default source
                new_source = f"{source_table.name}.{unnest_source}"
                unnest_source = DataTable(name=new_source, type="UNNEST")

            parsed_expression.tables.add(
                TableLineage(
                    target=parsed_expression.target,
                    source=unnest_source,
                    alias=unnest_alias,
                )
            )

        return parsed_expression

    def _parse_expression(
        self,
        expression: Expression,
        index: int,
        target: Optional[str] = None,
        type: Optional[TableType] = None,  # pylint: disable=redefined-builtin
    ) -> ParsedExpression:

        target = target or self._extract_target(expression, index)

        if type is None and isinstance(expression, Select):
            type = "QUERY"
        elif type is None:
            type = self._table_store.get(target, DATATABLE_DEFAULT).type

        if type == "TABLE":
            self._schema.add_if(target)

        parsed_expression = ParsedExpression(
            target=DataTable(name=target, type=type),
            expression=expression,
            dialect=self._dialect,
        )
        parsed_expression._schema = self._schema  # pylint: disable=protected-access

        for cte in expression.find_all(CTE):
            # CTE creates an alias for the table
            cte_target = DataTable(name=cte.alias_or_name, type="CTE")
            if cte_target.name not in self._table_store:
                self._table_store[cte_target.name] = cte_target

            # find the source tables for the CTE
            # parse the CTE
            parsed_cte = self._parse_expression(
                cte.this,
                index,
                target=cte_target.name,
                type=cte_target.type,
            )

            for table in parsed_cte.tables:
                parsed_expression.tables.add(table)

            # create a default source table
            tbl_name = f"expr{index:03}"
            if cte_source_table := cte.find(From):
                if hasattr(cte_source_table.this, "parts"):
                    tbl_name = self._join_parts(cte_source_table.this.parts)
                elif source_table_table := cte_source_table.find(Table):
                    tbl_name = self._join_parts(source_table_table.parts)

            source_table = self._get_or_create_source_table(tbl_name)

            parsed_expression.update_column_lineage(
                cte,
                source_table,
                cte_target,
                self._table_store,
            )

        unnests = set()

        # find joins for the main query
        for join in expression.find_all(Join):
            subqueries = list(join.find_all(Subquery))
            if subqueries:
                for subquery in subqueries:
                    # parse the subquery
                    self._process_subquery(
                        subquery,
                        parsed_expression,
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

                unnests.add((source_table, alias))

            else:
                source_table = self._add_table_to_expression(
                    parsed_expression,
                    join.this.parts,
                    parsed_expression.target,
                    join.alias_or_name,
                )

        # find the source tables for the main query
        source_table = None
        source = self._extract_source(expression)
        if source is None:
            raise sqlglot.errors.ParseError(f"Unable to find table source {expression}")

        if isinstance(expression, TruncateTable):
            # skip
            pass

        elif isinstance(source, Table):
            source_table = self._add_table_to_expression(
                parsed_expression,
                source.parts,
                parsed_expression.target,
                source.alias_or_name,
            )
            self._schema.add_if(source_table.name)
        elif isinstance(source, Subquery):
            # parse the subquery
            self._process_subquery(
                source,
                parsed_expression,
            )

        if source_table is None:
            source_table = self._get_or_create_source_table(f"expr{index:03}")

        # process the unnests
        for unnest in unnests:
            # the unnest will be a column so if it has 2 parts we know the alias
            # otherwise use the default source

            unnest_source, unnest_alias = unnest

            if len(unnest_source.split(".")) == 2:
                alias, col = unnest_source.split(".")
                for table in parsed_expression.tables:
                    if alias == table.alias:
                        new_source = f"{table.source.name}.{col}"
                        unnest_source = DataTable(name=new_source, type="UNNEST")
                        break

            else:
                # use the default source
                new_source = f"{source_table.name}.{unnest_source}"
                unnest_source = DataTable(name=new_source, type="UNNEST")

            parsed_expression.tables.add(
                TableLineage(
                    target=parsed_expression.target,
                    source=unnest_source,
                    alias=unnest_alias,
                )
            )

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
        target: DataTable,
        alias: Optional[str] = None,
        type: Optional[TableType] = None,  # pylint: disable=redefined-builtin
    ):
        st = self._join_parts(parts)

        source_table = DataTable(
            name=st,
            type=self._table_store.get(
                st, DataTable(name="", type=type) if type else DATATABLE_DEFAULT
            ).type,
        )

        if st not in self._table_store:
            self._table_store[st] = source_table
        parsed_expression.tables.add(
            TableLineage(
                target=target,
                source=source_table,
                alias=alias,
            )
        )

        return source_table

    def _add_column_to_expression(
        self,
        parsed_expression: ParsedExpression,
        source: DataColumn,
        target: DataColumn,
    ):
        parsed_expression.columns.add(
            ColumnLineage(
                target=target,
                source=source,
                action="COPY",
            )
        )

    def extract_lineage(
        self,
        sql: str,
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
        schema: Optional[dict[str, Any] | Schema] = None,
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
            schema (Optional[dict[str, Any] | Schema]): An optional schema object
                or dictionary to validate and use for lineage extraction. If provided,
                it will be used to update the internal schema representation with any
                new tables found during the extraction process.

        Returns:
            ParsedResult: An object containing the extracted lineage information.

        """
        return self.extract_lineages(
            [sql],
            dialect=dialect,
            pre_transform=pre_transform,
            schema=schema,
        )

    def extract_lineages(
        self,
        sqls: List[str],
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
        schema: Optional[dict[str, Any] | Schema] = None,
    ):
        """Extract table lineage information from a list of SQL statements.

        This method parses each SQL statement, optionally applies a pre-transform,
        and extracts source and target tables to build a lineage graph. It also
        updates the internal schema representation with any new tables found.

        Args:
            sqls (List[str]): A list of SQL statements to analyze.
            dialect (Optional[DialectType], optional): The SQL dialect to use for parsing.
                If not provided, uses the instance's default dialect.
            pre_transform (Optional[Callable[[str], str]], optional): An optional function
                to pre-process each SQL statement before parsing.
            schema (Optional[dict[str, Any] | Schema], optional): An optional schema object
                or dictionary to validate and use for lineage extraction.

        Returns:
            ParsedResult: An object containing the parsed lineage information for all SQL statements.

        Raises:
            None explicitly, but logs errors for schema validation and SQL parsing failures.

        """
        if schema is not None:
            if isinstance(schema, Schema):
                self._schema = schema
            else:
                try:
                    self._schema = Schema.model_validate(schema)
                except ValidationError as error:
                    logger.error("Error validating schema: %s", error)

        result = ParsedResult()
        expressions = []
        for sql in sqls:
            if pre_transform:
                sql = pre_transform(sql)
            try:
                parsed = sqlglot.parse(sql, read=dialect or self._dialect)

                for i, expression in enumerate(parsed):
                    if expression is None:
                        continue

                    expr = self._extract_tables_from_expression(expression, i)
                    expressions.append(expr)
                    # take the tables and add them to the schema
                    for table in expr.tables:
                        if table.source.type == "TABLE":
                            self._schema.add_if(table.source.name)
                    if expr.target.type == "TABLE":
                        self._schema.add_if(expr.target.name)

            except sqlglot.errors.ParseError as error:
                logger.error("Error parsing: %s", error)
                continue

        # sort the expressions based on their target table names
        expressions = self._sort_expressions(expressions)
        for i, expr in enumerate(expressions):
            # finish processing the expressions and build up the schema
            parsed_expression = self._parse_expression(
                expr.expression,
                i,
                target=expr.target.name,
                type=expr.target.type,
            )
            result.add(parsed_expression)

        return result

    def _sort_expressions(
        self, expressions: List[ParsedExpression]
    ) -> List[ParsedExpression]:
        """Sort expressions based on their target table names."""
        # sort the expressions to continue processing
        target_map: Dict[str, ParsedExpression] = {
            expr.target.name: expr for expr in expressions
        }
        # We’ll build:
        #    - 'adj': adjacency list mapping each Expression → set of Expressions that depend on it
        #    - 'in_degree': how many incoming “creator” edges each Expression has
        adj: Dict[ParsedExpression, Set[ParsedExpression]] = {
            expr: set() for expr in expressions
        }
        in_degree: Dict[ParsedExpression, int] = {expr: 0 for expr in expressions}

        # Populate adj & in_degree by scanning each expr’s table‐dependencies
        for expr in expressions:
            for dep_table in expr.tables:
                # If this dependency is itself produced by one of our expressions:
                producer = target_map.get(dep_table.source.name)
                if producer is not None:
                    # Edge: producer → expr
                    adj[producer].add(expr)
                    in_degree[expr] += 1

        # Kahn’s algorithm: start with all nodes of in_degree==0
        queue: List[ParsedExpression] = [
            expr for expr, deg in in_degree.items() if deg == 0
        ]
        ordered: List[ParsedExpression] = []

        while queue:
            node = queue.pop()  # pop any node with in_degree=0
            ordered.append(node)
            for child in adj[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        # If we didn’t visit all nodes, there is a cycle
        if len(ordered) < len(expressions):
            # Everything not in `ordered` is part of (or reachable from) a cycle.
            cycle_nodes = [expr for expr in expressions if expr not in ordered]
            cycle_targets = [expr.target.name for expr in cycle_nodes]
            logger.warning(
                "Cycle detected among SQL expressions → cannot resolve ordering. Impacted targets: %s",
                ", ".join(cycle_targets),
            )

            ordered.extend(cycle_nodes)

        return ordered

    async def aextract_lineages_from_file(
        self,
        path: StrPath,
        glob: Optional[str] = None,
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
        schema: Optional[dict[str, Any] | Schema] = None,
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
            schema (Optional[dict[str, Any] | Schema], optional): An optional schema object or dictionary to validate
                and use for lineage extraction. If provided, it will be used to update the internal schema
                representation with any new tables found during the extraction process.

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
            schema=schema,
        )

    def extract_lineages_from_file(
        self,
        path: StrPath,
        glob: Optional[str] = None,
        dialect: Optional[DialectType] = None,
        pre_transform: Optional[Callable[[str], str]] = None,
        schema: Optional[dict[str, Any] | Schema] = None,
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
            schema (Optional[dict[str, Any] | Schema]): An optional schema object or dictionary to validate
                and use for lineage extraction. If provided, it will be used to update the internal schema
                representation with any new tables found during the extraction process.

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
            schema=schema,
        )
