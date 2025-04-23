"""Expression parsing result model."""

# pylint: disable=no-member

import re
from typing import Dict, Optional, Set, Tuple

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    computed_field,
    model_serializer,
)
from pydantic.config import ConfigDict
from sqlglot import Expression
from sqlglot.expressions import Alias, Column, Star

from sql2lineage.types.model import TableType


class SourceTable(BaseModel):
    """Source table information."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    target: str = Field(..., description="The output table of the source.")
    source: str = Field(..., description="The source table of the expression.")
    alias: Optional[str] = Field(None, description="The alias of the source table.")
    type: TableType = Field(
        "TABLE",
        description="The type of the source table (e.g., 'TABLE', 'SUBQUERY', 'CTE').",
    )

    def __hash__(self):
        return hash((self.target, self.source, self.alias))

    @computed_field
    @property
    def node_type(self) -> str:
        """Get the node type of the source table."""
        return "TABLE"


class ColumnLineage(BaseModel):
    """Column lineage information."""

    target: str = Field(..., description="The ouput column name.")
    source: str = Field(..., description="The source column name.")
    action: Optional[str] = Field(
        None, description="The action performed on the column."
    )
    table_type: TableType = Field(
        "TABLE",
        description="The type of the source table (e.g., 'TABLE', 'SUBQUERY', 'CTE').",
    )

    def __hash__(self):
        return hash((self.target, self.source, self.action, self.table_type))

    @computed_field
    @property
    def node_type(self) -> str:
        """Get the node type of the column lineage."""
        return "COLUMN"


class ParsedExpression(BaseModel):
    """Parsed expression information."""

    target: str = Field(..., description="The output table of the expression.")

    columns: set[ColumnLineage] = Field(
        default_factory=set, description="The column lineage information."
    )

    tables: Set[SourceTable] = Field(
        default_factory=set, description="The source tables of the expression."
    )

    subqueries: Dict[str, "ParsedExpression"] = Field(
        default_factory=dict, description="The subqueries of the expression."
    )

    expression: str = Field(
        ...,
        description="The SQL expression.",
    )

    def __hash__(self):
        return hash((self.target, self.columns, self.tables))

    @model_serializer
    def serialise_to_dict(self):
        """Serialize the current object into a dictionary representation.

        Returns:
            dict: A dictionary containing the serialized data with the following structure:
                - "target" (str): The name of the output table.
                - "columns" (list): A list of dictionaries, each representing a column lineage with:
                    - "target" (str): The name of the output table for the column.
                    - "column" (str): The name of the column.
                    - "source" (str): The source column from which the current column is derived.
                    - "action" (str): The transformation or action applied to the column.
                - "tables" (list): A list of dictionaries, each representing a source table with:
                    - "target" (str): The name of the output table.
                    - "source" (str): The name of the source table.
                    - "alias" (str): The alias used for the source table.
                - "subqueries" (dict): A dictionary where keys are subquery identifiers and values are the serialized
                  dictionary representations of the subqueries.

        """
        return {
            "target": self.target,
            "columns": sorted(
                [
                    {
                        "target": col.target,
                        "source": col.source,
                        "action": col.action,
                        "node_type": col.node_type,
                        "table_type": col.table_type,
                    }
                    for col in self.columns
                ],
                key=lambda entry: (
                    entry["target"],
                    entry["source"],
                    entry["action"],
                    entry["node_type"],
                    entry["table_type"],
                ),
            ),
            "tables": sorted(
                [
                    {
                        "target": src.target,
                        "source": src.source,
                        "alias": src.alias,
                        "node_type": src.node_type,
                        "type": src.type,
                    }
                    for src in self.tables
                ],
                key=lambda entry: (
                    entry["target"],
                    entry["source"],
                    entry["alias"],
                    entry["node_type"],
                    entry["type"],
                ),
            ),
            "subqueries": {
                key: value.serialise_to_dict() for key, value in self.subqueries.items()
            },
            "expression": self.expression,
        }

    def _get_source_column(
        self,
        column: Column,
        source_table: Optional[str],
    ) -> Tuple[Optional[str], Optional[TableType]]:
        """Construct the fully qualified name of a source column.

        Args:
            source_tables (Set[SourceTable]): A set of source tables, where each entry contains
                information about the source table, its alias, and other metadata.
            source_table (Optional[str]): The name of the source table, if available.
            column (Column): The column object containing details about the column, such as its
                name, table, and parts.

        Returns:
            str: The fully qualified name of the source column in the format "table.column" or
            "source_table.column", depending on the provided inputs.

        """
        source_column = None
        if len(column.parts) == 1 and source_table:
            source_column = f"{source_table}.{column.parts[0].name}"
        elif column.table:
            for _source_table in self.tables:
                if column.table == _source_table.alias:
                    return f"{_source_table.source}.{column.name}", _source_table.type

            # check subqueries
            if source_column is None:
                subquery = self.subqueries.get(column.table)
                if subquery:
                    # does the column exist in the subquery?
                    for col in subquery.columns:
                        if col.target.split(".")[-1] == column.name:
                            return col.source, "SUBQUERY"

        if source_column is None:
            source_column = ".".join([identifier.name for identifier in column.parts])

        return source_column, "TABLE"

    def update_column_lineage(
        self,
        expression: Expression,
        source: Optional[str] = None,
        target: Optional[str] = None,
    ):
        """Update the column lineage information based on the provided SQL expression.

        This method processes the given SQL expression to extract column lineage details,
        including source columns, target columns, and the type of transformation or action
        (e.g., COPY or TRANSFORM). It supports handling column aliases, transformations,
        and wildcard column selections (e.g., `SELECT *`).

        Args:
            expression (Expression): The SQL expression to analyze for column lineage.
            source (Optional[str]): The source table or alias name. Defaults to None.
            target (Optional[str]): The target table or alias name. Defaults to None.

        Returns:
            None: The method updates the `self.columns` attribute with column lineage
            information and does not return any value.

        """
        if not hasattr(expression, "selects"):
            return

        if target is None:
            target = self.target

        c_target = []
        if target:
            c_target.append(target)

        for select in expression.selects:  # type: ignore

            if isinstance(select, Column):

                source_column, table_type = self._get_source_column(select, source)

                self.columns.add(
                    ColumnLineage(
                        target=".".join(c_target + [select.alias_or_name]),
                        source=source_column or "",
                        action="COPY",
                        table_type=table_type or "TABLE",
                    )
                )

            # find column aliases - transformations
            elif isinstance(select, Alias):
                if isinstance(select.this, Column):
                    # alias is a column
                    source_column, table_type = self._get_source_column(
                        select.this, source
                    )
                    self.columns.add(
                        ColumnLineage(
                            target=".".join(c_target + [select.alias_or_name]),
                            source=source_column or "",
                            action="COPY",
                            table_type=table_type or "TABLE",
                        )
                    )

                else:

                    for column in select.find_all(Column):
                        source_column, table_type = self._get_source_column(
                            column, source
                        )

                        pattern = re.compile(
                            f"(?: as) {column.alias_or_name}", re.IGNORECASE
                        )
                        sql = pattern.sub("", select.sql())
                        action = "TRANSFORM" if sql != column.sql() else "COPY"

                        self.columns.add(
                            ColumnLineage(
                                target=".".join(c_target + [select.alias_or_name]),
                                source=source_column or "",
                                action=action,
                                table_type=table_type or "TABLE",
                            )
                        )

            elif expression.find(Star):
                for table in self.tables:
                    if table.target == target:
                        for col in list(self.columns):
                            column_target = ".".join(col.target.split(".")[:-1])
                            column_column = col.target.split(".")[-1]
                            if column_target == table.source:
                                self.columns.add(
                                    ColumnLineage(
                                        target=".".join([target, column_column]),
                                        source=f"{table.source}.{column_column}",
                                        action="COPY",
                                        table_type=table.type,
                                    )
                                )


class ParsedResult(BaseModel):
    """Parsed result of the SQL expression."""

    _expressions: list[ParsedExpression] = PrivateAttr(
        default_factory=list,
    )
    _columns: Set[ColumnLineage] = PrivateAttr(
        default_factory=set,
    )
    _tables: Set[SourceTable] = PrivateAttr(
        default_factory=set,
    )

    @computed_field
    @property
    def expressions(self) -> list[ParsedExpression]:
        """List of parsed expressions."""
        return self._expressions

    @computed_field
    @property
    def columns(self) -> Set[ColumnLineage]:
        """List of column lineage information."""
        return self._columns

    @computed_field
    @property
    def tables(self) -> Set[SourceTable]:
        """List of source tables."""
        return self._tables

    def add(self, expression: ParsedExpression) -> None:
        """Add a parsed expression to the result."""
        self._expressions.append(expression)

        for column in list(expression.columns or []):
            self._columns.add(column)

        for table in list(expression.tables or []):
            self._tables.add(table)

    @model_serializer
    def serialise_to_dict(self):
        """Serialize the current object into a dictionary representation.

        Returns:
            dict: A dictionary containing the serialized data with the following keys:
                - "expressions": A list of serialized expressions, where each expression
                  is represented as a dictionary obtained by calling `serialise_to_dict`
                  on each expression in `self._expressions`.
                - "columns": A sorted list of dictionaries representing columns, where
                  each dictionary contains:
                    - "target": The target of the column.
                    - "column": The column name.
                    - "source": The source of the column.
                    - "action": The action performed on the column.
                  The list is sorted by the tuple (target, column, source, action).
                - "tables": A sorted list of dictionaries representing tables, where
                  each dictionary contains:
                    - "target": The target of the table.
                    - "source": The source of the table.
                    - "alias": The alias of the table.
                  The list is sorted by the tuple (target, source, alias).

        """
        return {
            "expressions": [
                expression.serialise_to_dict() for expression in self._expressions
            ],
            "columns": sorted(
                [
                    {
                        "target": col.target,
                        "source": col.source,
                        "action": col.action,
                        "node_type": col.node_type,
                        "table_type": col.table_type,
                    }
                    for col in self._columns
                ],
                key=lambda entry: (
                    entry["target"],
                    entry["source"],
                    entry["action"],
                    entry["node_type"],
                    entry["table_type"],
                ),
            ),
            "tables": sorted(
                [
                    {
                        "target": src.target,
                        "source": src.source,
                        "alias": src.alias,
                        "node_type": src.node_type,
                        "type": src.type,
                    }
                    for src in self._tables
                ],
                key=lambda entry: (
                    entry["target"],
                    entry["source"],
                    entry["alias"],
                    entry["node_type"],
                    entry["type"],
                ),
            ),
        }


class LineageResult(BaseModel):
    """Lineage result."""

    model_config = ConfigDict(extra="allow")

    source: str = Field(..., description="The source of the lineage.")
    target: str = Field(..., description="The target of the lineage.")
    node_type: Optional[str] = Field(
        None, description="The type of the node (e.g., 'COLUMN')."
    )
