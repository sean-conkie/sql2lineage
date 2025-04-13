"""Expression parsing result model."""

# pylint: disable=no-member

import re
from typing import Dict, Optional, Set

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    computed_field,
    model_serializer,
)
from sqlglot import Expression
from sqlglot.expressions import Alias, Column, Star


class SourceTable(BaseModel):
    """Source table information."""

    output_table: str = Field(..., description="The output table of the source.")
    source_table: str = Field(..., description="The source table of the expression.")
    alias: Optional[str] = Field(None, description="The alias of the source table.")

    def __hash__(self):
        return hash((self.output_table, self.source_table, self.alias))


class ColumnLineage(BaseModel):
    """Column lineage information."""

    output_table: Optional[str] = Field(
        None, description="The output table of the column."
    )
    column: Optional[str] = Field(None, description="The column name.")
    source_column: Optional[str] = Field(None, description="The source column name.")
    action: Optional[str] = Field(
        None, description="The action performed on the column."
    )

    def __hash__(self):
        return hash((self.output_table, self.column, self.source_column, self.action))


class ParsedExpression(BaseModel):
    """Parsed expression information."""

    output_table: str = Field(..., description="The output table of the expression.")

    column_lineage: set[ColumnLineage] = Field(
        default_factory=set, description="The column lineage information."
    )

    source_tables: Set[SourceTable] = Field(
        default_factory=set, description="The source tables of the expression."
    )

    subqueries: Dict[str, "ParsedExpression"] = Field(
        default_factory=dict, description="The subqueries of the expression."
    )

    def __hash__(self):
        return hash((self.output_table, self.column_lineage, self.source_tables))

    @model_serializer
    def serialise_to_dict(self):
        """Serialize the current object into a dictionary representation.

        Returns:
            dict: A dictionary containing the serialized data with the following structure:
                - "output_table" (str): The name of the output table.
                - "column_lineage" (list): A list of dictionaries, each representing a column lineage with:
                    - "output_table" (str): The name of the output table for the column.
                    - "column" (str): The name of the column.
                    - "source_column" (str): The source column from which the current column is derived.
                    - "action" (str): The transformation or action applied to the column.
                - "source_tables" (list): A list of dictionaries, each representing a source table with:
                    - "output_table" (str): The name of the output table.
                    - "source_table" (str): The name of the source table.
                    - "alias" (str): The alias used for the source table.
                - "subqueries" (dict): A dictionary where keys are subquery identifiers and values are the serialized
                  dictionary representations of the subqueries.

        """
        return {
            "output_table": self.output_table,
            "column_lineage": [
                {
                    "output_table": col.output_table,
                    "column": col.column,
                    "source_column": col.source_column,
                    "action": col.action,
                }
                for col in self.column_lineage
            ],
            "source_tables": [
                {
                    "output_table": src.output_table,
                    "source_table": src.source_table,
                    "alias": src.alias,
                }
                for src in self.source_tables
            ],
            "subqueries": {
                key: value.serialise_to_dict() for key, value in self.subqueries.items()
            },
        }

    def _get_source_column(
        self,
        column: Column,
        source_table: Optional[str],
    ) -> str:
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
            for _source_table in self.source_tables:
                if column.table == _source_table.alias:
                    source_column = f"{_source_table.source_table}.{column.name}"
                    break

            # check subqueries
            if source_column is None:
                subquery = self.subqueries.get(column.table)
                if subquery:
                    # does the column exist in the subquery?
                    for col in subquery.column_lineage:
                        if col.column == column.name:
                            source_column = col.source_column
                            break

        if source_column is None:
            source_column = ".".join([identifier.name for identifier in column.parts])

        return source_column

    def update_column_lineage(
        self,
        expression: Expression,
        source_table: Optional[str],
    ):

        if not hasattr(expression, "selects"):
            return

        for select in expression.selects:  # type: ignore

            if isinstance(select, Column):

                source_column = self._get_source_column(select, source_table)

                self.column_lineage.add(
                    ColumnLineage(
                        output_table=self.output_table,
                        column=select.alias_or_name,
                        source_column=source_column,
                        action="COPY",
                    )
                )

            # find column aliases - transformations
            elif isinstance(select, Alias):
                for column in select.find_all(Column):
                    source_column = self._get_source_column(column, source_table)

                    pattern = re.compile(
                        f"(?: as) {column.alias_or_name}", re.IGNORECASE
                    )
                    sql = pattern.sub("", select.sql())
                    action = "TRANSFORM" if sql != column.sql() else "COPY"

                    self.column_lineage.add(
                        ColumnLineage(
                            output_table=self.output_table,
                            column=column.alias_or_name,
                            source_column=source_column,
                            action=action,
                        )
                    )

            elif expression.find(Star):
                for target, source in self.source_tables:
                    if target == self.output_table:
                        for col in list(self.column_lineage):
                            if col.output_table == source:
                                self.column_lineage.add(
                                    ColumnLineage(
                                        output_table=self.output_table,
                                        column=col.source_column,
                                        source_column=f"{source}.{col.source_column}",
                                        action="COPY",
                                    )
                                )


class ParsedResult(BaseModel):
    """Parsed result of the SQL expression."""

    _expressions: list[ParsedExpression] = PrivateAttr(
        default_factory=list,
    )
    _column_lineage: Set[ColumnLineage] = PrivateAttr(
        default_factory=set,
    )
    _source_tables: Set[SourceTable] = PrivateAttr(
        default_factory=set,
    )

    @computed_field
    @property
    def expressions(self) -> list[ParsedExpression]:
        """List of parsed expressions."""
        return self._expressions

    @computed_field
    @property
    def column_lineage(self) -> Set[ColumnLineage]:
        """List of column lineage information."""
        return self._column_lineage

    def add(self, expression: ParsedExpression) -> None:
        """Add a parsed expression to the result."""
        self._expressions.append(expression)

        for lineage in list(expression.column_lineage or []):
            self._column_lineage.add(lineage)

        for source_table in list(expression.source_tables or []):
            self._source_tables.add(source_table)
