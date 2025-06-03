"""Expression parsing result model."""

# pylint: disable=no-member

import re
from typing import Dict, Optional, Set, Tuple, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    computed_field,
    model_serializer,
)
from sqlglot import Expression
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import Alias, Column, From, Star, Struct

from sql2lineage.types.model import (
    STRUCT_COLUMN_TYPES,
    ColumnLineage,
    DataColumn,
    DataTable,
    Schema,
    SchemaColumn,
    SchemaTable,
    TableLineage,
)
from sql2lineage.utils import SimpleTupleStore

SourceColumn: TypeAlias = DataColumn
TargetColumn: TypeAlias = DataColumn
ColumnAction: TypeAlias = str


class DummyParent:
    """Dummy parent class for expressions without a parent."""

    def find(self, o):  # pylint: disable=unused-argument
        """Find method to avoid errors.

        Dummy implementation that returns None.
        """
        return None


DUMMY_PARENT = DummyParent()


class ParsedExpression(BaseModel):
    """Parsed expression information."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    target: DataTable = Field(..., description="The output table of the expression.")

    columns: set[ColumnLineage] = Field(
        default_factory=set, description="The column lineage information."
    )

    tables: Set[TableLineage] = Field(
        default_factory=set, description="The source tables of the expression."
    )

    subqueries: Dict[str, "ParsedExpression"] = Field(
        default_factory=dict, description="The subqueries of the expression."
    )

    expression: Expression = Field(
        ...,
        description="The SQL expression.",
    )

    dialect: Optional[DialectType] = Field(
        None,
        description="The SQL dialect used for the expression.",
    )

    _schema: Optional[Schema] = PrivateAttr(
        default=None,
    )

    def __hash__(self):
        return hash((self.target, tuple(self.columns), tuple(self.tables)))

    @property
    def expression_str(self) -> str:
        """Get the string representation of the expression."""
        return self.expression.sql(pretty=True, dialect=self.dialect)

    @model_serializer(mode="plain")
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
            "target": self.target.model_dump(),
            "columns": [col.model_dump() for col in self.columns],
            "tables": [src.model_dump() for src in self.tables],
            "subqueries": {
                key: value.serialise_to_dict() for key, value in self.subqueries.items()
            },
            "expression": self.expression_str,
        }

    def _get_source_column(
        self,
        column: Column,
        source_table: Optional[DataTable],
        table_store: SimpleTupleStore[str, DataTable],
    ) -> DataColumn:
        _source_column = None
        _source_table = source_table
        if column.table:
            for tbl in self.tables:
                if column.table == tbl.alias:
                    _source_column = column.name
                    _source_table = tbl.source
                    break

            # check subqueries
            if _source_column is None:
                subquery = self.subqueries.get(column.table)
                if subquery:
                    # does the column exist in the subquery?
                    for col in subquery.columns:
                        if col.target.name == column.name:
                            _source_column = col.target.name
                            _source_table = subquery.target
                            break

        elif column.parts and source_table:
            joined_parts = ".".join([identifier.name for identifier in column.parts])
            _source_column = joined_parts.replace(source_table.name, "").strip(".")
        elif column.parts:
            _source_column = column.parts[-1].name
            _table = ".".join([identifier.name for identifier in column.parts[:-1]])
            _source_table = table_store.get(_table)

        if _source_column is None:
            # if we still don't have a source column, use the column name
            _source_column = column.name

        if _source_table and self._schema:
            self._schema.add_table_column(_source_table.name, _source_column)
        return DataColumn(name=_source_column or "", table=_source_table)

    def _process_struct_override(
        self,
        source_column: DataColumn,
        target: DataTable,
        target_column_name: Optional[str] = None,
    ) -> None:

        if self._schema is None:
            return None

        # if the column is a struct, we need to burst it out
        assert source_column.table is not None, "Source column must have a table."
        source_table = source_column.table

        if source_table.name not in self._schema:
            return None

        column = self._schema[source_table.name].get_column(source_column.name)
        fields = column.fields or []
        to_add = []
        for field in fields:
            source_column = DataColumn(
                name=f"{column.name}.{field.name}",
                table=source_table,
            )
            target_column = DataColumn(
                name=f"{target_column_name or column.name}.{field.name}", table=target
            )
            to_add.append((source_column, target_column, "COPY"))

        self._add_columns(*to_add)

    def _process_struct(
        self,
        expression: Expression,
        source: DataTable,
        target: DataTable,
        table_store: SimpleTupleStore[str, DataTable],
        alias: Optional[str] = None,
    ):

        # handle struct columns - burst them out
        if alias is None:
            alias = expression.alias
        else:
            alias = f"{alias}.{expression.alias}"

        schema_column = SchemaColumn(name=alias, type="STRUCT", fields=[])

        for expr in expression.this.expressions:

            # check if the column is a struct
            if isinstance(expr, Struct):
                self._process_struct(
                    expr,
                    source,
                    target,
                    table_store,
                    alias=alias,
                )
            else:

                expr_name = expr.alias_or_name or expr.name
                if not isinstance(expr, Column):
                    expr = expr.expression

                source_column = self._get_source_column(expr, source, table_store)
                target_column = DataColumn(name=f"{alias}.{expr_name}", table=target)

                if schema_column.fields is None:
                    schema_column.fields = []

                schema_column.fields.append(
                    SchemaColumn(
                        name=expr_name,
                        type="SIMPLE",
                        fields=None,
                    )
                )

                self.columns.add(
                    ColumnLineage(
                        target=target_column,
                        source=source_column,
                        action="COPY",
                    )
                )
        if self._schema:
            if target.name not in self._schema:
                self._schema.add(target.name)
            self._schema.add_table_column(target.name, schema_column)

    def _add_columns(
        self,
        *columns: Tuple[SourceColumn, TargetColumn, ColumnAction],
    ):
        for source, target, action in columns:
            self.columns.add(
                ColumnLineage(
                    target=target,
                    source=source,
                    action=action,
                )
            )

            if self._schema:
                if target.table is None:
                    continue

                self._schema.add_table_column(
                    target.table.name,
                    SchemaColumn(
                        name=target.name,
                        type="SIMPLE",
                        fields=None,
                    ),
                )

    def _check_struct(self, source_column: DataColumn) -> bool:
        """Check if the source column is a struct."""
        if not self._schema:
            return False

        if source_column.table and self._schema.is_struct(
            source_column.table.name, source_column.name
        ):
            return True
        return False

    def update_column_lineage(
        self,
        expression: Expression,
        source: DataTable,
        target: DataTable,
        table_store: SimpleTupleStore[str, DataTable],
    ):
        """
        Update the column lineage information based on the provided SQL expression.

        This method analyzes the given SQL expression and updates the lineage of columns
        between the source and target tables. It handles different types of SQL constructs
        such as column references, aliases, and wildcard selections (e.g., `*`).

        Args:
            expression (Expression): The SQL expression to analyze for column lineage.
            source (DataTable): The source table from which columns originate.
            target (DataTable): The target table to which columns are mapped.
            table_store (SimpleTupleStore[str, DataTable]): A store containing mappings
                of table names to DataTable objects.

        Returns:
            None

        """  # noqa: D212
        if not hasattr(expression, "selects"):
            return

        for select in expression.selects:  # type: ignore

            if isinstance(select.this, Struct):
                # handle struct columns - burst them out
                self._process_struct(
                    select,
                    source,
                    target,
                    table_store,
                )

            elif isinstance(select, Column):

                source_column = self._get_source_column(select, source, table_store)

                # check if the column is in the struct override
                if self._check_struct(source_column):
                    self._process_struct_override(source_column, target)
                else:
                    target_column = DataColumn(name=select.alias_or_name, table=target)
                    self._add_columns(
                        (source_column, target_column, "COPY"),
                    )

            # find column aliases - transformations
            elif isinstance(select, Alias):
                if isinstance(select.this, Column):
                    # alias is a column
                    source_column = self._get_source_column(
                        select.this, source, table_store
                    )

                    # check if the column is in the struct override
                    if self._check_struct(source_column):
                        self._process_struct_override(source_column, target)
                    else:
                        target_column = DataColumn(
                            name=select.alias_or_name, table=target
                        )

                        self.columns.add(
                            ColumnLineage(
                                target=target_column,
                                source=source_column,
                                action="COPY",
                            )
                        )

                else:

                    for column in select.find_all(Column):
                        source_column = self._get_source_column(
                            column, source, table_store
                        )

                        if self._check_struct(source_column):
                            self._process_struct_override(
                                source_column, target, select.alias_or_name
                            )
                        else:

                            pattern = re.compile(
                                f"(?: as) {column.alias_or_name}", re.IGNORECASE
                            )
                            sql = pattern.sub("", select.sql())
                            action = "TRANSFORM" if sql != column.sql() else "COPY"

                            target_column = DataColumn(
                                name=select.alias_or_name, table=target
                            )

                            self.columns.add(
                                ColumnLineage(
                                    target=target_column,
                                    source=source_column,
                                    action=action,
                                )
                            )

            # elif expression.find(Star):
            elif isinstance(select, Star):

                select_from = (
                    select.find(From)
                    or (select.parent or DUMMY_PARENT).find(From)
                    or (select.parent_select or DUMMY_PARENT).find(From)
                )
                if select_from:
                    # if the star is used in a FROM clause, we need to find the source table
                    source_table = table_store.get(select_from.alias_or_name)

                    # we need to find all columns in the source table
                    self._get_star_columns(target, source_table)

                else:
                    raise ValueError(
                        f"Unable to identify From clause for Star(*): {self.expression}."
                    )

    def _get_star_columns(
        self,
        target: DataTable,
        source_table: DataTable | None = None,
    ):
        if source_table is None:
            return

        # check the expression for CTE columns
        for column in list(self.columns):
            if column.target.table and column.target.table.name == source_table.name:
                if column.target.name == "*":
                    self._get_star_columns(target, column.source.table)
                else:
                    target_column = DataColumn(name=column.target.name, table=target)

                    self._add_columns(
                        (
                            column.target,
                            target_column,
                            "COPY",
                        )
                    )

        # check the schema for columns from other tables
        if self._schema:
            for column in self._schema.get(
                source_table.name, SchemaTable(name="default", type="NONE")
            ).columns:

                if column.type in STRUCT_COLUMN_TYPES:
                    # if the column is a struct, we need to burst it out
                    fields = column.fields or []
                    to_add = []
                    for field in fields:
                        source_column = DataColumn(
                            name=f"{column.name}.{field.name}",
                            table=source_table,
                        )
                        target_column = DataColumn(
                            name=f"{column.name}.{field.name}", table=target
                        )
                        to_add.append((source_column, target_column, "COPY"))

                    self._add_columns(*to_add)

                else:
                    target_column = DataColumn(name=column.name, table=target)

                    self._add_columns(
                        (
                            DataColumn(name=column.name, table=source_table),
                            target_column,
                            "COPY",
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
    _tables: Set[TableLineage] = PrivateAttr(
        default_factory=set,
    )

    _schema: Optional[Schema] = PrivateAttr(
        default=None,
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
    def tables(self) -> Set[TableLineage]:
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
            "columns": [col.model_dump() for col in self._columns],
            "tables": [src.model_dump() for src in self._tables],
        }
