"""Model types."""

# pylint: disable=no-member

from typing import Optional, TypeVar, overload

from pydantic import (
    BaseModel,
    Field,
    computed_field,
)
from pydantic.config import ConfigDict

from sql2lineage.types.table import TableType


class LineageNode(BaseModel):
    """Lineage node information."""

    model_config = ConfigDict(extra="allow")

    source: str = Field(..., description="The source of the edge.")
    target: str = Field(..., description="The target of the edge.")
    node_type: Optional[str] = Field(
        None, description="The type of the node (e.g., 'COLUMN')."
    )
    source_type: TableType = Field(
        ..., description="The type of the source (e.g., 'TABLE')."
    )
    target_type: TableType = Field(
        ..., description="The type of the target (e.g., 'TABLE')."
    )

    def __hash__(self):
        return hash((self.source, self.target, self.node_type, self.source_type))

    def __str__(self) -> str:
        """Get the string representation of the node."""
        return f"{self.source} -> {self.target}"


class DataTable(BaseModel):
    """Table information."""

    name: str = Field(..., description="The name of the table.")
    type: TableType = Field(
        "TABLE",
        description="The type of the table (e.g., 'TABLE', 'SUBQUERY', 'CTE').",
    )

    def __hash__(self):
        return hash((self.name, self.type))

    def __str__(self) -> str:
        """Get the string representation of the table."""
        return self.name

    @property
    def to_str(self) -> str:
        """Get the column as a string."""
        return str(self)


class TableLineage(BaseModel):
    """Source table information."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    target: DataTable = Field(..., description="The output table of the source.")
    source: DataTable = Field(..., description="The source table of the expression.")
    alias: Optional[str] = Field(None, description="The alias of the source table.")

    def __hash__(self):
        return hash((self.target, self.source, self.alias))

    @computed_field
    @property
    def node_type(self) -> str:
        """Get the node type of the source table."""
        return "TABLE"

    @computed_field
    @property
    def source_type(self) -> TableType:
        """Get the source type of the table lineage."""
        return self.source.type

    @computed_field
    @property
    def target_type(self) -> TableType:
        """Get the table type of the source table."""
        return self.target.type

    @property
    def as_edge(self):
        """Get the column lineage as an edge."""
        attrs = self.model_dump(
            exclude_unset=True, exclude_none=True, exclude={"target", "source"}
        )
        attrs["source"] = self.source.to_str
        attrs["target"] = self.target.to_str
        return LineageNode.model_validate(attrs)


class DataColumn(BaseModel):
    """Column information."""

    table: Optional[DataTable] = Field(
        None, description="The table to which the column belongs."
    )
    name: str = Field(..., description="The name of the column.")

    def __hash__(self):
        return hash((self.table, self.name))

    def __str__(self) -> str:
        """Get the string representation of the column."""
        parts = []
        if self.table:
            parts.append(self.table.name)
        parts.append(self.name)
        return ".".join(parts)

    @property
    def to_str(self) -> str:
        """Get the alias or name of the column."""
        return str(self)


class ColumnLineage(BaseModel):
    """Column lineage information."""

    target: DataColumn = Field(..., description="The ouput column name.")
    source: DataColumn = Field(..., description="The source column name.")
    action: Optional[str] = Field(
        None, description="The action performed on the column."
    )

    def __hash__(self):
        return hash((self.target, self.source, self.action))

    @computed_field
    @property
    def node_type(self) -> str:
        """Get the node type of the column lineage."""
        return "COLUMN"

    @computed_field
    @property
    def source_type(self) -> TableType:
        """Get the source type of the table lineage."""
        if self.source.table:
            return self.source.table.type
        return "NONE"

    @computed_field
    @property
    def target_type(self) -> TableType:
        """Get the table type of the source table."""
        if self.target.table:
            return self.target.table.type
        return "NONE"

    @property
    def as_edge(self):
        """Get the column lineage as an edge."""
        attrs = self.model_dump(
            exclude_unset=True, exclude_none=True, exclude={"target", "source"}
        )
        attrs["source"] = self.source.to_str
        attrs["target"] = self.target.to_str
        return LineageNode.model_validate(attrs)


# region schema

STRUCT_COLUMN_TYPES = {"STRUCT", "RECORD"}


class SchemaColumn(BaseModel):
    """Schema column information."""

    name: str = Field(..., description="The name of the column.")
    type: str = Field(..., description="The type of the column.")
    fields: Optional[list["SchemaColumn"]] = Field(
        None, description="The fields of the column if it is a complex type."
    )


class SchemaTable(DataTable):
    """Schema table information."""

    columns: list[SchemaColumn] = Field(
        description="The columns of the table.", default_factory=list
    )

    def __contains__(self, item: str | SchemaColumn) -> bool:
        """Check if the table contains a column with the given name."""
        if isinstance(item, str):
            return any(column.name == item for column in self.columns)
        return any(column.name == item.name for column in self.columns)

    def __getitem__(self, item: str) -> SchemaColumn:
        """Get a column by its name."""
        for column in self.columns:
            if column.name == item:
                return column
        raise KeyError(f"Column '{item}' not found in table '{self.name}'.")

    def __setitem__(self, key: str, value: SchemaColumn) -> None:
        """Set a column in the table by its name."""
        if key in self:
            raise ValueError(f"Column '{key}' already exists in table '{self.name}'.")
        self.columns.append(value)

    def add(
        self,
        column: str | SchemaColumn,
        type: Optional[str] = None,  # pylint: disable=redefined-builtin
    ) -> None:
        """Add a column to the table.

        If a string is provided, it is converted to a SchemaColumn with type "STRING".
        Raises a ValueError if a column with the same name already exists in the table.

        Args:
            column (str | SchemaColumn): The column to add, either as a name (str) or a SchemaColumn
                instance.
            type (Optional[str]): The type of the column. If not provided, defaults to "STRING".

        Raises:
            ValueError: If a column with the same name already exists in the table.

        """
        if isinstance(column, str):
            column = SchemaColumn(name=column, type=type or "STRING", fields=None)

        if column.name in self:
            raise ValueError(
                f"Column '{column.name}' already exists in table '{self.name}'."
            )
        self.columns.append(column)

    def add_if(
        self,
        column: str | SchemaColumn,
        type: Optional[str] = None,  # pylint: disable=redefined-builtin
    ) -> None:
        """Add a column to the table if it does not already exist.

        Args:
            column (str | SchemaColumn): The column to add, either as a string (column name) or a
                SchemaColumn instance.
            type (Optional[str]): The type of the column. If not provided, defaults to "STRING".

        Returns:
            None

        """
        if isinstance(column, str):
            column = SchemaColumn(name=column, type=type or "STRING", fields=None)

        if column.name not in self:
            self.columns.append(column)

    def get_column(self, column: str) -> SchemaColumn:
        """Get a column by its name.

        Args:
            column (str): The name of the column to retrieve.

        Returns:
            SchemaColumn: The column with the specified name.

        Raises:
            KeyError: If the column does not exist in the table.

        """
        for col in self.columns:
            if col.name == column:
                return col
        raise KeyError(f"Column '{column}' not found in table '{self.name}'.")


# Define two type variables for the key and value.
D = TypeVar("D")


class Schema(BaseModel):
    """Schema information."""

    tables: list[SchemaTable] = Field(
        description="The tables in the schema.", default_factory=list
    )

    def __contains__(self, item: str | SchemaTable) -> bool:
        """Check if the schema contains a table with the given name."""
        if isinstance(item, str):
            return any(table.name == item for table in self.tables)
        return any(table.name == item.name for table in self.tables)

    def __getitem__(self, item: str) -> SchemaTable:
        """Get a table by its name."""
        for table in self.tables:
            if table.name == item:
                return table
        raise KeyError(f"Table '{item}' not found in schema.")

    def __setitem__(self, key: str, value: SchemaTable) -> None:
        """Set a table in the schema by its name."""
        if key in self:
            raise ValueError(f"Table '{key}' already exists in the schema.")
        self.tables.append(value)

    def add(self, table: str | SchemaTable) -> None:
        """Add a table to the schema.

        If a string is provided, it is converted to a SchemaTable with type "TABLE".
        Raises a ValueError if a table with the same name already exists in the schema.

        Args:
            table (str | SchemaTable): The table to add, either as a name (str) or a SchemaTable instance.

        Raises:
            ValueError: If a table with the same name already exists in the schema.

        """
        if isinstance(table, str):
            table = SchemaTable(name=table, type="TABLE")

        if table.name in self:
            raise ValueError(f"Table '{table.name}' already exists in the schema.")
        self.tables.append(table)

    def add_if(self, table: str | SchemaTable) -> None:
        """Add a table to the collection if it does not already exist.

        Args:
            table (str | SchemaTable): The table to add, either as a string (table name) or a SchemaTable instance.

        Returns:
            None

        """
        if isinstance(table, str):
            table = SchemaTable(name=table, type="TABLE")

        if table.name not in self:
            self.tables.append(table)

    def add_table_column(self, table: str | SchemaTable, column: str | SchemaColumn):
        """Add a table and its columns to the collection if not already present.

        Args:
            table (str | SchemaTable): The name of the table as a string or a SchemaTable object.
            column (str | SchemaColumn): The column name as a string or a SchemaColumn object to add to the table.

        Notes:
            - If the table does not exist in the collection, it will be added.
            - The specified columns will be added to the table using the `add_if` method of the SchemaTable.

        """
        # if column is an empty string or * do not add it
        if (
            isinstance(column, str)
            and (not column or column == "*")
            or (not isinstance(column, str) and column.name == "*")
        ):
            return

        if table not in self:
            self.add(table)

        table_name = table if isinstance(table, str) else table.name
        schema_table = self[table_name]
        schema_table.add_if(column)

    @overload
    def get(self, table_name: str, default: D) -> SchemaTable | D: ...

    @overload
    def get(self, table_name: str) -> SchemaTable | None: ...

    def get(
        self, table_name: str, default: Optional[D] = None
    ) -> SchemaTable | D | None:
        """Retrieve a table from the collection by its name.

        Args:
            table_name (str): The name of the table to retrieve.
            default (Optional[D], optional): The value to return if the table is not found.
                Defaults to None.

        Returns:
            SchemaTable | D | None: The table with the specified name if found; otherwise, returns
                the provided default value.

        """
        for table in self.tables:
            if table.name == table_name:
                return table

        return default

    def is_struct(self, table: str, column: str) -> bool:
        """Check if a column is a struct type in the schema.

        Args:
            table (str): The name of the table to check.
            column (str): The name of the column to check.

        Returns:
            bool: True if the column is a struct type, False otherwise.

        """
        if table not in self:
            raise KeyError(f"Table '{table}' not found in schema.")
        schema_table = self[table]
        if column not in schema_table:
            return False
        column_info = schema_table[column]
        return column_info.type in STRUCT_COLUMN_TYPES


# endregion schema
