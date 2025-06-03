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

    name: str
    fields: Optional[list["SchemaColumn"]] = None

    @computed_field
    @property
    def type(self) -> str:
        """Return the type of the object as a string.

        If the object has no fields (`self.fields` is `None`), returns "SIMPLE".
        Otherwise, returns "RECORD".

        Returns:
            str: The type of the object, either "SIMPLE" or "RECORD".

        """
        if self.fields is None:
            return "SIMPLE"
        return "RECORD"


class SchemaTable(BaseModel):
    """Schema table information."""

    name: str
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

    def add_table(
        self, name: str, type: Optional[str] = None  # pylint: disable=redefined-builtin
    ) -> None:
        """Add a table to the collection if it does not already exist.

        Args:
            name (str): The name of the table to add.
            type (Optional[str], optional): The type of the table (e.g., "TABLE", "VIEW").
                Defaults to "TABLE".

        Returns:
            None

        """
        if name in self:
            return

        if type is None:
            type = "TABLE"
        self.tables.append(SchemaTable(name=name))

    def add_column(self, table_name: str, path: str) -> None:
        """Add a column to the schema for the specified table, supporting nested (dot-delimited) column paths.

        If the table does not exist, it is created. The column path is split by dots to support nested structures,
        creating intermediate columns as needed. Intermediate columns are created as records (with empty fields lists),
        while the final segment is created as a simple field (fields=None).

        Args:
            table_name (str): The name of the table to which the column should be added.
            path (str): The dot-delimited path representing the (possibly nested) column to add.

        Returns:
            None

        """
        parts = path.split(".")

        # 1. Find or create the table
        if table_name in self:
            tbl = self[table_name]
        else:
            tbl = SchemaTable(name=table_name)
            self.tables.append(tbl)

        # 2. Walk (and/or create) each segment of the path
        current_columns = tbl.columns  # start at top‐level columns list

        for idx, part in enumerate(parts):
            # See if this segment already exists at the current level
            try:
                col = next(c for c in current_columns if c.name == part)
            except StopIteration:
                # Not found, so we create a new SchemaColumn
                # If this is an intermediate level, we want fields=[] so that type="RECORD"
                if idx < len(parts) - 1:
                    col = SchemaColumn(name=part, fields=[])
                else:
                    # last segment: a SIMPLE field (fields=None)
                    col = SchemaColumn(name=part, fields=None)
                current_columns.append(col)

            # If this is not the last part, advance into its .fields (and ensure it's a list)
            if idx < len(parts) - 1:
                if col.fields is None:
                    # convert a SIMPLE into a RECORD so we can descend
                    col.fields = []
                current_columns = col.fields  # descend into nested fields

        # end of loop: the path has been created (or already existed) in nested form

    def get_column(self, table_name: str, column_path: str) -> SchemaColumn | None:
        """Retrieve a column from the schema by its table name and column path.

        Args:
            table_name (str): The name of the table containing the column.
            column_path (str): The dot-delimited path to the column.

        Returns:
            SchemaColumn: The requested column.

        Raises:
            KeyError: If the table or column does not exist.

        """
        if table_name not in self:
            raise KeyError(f"Table '{table_name}' not found in schema.")
        schema_table = self[table_name]

        parts = column_path.split(".")
        current_columns = schema_table.columns

        col = None

        for part in parts:
            try:
                col = next(c for c in current_columns if c.name == part)
            except StopIteration as exc:
                raise KeyError(
                    f"Column '{column_path}' not found in table '{table_name}'."
                ) from exc
            current_columns = col.fields if col.fields else []

        return col

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
