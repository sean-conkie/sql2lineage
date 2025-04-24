"""Model types."""

# pylint: disable=no-member

from typing import Literal, Optional, TypeAlias

from pydantic import (
    BaseModel,
    Field,
    computed_field,
)
from pydantic.config import ConfigDict


class Edge(BaseModel):
    """Edge information."""

    model_config = ConfigDict(extra="allow")

    source: str = Field(
        ..., description="The source of the edge.", serialization_alias="u_of_edge"
    )
    target: str = Field(
        ..., description="The target of the edge.", serialization_alias="v_of_edge"
    )


TableType: TypeAlias = Literal["TABLE", "SUBQUERY", "CTE", "UNNEST"]


class LineageNode(BaseModel):
    """A node in the lineage graph."""

    model_config = ConfigDict(extra="allow")

    source: str = Field(..., description="The source of the lineage.")
    target: str = Field(..., description="The target of the lineage.")
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
    def source_type(self) -> str:
        """Get the source type of the table lineage."""
        return self.source.type

    @computed_field
    @property
    def target_type(self) -> str:
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
        return Edge.model_validate(attrs)


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
    def source_type(self) -> Optional[str]:
        """Get the source type of the table lineage."""
        if self.source.table:
            return self.source.table.type

    @computed_field
    @property
    def target_type(self) -> Optional[str]:
        """Get the table type of the source table."""
        if self.target.table:
            return self.target.table.type

    @property
    def as_edge(self):
        """Get the column lineage as an edge."""
        attrs = self.model_dump(
            exclude_unset=True, exclude_none=True, exclude={"target", "source"}
        )
        attrs["source"] = self.source.to_str
        attrs["target"] = self.target.to_str
        return Edge.model_validate(attrs)
