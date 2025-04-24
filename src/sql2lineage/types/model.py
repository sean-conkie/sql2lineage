"""Model types."""

from typing import Literal, Optional, TypeAlias

from pydantic import (
    BaseModel,
    Field,
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
