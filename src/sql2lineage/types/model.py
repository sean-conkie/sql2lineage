"""Model types."""

from typing import Literal, TypeAlias

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
