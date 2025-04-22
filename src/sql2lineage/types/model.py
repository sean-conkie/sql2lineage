"""Model types."""

from typing import Literal, TypeAlias

TableType: TypeAlias = Literal["TABLE", "SUBQUERY", "CTE", "UNNEST"]
