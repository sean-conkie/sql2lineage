"""Table type."""

# pylint: disable=no-member

from typing import Literal, TypeAlias

TableType: TypeAlias = Literal["TABLE", "SUBQUERY", "CTE", "UNNEST", "NONE", "QUERY"]
