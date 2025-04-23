"""Lineage graph utilities."""

from typing import Protocol, TypeAlias, runtime_checkable

from sql2lineage.types.model import Edge


class Stringable(Protocol):
    """Protocol for stringable objects."""

    def __str__(self) -> str: ...


@runtime_checkable
class NodeProtocol(Protocol):
    """Protocol for a node in the lineage graph.

    A node must have a `source` and `target` attribute, both of which are can be interpreted
    as a string.
    """

    @property
    def source(  # noqa: D102 # pylint: disable=missing-function-docstring
        self,
    ) -> Stringable: ...

    @property
    def target(  # noqa: D102 # pylint: disable=missing-function-docstring
        self,
    ) -> Stringable: ...


@runtime_checkable
class NodeEdgeProtocol(NodeProtocol, Protocol):
    """Protocol for a node in the lineage graph that can be converted to an edge.

    A node must have a `source` and `target` attribute, both of which are can be interpreted
    as a string. It must also have an `as_edge` method that returns an `Edge` object.
    """

    @property
    def as_edge(  # noqa: D102 # pylint: disable=missing-function-docstring
        self,
    ) -> Edge: ...


NodeType: TypeAlias = NodeProtocol | NodeEdgeProtocol
