"""Lineage graph utilities."""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class NodeProtocol(Protocol):
    """Protocol for a node in the lineage graph."""

    source: str
    target: Optional[str]
