"""Utility functions for SQL lineage extraction."""

from typing import List, Protocol, Sequence, TypeVar

from pydantic import BaseModel


class NodeProtocol(Protocol):
    """Protocol for a node in the lineage graph."""

    source: str
    target: str


T = TypeVar("T", bound=BaseModel)


class IntermediateNodeStore:
    """Class to store intermediate nodes."""

    def __init__(self):
        self._store = []

    def __setitem__(self, key: str, value: str):
        self._store.append((key, value))

    def __getitem__(self, key: str) -> str:
        for node in self._store:
            if node[0] == key:
                return node[1]
        raise KeyError(f"Node {key} not found")

    def __contains__(self, item: str) -> bool:
        return any(key == item for key, _ in self._store)

    def add(self, node: tuple[str, str]):
        """Add a node to the internal storage.

        Args:
            node (tuple[str, str]): A tuple containing two strings representing the node to be added.

        """
        self._store.append(node)

    def get(self, target: str) -> list[str]:
        """Retrieve a list of values associated with a specific target from the internal store.

        Args:
            target (str): The target key to search for in the internal store.

        Returns:
            list[str]: A list of values corresponding to the given target key.

        """
        return [node[1] for node in self._store if node[0] == target]


def filter_intermediate_nodes(
    chains: Sequence[Sequence[NodeProtocol]],
) -> List[List[NodeProtocol]]:
    """Filter out intermediate nodes from a sequence of chain.

    Intermediate nodes are identified based on their type and are replaced with their
    root nodes in the resulting chains.

    Args:
        chains (Sequence[Sequence[NodeProtocol]]): A sequence of chains, where each chain
            is a sequence of nodes implementing the `NodeProtocol`.

    Returns:
        List[List[NodeProtocol]]: A list of updated chains with intermediate nodes removed
        and replaced by their root nodes. Empty chains are excluded from the result.

    Notes:
        - A node is considered intermediate if its type is not "TABLE".
        - The function uses an internal `IntermediateNodeStore` to track intermediate nodes
          and their relationships.
        - The `find_roots` function is used to recursively find the root nodes of a given node.
        - The `check_type` function determines whether a node is of a specific type.

    """
    intermediate_nodes = IntermediateNodeStore()

    def find_roots(node):
        """Find the root nodes of a given node in the graph."""
        if node in intermediate_nodes:
            return [
                s
                for sublist in [find_roots(s) for s in intermediate_nodes.get(node)]
                for s in sublist
            ]
        else:
            return [node]

    def check_type(node: NodeProtocol, check: str = "TABLE"):
        """Check the type of a node."""
        _type = None
        if hasattr(node, "table_type"):
            _type = node.table_type  # type: ignore
        elif hasattr(node, "type"):
            _type = node.type  # type: ignore
        if _type is None:
            return False
        return _type != check

    chains = [list(chain) for chain in chains]

    # first identify the intermediate nodes
    for chain in list(chains):
        for step in list(chain):
            if check_type(step):
                intermediate_nodes[step.target] = step.source
                chain.remove(step)

    # now we need to update the remaining chains with the intermediate nodes
    new_chains = []
    for chain in list(chains):
        new_chain = []
        for step in list(chain):

            if step.source in intermediate_nodes:
                sources = [
                    sl
                    for src in (
                        find_roots(s) for s in intermediate_nodes.get(step.source)
                    )
                    for sl in src
                ]
                for source in sources:
                    step.source = source
                    new_chain.append(step)
        new_chains.append(new_chain)

    # remove empty chains
    new_chains = [chain for chain in new_chains if len(chain) > 0]

    return new_chains
