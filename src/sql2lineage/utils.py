"""Utility functions for SQL lineage extraction."""

from typing import Generic, Iterator, List, Sequence, Tuple, TypeVar, Union

from sql2lineage.types.utils import NodeProtocol

# Define two type variables for the key and value.
T = TypeVar("T")
V = TypeVar("V")


class SimpleTupleStore(Generic[T, V]):
    """A simple tuple store that allows for storing unique tuples.

    The store is generic in that it can store tuples of any (T, V).
    For example, SimpleTupleStore[str, int] would store tuples with a str as key and int as value.
    """

    def __init__(self) -> None:
        self._store: List[Tuple[T, V]] = []

    def __setitem__(self, key: T, value: V) -> None:
        """Add a new tuple if it does not exist already."""
        if (key, value) not in self._store:
            self._store.append((key, value))

    def __getitem__(self, key: T) -> V:
        """Retrieve the value corresponding to the given key.

        If multiple tuples have the same key, the value from the first encountered tuple is
        returned.
        """
        for k, v in self._store:
            if k == key:
                return v
        raise KeyError(f"Node {key} not found")

    def __contains__(self, item: T) -> bool:
        """Check if any tuple in the store has the given key."""
        return any(k == item for k, _ in self._store)

    def __len__(self) -> int:
        """Return the number of unique tuples in the store."""
        return len(self._store)

    def __iter__(self) -> Iterator[Tuple[T, V]]:
        """Iterate over the stored tuples."""
        return iter(self._store)

    def __repr__(self) -> str:
        return f"SimpleTupleStore({self._store})"

    def add(self, node: Tuple[T, V]) -> None:
        """Add a node (tuple) to the internal storage if it doesn't already exist.

        Args:
            node (Tuple[T, V]): A tuple containing a key of type T and a value of type V.

        """
        if node not in self._store:
            self._store.append(node)

    def get(self, target: T) -> List[V]:
        """Retrieve all values associated with a specific target key.

        Args:
            target (T): The target key to search for in the internal store.

        Returns:
            List[V]: A list of values corresponding to the given target key.

        """
        return [v for k, v in self._store if k == target]


def validate_chains(
    chains: Union[
        Sequence[Sequence[NodeProtocol]], Sequence[NodeProtocol], NodeProtocol
    ],
) -> List[List[NodeProtocol]]:
    """Validate and normalise the input `chains` into a list of lists of `NodeProtocol` instances.

    This function handles three cases:
    1. If `chains` is a single `NodeProtocol` instance, it wraps it in a nested list.
    2. If `chains` is a sequence of `NodeProtocol` instances, it wraps the sequence in a list.
    3. If `chains` is a sequence of sequences of `NodeProtocol` instances, it validates and normalizes each sequence.

    Args:
        chains (Union[Sequence[Sequence[NodeProtocol]], Sequence[NodeProtocol], NodeProtocol]):
            The input to validate and normalize. It can be:
            - A single `NodeProtocol` instance.
            - A sequence of `NodeProtocol` instances.
            - A sequence of sequences of `NodeProtocol` instances.

    Returns:
        List[List[NodeProtocol]]: A normalized list of lists of `NodeProtocol` instances.

    Raises:
        ValueError: If `chains` is not a `NodeProtocol` or a sequence thereof, or if any element
                    in the sequence does not conform to `NodeProtocol`.

    """
    # Case 1: chains is a single NodeProtocol instance.
    if isinstance(chains, NodeProtocol):
        return [[chains]]

    # At this point we expect chains to be a sequence.
    if not isinstance(chains, Sequence):
        raise ValueError("chains must be a NodeProtocol or a sequence thereof.")

    # If chains is an empty sequence, return an empty list.
    if not chains:
        return []

    # Case 2: chains is a sequence of NodeProtocol instances.
    # Check the first element.
    if isinstance(chains[0], NodeProtocol):
        # Further verify each element in the sequence.
        for item in chains:
            if not isinstance(item, NodeProtocol):
                raise ValueError(
                    "All items in the sequence must conform to NodeProtocol."
                )
        # Wrap in a list since the outer structure should be a list of lists.
        return [list(chains)]  # type: ignore

    # Case 3: chains is a sequence of sequences of NodeProtocol instances.
    normalized_chains = []
    for idx, chain in enumerate(chains):
        if not isinstance(chain, Sequence):
            raise ValueError(
                f"Element at index {idx} is not a sequence of NodeProtocol."
            )
        # Allow empty chains, or verify that all elements in the chain are NodeProtocol.
        chain_list = list(chain)
        for node in chain_list:
            if not isinstance(node, NodeProtocol):
                raise ValueError(
                    f"An item in chain {idx} does not conform to NodeProtocol."
                )
        normalized_chains.append(chain_list)

    return normalized_chains


def filter_intermediate_nodes(
    chains: Sequence[Sequence[NodeProtocol]] | Sequence[NodeProtocol] | NodeProtocol,
) -> List[List[NodeProtocol]]:
    """Filter out intermediate table nodes from a sequence of chains.

    Intermediate nodes are identified based on their type and are replaced with their
    root nodes in the resulting chains.

    Args:
        chains (Sequence[Sequence[NodeProtocol]] | Sequence[NodeProtocol] | NodeProtocol): A
            sequence of chains, where each chain is a sequence of nodes implementing the
            `NodeProtocol`.

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
    validated_chains = validate_chains(chains)

    # first identify the intermediate nodes
    intermediate_nodes, validated_chains = identify_intermediate_steps(validated_chains)
    assert isinstance(
        intermediate_nodes, SimpleTupleStore
    ), "intermediate_nodes should be an instance of IntermediateNodeStore"

    # now we need to update the remaining chains with the intermediate nodes
    new_chains = []
    for chain in validated_chains:
        new_chain = []
        for step in list(chain):
            if step.source is None:
                new_chain.append(step)
                continue

            if step.source in intermediate_nodes:
                sources = [
                    sl
                    for src in (
                        find_roots(s, intermediate_nodes)
                        for s in intermediate_nodes.get(step.source)
                    )
                    for sl in src
                ]
                for source in sources:
                    step.source = source
                    new_chain.append(step)
            else:
                new_chain.append(step)
        new_chains.append(new_chain)

    # remove empty chains
    new_chains = [chain for chain in new_chains if len(chain) > 0]

    return new_chains


def find_roots(node: str, intermediate_nodes: SimpleTupleStore) -> List[str]:
    """Find the root nodes in a directed graph starting from a given node.

    This function recursively traverses a graph represented by an intermediate node store
    to find all root nodes (nodes with no incoming edges) that are reachable from the given node.

    Args:
        node (str): The starting node for the traversal.
        intermediate_nodes (IntermediateNodeStore): A mapping of nodes to their connected child
            nodes.

    Returns:
        List[str]: A list of root nodes reachable from the given starting node.

    """
    if node in intermediate_nodes:
        return [
            s
            for sublist in [
                find_roots(s, intermediate_nodes) for s in intermediate_nodes.get(node)
            ]
            for s in sublist
        ]
    else:
        return [node]


def identify_intermediate_steps(
    chains: Sequence[Sequence[NodeProtocol]] | Sequence[NodeProtocol] | NodeProtocol,
) -> Tuple[SimpleTupleStore[str, str], List[List[NodeProtocol]]]:
    """Identify and extract intermediate steps from a sequence of chains of nodes.

    Args:
        chains (Sequence[Sequence[NodeProtocol]] | Sequence[NodeProtocol] | NodeProtocol):
            A sequence of chains, where each chain is a sequence of nodes, or a single node.

    Returns:
        Tuple[SimpleTupleStore, List[List[NodeProtocol]]]: A tuple containing:
            - An `SimpleTupleStore` instance containing the intermediate nodes.
            - A list of lists of `NodeProtocol` instances representing the validated chains.

    Notes:
        - The function validates the input chains before processing.
        - Nodes are checked for their type using the `check_type` function, which defaults to checking for "TABLE" type.
        - Intermediate nodes are removed from the chains and stored in the `SimpleTupleStore`.

    """
    validated_chains = validate_chains(chains)

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

    intermediate_nodes = SimpleTupleStore[str, str]()

    for chain in list(validated_chains):
        for step in list(chain):
            if check_type(step):
                if step.target is None:
                    continue
                intermediate_nodes[step.target] = step.source  # type: ignore
                chain.remove(step)

    return intermediate_nodes, validated_chains
