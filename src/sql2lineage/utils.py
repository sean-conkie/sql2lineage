"""Utility functions for SQL lineage extraction."""

from typing import (
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from pydantic import BaseModel

from sql2lineage.types.model import LineageNode
from sql2lineage.types.utils import NodeType

# Define two type variables for the key and value.
D = TypeVar("D")
T = TypeVar("T")
V = TypeVar("V")


class SimpleTupleStore(Generic[T, V]):
    """A simple tuple store that allows for storing unique tuples.

    The store is generic in that it can store tuples of any (T, V).
    For example, SimpleTupleStore[str, int] would store tuples with a str as key and int as value.
    """

    def __init__(self, value: Optional[List[Tuple[T, V]]] = None) -> None:
        if value is None:
            self._store: List[Tuple[T, V]] = []
        elif isinstance(value, list) and all(
            isinstance(item, tuple) and len(item) == 2 for item in value
        ):
            self._store = value
        else:
            raise ValueError(
                "value must be a list of tuples, each containing exactly two elements."
            )

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

    def get_all(self, target: T) -> List[V]:
        """Retrieve all values associated with a specific target key.

        Args:
            target (T): The target key to search for in the internal store.

        Returns:
            List[V]: A list of values corresponding to the given target key.

        """
        return [v for k, v in self._store if k == target]

    @overload
    def get(self, target: T, default: D) -> V | D: ...

    @overload
    def get(self, target: T) -> V | None: ...

    def get(self, target: T, default: Optional[D] = None) -> V | D | None:
        """Retrieve the value associated with the given target key from the internal store.

        Args:
            target (T): The key to search for in the store.
            default (Optional[D], optional): The default value to return if the key is not found. Defaults to None.

        Returns:
            V | D | None: The value associated with the target key if found, otherwise the default value or None.

        """
        for k, v in self._store:
            if k == target:
                return v

        return default


def validate_chains(
    chains: Union[Sequence[Sequence[NodeType]], Sequence[NodeType], NodeType],
) -> List[List[NodeType]]:
    """Validate and normalise the input `chains` into a list of lists of `NodeType` instances.

    This function handles three cases:
    1. If `chains` is a single `NodeType` instance, it wraps it in a nested list.
    2. If `chains` is a sequence of `NodeType` instances, it wraps the sequence in a list.
    3. If `chains` is a sequence of sequences of `NodeType` instances, it validates and normalizes each sequence.

    Args:
        chains (Union[Sequence[Sequence[NodeType]], Sequence[NodeType], NodeType]):
            The input to validate and normalize. It can be:
            - A single `NodeType` instance.
            - A sequence of `NodeType` instances.
            - A sequence of sequences of `NodeType` instances.

    Returns:
        List[List[NodeType]]: A normalized list of lists of `NodeType` instances.

    Raises:
        ValueError: If `chains` is not a `NodeType` or a sequence thereof, or if any element
                    in the sequence does not conform to `NodeType`.

    """
    # Case 1: chains is a single NodeType instance.
    if isinstance(chains, NodeType):
        return [[chains]]

    # At this point we expect chains to be a sequence.
    if not isinstance(chains, Sequence):
        raise ValueError("chains must be a NodeType or a sequence thereof.")

    # If chains is an empty sequence, return an empty list.
    if not chains:
        return []

    # Case 2: chains is a sequence of NodeType instances.
    # Check the first element.
    if isinstance(chains[0], NodeType):
        # Further verify each element in the sequence.
        for item in chains:
            if not isinstance(item, NodeType):
                raise ValueError("All items in the sequence must conform to NodeType.")
        # Wrap in a list since the outer structure should be a list of lists.
        return [list(chains)]  # type: ignore

    # Case 3: chains is a sequence of sequences of NodeType instances.
    normalized_chains = []
    for idx, chain in enumerate(chains):
        if not isinstance(chain, Sequence):
            raise ValueError(f"Element at index {idx} is not a sequence of NodeType.")
        # Allow empty chains, or verify that all elements in the chain are NodeType.
        chain_list = list(chain)
        for node in chain_list:
            if not isinstance(node, NodeType):
                raise ValueError(
                    f"An item in chain {idx} does not conform to NodeType."
                )
        normalized_chains.append(chain_list)

    return normalized_chains


def filter_intermediate_nodes(
    chains: Sequence[Sequence[NodeType]] | Sequence[NodeType] | NodeType,
) -> List[List[NodeType]]:
    """Filter out intermediate table nodes from a sequence of chains.

    Intermediate nodes are identified based on their type and are replaced with their
    root nodes in the resulting chains.

    Args:
        chains (Sequence[Sequence[NodeType]] | Sequence[NodeType] | NodeType): A
            sequence of chains, where each chain is a sequence of nodes implementing the
            `NodeType`.

    Returns:
        List[List[NodeType]]: A list of updated chains with intermediate nodes removed
        and replaced by their root nodes. Empty chains are excluded from the result.

    Notes:
        - A node is considered intermediate if its type is not "TABLE".
        - The function uses an internal `IntermediateNodeStore` to track intermediate nodes
          and their relationships.
        - The `find_roots` function is used to recursively find the root nodes of a given node.
        - The `check_type` function determines whether a node is of a specific type.

    """
    validated_chains = validate_chains(chains)
    source_nodes = identify_non_table_source_nodes(validated_chains)
    target_nodes = identify_non_table_target_nodes(validated_chains)

    new_chains = set()
    for chain in validated_chains:
        new_chain = set()
        for step in chain:
            # if source and target are TABLE, skip
            if all(
                (
                    (step.source_type or "TABLE") == "TABLE",
                    (step.target_type or "TABLE") == "TABLE",
                )
            ):
                continue

            node_attrs = {"source_type": "TABLE", "target_type": "TABLE"}
            if isinstance(step, BaseModel):
                node_attrs.update(
                    step.model_dump(
                        exclude_none=True,
                        exclude_unset=True,
                        exclude={"source_type", "target_type", "source", "target"},
                    )
                )

            # check the target
            if step.target_type == "TABLE":
                # target is a table so let's copy it to the new node
                node_attrs["target"] = step.target
            else:
                # target is not a table so let's find the root nodes for that chain
                node_attrs["target"] = target_nodes[step.target]

            # check the source
            if step.source_type == "TABLE":
                # source is a table so let's copy it to the new node
                node_attrs["source"] = step.source
            else:
                # source is not a table so let's find the root nodes for that chain
                node_attrs["source"] = source_nodes[step.source]

            new_chain.add(LineageNode.model_validate(node_attrs))
        new_chains.add(frozenset(new_chain))

    return [list(chain) for chain in new_chains if len(chain) > 0]


def find_roots(node: str, intermediate_nodes: SimpleTupleStore[str, str]) -> List[str]:
    """Find the root nodes in a directed graph starting from a given node.

    This function recursively traverses a graph represented by an intermediate node store
    to find all root nodes (nodes with no incoming edges) that are reachable from the given node.

    Args:
        node (str): The starting node for the traversal.
        intermediate_nodes (SimpleTupleStore[str, str]): A mapping of nodes to their connected child
            nodes.

    Returns:
        List[str]: A list of root nodes reachable from the given starting node.

    """
    if node in intermediate_nodes:
        return [
            s
            for sublist in [
                find_roots(s, intermediate_nodes)
                for s in intermediate_nodes.get_all(node)
            ]
            for s in sublist
        ]
    else:
        return [node]


def identify_non_table_source_nodes(
    chains: Sequence[Sequence[NodeType]] | Sequence[NodeType] | NodeType,
) -> SimpleTupleStore[str, str]:
    validated_chains = validate_chains(chains)

    node_store = SimpleTupleStore[str, str](
        [
            (str(step.target), str(step.source))
            for chain in validated_chains
            for step in chain
        ]
    )

    sources = {
        node
        for chain in validated_chains
        for node in chain
        if node.source_type != "TABLE"
    }

    # for each target find the roots - recursively
    source_store = SimpleTupleStore[str, str]()

    for source in sources:
        # get all the upstream nodes
        nodes = [
            sl
            for src in (
                find_roots(s, node_store)
                for s in node_store.get_all(str(source.source))
            )
            for sl in src
        ]
        # add the nodes to the target store
        for node in nodes:
            source_store.add((str(source.source), node))

    return source_store


def identify_non_table_target_nodes(
    chains: Sequence[Sequence[NodeType]] | Sequence[NodeType] | NodeType,
) -> SimpleTupleStore[str, str]:
    validated_chains = validate_chains(chains)

    node_store = SimpleTupleStore[str, str](
        [
            (str(step.source), str(step.target))
            for chain in validated_chains
            for step in chain
        ]
    )

    targets = {
        node
        for chain in validated_chains
        for node in chain
        if node.target_type != "TABLE"
    }

    # for each target find the leaves - recursively
    target_store = SimpleTupleStore[str, str]()

    for target in targets:
        # get all the downstream nodes
        nodes = [
            sl
            for src in (
                find_roots(s, node_store)
                for s in node_store.get_all(str(target.target))
            )
            for sl in src
        ]
        # add the nodes to the target store
        for node in nodes:
            target_store.add((str(target.target), node))

    return target_store
