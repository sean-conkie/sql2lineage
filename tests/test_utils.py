from typing import Optional

import pytest

from sql2lineage.utils import (
    SimpleTupleStore,
    filter_intermediate_nodes,
    validate_chains,
)


class TestSimpleTupleStore:
    """Test SimpleTupleStore[str, str]."""

    def test_setitem_and_getitem(self):
        """Test that using __setitem__ and __getitem__ works correctly."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        store["node2"] = "value2"
        assert store["node1"] == "value1"
        assert store["node2"] == "value2"

    def test_getitem_keyerror(self):
        """Test that __getitem__ raises a KeyError for missing keys."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        with pytest.raises(KeyError):
            _ = store["non_existing_node"]

    def test_contains(self):
        """Test that __contains__ correctly identifies existing and non-existing keys."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        store.add(("node2", "value2"))
        assert "node1" in store
        assert "node2" in store
        assert "non_existing" not in store

    def test_add_method(self):
        """Test that the add() method correctly adds nodes."""
        store = SimpleTupleStore[str, str]()
        store.add(("node3", "value3"))
        assert store["node3"] == "value3"

    def test_duplicate_keys(self):
        """Test that duplicate keys return the first inserted value when using __getitem__."""
        store = SimpleTupleStore[str, str]()
        store["node_dup"] = "first_value"
        store.add(("node_dup", "second_value"))
        # __getitem__ should return the value from the first occurrence.
        assert store["node_dup"] == "first_value"

    def test_len(self):
        """Test that __len__ returns the correct number of items."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        store.add(("node2", "value2"))
        assert len(store) == 2

    def test_iter(self):
        """Test that __iter__ returns an iterator over the store."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        store.add(("node2", "value2"))
        keys = [key for key, _ in store]
        assert keys == ["node1", "node2"]

    def test_repr(self):
        """Test that __repr__ returns a string representation of the store."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        store.add(("node2", "value2"))
        assert (
            repr(store)
            == "SimpleTupleStore([('node1', 'value1'), ('node2', 'value2')])"
        )

    def test_get(self):
        """Test that get() returns the correct value for existing and non-existing keys."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        assert store.get("node1") == "value1"
        assert store.get("non_existing_node") is None

    def test_get_with_default(self):
        """Test that get() returns the default value for non-existing keys."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        assert store.get("node1", "default") == "value1"
        assert store.get("non_existing_node", "default") == "default"

    def test_get_all(self):
        """Test that get_all() returns all items in the store."""
        store = SimpleTupleStore[str, str]()
        store["node1"] = "value1"
        store.add(("node1", "value2"))
        assert store.get_all("node1") == ["value1", "value2"]


class DummyNode:
    """Dummy node class for testing purposes."""

    def __init__(
        self,
        source: str,
        target: str,
        source_type: Optional[str] = "TABLE",
        target_type: Optional[str] = "TABLE",
    ):
        self.source = source
        self.target = target
        self.source_type = source_type
        self.target_type = target_type

    def __str__(self):
        """Get the string representation of the node."""
        return f"{self.source} -> {self.target}"


class TestValidateChains:
    """Test validate_chains function."""

    @pytest.fixture(scope="class")
    def nodes(self):
        """Create some dummy nodes for testing."""
        return [
            DummyNode("A", "B"),
            DummyNode("B", "C"),
            DummyNode("C", "D"),
        ]

    def test_single_node(self, nodes):
        """Test that a single node is returned as a list of lists."""
        node1, _, _ = nodes
        result = validate_chains(node1)
        assert result == [[node1]]

    def test_list_of_nodes(self, nodes):
        """Test that a list of nodes is returned as a list of lists."""
        node1, node2, _ = nodes
        result = validate_chains([node1, node2])
        assert result == [[node1, node2]]

    def test_list_of_list_of_nodes(self, nodes):
        """Test that a list of lists of nodes is returned as a list of lists."""
        node1, node2, node3 = nodes
        result = validate_chains([[node1, node2], [node2, node3]])
        assert result == [[node1, node2], [node2, node3]]

    def test_empty_input(self):
        """Test that an empty input returns an empty list."""
        result = validate_chains([])
        assert result == []

    @pytest.mark.parametrize(
        "input",
        [
            pytest.param("invalid_string", id="string"),
            pytest.param(123, id="integer"),
            pytest.param(123.45, id="float"),
            pytest.param({"key": "value"}, id="dict"),
            pytest.param(SimpleTupleStore[str, str](), id="intermediate_node_store"),
            pytest.param(
                [SimpleTupleStore[str, str]()], id="list_of_intermediate_node_store"
            ),
            pytest.param(
                [[SimpleTupleStore[str, str]()], [SimpleTupleStore[str, str]()]],
                id="list_of_list_of_intermediate_node_store",
            ),
        ],
    )
    def test_invalid_inputs(self, input):
        """Test that invalid input raises a TypeError."""
        with pytest.raises(ValueError):
            validate_chains(input)


class TestFilterIntermediateNodes:
    """Test filter_intermediate_nodes function."""

    @pytest.mark.parametrize(
        "input, expected, error",
        [
            pytest.param(
                [
                    DummyNode("A", "B", "TABLE", "CTE"),
                    DummyNode("B", "C", "CTE", "CTE"),
                    DummyNode("C", "D", "CTE", "CTE"),
                    DummyNode("D", "E", "CTE", "TABLE"),
                ],
                ["A -> E"],
                "only A and E should remain after filtering",
                id="intermediate_nodes",
            ),
            pytest.param(
                [
                    DummyNode("A", "B"),
                    DummyNode("B", "C"),
                    DummyNode("C", "D"),
                    DummyNode("D", "E"),
                ],
                [
                    "A -> B",
                    "B -> C",
                    "C -> D",
                    "D -> E",
                ],
                "all nodes should remain after filtering",
                id="no_intermediate_nodes",
            ),
        ],
    )
    def test_filter_intermediate_nodes(self, input, expected, error):
        """Test that filter_intermediate_nodes correctly filters out intermediate nodes."""
        result = filter_intermediate_nodes(input)
        assert len(result[0]) == len(expected), error
        assert all(
            str(n) in expected for node in result for n in node
        ), f"Expected nodes: {expected}, but got: {[str(node) for node in result]}"
