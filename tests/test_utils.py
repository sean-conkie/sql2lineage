import pytest

from sql2lineage.utils import IntermediateNodeStore, validate_chains


class TestIntermediateNodeStore:
    """Test IntermediateNodeStore."""

    def test_setitem_and_getitem(self):
        """Test that using __setitem__ and __getitem__ works correctly."""
        store = IntermediateNodeStore()
        store["node1"] = "value1"
        store["node2"] = "value2"
        assert store["node1"] == "value1"
        assert store["node2"] == "value2"

    def test_getitem_keyerror(self):
        """Test that __getitem__ raises a KeyError for missing keys."""
        store = IntermediateNodeStore()
        store["node1"] = "value1"
        with pytest.raises(KeyError):
            _ = store["non_existing_node"]

    def test_contains(self):
        """Test that __contains__ correctly identifies existing and non-existing keys."""
        store = IntermediateNodeStore()
        store["node1"] = "value1"
        store.add(("node2", "value2"))
        assert "node1" in store
        assert "node2" in store
        assert "non_existing" not in store

    def test_add_method(self):
        """Test that the add() method correctly adds nodes."""
        store = IntermediateNodeStore()
        store.add(("node3", "value3"))
        assert store["node3"] == "value3"

    def test_duplicate_keys(self):
        """Test that duplicate keys return the first inserted value when using __getitem__."""
        store = IntermediateNodeStore()
        store["node_dup"] = "first_value"
        store.add(("node_dup", "second_value"))
        # __getitem__ should return the value from the first occurrence.
        assert store["node_dup"] == "first_value"


class TestValidateChains:
    """Test validate_chains function."""

    class DummyNode:
        """Dummy node class for testing purposes."""

        def __init__(self, source: str, target: str):
            self.source = source
            self.target = target

    @pytest.fixture(scope="class")
    def nodes(self):
        """Create some dummy nodes for testing."""
        return [
            self.DummyNode("A", "B"),
            self.DummyNode("B", "C"),
            self.DummyNode("C", "D"),
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
            pytest.param(IntermediateNodeStore(), id="intermediate_node_store"),
            pytest.param(
                [IntermediateNodeStore()], id="list_of_intermediate_node_store"
            ),
            pytest.param(
                [[IntermediateNodeStore()], [IntermediateNodeStore()]],
                id="list_of_list_of_intermediate_node_store",
            ),
        ],
    )
    def test_invalid_inputs(self, input):
        """Test that invalid input raises a TypeError."""
        with pytest.raises(ValueError):
            validate_chains(input)
