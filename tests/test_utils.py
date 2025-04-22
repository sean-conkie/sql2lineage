import pytest

from sql2lineage.utils import IntermediateNodeStore


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
