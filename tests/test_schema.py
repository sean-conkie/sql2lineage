import pytest
from pydantic import ValidationError

# Adjust import paths as needed; assuming the refactored code is in schema.py
from sql2lineage.types.model import (
    STRUCT_COLUMN_TYPES,
    Schema,
    SchemaColumn,
    SchemaTable,
)


class TestSchemaColumn:
    def test_type_simple_and_record(self):
        # When fields is None, type should be "SIMPLE"
        col_simple = SchemaColumn(name="simple_col")
        assert col_simple.fields is None
        assert col_simple.type == "SIMPLE"

        # When fields is an empty list or non-empty list, type should be "RECORD"
        col_record_empty = SchemaColumn(name="rec_col", fields=[])
        assert isinstance(col_record_empty.fields, list)
        assert col_record_empty.type == "RECORD"

        nested_fields = [SchemaColumn(name="nested", fields=None)]
        col_record = SchemaColumn(name="parent", fields=nested_fields)
        assert col_record.type == "RECORD"
        # Ensure nested child still reports SIMPLE
        child = col_record.fields[0]
        assert child.fields is None
        assert child.type == "SIMPLE"

    def test_validation_requires_name(self):
        # Missing name should raise ValidationError
        with pytest.raises(ValidationError):
            SchemaColumn()


class TestSchemaTable:
    @pytest.fixture
    def empty_table(self):
        return SchemaTable(name="test_table")

    @pytest.fixture
    def populated_table(self):
        tbl = SchemaTable(name="my_table")
        tbl.columns = [
            SchemaColumn(name="col1"),
            SchemaColumn(name="col2", fields=[SchemaColumn(name="inner")]),
        ]
        return tbl

    def test_contains_by_name_and_column_obj(self, populated_table):
        # __contains__ using column name
        assert "col1" in populated_table
        assert "col2" in populated_table
        assert "nope" not in populated_table

        # __contains__ using SchemaColumn instance
        col = SchemaColumn(name="col1")
        assert col in populated_table
        missing = SchemaColumn(name="absent")
        assert missing not in populated_table

    def test_getitem_success_and_keyerror(self, populated_table):
        col1 = populated_table["col1"]
        assert isinstance(col1, SchemaColumn)
        assert col1.name == "col1"

        with pytest.raises(KeyError) as excinfo:
            _ = populated_table["no_col"]
        assert "Column 'no_col' not found in table 'my_table'." in str(excinfo.value)

    def test_setitem_add_new_and_error_on_duplicate(self, empty_table):
        new_col = SchemaColumn(name="newcol")
        empty_table["newcol"] = new_col
        assert "newcol" in empty_table
        assert empty_table["newcol"] is new_col

        with pytest.raises(ValueError) as excinfo:
            empty_table["newcol"] = SchemaColumn(name="newcol")
        assert "Column 'newcol' already exists in table 'test_table'." in str(
            excinfo.value
        )


class TestSchema:
    @pytest.fixture
    def empty_schema(self):
        return Schema()

    @pytest.fixture
    def populated_schema(self):
        sch = Schema()
        # Create table t1 with a simple and a record column
        tbl1 = SchemaTable(name="t1")
        tbl1.columns = [
            SchemaColumn(name="a"),
            SchemaColumn(
                name="b", fields=[SchemaColumn(name="b1"), SchemaColumn(name="b2")]
            ),
        ]
        # Create table t2 with one column
        tbl2 = SchemaTable(name="t2")
        tbl2.columns = [SchemaColumn(name="x")]
        sch.tables = [tbl1, tbl2]
        return sch

    def test_contains_by_name_and_table_obj(self, populated_schema):
        assert "t1" in populated_schema
        assert "t2" in populated_schema
        assert "no_table" not in populated_schema

        tbl1_copy = SchemaTable(name="t1")
        assert tbl1_copy in populated_schema
        missing_tbl = SchemaTable(name="t3")
        assert missing_tbl not in populated_schema

    def test_getitem_success_and_keyerror(self, populated_schema):
        t1 = populated_schema["t1"]
        assert isinstance(t1, SchemaTable)
        assert t1.name == "t1"

        with pytest.raises(KeyError) as excinfo:
            _ = populated_schema["nope"]
        assert "Table 'nope' not found in schema." in str(excinfo.value)

    def test_setitem_add_new_and_error_on_duplicate(self, empty_schema):
        tbl = SchemaTable(name="new_table")
        empty_schema["new_table"] = tbl
        assert "new_table" in empty_schema
        assert empty_schema["new_table"] is tbl

        with pytest.raises(ValueError) as excinfo:
            empty_schema["new_table"] = SchemaTable(name="new_table")
        assert "Table 'new_table' already exists in the schema." in str(excinfo.value)

    def test_add_table_only_adds_nonexistent(self, empty_schema):
        # First add should create
        empty_schema.add_table("alpha")
        assert "alpha" in empty_schema
        before_count = len(empty_schema.tables)
        # Second add should do nothing (no error, no duplicate)
        empty_schema.add_table("alpha")
        assert len(empty_schema.tables) == before_count

    def test_add_column_creates_table_and_simple_column(self, empty_schema):
        # Add a simple column to a new table
        empty_schema.add_column("tbl1", "colA")
        assert "tbl1" in empty_schema
        tbl1 = empty_schema["tbl1"]
        assert "colA" in tbl1
        colA = tbl1["colA"]
        assert colA.fields is None
        assert colA.type == "SIMPLE"

    def test_add_column_nested_path_creates_records(self, empty_schema):
        # Add nested path a.b.c in new table
        empty_schema.add_column("tblX", "a.b.c")
        tbl = empty_schema["tblX"]
        # Top-level column 'a' should exist and be a RECORD
        assert "a" in tbl
        col_a = tbl["a"]
        assert col_a.type == "RECORD"
        assert isinstance(col_a.fields, list)
        # Nested 'b' under a
        col_b = next((c for c in col_a.fields if c.name == "b"), None)
        assert col_b is not None
        assert col_b.type == "RECORD"
        # Deep nested 'c' under b
        col_c = next((c for c in col_b.fields if c.name == "c"), None)
        assert col_c is not None
        assert col_c.type == "SIMPLE"

        # Adding the same path again should not duplicate columns
        before_a_fields = len(col_a.fields)
        empty_schema.add_column("tblX", "a.b.c")
        assert len(col_a.fields) == before_a_fields

    def test_get_column_success_and_keyerror(self, populated_schema):
        # Simple column retrieval
        col_a = populated_schema.get_column("t1", "a")
        assert isinstance(col_a, SchemaColumn)
        assert col_a.name == "a"

        # Nested column retrieval
        col_b1 = populated_schema.get_column("t1", "b.b1")
        assert col_b1.name == "b1"
        assert col_b1.type == "SIMPLE"

        # Nonexistent table
        with pytest.raises(KeyError) as excinfo_tbl:
            populated_schema.get_column("no_table", "any")
        assert "Table 'no_table' not found in schema." in str(excinfo_tbl.value)

        # Nonexistent column path
        with pytest.raises(KeyError) as excinfo_col:
            populated_schema.get_column("t1", "b.nope")
        assert "Column 'b.nope' not found in table 't1'." in str(excinfo_col.value)

    def test_get_with_and_without_default(self, populated_schema):
        t1 = populated_schema.get("t1")
        assert isinstance(t1, SchemaTable)
        assert t1.name == "t1"

        default = "DEF"
        result = populated_schema.get("not_here", default=default)
        assert result == default

        assert populated_schema.get("also_missing") is None

    def test_is_struct_true_false_and_keyerror(self, empty_schema, populated_schema):
        # Create a nested structure in empty_schema
        empty_schema.add_column("tblS", "parent.child")
        # 'parent' is RECORD because it has a child
        assert empty_schema.is_struct("tblS", "parent") is True
        # 'child' is SIMPLE
        assert empty_schema.is_struct("tblS", "child") is False

        # In populated_schema, 'b' in 't1' was defined with fields
        assert populated_schema.is_struct("t1", "b") is True
        # 'a' is SIMPLE
        assert populated_schema.is_struct("t1", "a") is False
        # Nonexistent column returns False
        assert populated_schema.is_struct("t1", "no_col") is False

        with pytest.raises(KeyError) as excinfo:
            populated_schema.is_struct("no_table", "x")
        assert "Table 'no_table' not found in schema." in str(excinfo.value)

    def test_schema_and_table_pydantic_validation(self):
        # SchemaTable requires name; missing name should raise
        with pytest.raises(ValidationError):
            SchemaTable()

        # Schema requires no arguments (tables defaults to empty)
        s = Schema()
        assert isinstance(s, Schema)
        assert s.tables == []

        # SchemaColumn validation tested above


# To run these tests, execute `pytest` in the directory containing this file.
