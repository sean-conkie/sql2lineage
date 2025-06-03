import pytest
from pydantic import ValidationError

# Adjust the import path as needed; assuming the code is in schema.py
from sql2lineage.types.model import (
    STRUCT_COLUMN_TYPES,
    Schema,
    SchemaColumn,
    SchemaTable,
)


class TestSchemaTable:
    @pytest.fixture
    def empty_table(self):
        return SchemaTable(name="test_table", type="TABLE")

    @pytest.fixture
    def populated_table(self):
        tbl = SchemaTable(name="my_table", type="TABLE")
        tbl.columns = [
            SchemaColumn(name="col1", type="STRING"),  # type: ignore
            SchemaColumn(name="col2", type="INTEGER"),  # type: ignore
        ]
        return tbl

    def test_contains_by_name_and_column_obj(self, populated_table):
        # Test __contains__ using column name
        assert "col1" in populated_table
        assert "col2" in populated_table
        assert "nonexistent" not in populated_table

        # Test __contains__ using SchemaColumn instance
        col = SchemaColumn(name="col1", type="STRING")  # type: ignore
        assert col in populated_table
        col_missing = SchemaColumn(name="colX", type="STRING")  # type: ignore
        assert col_missing not in populated_table

    def test_getitem_success_and_keyerror(self, populated_table):
        # __getitem__ returns the correct SchemaColumn
        col1 = populated_table["col1"]
        assert isinstance(col1, SchemaColumn)
        assert col1.name == "col1"
        assert col1.type == "STRING"

        # Accessing nonexistent column raises KeyError
        with pytest.raises(KeyError) as excinfo:
            _ = populated_table["nope"]
        assert "Column 'nope' not found in table 'my_table'." in str(excinfo.value)

    def test_setitem_add_new_and_error_on_duplicate(self, empty_table):
        new_col = SchemaColumn(name="newcol", type="STRING")  # type: ignore
        # Add via __setitem__
        empty_table["newcol"] = new_col
        assert "newcol" in empty_table
        assert empty_table["newcol"] is new_col

        # Setting the same key again raises ValueError
        with pytest.raises(ValueError) as excinfo:
            empty_table["newcol"] = SchemaColumn(name="newcol", type="STRING")  # type: ignore
        assert "Column 'newcol' already exists in table 'test_table'." in str(
            excinfo.value
        )

    def test_add_with_string_and_column_obj(self, empty_table):
        # Add by providing string (defaults to type STRING)
        empty_table.add("a_string_col")
        assert "a_string_col" in empty_table
        added = empty_table["a_string_col"]
        assert added.name == "a_string_col"
        assert added.type == "STRING"

        # Add by providing string and explicit type
        empty_table.add("int_col", type="INTEGER")
        assert "int_col" in empty_table
        int_col = empty_table["int_col"]
        assert int_col.type == "INTEGER"

        # Add by providing a SchemaColumn instance
        custom_col = SchemaColumn(name="custom", type="FLOAT")
        empty_table.add(custom_col)
        assert "custom" in empty_table
        assert empty_table["custom"].type == "FLOAT"

        # Adding a duplicate by name (whether string or object) raises ValueError
        with pytest.raises(ValueError) as excinfo:
            empty_table.add("custom")
        assert "Column 'custom' already exists in table 'test_table'." in str(
            excinfo.value
        )

        with pytest.raises(ValueError) as excinfo2:
            empty_table.add(SchemaColumn(name="int_col", type="INTEGER"))
        assert "Column 'int_col' already exists in table 'test_table'." in str(
            excinfo2.value
        )

    def test_add_if_only_adds_nonexistent(self, populated_table):
        # 'col1' already present, so add_if does nothing
        before_count = len(populated_table.columns)
        populated_table.add_if("col1", type="STRING")
        assert len(populated_table.columns) == before_count

        # Add a new column via add_if
        populated_table.add_if("newcol", type="BOOLEAN")
        assert "newcol" in populated_table
        assert populated_table["newcol"].type == "BOOLEAN"

    def test_get_column_success_and_keyerror(self, populated_table):
        col2 = populated_table.get_column("col2")
        assert isinstance(col2, SchemaColumn)
        assert col2.name == "col2"
        assert col2.type == "INTEGER"

        with pytest.raises(KeyError) as excinfo:
            populated_table.get_column("does_not_exist")
        assert "Column 'does_not_exist' not found in table 'my_table'." in str(
            excinfo.value
        )


class TestSchema:
    @pytest.fixture
    def empty_schema(self):
        return Schema()

    @pytest.fixture
    def populated_schema(self):
        sch = Schema()
        tbl1 = SchemaTable(name="t1", type="TABLE")
        tbl1.columns = [
            SchemaColumn(name="a", type="STRING"),
            SchemaColumn(
                name="b",
                type="STRUCT",
                fields=[
                    SchemaColumn(name="b1", type="INTEGER"),
                    SchemaColumn(name="b2", type="STRING"),
                ],
            ),
        ]
        tbl2 = SchemaTable(name="t2", type="TABLE")
        tbl2.columns = [SchemaColumn(name="x", type="BOOLEAN")]
        sch.tables = [tbl1, tbl2]
        return sch

    def test_contains_by_name_and_table_obj(self, populated_schema):
        assert "t1" in populated_schema
        assert "t2" in populated_schema
        assert "no_table" not in populated_schema

        # Using SchemaTable instance
        tbl1_copy = SchemaTable(name="t1", type="TABLE")
        assert tbl1_copy in populated_schema
        tbl_missing = SchemaTable(name="t3", type="TABLE")
        assert tbl_missing not in populated_schema

    def test_getitem_success_and_keyerror(self, populated_schema):
        t1 = populated_schema["t1"]
        assert isinstance(t1, SchemaTable)
        assert t1.name == "t1"

        with pytest.raises(KeyError) as excinfo:
            _ = populated_schema["nope"]
        assert "Table 'nope' not found in schema." in str(excinfo.value)

    def test_setitem_add_new_and_error_on_duplicate(self, empty_schema):
        tbl = SchemaTable(name="new_table", type="TABLE")
        empty_schema["new_table"] = tbl
        assert "new_table" in empty_schema
        assert empty_schema["new_table"] is tbl

        # Setting duplicate key
        with pytest.raises(ValueError) as excinfo:
            empty_schema["new_table"] = SchemaTable(name="new_table", type="TABLE")
        assert "Table 'new_table' already exists in the schema." in str(excinfo.value)

    def test_add_with_string_and_table_obj(self, empty_schema):
        # Add by string
        empty_schema.add("s_tbl")
        assert "s_tbl" in empty_schema
        added = empty_schema["s_tbl"]
        assert isinstance(added, SchemaTable)
        assert added.name == "s_tbl"

        # Add by SchemaTable instance
        new_tbl = SchemaTable(name="another", type="TABLE")
        empty_schema.add(new_tbl)
        assert "another" in empty_schema

        # Adding duplicate raises ValueError
        with pytest.raises(ValueError) as excinfo:
            empty_schema.add("s_tbl")
        assert "Table 's_tbl' already exists in the schema." in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo2:
            empty_schema.add(SchemaTable(name="another", type="TABLE"))
        assert "Table 'another' already exists in the schema." in str(excinfo2.value)

    def test_add_if_only_adds_nonexistent(self, empty_schema):
        empty_schema.add_if("if_tbl")
        assert "if_tbl" in empty_schema
        before_count = len(empty_schema.tables)
        empty_schema.add_if("if_tbl")
        assert len(empty_schema.tables) == before_count

    def test_get_with_and_without_default(self, populated_schema):
        # Existing table returns the table
        t1 = populated_schema.get("t1")
        assert isinstance(t1, SchemaTable)
        assert t1.name == "t1"

        # Nonexistent with default returns default
        default_val = "DEFAULT"
        result = populated_schema.get("nope", default=default_val)
        assert result == default_val

        # Nonexistent without default returns None
        assert populated_schema.get("nope_other") is None

    def test_add_table_column_creates_and_adds_column(self, empty_schema):
        # Add a new table and column via add_table_column
        empty_schema.add_table_column("tbl3", "colA")
        assert "tbl3" in empty_schema
        tbl3 = empty_schema["tbl3"]
        assert "colA" in tbl3
        assert tbl3["colA"].type == "STRING"

        # Add a second column to the same table
        empty_schema.add_table_column("tbl3", SchemaColumn(name="colB", type="INTEGER"))
        assert "colB" in tbl3
        assert tbl3["colB"].type == "INTEGER"

        # Adding existing column via add_table_column does not raise, and does not duplicate
        before_count = len(tbl3.columns)
        empty_schema.add_table_column("tbl3", "colB")
        assert len(tbl3.columns) == before_count

    def test_add_table_column_ignores_empty_and_asterisk(self, empty_schema):
        # Using empty string should do nothing
        empty_schema.add_table_column("tblX", "")
        assert "tblX" not in empty_schema

        # Using "*" should do nothing
        empty_schema.add_table_column("tblY", "*")
        assert "tblY" not in empty_schema

        # If column object has name "*", ignore as well
        starred = SchemaColumn(name="*", type="STRING")
        empty_schema.add_table_column("tblZ", starred)
        assert "tblZ" not in empty_schema

    def test_is_struct_true_false_and_keyerror(self, populated_schema):
        # 'b' column in 't1' is a STRUCT
        assert populated_schema.is_struct("t1", "b") is True

        # 'a' column in 't1' is STRING, so False
        assert populated_schema.is_struct("t1", "a") is False

        # Nonexistent column returns False
        assert populated_schema.is_struct("t1", "no_col") is False

        # Nonexistent table raises KeyError
        with pytest.raises(KeyError) as excinfo:
            populated_schema.is_struct("no_table", "whatever")
        assert "Table 'no_table' not found in schema." in str(excinfo.value)

    def test_struct_column_fields_are_retained(self, populated_schema):
        # Ensure that nested fields exist on the struct column
        tbl = populated_schema["t1"]
        struct_col = tbl["b"]
        assert struct_col.type == "STRUCT"
        assert isinstance(struct_col.fields, list)
        names = [f.name for f in struct_col.fields]
        assert set(names) == {"b1", "b2"}

    def test_schema_and_table_pydantic_validation(self):
        # SchemaColumn requires name and type
        with pytest.raises(ValidationError):
            SchemaColumn()

        # SchemaTable requires name and type
        with pytest.raises(ValidationError):
            SchemaTable()

        # Schema requires no special required fields (tables defaults to empty list)
        s = Schema()
        assert isinstance(s, Schema)
        assert s.tables == []


# Run `pytest` in the directory containing this file to execute the tests.
