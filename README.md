# sql2lineage

![PyPI - Version](https://img.shields.io/pypi/v/sql2lineage)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sql2lineage)
![PyPI - Types](https://img.shields.io/pypi/types/sql2lineage)
[![Lint Code Base](https://github.com/sean-conkie/sql2lineage/actions/workflows/lint.yml/badge.svg)](https://github.com/sean-conkie/sql2lineage/actions/workflows/lint.yml)
[![Test Code Base](https://github.com/sean-conkie/sql2lineage/actions/workflows/test.yml/badge.svg)](https://github.com/sean-conkie/sql2lineage/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/sean-conkie/sql2lineage/graph/badge.svg?token=ZOM3PNJ2SL)](https://codecov.io/gh/sean-conkie/sql2lineage)

The `sql2lineage` package makes it easy to understand the data lineage of your SQL ETL files.

## Features

- Parse SQL strings to create data lineage
- Build a graph to represent the data lineage
- Search and print neighbourhoods

## Example

### Creating Lineage

With an example SQL file:

```sql
WITH orders_with_tax AS (
    SELECT
        order_id,
        customer_id,
        order_total * 1.2 AS total_with_tax
    FROM raw.orders
),
filtered_orders AS (
    SELECT
        order_id,
        customer_id,
        total_with_tax
    FROM orders_with_tax
)

CREATE TABLE big_orders AS
SELECT * FROM filtered_orders;
```

We can parse the content to create lineage.

```python
from sql2lineage.graph import LineageGraph
from sql2lineage.parser import SQLLineageParser

with open("example.sql") as f:
    sql = f.read()

parser = SQLLineageParser()
r = parser.extract_lineage(sql)


graph = LineageGraph()
graph.from_parsed(r.expressions)
graph.pretty_print()
```

Output

```text
filtered_orders --> big_orders [type: TABLE]
raw.orders --> orders_with_tax [type: TABLE]
orders_with_tax --> filtered_orders [type: TABLE]
filtered_orders.customer_id --> big_orders.customer_id [type: COLUMN, action: COPY]
filtered_orders.total_with_tax --> big_orders.total_with_tax [type: COLUMN, action: COPY]
filtered_orders.order_id --> big_orders.order_id [type: COLUMN, action: COPY]
orders_with_tax.customer_id --> filtered_orders.customer_id [type: COLUMN, action: COPY]
orders_with_tax.total_with_tax --> filtered_orders.total_with_tax [type: COLUMN, action: COPY]
orders_with_tax.order_id --> filtered_orders.order_id [type: COLUMN, action: COPY]
raw.orders.order_id --> orders_with_tax.order_id [type: COLUMN, action: COPY]
raw.orders.order_total --> orders_with_tax.order_total [type: COLUMN, action: TRANSFORM]
raw.orders.customer_id --> orders_with_tax.customer_id [type: COLUMN, action: COPY]
```

### Searching Neighbours

Using the previously created graph, we can find all the neighbours of node `orders_with_tax.order_id`:

```python
paths = graph.get_node_neighbours("orders_with_tax.order_id")
graph.print_neighbourhood(paths)
```

Output

```text
Neighbourhood:
  ↳ {'source': 'raw.orders.order_id', 'target': 'orders_with_tax.order_id', 'type': 'COLUMN', 'action': 'COPY'}
  ↳ {'source': 'orders_with_tax.order_id', 'target': 'filtered_orders.order_id', 'type': 'COLUMN', 'action': 'COPY'}
  ↳ {'source': 'filtered_orders.order_id', 'target': 'big_orders.order_id', 'type': 'COLUMN', 'action': 'COPY'}
```

### Pre Transform

To allow for SQL strings to have pre-processing applied you can pass a callable to the `extract_lineage` and `extract_lineages` methods
of the `SQLLineageParser`.

This is intended to simplify applying transformations to the SQL strings. The callable should accept a `str` and return a transformed string.

An example use case is where a SQL statement is preceeded by a BigQuery write disposition, this must be replaced with the correct DDL.

A pre-transform function can be written:

```python
import re


def preprocess(sql):
    pattern = re.compile(r"(?P<object>(?:\w+)?\.?\w+\.\w+)\:(?P<action>\w+)\:")
    matches = pattern.findall(sql)
    for m in matches:
        object_name, action = m
        match action:
            case "DELETE":
                # used for truncate table & delete, DML not needed
                sql = sql.replace(f"{object_name}:{action}:", "")
            case "WRITE_APPEND":
                # used for insert, DML needed
                sql = sql.replace(
                    f"{object_name}:{action}:", f"insert into {object_name}"
                )
            case "WRITE_TRUNCATE":
                # used for insert overwrite, DML needed
                sql = sql.replace(
                    f"{object_name}:{action}:", f"create or replace table {object_name}"
                )
            case _:
                # fallback
                sql = sql.replace(f"{object_name}:{action}:", "")

    return sql
```

When the `extract_lineage` method is called with our `preprocess` callable (`parser.extract_lineage(sql, pre_transform=preprocess)`)
and the following SQL, the `pre_transform` callable will be called before parsing the SQL string.

Input

```sql
WITH orders_with_tax AS (
    SELECT
        order_id,
        customer_id,
        order_total * 1.2 AS total_with_tax
    FROM raw.orders
),
filtered_orders AS (
    SELECT
        order_id,
        customer_id,
        total_with_tax
    FROM orders_with_tax
)

orders_dataset.big_orders:WRITE_TRUNCATE:
SELECT * FROM filtered_orders;

```

Output

```sql
WITH orders_with_tax AS (
    SELECT
        order_id,
        customer_id,
        order_total * 1.2 AS total_with_tax
    FROM raw.orders
),
filtered_orders AS (
    SELECT
        order_id,
        customer_id,
        total_with_tax
    FROM orders_with_tax
)

create or replace table orders_dataset.big_orders
SELECT * FROM filtered_orders;

```

## Structs

When a struct is created in a statement, for example using `struct()`, the lineage will be maintained. However, if the struct is selected from an existing table, to maintain full lineage a `struct_override` must be provided.

Given the following SQL:

```sql

with customers as (
select c.id,
       c.name,
       struct(
        a.address_line_1,
        a.address_line_2,
        a.city,
        a.zip
       ) address
  from customer_raw c
  join customer_address a
    on c.id = a.customer_id
)

select *
  from customers
;

```

### Without Override

Without the override the lineage for the final statement would only include `customers.address`:

```text
customers.address --> expr0001.address [type: COLUMN]
```

### With Override

With the override the lineage for the final statement will include the columns which form the struct `customers.address`:

```text
customers.address_line_1 --> expr0001.address_line_1 [type: COLUMN]
customers.address_line_2 --> expr0001.address_line_2 [type: COLUMN]
customers.city --> expr0001.city [type: COLUMN]
customers.zip --> expr0001.zip [type: COLUMN]
```

By providing the override we also ensure the unbroken lineage back to the source table `customer_address`.

### Providing an Override

The override should be a dictionary where the key value is the fully qualified name of the source struct, the value is
a list of the field names contained within the struct:

```python
{
    "customers.address": [
        "address_line_1",
        "address_line_2",
        "city",
        "zip",
    ]
}
```

## Bugs/Requests

Please use the [GitHub Issue Tracker](https://github.com/sean-conkie/sql2lineage/issues) to submit bugs or requests.

## License

Copyright Sean Conkie, 2025.

Distributed under the terms of the [MIT](https://github.com/sean-conkie/sql2lineage?tab=MIT-1-ov-file#readme) license, sql2lineage is free and open source software.
