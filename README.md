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

## Bugs/Requests

Please use the [GitHub Issue Tracker](https://github.com/sean-conkie/sql2lineage/issues) to submit bugs or requests.

## License

Copyright Sean Conkie, 2025.

Distributed under the terms of the [MIT](https://github.com/sean-conkie/sql2lineage?tab=MIT-1-ov-file#readme) license, sql2lineage is free and open source software.
