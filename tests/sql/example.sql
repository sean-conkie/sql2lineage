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