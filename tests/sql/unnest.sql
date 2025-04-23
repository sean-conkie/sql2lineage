CREATE OR REPLACE TABLE unnest_example AS
SELECT
    orderid AS order_id,
    ad.address1,
    ad.address2,
    ad.city,
    ad.state,
    ad.zip,
    li.product
FROM raw_orders AS src, UNNEST(src.address) AS ad, unnest(lines_items) AS li