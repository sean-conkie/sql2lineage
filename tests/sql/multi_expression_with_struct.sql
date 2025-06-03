with customers_base as (
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

create or replace table customers
select id,
       name,
       address
  from customers_base
;


select *
  from customers
;
