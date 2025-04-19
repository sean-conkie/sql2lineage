with cte_with_subquery as (
    select
        id,
        name,
        (select count(*) from test_table) as total_count
    from test_table
)
select
    id,
    name,
    total_count
from cte_with_subquery
where total_count > 0
order by id;