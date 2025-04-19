with cte_with_subquery as (
    select
        id,
        name,
        total_count
    from (select id, name, count(*) total_count from test_table group by 1,2)
)
select
    id,
    name,
    total_count
from cte_with_subquery
where total_count > 0
order by id;