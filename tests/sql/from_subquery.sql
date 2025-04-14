create table from_subquery
select 
    a.id as id,
    a.age as age
from (
        select id, age
        from raw.user_details
    ) a
;