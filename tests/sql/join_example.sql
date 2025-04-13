create table join_example
select 
    a.id as id,
    a.name,
    b.age as age
from
    raw.users a
join
    (
        select id, age
        from raw.user_details
    ) b
on
    a.id = b.id
where
    a.name is not null
    and b.age > 18;