select first_name, last_name
from
(
    select first_name, last_name, count(distinct suffix_name) as count
    from data_officer
    group by first_name, last_name
) a
where count > 1