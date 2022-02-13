-- allegation and officer table
-- select b.id, a.officer_id, count(*) as count
-- from data_officerallegation a, data_allegation b
-- where a.allegation_id = b.id
-- group by b.id, a.officer_id
-- order by b.id;

select e.officer_id, f.officer_id, sum(e.count) as count
from (
    select b.id, a.officer_id, count(distinct a.officer_id) as count
    from data_officerallegation a, data_allegation b
    where a.allegation_id = b.id
    group by b.id, a.officer_id
    order by b.id
) e
full outer join (
    select b.id, a.officer_id, count(distinct a.officer_id) as count
    from data_officerallegation a, data_allegation b
    where a.allegation_id = b.id
    group by b.id, a.officer_id
    order by b.id
) f
on e.id = f.id
where e.officer_id <> f.officer_id
group by e.officer_id, f.officer_id
order by 3 desc
limit 1