select avg(e.award_count) as avg_award_count
from (
    select c.id, count(distinct d.id) as award_count
    from (
        select a.id, count(distinct b.allegation_id) as allegation_count
        from data_officer a
        left join data_officerallegation b
        on a.id = b.officer_id
        group by a.id
        order by 2 desc) c
    left join data_award d
    on c.id = d.officer_id
    where c.allegation_count < 10
    group by c.id
    order by 2 desc
) e

-- id and allegation count table
-- select a.id, count(distinct b.allegation_id)
-- from data_officer a
-- left join data_officerallegation b
-- on a.id = b.officer_id
-- group by a.id
-- order by 2 asc