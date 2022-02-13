select avg(c.award_count) as avg_award_count
from (
        select count(distinct a.allegation_id) as allegation_count, count(distinct b.id) as award_count
        from data_officerallegation a, data_award b
        where a.officer_id = b.officer_id
        group by a.officer_id
        order by 1 desc
) c
where c.allegation_count >= 100
