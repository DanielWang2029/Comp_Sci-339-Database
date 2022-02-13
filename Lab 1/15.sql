select avg(d.salary) as avg_salary
from (
        select a.officer_id, count(distinct a.allegation_id) as allegation_count, max(b.year) as year
        from data_officerallegation a, data_salary b
        where a.officer_id = b.officer_id
        group by a.officer_id
        order by 2 desc
) c, data_salary d
where c.officer_id = d.officer_id and c.year = d.year and c.allegation_count >= 100
