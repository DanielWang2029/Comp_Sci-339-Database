-- name and allegation table
-- select distinct a.allegation_name, b.allegation_id
-- from data_allegationcategory a, data_officerallegation b
-- where a.id = b.allegation_category_id;

select c.allegation_name, count(distinct d.id) as count
from (
         select distinct a.allegation_name, b.allegation_id
         from data_allegationcategory a,
              data_officerallegation b
         where a.id = b.allegation_category_id
     ) c, data_allegation d
where c.allegation_id = d.id and not d.is_officer_complaint
group by c.allegation_name
order by 2 desc
limit 5