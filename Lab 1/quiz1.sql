select bb.first_name, bb.last_name, cast(aa.count as float4) / cast(aa.length as float4) as avg
from (
    select count(distinct(a.id)) as count, a.officer_id, 2018 - extract(year from c.appointed_date) as length
    from data_officerallegation a, data_allegation b, data_officer c
    where a.allegation_id = b.id and a.officer_id = c.id and not b.is_officer_complaint and c.appointed_date is not null
    group by 2, 3
    ) aa, data_officer bb
where aa.officer_id = bb.id
order by 3 desc
limit 5;


select c.category, count(distinct d.id) as count
from (
         select distinct a.category, b.allegation_id
         from data_allegationcategory a,
              data_officerallegation b
         where a.id = b.allegation_category_id and a.category = 'Use Of Force'
     ) c, data_allegation d
where c.allegation_id = d.id and not d.is_officer_complaint
group by c.category
order by 2 desc