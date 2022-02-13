select b.race, count(distinct b.id) as count
from data_allegation a, data_victim b
where a.id = b.allegation_id and not a.is_officer_complaint and b.race <> ''
group by b.race
order by 2 desc
