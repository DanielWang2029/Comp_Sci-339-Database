select e.name, cast(max(e.count) as float) / cast(sum(e.population) as float) as ratio
from (
    select c.name, d.race, count(distinct a.id) as count, max(d.count) as population
    from data_allegation a
    join data_allegation_areas b
    on a.id = b.allegation_id
    join data_area c
    on b.area_id = c.id
    join data_racepopulation d
    on c.id = d.area_id
    group by c.name, d.race
) e
group by e.name
order by ratio desc
limit 5