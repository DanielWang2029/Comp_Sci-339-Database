select a.last_name, a.first_name
from data_officer a
where a.birth_year > 1992
group by a.last_name, a.first_name
order by a.last_name, a.first_name;