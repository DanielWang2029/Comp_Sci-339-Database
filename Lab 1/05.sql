select a.last_name, a.first_name
from data_officer a
where a.birth_year IS NOT NULL
order by a.birth_year desc
limit 1;