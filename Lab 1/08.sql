select rank, max(salary) - min(salary) as range
from data_salary
group by rank
order by 2 desc
limit 1