select name,
    (case when length(median_income) = 7 then cast(concat(substring(median_income, 2, 2), substring(median_income, 5, 3)) as int)
    else cast(concat(substring(median_income, 2, 3), substring(median_income, 6, 3)) as int) end) as income
from data_area
where median_income is not null
order by 2 desc
limit 3
