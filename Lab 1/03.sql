select a.star
from data_officerbadgenumber a
group by a.star
having count(distinct officer_id) > 1;

-- select a.star, a.id
-- from data_officerbadgenumber a
-- where a.star = '7266';