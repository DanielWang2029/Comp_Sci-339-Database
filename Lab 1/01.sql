select a.id
from trr_trr a, trr_weapondischarge b
where a.id = b.trr_id and b.total_number_of_shots >= 10
order by a.id asc