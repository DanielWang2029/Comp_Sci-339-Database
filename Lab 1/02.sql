select a.id, a.first_name, a.last_name, a. complaint_percentile, a.honorable_mention_percentile
from data_officer a
where a.first_name = 'Jason' and left(a.last_name, 1)='V';