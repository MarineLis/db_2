select year, min(histball100) minimal_grade
from ZNO_RESULTS_19_20
where histteststatus = 'Зараховано'
group by year