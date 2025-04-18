select month_start, sum(vacant_days)/sum(calendar_days) as vacancy_rate
from {{ ref('int_vacancy') }}
group by month_start