select month_start, number_of_bedrooms, sum(vacant_days)/sum(calendar_days) as vacancy_rate
from {{ ref('int_vacancy') }} as t1
inner join {{ ref('dim_unit_attributes') }} as t2 on t1.unit_key = t2.unit_id
group by month_start,number_of_bedrooms