select month_start, property_name, sum(vacant_days)/sum(calendar_days) as vacancy_rate
from {{ ref('int_vacancy') }} as t1
inner join {{ ref('dim_unit_attributes') }} as t2 on t1.unit_key = t2.unit_id
inner join {{ ref('dim_properties') }} as t3 on t2.property_id = t3.property_id
group by month_start, property_name