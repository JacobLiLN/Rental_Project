select
    tenant_id,
    unit_key,
    move_in_date,
    move_out_date,
    load_ts
from {{ source('raw', 'tenants') }}