select
    unit_key,
    tenant_id,
    event_type,
    dt_occurred,
    load_ts
from {{ source('raw', 'events') }}