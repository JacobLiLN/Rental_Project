select
    property_key as property_id,
    property_name,
    property_address
from {{ ref('stg_properties') }}