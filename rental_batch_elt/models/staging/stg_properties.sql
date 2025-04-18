select
    property_key,
    property_name,
    property_address
from {{ source('raw', 'properties') }}