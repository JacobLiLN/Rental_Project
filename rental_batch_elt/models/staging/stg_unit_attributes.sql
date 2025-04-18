select
    unit_key,
    property_key,
    number_of_bedrooms,
    sqft
from {{ source('raw', 'unit_attributes') }}