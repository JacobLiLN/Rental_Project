select
    unit_key as unit_id,
    property_key as property_id,
    number_of_bedrooms,
    sqft
from {{ ref('stg_unit_attributes') }}