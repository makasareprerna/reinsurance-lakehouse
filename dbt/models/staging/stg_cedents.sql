select
    cedent_id,
    cedent_name,
    country,
    updated_at
from {{ source('silver', 'cedents') }}
