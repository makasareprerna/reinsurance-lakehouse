select
    cedent_id,
    cedent_name,
    country
from {{ ref('stg_cedents') }}
