select
    quote_id,
    cedent_id,
    treaty_type,
    line_of_business,
    currency,
    cast(quoted_premium as double) as quoted_premium,
    quote_date,
    status as quote_status,
    version as quote_version,
    updated_at
from {{ source('silver', 'quotes') }}
