select
    treaty_id,
    quote_id,
    cedent_id,
    treaty_type,
    line_of_business,
    currency,
    underwriting_year,
    inception_date,
    expiry_date,
    cast(premium as double)      as premium,
    cast(cession_pct as double)  as cession_pct,
    cast(retention as double)    as retention,
    cast(limit_amount as double) as limit_amount,
    status as treaty_status,
    updated_at
from {{ source('silver', 'treaties') }}
