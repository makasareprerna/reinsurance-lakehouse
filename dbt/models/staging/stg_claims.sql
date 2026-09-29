select
    claim_id,
    treaty_id,
    loss_date,
    reported_date,
    cast(paid_amount as double)    as paid_amount,
    cast(reserve_amount as double) as reserve_amount,
    currency,
    claim_status,
    updated_at,
    -- set by the pipeline when the claim arrived before its treaty
    list_contains(_dq_warnings, 'treaty_not_yet_known') as flagged_unknown_treaty
from {{ source('silver', 'claims') }}
