-- Incurred = paid + reserve, so it can never be lower than paid.
select claim_id, paid_usd, incurred_usd
from {{ ref('fact_claim') }}
where incurred_usd < paid_usd - 0.01
