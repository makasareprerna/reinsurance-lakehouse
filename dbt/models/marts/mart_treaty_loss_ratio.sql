-- Loss ratio per treaty: incurred losses / premium, both in USD.
-- Claims whose treaty is not yet known are excluded until it arrives.
with treaties as (
    select * from {{ ref('dim_treaty') }} where is_current
),

losses as (
    select
        treaty_id,
        count(*)            as claim_count,
        sum(paid_usd)       as paid_usd,
        sum(incurred_usd)   as incurred_usd
    from {{ ref('fact_claim') }}
    where treaty_known
    group by treaty_id
)

select
    t.treaty_id,
    t.cedent_id,
    t.line_of_business,
    t.treaty_type,
    t.underwriting_year,
    t.premium_usd,
    coalesce(l.claim_count, 0)                                  as claim_count,
    coalesce(l.paid_usd, 0)                                     as paid_usd,
    coalesce(l.incurred_usd, 0)                                 as incurred_usd,
    case when t.premium_usd > 0
         then coalesce(l.incurred_usd, 0) / t.premium_usd end   as loss_ratio
from treaties t
left join losses l on t.treaty_id = l.treaty_id
