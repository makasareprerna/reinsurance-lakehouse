-- Grain: one row per claim (latest known version).
with claims as (
    select * from {{ ref('stg_claims') }}
),

treaties as (
    select treaty_id, cedent_id, line_of_business, underwriting_year, treaty_type
    from {{ ref('dim_treaty') }}
    where is_current
),

fx as (
    select * from {{ ref('fx_rates') }}
)

select
    c.claim_id,
    c.treaty_id,
    t.cedent_id,
    t.line_of_business,
    t.treaty_type,
    t.underwriting_year,
    c.loss_date,
    c.reported_date,
    date_diff('day', c.loss_date, c.reported_date)       as reporting_lag_days,
    c.claim_status,
    c.currency,
    c.paid_amount * fx.usd_rate                          as paid_usd,
    c.reserve_amount * fx.usd_rate                       as reserve_usd,
    (c.paid_amount + c.reserve_amount) * fx.usd_rate     as incurred_usd,
    t.treaty_id is not null                              as treaty_known,
    c.updated_at
from claims c
left join treaties t on c.treaty_id = t.treaty_id
left join fx on c.currency = fx.currency
