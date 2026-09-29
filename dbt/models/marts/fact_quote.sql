-- Grain: one row per quote (latest status).
with quotes as (
    select * from {{ ref('stg_quotes') }}
),

fx as (
    select * from {{ ref('fx_rates') }}
)

select
    q.quote_id,
    q.cedent_id,
    q.treaty_type,
    q.line_of_business,
    q.quote_date,
    strftime(q.quote_date, '%Y-%m')          as quote_month,
    q.quote_status,
    q.quote_version,
    q.quoted_premium * fx.usd_rate           as quoted_premium_usd,
    q.quote_status = 'BOUND'                 as is_bound,
    q.quote_status = 'DECLINED'              as is_declined
from quotes q
left join fx on q.currency = fx.currency
