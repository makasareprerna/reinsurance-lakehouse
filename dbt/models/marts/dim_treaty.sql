-- One row per treaty VERSION (SCD Type 2). Filter is_current for today's view.
with versions as (
    select * from {{ ref('snap_treaties') }}
),

fx as (
    select * from {{ ref('fx_rates') }}
)

select
    v.treaty_id || '|' || strftime(v.dbt_valid_from, '%Y%m%d%H%M%S') as treaty_version_key,
    v.treaty_id,
    v.quote_id,
    v.cedent_id,
    v.treaty_type,
    v.line_of_business,
    v.currency,
    v.underwriting_year,
    v.inception_date,
    v.expiry_date,
    v.premium,
    v.premium * fx.usd_rate                              as premium_usd,
    v.cession_pct,
    v.retention,
    v.limit_amount,
    v.treaty_status,
    v.dbt_valid_from                                     as valid_from,
    coalesce(v.dbt_valid_to, timestamp '9999-12-31')     as valid_to,
    v.dbt_valid_to is null                               as is_current
from versions v
left join fx on v.currency = fx.currency
