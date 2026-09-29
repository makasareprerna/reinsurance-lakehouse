-- Hit ratio = bound / decided quotes, by cedent, line of business and month.
select
    cedent_id,
    line_of_business,
    quote_month,
    count(*)                                            as quotes,
    sum(case when is_bound then 1 else 0 end)           as bound,
    sum(case when is_declined then 1 else 0 end)        as declined,
    sum(case when is_bound then 1 else 0 end)::double
        / nullif(sum(case when is_bound or is_declined then 1 else 0 end), 0) as hit_ratio
from {{ ref('fact_quote') }}
group by cedent_id, line_of_business, quote_month
