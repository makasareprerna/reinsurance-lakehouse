select
    cast(d as date)                    as date_day,
    year(d)                            as year,
    quarter(d)                         as quarter,
    month(d)                           as month,
    strftime(d, '%Y-%m')               as year_month,
    dayofweek(d) in (0, 6)             as is_weekend
from range(timestamp '2024-01-01', timestamp '2029-01-01', interval 1 day) as t(d)
