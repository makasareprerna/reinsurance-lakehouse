-- SCD2 integrity: every treaty must have exactly one current version.
select treaty_id, count(*) as current_versions
from {{ ref('dim_treaty') }}
where is_current
group by treaty_id
having count(*) <> 1
