select treaty_id, loss_ratio
from {{ ref('mart_treaty_loss_ratio') }}
where loss_ratio < 0
