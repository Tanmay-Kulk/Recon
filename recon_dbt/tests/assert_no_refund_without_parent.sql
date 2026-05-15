select *
from {{ ref('stg_stripe_refunds') }}
where is_orphan_refund
