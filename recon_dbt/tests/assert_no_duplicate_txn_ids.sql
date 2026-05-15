select
    merchant_id,
    shopify_order_id,
    count(distinct source_store) as store_cnt
from {{ ref('stg_stripe_charges') }}
group by 1, 2
having store_cnt > 1
