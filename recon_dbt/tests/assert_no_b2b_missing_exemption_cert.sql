select *
from {{ ref('stg_stripe_charges') }}
where is_b2b_missing_cert
