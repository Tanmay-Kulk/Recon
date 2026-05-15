select *
from {{ ref('stg_stripe_charges') }}
where
    coalesce(tax_amount_collected_cents, 0) > 0
    and (
        billing_state is null
        or trim(cast(billing_state as varchar)) = ''
    )
