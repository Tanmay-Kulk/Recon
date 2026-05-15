select *
from {{ ref('stg_stripe_charges') }}
where is_zero_tax_taxable_state
