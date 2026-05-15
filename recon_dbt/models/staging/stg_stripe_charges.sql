{{
  config(
    materialized = 'view'
  )
}}

/*
Staging models sit directly on top of raw source tables and handle only column renaming,
type casting, and source-level flag derivation — no joins, no cross-source logic, and no
business rules beyond what can be determined from a single row of this source system.
*/

with src as (

    select *
    from {{ source('recon_raw', 'raw_stripe_charges') }}
    where status = 'succeeded'

)

select
    id,
    object,
    merchant_id,
    cast(amount as bigint) as amount_cents,
    lower(cast(currency as varchar)) as currency,
    status,
    to_timestamp(cast(created as bigint)) as created_at,
    upper(trim(cast(billing_state as varchar))) as billing_state,
    billing_zip,
    upper(trim(cast(billing_country as varchar))) as billing_country,
    cast(is_b2b as boolean) as is_b2b,
    exemption_cert_id,
    cast(shopify_order_id as varchar) as shopify_order_id,
    cast(tax_amount_collected as bigint) as tax_amount_collected_cents,
    source_store,
    (
        billing_state is null
        or trim(cast(billing_state as varchar)) = ''
    ) as is_missing_state,
    (
        billing_zip is null
        or trim(cast(billing_zip as varchar)) = ''
    ) as is_missing_zip,
    (
        cast(is_b2b as boolean) = true
        and (
            exemption_cert_id is null
            or trim(cast(exemption_cert_id as varchar)) = ''
        )
    ) as is_b2b_missing_cert,
    (
        coalesce(cast(tax_amount_collected as bigint), 0) = 0
        and upper(trim(cast(billing_state as varchar))) in (
            'CA', 'TX', 'NY', 'FL', 'WA', 'IL', 'PA', 'OH', 'GA', 'NC', 'NJ', 'VA', 'AZ', 'TN',
            'CO', 'IN', 'MO', 'MI', 'WI', 'MN', 'SC', 'AL', 'KY', 'MA', 'CT', 'MD', 'RI', 'ID',
            'KS', 'MS', 'NE', 'NV', 'NM', 'ND', 'OK', 'SD', 'UT', 'VT', 'WV', 'WY', 'HI', 'AR',
            'LA', 'ME', 'IA'
        )
    ) as is_zero_tax_taxable_state
from src
