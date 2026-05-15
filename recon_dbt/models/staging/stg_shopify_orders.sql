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

select
    cast(id as bigint) as id,
    name,
    merchant_id,
    source_store,
    try_cast(created_at as timestamp) as created_at,
    lower(cast(currency as varchar)) as currency,
    cast(total_price as double) as total_price,
    cast(subtotal_price as double) as subtotal_price,
    cast(total_tax as double) as total_tax,
    taxes_included,
    cast(financial_status as varchar) as financial_status,
    upper(trim(cast(billing_state as varchar))) as billing_state,
    billing_zip,
    upper(trim(cast(billing_country as varchar))) as billing_country,
    stripe_charge_id,
    (
        billing_state is null
        or trim(cast(billing_state as varchar)) = ''
    ) as is_missing_state,
    (
        billing_zip is null
        or trim(cast(billing_zip as varchar)) = ''
    ) as is_missing_zip
from {{ source('recon_raw', 'raw_shopify_orders') }}
