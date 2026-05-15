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
    id,
    object,
    merchant_id,
    cast(amount as bigint) as amount_cents,
    lower(cast(currency as varchar)) as currency,
    status,
    to_timestamp(cast(created as bigint)) as created_at,
    charge_id,
    (
        not exists (
            select 1
            from {{ ref('stg_stripe_charges') }} c
            where c.id = r.charge_id
        )
    ) as is_orphan_refund
from {{ source('recon_raw', 'raw_stripe_refunds') }} r
