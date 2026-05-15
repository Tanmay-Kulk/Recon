{{
  config(
    materialized = 'table'
  )
}}

/*
Intermediate models join and enrich data across multiple staging sources and apply business logic
that requires understanding relationships between sources — this is the layer where Stripe and
Shopify data are reconciled into a single version of truth for downstream analytics.
*/

with stripe_norm as (

    select
        md5('stripe' || id) as txn_key,
        cast(id as varchar) as source_txn_id,
        merchant_id,
        source_store,
        'stripe' as source_system,
        created_at as occurred_at,
        (amount_cents - tax_amount_collected_cents) / 100.0 as subtotal_usd,
        tax_amount_collected_cents / 100.0 as tax_collected_usd,
        currency,
        false as is_refund,
        cast(null as varchar) as parent_txn_id,
        billing_state,
        billing_zip,
        billing_country,
        is_b2b,
        exemption_cert_id,
        is_missing_state,
        is_missing_zip,
        is_b2b_missing_cert,
        is_zero_tax_taxable_state,
        cast(id as varchar) as link_id
    from {{ ref('stg_stripe_charges') }}

),

shopify_norm as (

    select
        md5('shopify' || cast(id as varchar)) as txn_key,
        cast(id as varchar) as source_txn_id,
        merchant_id,
        source_store,
        'shopify' as source_system,
        created_at as occurred_at,
        subtotal_price as subtotal_usd,
        total_tax as tax_collected_usd,
        currency,
        false as is_refund,
        cast(null as varchar) as parent_txn_id,
        billing_state,
        billing_zip,
        billing_country,
        false as is_b2b,
        cast(null as varchar) as exemption_cert_id,
        is_missing_state,
        is_missing_zip,
        false as is_b2b_missing_cert,
        (
            coalesce(total_tax, 0) = 0
            and upper(trim(cast(billing_state as varchar))) in (
                'CA', 'TX', 'NY', 'FL', 'WA', 'IL', 'PA', 'OH', 'GA', 'NC', 'NJ', 'VA', 'AZ', 'TN',
                'CO', 'IN', 'MO', 'MI', 'WI', 'MN', 'SC', 'AL', 'KY', 'MA', 'CT', 'MD', 'RI', 'ID',
                'KS', 'MS', 'NE', 'NV', 'NM', 'ND', 'OK', 'SD', 'UT', 'VT', 'WV', 'WY', 'HI', 'AR',
                'LA', 'ME', 'IA'
            )
            and financial_status in ('paid', 'partially_refunded')
        ) as is_zero_tax_taxable_state,
        cast(stripe_charge_id as varchar) as link_id
    from {{ ref('stg_shopify_orders') }}

),

unioned as (

    select * from stripe_norm
    union all
    select * from shopify_norm

),

deduped as (

    select
        txn_key,
        source_txn_id,
        merchant_id,
        source_store,
        source_system,
        occurred_at,
        subtotal_usd,
        tax_collected_usd,
        currency,
        is_refund,
        parent_txn_id,
        billing_state,
        billing_zip,
        billing_country,
        is_b2b,
        exemption_cert_id,
        is_missing_state,
        is_missing_zip,
        is_b2b_missing_cert,
        is_zero_tax_taxable_state
    from (
        select
            *,
            row_number() over (
                partition by link_id
                order by
                    case when source_system = 'stripe' then 1 else 2 end,
                    source_txn_id
            ) as rn
        from unioned
    ) ranked
    where rn = 1

)

select *
from deduped
