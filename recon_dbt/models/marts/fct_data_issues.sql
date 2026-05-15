{{
  config(
    materialized = 'table'
  )
}}

/*
This mart is the direct data source for the onboarding diagnostic dashboard and represents the output
a Numeral solutions engineer would use to triage a new customer's data quality before their first filing.
*/

with missing_billing as (

    select
        merchant_id,
        'missing_billing_address' as issue_type,
        count(*)::bigint as issue_count,
        'high' as severity,
        'Backfill missing billing state and ZIP on Stripe charges and matching Shopify orders before filing.' as recommended_action
    from {{ ref('int_unified_transactions') }}
    where is_missing_state or is_missing_zip
    group by 1

),

zero_tax as (

    select
        merchant_id,
        'zero_tax_in_taxable_state' as issue_type,
        count(*)::bigint as issue_count,
        'high' as severity,
        'Investigate tax configuration and remittance for succeeded charges in states where sales tax applies.' as recommended_action
    from {{ ref('int_unified_transactions') }}
    where is_zero_tax_taxable_state
    group by 1

),

b2b_certs as (

    select
        merchant_id,
        'b2b_missing_exemption_cert' as issue_type,
        count(*)::bigint as issue_count,
        'medium' as severity,
        'Attach valid resale or exemption certificates for B2B transactions before claiming exempt sales.' as recommended_action
    from {{ ref('int_unified_transactions') }}
    where is_b2b_missing_cert
    group by 1

),

currency_mismatch as (

    select
        merchant_id,
        'currency_mismatch' as issue_type,
        count(*)::bigint as issue_count,
        'medium' as severity,
        'Align Stripe charge currency with the merchant default currency to avoid reconciliation and FX errors.' as recommended_action
    from {{ ref('int_unified_transactions') }}
    where lower(currency) <> case merchant_id
        when 'merch_surf_co' then 'eur'
        else 'usd'
    end
    group by 1

),

orphan_refunds as (

    select
        merchant_id,
        'orphan_refund' as issue_type,
        count(*)::bigint as issue_count,
        'high' as severity,
        'Reconcile refunds to existing Stripe charges or remove invalid refund rows from ingestion.' as recommended_action
    from {{ ref('stg_stripe_refunds') }}
    where is_orphan_refund
    group by 1

)

select * from missing_billing
union all
select * from zero_tax
union all
select * from b2b_certs
union all
select * from currency_mismatch
union all
select * from orphan_refunds
