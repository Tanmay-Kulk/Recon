{{
  config(
    materialized = 'table'
  )
}}

/*
Mart models produce the final analytical outputs consumed by end users or dashboards — they contain
no raw source payloads, only aggregated business metrics derived from intermediate models.
*/

with taxable_window as (

    select
        merchant_id,
        billing_state as state_code,
        sum(subtotal_usd + tax_collected_usd) as trailing_12m_sales_usd,
        count(*)::bigint as trailing_12m_transactions
    from {{ ref('int_unified_transactions') }}
    where
        is_refund = false
        and occurred_at >= date_trunc('day', current_date) - interval '12 months'
        and billing_state is not null
        and trim(cast(billing_state as varchar)) <> ''
        and upper(trim(cast(billing_country as varchar))) in ('US', 'USA')
    group by 1, 2

),

thresholds as (

    select
        state_code,
        state_name,
        lower(trim(cast(has_sales_tax as varchar))) as has_sales_tax,
        try_cast(nullif(trim(cast(sales_threshold_usd as varchar)), '') as double) as sales_threshold_usd,
        try_cast(nullif(trim(cast(transaction_threshold as varchar)), '') as bigint) as transaction_threshold,
        nullif(trim(cast(threshold_logic as varchar)), '') as threshold_logic
    from {{ ref('state_nexus_thresholds') }}

),

joined as (

    select
        t.*,
        s.state_name,
        s.has_sales_tax,
        s.sales_threshold_usd,
        s.transaction_threshold,
        s.threshold_logic,
        case
            when s.has_sales_tax = 'false' then false
            when upper(coalesce(s.threshold_logic, '')) = 'AND' then
                t.trailing_12m_sales_usd >= s.sales_threshold_usd
                and t.trailing_12m_transactions >= s.transaction_threshold
            when s.transaction_threshold is null then
                t.trailing_12m_sales_usd >= s.sales_threshold_usd
            else
                t.trailing_12m_sales_usd >= s.sales_threshold_usd
                or t.trailing_12m_transactions >= s.transaction_threshold
        end as has_crossed_nexus_threshold
    from taxable_window t
    inner join thresholds s
        on t.state_code = s.state_code

),

tiered as (

    select
        *,
        case
            when has_sales_tax = 'false' then 'no_sales_tax'
            when not has_crossed_nexus_threshold then 'below_threshold'
            when trailing_12m_sales_usd >= 2 * sales_threshold_usd then 'high_exposure'
            else 'at_threshold'
        end as exposure_tier
    from joined

)

select
    merchant_id,
    state_code,
    state_name,
    trailing_12m_sales_usd,
    trailing_12m_transactions,
    sales_threshold_usd,
    transaction_threshold,
    threshold_logic,
    has_sales_tax,
    has_crossed_nexus_threshold,
    exposure_tier
from tiered
