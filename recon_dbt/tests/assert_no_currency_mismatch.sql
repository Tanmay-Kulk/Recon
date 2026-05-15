select *
from {{ ref('stg_stripe_charges') }}
where lower(currency) <> case merchant_id
    when 'merch_surf_co' then 'eur'
    else 'usd'
end
