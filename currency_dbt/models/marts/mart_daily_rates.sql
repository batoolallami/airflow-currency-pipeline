with staging as (
    select
       *
    from {{ ref('stg_currency_rates') }}
)
select rate_date, base_currency,
max(case when target_currency = 'GBP' then exchange_rate end) as gbp_rate,
max(case when target_currency = 'JPY' then exchange_rate end) as jpy_rate,
max(case when target_currency = 'TRY' then exchange_rate end) as try_rate,
max(case when target_currency = 'SAR' then exchange_rate end) as sar_rate,
max(case when target_currency = 'AED' then exchange_rate end) as aed_rate,
COUNT(distinct target_currency) as loaded_currencies,
max(created_at) as last_updated
from staging
group by rate_date, base_currency
order by rate_date desc