with source as (
    select
        *
    from {{ source('public', 'currency_rates') }}
)
select 
    id,
    date as rate_date,
    upper(base) as base_currency,
    upper(currency) as target_currency,
   round(rate::numeric, 6) as exchange_rate,
    created_at
    from source
    where rate is not null and rate > 0