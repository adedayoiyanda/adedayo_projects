with bus as (
    select
        'Bus Ticket'                        as revenue_source,
        transaction_day                     as revenue_date,
        transaction_month                   as revenue_month,
        transaction_year                    as revenue_year,
        null::varchar                       as lga,
        null::varchar                       as transaction_channel,
        null::varchar                       as payment_method_clean,
        null::varchar                       as category,
        null::varchar                       as product_code,
        null::varchar                       as product_name,
        count(distinct ticket_id)           as transaction_count,
        count(distinct phone)               as unique_payers,
        count(distinct issuer_id)           as unique_agents,
        sum(amount)                         as total_amount,
        avg(amount)                         as avg_amount,
        min(amount)                         as min_amount,
        max(amount)                         as max_amount,
        null::numeric                       as expected_amount,
        null::numeric                       as total_shortfall,
        null::bigint                        as reversed_count,
        null::bigint                        as fully_paid_count
    from {{ ref('stg_bus_ticketing_transaction') }}
    group by transaction_day, transaction_month, transaction_year
),

concession as (
    select
        case plan_code
            when 'Trader'  then 'Market Ticket'
            when 'Vehicle' then 'Vehicle Ticket'
        end                                 as revenue_source,
        payment_day                         as revenue_date,
        payment_month                       as revenue_month,
        payment_year                        as revenue_year,
        lga,
        transaction_channel,
        payment_method_clean,
        category,
        product_code,
        product_name,
        transaction_count,
        unique_taxpayers                    as unique_payers,
        unique_agents,
        total_paid_amount                   as total_amount,
        avg_paid_amount                     as avg_amount,
        min_paid_amount                     as min_amount,
        max_paid_amount                     as max_amount,
        expected_amount,
        total_shortfall,
        reversed_count,
        fully_paid_count
    from {{ ref('agg_concession_by_plancode') }}
),

combined as (
    select * from bus
    union all
    select * from concession
)

select * from combined
