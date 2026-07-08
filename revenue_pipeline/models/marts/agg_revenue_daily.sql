with base as (
    select * from {{ ref('fct_transactions') }}
    where is_completed = true
),

daily as (
    select
        payment_day,
        payment_month,
        payment_year,
        lga,
        mda,
        revenue_item,
        transaction_channel,
        payment_method_clean,

        count(*)                                    as transaction_count,
        count(distinct agent_user)                  as unique_agents,
        count(distinct taxpayer_name)               as unique_taxpayers,
        sum(amount)                                 as total_amount,
        avg(amount)                                 as avg_amount,
        min(amount)                                 as min_amount,
        max(amount)                                 as max_amount,
        count(case when is_reversed  then 1 end)    as reversed_count,
        count(case when is_negative_amount then 1 end) as negative_amount_count

    from base
    group by
        payment_day,
        payment_month,
        payment_year,
        lga,
        mda,
        revenue_item,
        transaction_channel,
        payment_method_clean
)

select * from daily
