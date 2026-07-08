with base as (
    select * from {{ ref('fct_concession_permits') }}
    where is_completed = true
),

agg as (
    select
        payment_day,
        payment_month,
        payment_year,
        plan_code,
        category,
        product_code,
        product_name,
        lga,
        transaction_channel,
        payment_method_clean,

        count(distinct transaction_id)                                                      as transaction_count,
        count(distinct taxpayer_name)                                                       as unique_taxpayers,
        count(distinct agent_user)                                                          as unique_agents,
        sum(paid_amount)                                                                    as total_paid_amount,
        avg(paid_amount)                                                                    as avg_paid_amount,
        min(paid_amount)                                                                    as min_paid_amount,
        max(paid_amount)                                                                    as max_paid_amount,
        max(expected_amount)                                                                as expected_amount,
        max(previous_expected_amount)                                                       as previous_expected_amount,
        (max(expected_amount) * count(distinct transaction_id)) - sum(paid_amount)          as total_shortfall,
        count(case when is_reversed   then 1 end)                                           as reversed_count,
        count(case when is_fully_paid then 1 end)                                           as fully_paid_count

    from base
    group by
        payment_day,
        payment_month,
        payment_year,
        plan_code,
        category,
        product_code,
        product_name,
        lga,
        transaction_channel,
        payment_method_clean
)

select * from agg
