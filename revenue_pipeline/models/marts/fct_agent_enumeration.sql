{{ config(materialized='table') }}

with enumerations as (
    select * from {{ ref('stg_enumeration') }}
),

agents as (
    select * from {{ ref('stg_merchant_user') }}
),

final as (
    select
        -- agent identity
        m.user_id,
        m.email             as agent_email,
        m.first_name,
        m.last_name,
        m.phone_no,
        m.lga,
        m.user_cat,
        m.bank,
        m.bank_account,
        m.agent_status,
        m.agent_code,

        -- enumeration dimensions
        e.revenue_item,
        case e.revenue_item
            when 'MarketEnumeration'    then 'Market'
            when 'TransportEnumeration' then 'Transport'
        end                             as revenue_type,
        e.category,

        -- create time
        cast(e.create_time as date)                  as create_day,
        to_char(e.create_time, 'YYYY-MM')            as create_month,
        extract(year from e.create_time)::int        as create_year,

        -- metric
        count(e.enumeration_pk)     as enumeration_count

    from enumerations e
    left join agents m
        on e.created_by = m.email

    group by
        m.user_id,
        m.email,
        m.first_name,
        m.last_name,
        m.phone_no,
        m.lga,
        m.user_cat,
        m.bank,
        m.bank_account,
        m.agent_status,
        m.agent_code,
        e.revenue_item,
        case e.revenue_item
            when 'MarketEnumeration'    then 'Market'
            when 'TransportEnumeration' then 'Transport'
        end,
        e.category,
        cast(e.create_time as date),
        to_char(e.create_time, 'YYYY-MM'),
        extract(year from e.create_time)::int
)

select * from final
