with transactions as (
    select * from {{ ref('stg_agent_transactions') }}
),

rev_codes as (
    select * from {{ ref('stg_rev_sub_newcode') }}
),

final as (
    select
        -- keys
        t.transaction_id,
        t.transaction_ref,
        t.payment_ref,

        -- dates
        t.payment_date,
        t.payment_day,
        t.payment_month,
        t.payment_year,
        t.transaction_date,

        -- agent
        t.agent_user,
        t.agent_code,
        t.agency,
        t.transaction_channel,
        t.terminal_id,

        -- geography
        t.lga,
        t.state_id,
        t.tax_office,

        -- revenue classification (from ref table where available, else raw)
        coalesce(r.mda,               'Unknown')         as mda,
        t.revenue_item                                   as revenue_item,
        coalesce(r.revenue_item_full, t.revenue_item)    as revenue_item_name,
        t.revenue_head,
        t.revenue_code,

        -- taxpayer
        t.taxpayer_name,
        t.taxpayer_email,
        t.taxpayer_phone,
        t.taxpayer_type,
        t.plate_number,
        t.enumeration_id,

        -- transaction detail
        t.amount,
        t.status,
        t.transaction_type,
        t.payment_method,
        t.payment_period,
        t.notice_number,
        t.notice_number_fiscal_year,

        -- vehicle / market
        t.vehicle_type,
        t.vehicle_tonnage,
        t.take_off_point,
        t.drop_off_destination,
        t.market,
        t.zone_line,
        t.shop_number,

        -- metadata
        t.valid_date,
        t.updated_at,

        -- derived flags
        case when t.status = 'Completed'  then true else false end  as is_completed,
        case when t.status = 'Reversed'   then true else false end  as is_reversed,
        case when t.amount < 0            then true else false end  as is_negative_amount,
        case
            when t.payment_method in ('Card', 'card')         then 'Card'
            when t.payment_method in ('Bank', 'bank', 'Bank Transfer') then 'Bank Transfer'
            when t.payment_method in ('Cash', 'cash')         then 'Cash'
            when t.payment_method in ('USSD', 'ussd')         then 'USSD'
            else coalesce(t.payment_method, 'Unknown')
        end as payment_method_clean

    from transactions t
    left join rev_codes r
        on t.revenue_code = r.rev_code

    where t.amount is not null
)

select * from final
