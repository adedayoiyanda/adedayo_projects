with raw_transactions as (
    select transaction_id, revenue_item
    from {{ ref('stg_agent_transactions') }}
),

permits as (
    select * from {{ ref('stg_concession_permit') }}
),

-- join on raw revenue_item before it gets coalesced/enriched in fct_transactions
permit_transactions as (
    select
        r.transaction_id,
        p.permit_id,
        p.plan_code,
        p.category,
        p.product_code,
        p.product_name,
        p.product_tag,
        p.merchant_id,
        p.expected_amount,
        p.previous_expected_amount
    from raw_transactions r
    inner join permits p
        on r.revenue_item = p.product_code
),

transactions as (
    select * from {{ ref('fct_transactions') }}
),

final as (
    select
        -- keys
        t.transaction_id,
        t.transaction_ref,
        t.payment_ref,
        pt.permit_id,

        -- permit dimensions
        pt.plan_code,
        pt.category,
        pt.product_code,
        pt.product_name,
        pt.product_tag,
        pt.merchant_id,

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

        -- revenue classification
        t.mda,
        t.revenue_item,
        t.revenue_item_name,
        t.revenue_head,
        t.revenue_code,

        -- taxpayer
        t.taxpayer_name,
        t.taxpayer_email,
        t.taxpayer_phone,
        t.taxpayer_type,
        t.plate_number,
        t.enumeration_id,

        -- amounts
        t.amount                            as paid_amount,
        pt.expected_amount,
        pt.previous_expected_amount,
        t.amount - pt.expected_amount       as variance_amount,

        -- transaction detail
        t.status,
        t.transaction_type,
        t.payment_method,
        t.payment_method_clean,
        t.payment_period,
        t.notice_number,
        t.notice_number_fiscal_year,

        -- vehicle / market context
        t.vehicle_type,
        t.vehicle_tonnage,
        t.take_off_point,
        t.drop_off_destination,
        t.market,
        t.zone_line,
        t.shop_number,

        -- derived flags
        t.is_completed,
        t.is_reversed,
        t.is_negative_amount,
        case when t.amount >= pt.expected_amount then true else false end as is_fully_paid,

        -- metadata
        t.valid_date,
        t.updated_at

    from permit_transactions pt
    inner join transactions t
        on pt.transaction_id = t.transaction_id
)

select * from final
