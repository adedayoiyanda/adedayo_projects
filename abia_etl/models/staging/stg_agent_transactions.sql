with source as (
    select * from {{ source('abia_central', 'agent_transactions') }}
),

renamed as (
    select
        idagent_transactions        as transaction_id,
        agency,
        state_id,
        agent_user,
        agent_code,
        trans_date                  as transaction_date,
        trans_ref                   as transaction_ref,
        payment_ref,
        rev_head                    as revenue_head,
        rev_code                    as revenue_code,
        refcode,
        reference,
        amount,
        payment_period,
        trans_channel               as transaction_channel,
        status,
        trans_type                  as transaction_type,
        lga,
        taxpayer_type,
        taxpayer_name,
        taxpayer_email,
        taxpayer_phone,
        market,
        zone_line,
        shop_number,
        revenue_item,
        payment_method,
        plate_number,
        enumeration_id,
        vehicle_type,
        vehicle_tonnage,
        vehicle_content,
        take_off_point,
        drop_off_destination,
        taxoffice                   as tax_office,
        notice_number,
        notice_number_fiscal_year,
        payment_date,
        valid_date,
        terminalid                  as terminal_id,
        terminal_serial_number,
        updated_at,

        -- derived
        date(payment_date)          as payment_day,
        date_trunc('month', payment_date) as payment_month,
        extract(year from payment_date)::int as payment_year

    from source
    where payment_date is not null
)

select * from renamed
