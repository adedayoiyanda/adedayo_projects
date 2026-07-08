with source as (
    select * from {{ source('abia_central', 'bus_ticketing_transaction') }}
),

renamed as (
    select
        id                          as ticket_id,
        event,
        event_name,
        trans_id                    as transaction_ref,
        amount,
        card_serial,
        phone,
        trans_date                  as transaction_date,
        checkout,
        bus_id,
        route_name,
        entry_point,
        exit_point,
        i_type                      as instrument_type,
        issuer_id,
        issuer_name,
        created_at,
        updated_at,

        -- derived
        date(trans_date)                        as transaction_day,
        date_trunc('month', trans_date)         as transaction_month,
        extract(year from trans_date)::int      as transaction_year

    from source
    where trans_date is not null
)

select * from renamed
