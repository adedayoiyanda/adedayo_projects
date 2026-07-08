with source as (
    select * from {{ source('abia_central', 'merchant_user') }}
),

renamed as (
    select
        user_id,
        abssin,
        user_cat,
        first_name,
        last_name,
        company,
        merchant_id,
        agent_code,
        collection_type,
        phone_no,
        email,
        status          as agent_status,
        create_by,
        lga,
        lga_zone,
        bank,
        bank_account,
        balance,
        create_time,
        createdate,
        update_by,
        update_time
    from source
)

select * from renamed
