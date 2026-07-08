with source as (
    select * from {{ source('abia_central', 'concession_permit') }}
),

renamed as (
    select
        id                  as permit_id,
        merchant_id,
        category,
        product_tag,
        product_code,
        product_name,
        amount              as expected_amount,
        p_amount            as previous_expected_amount,
        plan_code,
        status
    from source
    where status = 'Active'
      and plan_code in ('Vehicle', 'Trader')
)

select * from renamed
