with source as (
    select * from {{ source('abia_central', 'enumeration') }}
),

renamed as (
    select
        id                      as enumeration_pk,
        site_id,
        enumeration_id,
        park,
        taxpayer_id,
        taxpayer_name,
        revenue_year,
        location,
        revenue_item,
        category,
        union_name,
        plate_number,
        market,
        market_id,
        zone_line,
        shop_number,
        have_abssin,
        taxpayer_phone,
        shop_occupants,
        income_category,
        monthly_income,
        income_amount,
        shop_category,
        occupant_income_amount,
        payment_method,
        enumeration_plan,
        enumeration_fee,
        status,
        enumeration_status,
        asset_code,
        enumeration_type,
        notice_generated,
        created_by,
        create_time
    from source
)

select * from renamed
