with source as (
    select * from {{ source('abia_central', 'rev_sub_newcode') }}
),

renamed as (
    select
        id              as rev_sub_id,
        mda,
        rev_head,
        rev_code,
        item            as revenue_item_name,
        receipt,
        idrev_sub       as rev_sub_ref_id,
        revenue_item    as revenue_item_full,
        enter_by,
        created_at
    from source
)

select * from renamed
